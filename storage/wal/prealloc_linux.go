//go:build linux

package wal

import (
	"os"
	"syscall"
)

// preallocate reserves size bytes for f up front.
//
// Without it, every append that grows the file also updates filesystem metadata,
// so each fsync writes the inode as well as the data — roughly doubling the
// synchronous work on the hot path. Reserving the space once moves that cost to
// segment creation.
//
// Failure is not an error: some filesystems do not support fallocate, and the
// log is correct without preallocation, only slower.
func preallocate(f *os.File, size int64) {
	// Mode 0 allocates and marks the range as written, which is what avoids the
	// later metadata updates. FALLOC_FL_KEEP_SIZE would leave the file logically
	// empty and defeat the purpose.
	_ = syscall.Fallocate(int(f.Fd()), 0, 0, size)
}
