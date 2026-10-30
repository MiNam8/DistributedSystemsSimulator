//go:build !linux

package wal

import "os"

// preallocate is a no-op where fallocate is unavailable. The log is correct
// without it, only slower.
func preallocate(f *os.File, size int64) {}
