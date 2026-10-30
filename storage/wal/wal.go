// Package wal is a segmented write-ahead log: the durable half of a Raft node.
//
// # Why a log and not a B-tree
//
// Raft's storage needs are append, truncate-suffix (on a term conflict) and
// truncate-prefix (on compaction). A log does all three at sequential-write
// speed. A B-tree would fsync a tree update per commit for no benefit, which is
// why etcd uses a custom WAL for the raft log even though it ships BoltDB for
// the state machine.
//
// # Torn writes
//
// Every record carries a CRC over its type and payload. On replay the log stops
// at the first record that is short or fails its checksum, and everything after
// it is discarded. That is not damage control, it is the definition of the
// durable log: a record that was not completely written was never fsync'd, so it
// was never acknowledged to any peer, so no one can be relying on it.
//
// # Truncation is append-only
//
// A conflicting suffix is discarded by *appending a truncation marker*, not by
// rewriting the file. Replay applies markers in order. This keeps the write path
// purely sequential and means a crash during a truncation is indistinguishable
// from a crash before it — the marker is either durable or it is not.
package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/storage"
)

// recTruncate is declared here rather than in record.go because it exists to
// serve this file's append-only truncation strategy.
const recTruncate recordType = 4

// DefaultSegmentSize is the size at which a segment is rolled over.
const DefaultSegmentSize = 16 << 20

// WAL is a node's durable state.
//
// Entries are mirrored in memory, bounded by compaction — the same argument the
// core makes for owning its log. Reads never touch the disk; the file exists to
// survive a crash, not to be queried.
type WAL struct {
	dir     string
	segSize int64

	segments []*segment
	entries  []raft.Entry
	hard     raft.HardState
	config   raft.Config

	snapMeta raft.SnapshotMeta
	snapData []byte
	hasSnap  bool

	// Syncs counts fsync calls, so tests can assert durability was actually
	// requested rather than merely assumed.
	Syncs int

	// noSync skips every fsync. It exists purely to measure what durability
	// costs, by running the identical code path without it. A node in this mode
	// loses acknowledged writes on power loss — it is not a tuning option, and
	// nothing outside a benchmark may set it.
	noSync bool
}

// DisableSync turns off fsync. UNSAFE: acknowledged writes are lost on power
// loss. It exists only so the durability cost can be measured rather than
// guessed at.
func (w *WAL) DisableSync() { w.noSync = true }

var _ storage.Storage = (*WAL)(nil)

type segment struct {
	path  string
	f     *os.File
	first raft.Index // lowest entry index this segment may contain
	last  raft.Index // highest entry index actually written to it
	size  int64
}

// Open recovers a WAL from dir, creating it if absent. cluster is the bootstrap
// configuration, used only when there is nothing to recover.
func Open(dir string, cluster raft.Config) (*WAL, error) {
	return OpenSize(dir, cluster, DefaultSegmentSize)
}

// OpenSize is Open with an explicit segment size. Tests use a small size to
// exercise rollover without writing tens of megabytes.
func OpenSize(dir string, cluster raft.Config, segSize int64) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, segSize: segSize, config: cluster}

	if err := w.loadSnapshot(); err != nil {
		return nil, err
	}
	names, err := segmentNames(dir)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		if err := w.roll(w.snapMeta.LastIncludedIndex + 1); err != nil {
			return nil, err
		}
		return w, nil
	}
	if err := w.replay(names); err != nil {
		return nil, err
	}
	return w, nil
}

func segmentNames(dir string) ([]string, error) {
	es, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range es {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".wal") {
			out = append(out, e.Name())
		}
	}
	// Names are fixed-width hex of the first index, so lexical order is index order.
	sort.Strings(out)
	return out, nil
}

func segmentName(first raft.Index) string { return fmt.Sprintf("%016x.wal", uint64(first)) }

// replay rebuilds in-memory state, stopping at the first torn record.
func (w *WAL) replay(names []string) error {
	for i, name := range names {
		path := filepath.Join(w.dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		seg := &segment{path: path, f: f}
		_, _ = fmt.Sscanf(name, "%016x.wal", &seg.first)

		torn := false
		off := 0
		for off < len(data) {
			t, payload, n, err := readRecord(data[off:])
			if err != nil {
				// End of the durable log. Everything past here was never fsync'd.
				torn = true
				break
			}
			idx, err := w.applyRecord(t, payload)
			if err != nil {
				return err
			}
			// Track the segment's range from the record on disk, not from the
			// in-memory index: an entry the snapshot already covers is skipped in
			// memory but is still physically in this segment, and segment
			// deletion decisions are about the file.
			if idx > 0 {
				seg.last = idx
			}
			off += n
		}
		seg.size = int64(off)
		w.segments = append(w.segments, seg)

		if torn {
			// Truncate the file to the last good record so the next append does
			// not sit behind a hole that replay would stop at forever.
			if err := f.Truncate(int64(off)); err != nil {
				return err
			}
			// Re-reserve the space the truncate just gave back, or every restart
			// would silently put this segment back on the slow path where each
			// append also updates filesystem metadata.
			preallocate(f, w.segSize)
			if err := f.Sync(); err != nil {
				return err
			}
			w.Syncs++
			// A torn record in a segment that is not the last one means real
			// corruption rather than an interrupted append, and continuing would
			// silently skip a gap.
			if i != len(names)-1 {
				return fmt.Errorf("wal: torn record in %s, which is not the final segment: log has a hole", name)
			}
			break
		}
	}
	// Seek every retained segment to its end so appends continue from there.
	for _, seg := range w.segments {
		if _, err := seg.f.Seek(seg.size, 0); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) applyRecord(t recordType, payload []byte) (raft.Index, error) {
	switch t {
	case recEntry:
		e, err := decodeEntry(payload)
		if err != nil {
			return 0, err
		}
		// Entries at or below the snapshot boundary are covered by the snapshot
		// and must not enter the index. Segment files legitimately still hold
		// them — compaction deletes whole segments — but keeping them in memory
		// would make lastIndex() report a value below the boundary, and the next
		// append would be rejected as non-contiguous against state the snapshot
		// already superseded.
		if e.Index <= w.offset() {
			return e.Index, nil
		}
		w.entries = append(w.entries, e)
		return e.Index, nil
	case recHardState:
		hs, err := decodeHardState(payload)
		if err != nil {
			return 0, err
		}
		w.hard = hs
	case recConfig:
		c, err := decodeConfig(payload)
		if err != nil {
			return 0, err
		}
		w.config = c
	case recTruncate:
		hs, err := decodeHardState(payload) // reuses the 24-byte layout; index in Term
		if err != nil {
			return 0, err
		}
		w.truncateMemory(raft.Index(hs.Term))
	default:
		return 0, fmt.Errorf("wal: unknown record type %d", t)
	}
	return 0, nil
}

func (w *WAL) truncateMemory(index raft.Index) {
	for i, e := range w.entries {
		if e.Index >= index {
			w.entries = w.entries[:i]
			return
		}
	}
}

// active returns the segment currently being appended to.
func (w *WAL) active() *segment { return w.segments[len(w.segments)-1] }

// roll creates a new segment beginning at first and makes it active. The
// directory is fsync'd after creation: without that, a crash can leave a file
// that was written and fsync'd but whose *name* never reached the disk, and the
// data is unreachable. It is the classic durability bug that survives every test
// except a real power loss.
func (w *WAL) roll(first raft.Index) error {
	path := filepath.Join(w.dir, segmentName(first))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	preallocate(f, w.segSize)

	seg := &segment{path: path, f: f, first: first, last: first - 1}
	w.segments = append(w.segments, seg)
	if err := syncDir(w.dir); err != nil {
		return err
	}
	w.Syncs++

	// Every segment opens with the current configuration and hard state, so that
	// compaction can delete earlier segments without losing them.
	if err := w.write(recConfig, encodeConfig(w.config)); err != nil {
		return err
	}
	if !w.hard.IsEmpty() {
		if err := w.write(recHardState, encodeHardState(w.hard)); err != nil {
			return err
		}
	}
	return w.sync()
}

// write appends one record to the active segment without fsyncing.
func (w *WAL) write(t recordType, payload []byte) error {
	seg := w.active()
	buf := appendRecord(nil, t, payload)
	n, err := seg.f.Write(buf)
	seg.size += int64(n)
	return err
}

func (w *WAL) sync() error {
	if w.noSync {
		return nil
	}
	if err := w.active().f.Sync(); err != nil {
		return err
	}
	w.Syncs++
	return nil
}

// InitialState returns the recovered hard state and configuration.
func (w *WAL) InitialState() (raft.HardState, raft.Config, error) {
	return w.hard, w.config, nil
}

// SetHardState durably records term, vote and commit index.
func (w *WAL) SetHardState(hs raft.HardState) error {
	if err := w.write(recHardState, encodeHardState(hs)); err != nil {
		return err
	}
	if err := w.sync(); err != nil {
		return err
	}
	w.hard = hs
	return nil
}

// Append durably appends entries, rolling to a new segment when the active one
// is full.
func (w *WAL) Append(entries []raft.Entry) error {
	for _, e := range entries {
		if want := w.lastIndex() + 1; e.Index != want {
			return fmt.Errorf("wal: non-contiguous append: got index %d, want %d", e.Index, want)
		}
		if w.active().size >= w.segSize {
			if err := w.roll(e.Index); err != nil {
				return err
			}
		}
		if err := w.write(recEntry, encodeEntry(e)); err != nil {
			return err
		}
		w.entries = append(w.entries, e)
		w.active().last = e.Index
	}
	return w.sync()
}

// TruncateSuffix discards entries from index onward by appending a marker.
func (w *WAL) TruncateSuffix(index raft.Index) error {
	if index <= w.offset() {
		return raft.ErrCompacted
	}
	if len(w.entries) == 0 {
		return nil // nothing above the boundary to discard
	}
	if err := w.write(recTruncate, encodeHardState(raft.HardState{Term: raft.Term(index)})); err != nil {
		return err
	}
	if err := w.sync(); err != nil {
		return err
	}
	w.truncateMemory(index)
	return nil
}

// TruncatePrefix discards whole segments below index, following a snapshot.
//
// The current configuration and hard state are re-appended to the active segment
// first: they may only exist in a segment that is about to be deleted, and
// losing the hard state would let this node vote twice in one term after a
// restart.
func (w *WAL) TruncatePrefix(index raft.Index) error {
	// Guard on what the segment *files* still hold. Guarding on the snapshot
	// boundary makes compaction a permanent no-op (SaveSnapshot advances it
	// first); guarding on the in-memory first index does the same once
	// SaveSnapshot drops the entries it covers. Both leave the log growing
	// forever with every test green — this is the third variant of that mistake
	// in this file, which is why the reason is written down here.
	if len(w.segments) == 0 || index < w.segments[0].first {
		return nil
	}
	if err := w.write(recConfig, encodeConfig(w.config)); err != nil {
		return err
	}
	if !w.hard.IsEmpty() {
		if err := w.write(recHardState, encodeHardState(w.hard)); err != nil {
			return err
		}
	}
	if err := w.sync(); err != nil {
		return err
	}

	// Only whole segments entirely below the boundary may go. Keeping more than
	// asked is always safe; keeping less is not.
	keep := 0
	for i, seg := range w.segments {
		if i == len(w.segments)-1 || seg.last >= index {
			break
		}
		keep = i + 1
	}
	for _, seg := range w.segments[:keep] {
		_ = seg.f.Close()
		if err := os.Remove(seg.path); err != nil {
			return err
		}
	}
	if keep > 0 {
		w.segments = w.segments[keep:]
		if err := syncDir(w.dir); err != nil {
			return err
		}
		w.Syncs++
	}

	// The in-memory index already excludes everything the snapshot covers, so
	// nothing further needs trimming here. Segment files may legitimately still
	// hold covered entries; replay filters them out the same way.
	return nil
}

func (w *WAL) offset() raft.Index { return w.snapMeta.LastIncludedIndex }

func (w *WAL) lastIndex() raft.Index {
	if len(w.entries) == 0 {
		return w.offset()
	}
	return w.entries[len(w.entries)-1].Index
}

func (w *WAL) firstIndex() raft.Index {
	if len(w.entries) == 0 {
		return w.offset() + 1
	}
	return w.entries[0].Index
}

// Entries returns log entries in [lo, hi).
func (w *WAL) Entries(lo, hi raft.Index) ([]raft.Entry, error) {
	switch {
	case lo < w.firstIndex():
		return nil, raft.ErrCompacted
	case hi > w.lastIndex()+1:
		return nil, raft.ErrUnavailable
	case lo >= hi:
		return nil, nil
	}
	base := w.firstIndex()
	out := make([]raft.Entry, hi-lo)
	copy(out, w.entries[lo-base:hi-base])
	return out, nil
}

// Term returns the term of the entry at index, answering for the snapshot
// boundary itself.
func (w *WAL) Term(index raft.Index) (raft.Term, error) {
	if index == w.offset() {
		return w.snapMeta.LastIncludedTerm, nil
	}
	es, err := w.Entries(index, index+1)
	if err != nil {
		return 0, err
	}
	return es[0].Term, nil
}

func (w *WAL) FirstIndex() (raft.Index, error) { return w.firstIndex(), nil }
func (w *WAL) LastIndex() (raft.Index, error)  { return w.lastIndex(), nil }

// Sync flushes the active segment.
func (w *WAL) Sync() error { return w.sync() }

// Close releases every open segment.
func (w *WAL) Close() error {
	var first error
	for _, seg := range w.segments {
		if err := seg.f.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Segments returns the number of segment files, for tests.
func (w *WAL) Segments() int { return len(w.segments) }

// syncDir fsyncs a directory so that file creations and deletions are durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !errors.Is(err, os.ErrInvalid) {
		return err
	}
	return nil
}
