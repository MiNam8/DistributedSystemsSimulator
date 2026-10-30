// Package mem is an in-memory [storage.Storage] for the deterministic simulator.
//
// It is not a stub. A simulated crash discards every in-process structure
// *except* this one, and a restart rebuilds the node from exactly what it holds
// — so this is the real recovery path, exercised on every simulated restart long
// before Phase 4 puts a write-ahead log behind the same interface.
package mem

import (
	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/storage"
)

// Storage holds a node's durable state in memory.
type Storage struct {
	hard     raft.HardState
	config   raft.Config
	entries  []raft.Entry // entries[i] has index snapMeta.LastIncludedIndex+i+1
	snapMeta raft.SnapshotMeta
	snapData []byte
	hasSnap  bool

	// Syncs counts fsync barriers, so tests can assert that durability was
	// actually requested rather than merely assumed.
	Syncs int
}

var _ storage.Storage = (*Storage)(nil)

// New returns empty storage for a node bootstrapping into cluster.
func New(cluster raft.Config) *Storage {
	return &Storage{config: cluster}
}

func (s *Storage) InitialState() (raft.HardState, raft.Config, error) {
	return s.hard, s.config, nil
}

func (s *Storage) SetHardState(hs raft.HardState) error {
	s.hard = hs
	s.Syncs++
	return nil
}

// offset is the snapshot boundary: the highest index a snapshot covers.
func (s *Storage) offset() raft.Index { return s.snapMeta.LastIncludedIndex }

// firstIndex is the lowest index actually held as an entry.
//
// It is derived from the entries themselves, not from the snapshot boundary.
// SaveSnapshot advances the boundary before TruncatePrefix removes anything, so
// for a window the two disagree — and indexing off the boundary during that
// window reads the wrong slot and hands the core a log that starts below its own
// snapshot. That is exactly the bug this method exists to prevent, and it is the
// same one the write-ahead log had.
func (s *Storage) firstIndex() raft.Index {
	if len(s.entries) == 0 {
		return s.offset() + 1
	}
	return s.entries[0].Index
}

func (s *Storage) Append(entries []raft.Entry) error {
	for _, e := range entries {
		want := s.lastIndex() + 1
		if e.Index != want {
			return raft.ErrUnavailable // a gap: the caller has a bug
		}
		s.entries = append(s.entries, e)
	}
	s.Syncs++
	return nil
}

func (s *Storage) lastIndex() raft.Index {
	if len(s.entries) == 0 {
		return s.offset()
	}
	return s.entries[len(s.entries)-1].Index
}

func (s *Storage) TruncateSuffix(index raft.Index) error {
	if index < s.firstIndex() {
		return raft.ErrCompacted
	}
	if keep := int(index - s.firstIndex()); keep < len(s.entries) {
		// Reslice rather than reuse the array: the discarded tail must not be
		// observable through any slice handed out earlier.
		kept := make([]raft.Entry, keep)
		copy(kept, s.entries[:keep])
		s.entries = kept
	}
	s.Syncs++
	return nil
}

func (s *Storage) TruncatePrefix(index raft.Index) error {
	if len(s.entries) == 0 || index < s.firstIndex() {
		return nil
	}
	drop := min(int(index-s.firstIndex())+1, len(s.entries))
	kept := make([]raft.Entry, len(s.entries)-drop)
	copy(kept, s.entries[drop:])
	s.entries = kept
	return nil
}

func (s *Storage) Entries(lo, hi raft.Index) ([]raft.Entry, error) {
	switch {
	case lo < s.firstIndex():
		return nil, raft.ErrCompacted
	case hi > s.lastIndex()+1:
		return nil, raft.ErrUnavailable
	case lo >= hi:
		return nil, nil
	}
	base := s.firstIndex()
	out := make([]raft.Entry, hi-lo)
	copy(out, s.entries[lo-base:hi-base])
	return out, nil
}

func (s *Storage) Term(index raft.Index) (raft.Term, error) {
	if index == s.offset() {
		return s.snapMeta.LastIncludedTerm, nil
	}
	es, err := s.Entries(index, index+1)
	if err != nil {
		return 0, err
	}
	return es[0].Term, nil
}

func (s *Storage) FirstIndex() (raft.Index, error) { return s.firstIndex(), nil }
func (s *Storage) LastIndex() (raft.Index, error)  { return s.lastIndex(), nil }

func (s *Storage) SaveSnapshot(meta raft.SnapshotMeta, data []byte) error {
	s.snapMeta, s.hasSnap = meta, true
	s.snapData = append([]byte(nil), data...)
	// Entries the snapshot covers are dropped so firstIndex and lastIndex stay
	// consistent with the boundary; otherwise lastIndex could report a value
	// below it and reject the next append as non-contiguous.
	for i, e := range s.entries {
		if e.Index > meta.LastIncludedIndex {
			s.entries = s.entries[i:]
			s.Syncs++
			return nil
		}
	}
	s.entries = nil
	s.Syncs++
	return nil
}

func (s *Storage) LoadSnapshot() (raft.SnapshotMeta, []byte, error) {
	if !s.hasSnap {
		return raft.SnapshotMeta{}, nil, raft.ErrUnavailable
	}
	return s.snapMeta, s.snapData, nil
}

func (s *Storage) Sync() error  { s.Syncs++; return nil }
func (s *Storage) Close() error { return nil }

// RecoverLog rebuilds the in-memory log a restarting node starts from.
// Equivalent to [storage.RecoverLog], kept as a direct path because the
// simulator calls it on every restart.
//
// Entries at or below the snapshot boundary are skipped: compaction may lag the
// boundary, and the core's log must start above it.
func (s *Storage) RecoverLog() *raft.Log {
	var entries []raft.Entry
	for _, e := range s.entries {
		if e.Index > s.offset() {
			entries = append(entries, e)
		}
	}
	out := make([]raft.Entry, len(entries))
	copy(out, entries)
	return raft.NewLogFrom(s.snapMeta, out)
}
