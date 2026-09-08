package raft_test

import (
	"errors"
	"testing"

	"github.com/MiNam8/kvstore/raft"
)

// ents builds contiguous entries starting at index 1, one per term given.
func ents(terms ...raft.Term) []raft.Entry {
	out := make([]raft.Entry, len(terms))
	for i, t := range terms {
		out[i] = raft.Entry{Index: raft.Index(i + 1), Term: t}
	}
	return out
}

func TestEmptyLog(t *testing.T) {
	l := raft.NewLog()
	if got := l.LastIndex(); got != 0 {
		t.Errorf("LastIndex = %d, want 0", got)
	}
	if got := l.LastTerm(); got != 0 {
		t.Errorf("LastTerm = %d, want 0", got)
	}
	if got := l.FirstIndex(); got != 1 {
		t.Errorf("FirstIndex = %d, want 1", got)
	}
	if l.Len() != 0 {
		t.Errorf("Len = %d, want 0", l.Len())
	}
	// Index 0 is the empty prefix every log shares; a leader probing a brand new
	// follower sends PrevLogIndex=0, and it must match or replication can never
	// start.
	if !l.Match(0, 0) {
		t.Error("empty log should Match(0, 0) — the shared empty prefix")
	}
}

func TestTermAtCompactionBoundary(t *testing.T) {
	l := raft.NewLog()
	l.Append(ents(1, 1, 2, 3)...)
	l.Compact(2, 1)

	// The boundary index itself must still answer. A follower that snapshotted
	// through index 2 and cannot report term(2) would reject every AppendEntries
	// probing at PrevLogIndex=2 and never make progress again.
	got, err := l.Term(2)
	if err != nil {
		t.Fatalf("Term(2) at the compaction boundary: %v", err)
	}
	if got != 1 {
		t.Errorf("Term(2) = %d, want 1", got)
	}
	if !l.Match(2, 1) {
		t.Error("Match at the compaction boundary should succeed")
	}

	if _, err := l.Term(1); !errors.Is(err, raft.ErrCompacted) {
		t.Errorf("Term(1) below boundary: err = %v, want ErrCompacted", err)
	}
	if _, err := l.Term(99); !errors.Is(err, raft.ErrUnavailable) {
		t.Errorf("Term(99) past the end: err = %v, want ErrUnavailable", err)
	}
	if got, want := l.FirstIndex(), raft.Index(3); got != want {
		t.Errorf("FirstIndex after compaction = %d, want %d", got, want)
	}
	if got, want := l.LastIndex(), raft.Index(4); got != want {
		t.Errorf("LastIndex after compaction = %d, want %d", got, want)
	}
}

func TestCompactBeyondLogResetsToSentinel(t *testing.T) {
	// A snapshot from the leader can overtake everything this node holds.
	l := raft.NewLog()
	l.Append(ents(1, 1)...)
	l.Compact(50, 7)

	if got, want := l.LastIndex(), raft.Index(50); got != want {
		t.Errorf("LastIndex = %d, want %d", got, want)
	}
	if got, want := l.LastTerm(), raft.Term(7); got != want {
		t.Errorf("LastTerm = %d, want %d", got, want)
	}
	if l.Len() != 0 {
		t.Errorf("Len = %d, want 0 after a snapshot past the tail", l.Len())
	}
	// Appends must resume from the snapshot boundary, not from index 3.
	l.Append(raft.Entry{Index: 51, Term: 7})
	if got := l.LastIndex(); got != 51 {
		t.Errorf("LastIndex after append = %d, want 51", got)
	}
}

func TestCompactIsMonotonic(t *testing.T) {
	l := raft.NewLog()
	l.Append(ents(1, 1, 2)...)
	l.Compact(2, 1)
	l.Compact(1, 1) // stale request: must not un-compact
	if got, want := l.FirstIndex(), raft.Index(3); got != want {
		t.Errorf("FirstIndex = %d, want %d — compaction went backwards", got, want)
	}
}

func TestTruncateSuffix(t *testing.T) {
	l := raft.NewLog()
	l.Append(ents(1, 1, 2, 2, 3)...)

	l.TruncateSuffix(3)
	if got, want := l.LastIndex(), raft.Index(2); got != want {
		t.Errorf("LastIndex = %d, want %d", got, want)
	}
	// Truncating past the tail is a no-op rather than an error: a conflict
	// resolved by a message that is already stale must not corrupt the log.
	l.TruncateSuffix(99)
	if got, want := l.LastIndex(), raft.Index(2); got != want {
		t.Errorf("LastIndex after no-op truncate = %d, want %d", got, want)
	}
	l.Append(raft.Entry{Index: 3, Term: 5})
	if got, _ := l.Term(3); got != 5 {
		t.Errorf("Term(3) = %d, want 5 after re-append", got)
	}
}

func TestSliceCopiesRatherThanAliases(t *testing.T) {
	// Entries handed out inside an Effect may be transmitted after the log has
	// moved on. If Slice aliased the backing array, a later truncation would
	// silently rewrite a message already queued for send.
	l := raft.NewLog()
	l.Append(ents(1, 1, 1)...)

	got, err := l.Slice(1, 4)
	if err != nil {
		t.Fatalf("Slice: %v", err)
	}
	l.TruncateSuffix(1)
	l.Append(raft.Entry{Index: 1, Term: 9})

	if got[0].Term != 1 {
		t.Errorf("previously returned slice was mutated by a later truncation: term = %d, want 1", got[0].Term)
	}
}

func TestSliceBounds(t *testing.T) {
	l := raft.NewLog()
	l.Append(ents(1, 1, 2)...)
	l.Compact(1, 1)

	if _, err := l.Slice(1, 2); !errors.Is(err, raft.ErrCompacted) {
		t.Errorf("Slice below boundary: err = %v, want ErrCompacted", err)
	}
	if _, err := l.Slice(2, 9); !errors.Is(err, raft.ErrUnavailable) {
		t.Errorf("Slice past the end: err = %v, want ErrUnavailable", err)
	}
	if got, err := l.Slice(2, 2); err != nil || len(got) != 0 {
		t.Errorf("empty slice: got %v, %v; want empty, nil", got, err)
	}
}

// TestIsUpToDateComparesTermBeforeIndex guards the election restriction
// (§5.4.1). This is the single most consequential comparison in Raft: getting
// the order backwards yields a cluster that passes every happy-path test and
// then silently loses committed data under partition, because a node that
// missed entries can win an election on log length alone and overwrite them.
func TestIsUpToDateComparesTermBeforeIndex(t *testing.T) {
	// Local log ends at index 2, term 2.
	l := raft.NewLog()
	l.Append(ents(1, 2)...)

	tests := []struct {
		name  string
		index raft.Index
		term  raft.Term
		want  bool
	}{
		{"higher term, shorter log wins", 1, 3, true},
		{"lower term, longer log loses", 100, 1, false},
		{"same term, longer log wins", 3, 2, true},
		{"same term, same length wins", 2, 2, true},
		{"same term, shorter log loses", 1, 2, false},
		{"empty candidate loses", 0, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := l.IsUpToDate(tc.index, tc.term); got != tc.want {
				t.Errorf("IsUpToDate(%d, %d) = %v, want %v", tc.index, tc.term, got, tc.want)
			}
		})
	}
}

func TestNonContiguousAppendPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("a gap in the log should panic rather than corrupt it silently")
		}
	}()
	l := raft.NewLog()
	l.Append(raft.Entry{Index: 5, Term: 1})
}

func TestNewLogFromRestart(t *testing.T) {
	meta := raft.SnapshotMeta{LastIncludedIndex: 10, LastIncludedTerm: 3}
	l := raft.NewLogFrom(meta, []raft.Entry{
		{Index: 11, Term: 3},
		{Index: 12, Term: 4},
	})
	if got, want := l.FirstIndex(), raft.Index(11); got != want {
		t.Errorf("FirstIndex = %d, want %d", got, want)
	}
	if got, want := l.LastIndex(), raft.Index(12); got != want {
		t.Errorf("LastIndex = %d, want %d", got, want)
	}
	if !l.Match(10, 3) {
		t.Error("recovered log must Match its snapshot boundary")
	}
}
