package raft

// Log is the replicated log, held entirely in memory above the compaction
// boundary.
//
// # Why the core owns the log
//
// etcd's raft calls out to a Storage interface for entries below its unstable
// tail, which is why that interface carries a "must not block" caveat: the core
// is reaching toward a disk it is not allowed to wait for. That is incompatible
// with the purity contract here, so this core holds the whole log instead. The
// runtime replays the write-ahead log at startup and hands back a reconstructed
// Log; from then on the core never asks anyone for an entry.
//
// The memory cost is bounded by exactly the mechanism that already exists to
// bound it — snapshotting and compaction. An unbounded in-memory log means
// compaction is not running, which is a bug worth surfacing rather than hiding
// behind a disk read.
//
// # The sentinel
//
// entries[0] is a sentinel carrying (LastIncludedIndex, LastIncludedTerm) of the
// most recent snapshot, and holds no data. It exists so that a consistency check
// landing exactly on the compaction boundary can still be answered: a follower
// that has snapshotted through index 50 must still be able to say what term
// entry 50 had, or a leader probing at PrevLogIndex=50 could never make progress.
// On a fresh log the sentinel is {Index: 0, Term: 0}, so index 0 means "before
// the beginning" and needs no special case.
type Log struct {
	entries []Entry
}

// NewLog returns an empty log with a zero sentinel.
func NewLog() *Log {
	return &Log{entries: []Entry{{Index: 0, Term: 0}}}
}

// NewLogFrom reconstructs a log after a restart: a snapshot boundary followed by
// whatever entries the write-ahead log replay recovered. Entries must be
// contiguous and start at meta.LastIncludedIndex+1.
func NewLogFrom(meta SnapshotMeta, entries []Entry) *Log {
	l := &Log{entries: make([]Entry, 1, len(entries)+1)}
	l.entries[0] = Entry{Index: meta.LastIncludedIndex, Term: meta.LastIncludedTerm}
	l.Append(entries...)
	return l
}

// offset is the index of the sentinel: the highest index this log has compacted
// away, and therefore the index immediately below the first real entry.
func (l *Log) offset() Index { return l.entries[0].Index }

// FirstIndex returns the lowest index still present as a real entry.
func (l *Log) FirstIndex() Index { return l.offset() + 1 }

// LastIndex returns the highest index in the log, or the compaction boundary if
// no real entries remain.
func (l *Log) LastIndex() Index { return l.entries[len(l.entries)-1].Index }

// LastTerm returns the term of the last entry, or the snapshot's term if the log
// holds no real entries.
func (l *Log) LastTerm() Term { return l.entries[len(l.entries)-1].Term }

// Len returns the number of real entries held in memory.
func (l *Log) Len() int { return len(l.entries) - 1 }

// Term returns the term of the entry at index i. It answers for the compaction
// boundary itself, which the consistency check depends on.
func (l *Log) Term(i Index) (Term, error) {
	switch {
	case i < l.offset():
		return 0, ErrCompacted
	case i > l.LastIndex():
		return 0, ErrUnavailable
	default:
		return l.entries[i-l.offset()].Term, nil
	}
}

// Match reports whether the log contains an entry at index i with term t. This
// is the AppendEntries consistency check, phrased as Raft states it: a follower
// accepts entries only if it holds this exact (index, term) pair.
//
// Index 0 with term 0 always matches — it denotes the empty prefix that every
// log shares.
func (l *Log) Match(i Index, t Term) bool {
	got, err := l.Term(i)
	return err == nil && got == t
}

// Slice returns a copy of entries in [lo, hi). The copy is deliberate: the
// returned entries are handed to the runtime inside an Effect and may outlive
// the next mutation of this log, so aliasing the backing array would let a
// later truncation silently rewrite a message already queued for send.
func (l *Log) Slice(lo, hi Index) ([]Entry, error) {
	switch {
	case lo > hi:
		panic("raft: Log.Slice called with lo > hi")
	case lo <= l.offset() && lo != hi:
		return nil, ErrCompacted
	case hi > l.LastIndex()+1:
		return nil, ErrUnavailable
	case lo == hi:
		return nil, nil
	}
	out := make([]Entry, hi-lo)
	copy(out, l.entries[lo-l.offset():hi-l.offset()])
	return out, nil
}

// Append adds entries to the end of the log. Entries must be contiguous and
// begin at LastIndex+1; anything else is a programming error in the caller
// rather than a condition to tolerate, so it panics rather than corrupting the
// log silently.
func (l *Log) Append(entries ...Entry) {
	for _, e := range entries {
		if want := l.LastIndex() + 1; e.Index != want {
			panic("raft: non-contiguous append to log")
		}
		l.entries = append(l.entries, e)
	}
}

// TruncateSuffix discards entries from index onward.
//
// This is only ever called on a genuine term conflict. Truncating on any
// AppendEntries whose prefix matches — rather than only where terms actually
// disagree — is the most common way a from-scratch Raft loses committed data:
// a delayed or duplicated message arrives carrying an older, shorter view of
// the log, and blind truncation throws away entries that a quorum already
// acknowledged. The reorder and duplicate faults in the simulator exist
// specifically to catch that mistake.
func (l *Log) TruncateSuffix(index Index) {
	switch {
	case index <= l.offset():
		panic("raft: attempt to truncate at or below the compaction boundary")
	case index > l.LastIndex():
		return // nothing to discard
	}
	l.entries = l.entries[:index-l.offset()]
}

// Compact discards the prefix up to and including index, replacing it with a
// sentinel carrying that index and term. Compacting at or below the current
// boundary is a no-op, since discarding less than requested is always safe.
//
// If index is beyond the last entry — a snapshot from the leader that overtakes
// everything this node holds — the log is reset to the sentinel alone.
func (l *Log) Compact(index Index, term Term) {
	if index <= l.offset() {
		return
	}
	if index >= l.LastIndex() {
		l.entries = []Entry{{Index: index, Term: term}}
		return
	}
	kept := l.entries[index-l.offset():]
	rebuilt := make([]Entry, len(kept))
	copy(rebuilt, kept)
	rebuilt[0] = Entry{Index: index, Term: term}
	l.entries = rebuilt
}

// Reset discards the log entirely, replacing it with a snapshot boundary.
//
// This is the "otherwise, discard the entire log" half of §7, and it is not the
// same as [Log.Compact]. Compact keeps whatever follows the boundary, which is
// right when this node already holds the boundary entry with a matching term —
// the Log Matching Property then says the tail is legitimate. When the boundary
// does *not* match, the tail belongs to a different history: keeping it splices
// a divergent suffix onto a prefix it never followed, and a node that later wins
// an election will replicate that fabrication over entries a quorum committed.
func (l *Log) Reset(index Index, term Term) {
	l.entries = []Entry{{Index: index, Term: term}}
}

// IsUpToDate reports whether a candidate whose log ends at (index, term) is at
// least as up to date as this one — Raft's election restriction (§5.4.1).
//
// Term is compared first, and only on a tie does index matter. This ordering is
// the entire mechanism preventing a node that missed committed entries from
// winning an election and then overwriting them. Comparing index first produces
// a cluster that passes every happy-path test and silently loses committed data
// under partition.
func (l *Log) IsUpToDate(index Index, term Term) bool {
	if last := l.LastTerm(); term != last {
		return term > last
	}
	return index >= l.LastIndex()
}
