// Package storage defines the durable state a Raft node depends on.
//
// The interface lives outside the raft package because the core may not touch
// a disk. Two implementations exist: an in-memory one used by the deterministic
// simulator (where a "crash" is simply dropping every in-process structure
// *except* this one, and a restart rebuilds from it), and a segmented
// write-ahead log used by real nodes.
//
// Both are driven by the same effect stream, so the simulator exercises the
// real recovery path even when it is not touching a real disk.
package storage

import "github.com/MiNam8/kvstore/raft"

// Storage is the durable state of one Raft node.
//
// Every method that mutates state is a durability barrier in the sense defined
// by [raft.Effect]: when it returns without error, the change MUST survive a
// power loss. An implementation that buffers without fsync satisfies this
// interface's types but violates its contract, and will lose committed data.
type Storage interface {
	// InitialState returns the persisted hard state and cluster configuration,
	// as recovered at startup. A node that has never run returns a zero
	// HardState and the bootstrap configuration.
	InitialState() (raft.HardState, raft.Config, error)

	// SetHardState durably records term, vote and commit index. It must be
	// durable before the caller replies to a vote request (obligation A).
	SetHardState(raft.HardState) error

	// Append durably appends entries to the log. Entries are contiguous and
	// begin at the current last index plus one. It must be durable before the
	// caller acknowledges replication (obligation B).
	Append(entries []raft.Entry) error

	// TruncateSuffix durably discards entries from index onward, on a genuine
	// term conflict only.
	TruncateSuffix(index raft.Index) error

	// TruncatePrefix discards entries up to and including index, following a
	// snapshot. Implementations may discard less than requested — keeping extra
	// entries is always safe — but must never discard more.
	TruncatePrefix(index raft.Index) error

	// Entries returns log entries in [lo, hi). It returns [raft.ErrCompacted] if
	// lo is below the compaction boundary and [raft.ErrUnavailable] if hi
	// exceeds the last index.
	Entries(lo, hi raft.Index) ([]raft.Entry, error)

	// Term returns the term of the entry at index. It answers correctly for the
	// snapshot boundary index itself — the AppendEntries consistency check can
	// land exactly there, and failing to answer would strand the follower.
	Term(index raft.Index) (raft.Term, error)

	// FirstIndex returns the lowest index available in the log; LastIndex
	// returns the highest. On an empty, never-snapshotted log FirstIndex is 1
	// and LastIndex is 0.
	FirstIndex() (raft.Index, error)
	LastIndex() (raft.Index, error)

	// SaveSnapshot durably records a snapshot. The write must be atomic: a
	// crash must leave either the old snapshot or the new one, never a partial
	// one, since a half-written snapshot is unrecoverable rather than merely
	// incomplete.
	SaveSnapshot(meta raft.SnapshotMeta, data []byte) error

	// LoadSnapshot returns the most recent durable snapshot. It returns
	// [raft.ErrUnavailable] if none exists.
	LoadSnapshot() (raft.SnapshotMeta, []byte, error)

	// Sync flushes and fsyncs any pending writes. Implementations that fsync on
	// every mutation may make this a no-op; batching implementations use it to
	// close a group commit.
	Sync() error

	// Close releases resources.
	Close() error
}

// RecoverState rebuilds everything a restarting node needs: durable term and
// vote, the configuration to start from, and the log.
//
// The configuration comes from the snapshot when there is one, and only falls
// back to the bootstrap configuration for a node that has never run. Using the
// bootstrap configuration when a snapshot exists silently reverts every
// membership change the cluster has made — the node comes back counting quorums
// against a voter set that no longer exists. The caller then replays the log's
// configuration entries on top, which [raft.New] does.
func RecoverState(s Storage) (raft.HardState, raft.Config, *raft.Log, error) {
	hs, bootstrap, err := s.InitialState()
	if err != nil {
		return hs, bootstrap, nil, err
	}
	cfg := bootstrap
	if meta, _, err := s.LoadSnapshot(); err == nil && len(meta.Config.Voters) > 0 {
		cfg = meta.Config
	}
	log, err := RecoverLog(s)
	return hs, cfg, log, err
}

// RecoverLog rebuilds the in-memory log a restarting node starts from.
//
// The core owns its log in memory, so recovery means handing back everything
// durable: the snapshot boundary, then every entry above it. Both the in-memory
// and write-ahead-log implementations go through this one function, so the
// simulator exercises the same recovery path a real node uses.
func RecoverLog(s Storage) (*raft.Log, error) {
	var meta raft.SnapshotMeta
	if m, _, err := s.LoadSnapshot(); err == nil {
		meta = m
	}
	first, err := s.FirstIndex()
	if err != nil {
		return nil, err
	}
	last, err := s.LastIndex()
	if err != nil {
		return nil, err
	}
	// Start above the snapshot boundary. Compaction works at segment
	// granularity, so storage may legitimately still hold entries below it;
	// handing those to the core would be a non-contiguous append.
	if first <= meta.LastIncludedIndex {
		first = meta.LastIncludedIndex + 1
	}
	var entries []raft.Entry
	if last >= first {
		if entries, err = s.Entries(first, last+1); err != nil {
			return nil, err
		}
	}
	return raft.NewLogFrom(meta, entries), nil
}
