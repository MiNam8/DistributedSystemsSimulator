package raft

import "errors"

// Term is a Raft term number. Terms are monotonically increasing and act as a
// logical clock: any message carrying a higher term causes the receiver to step
// down and adopt it before any other processing.
type Term uint64

// Index is a position in the replicated log. The log is 1-based; index 0 means
// "no entry" and is the value of LastIncludedIndex for a cluster that has never
// snapshotted.
type Index uint64

// NodeID identifies a cluster member. Zero is reserved to mean "no node"
// (e.g. VotedFor when no vote has been cast in the current term).
type NodeID uint64

// None is the zero NodeID: no leader, no vote.
const None NodeID = 0

// Time is a monotonic instant in nanoseconds, supplied by the caller through
// [Raft.Tick]. The core never reads a clock itself: the simulator passes virtual
// nanoseconds and the real runtime passes elapsed monotonic nanoseconds, and the
// core cannot tell the difference. Wall-clock time is never used, for any purpose.
type Time int64

// Duration is a span of nanoseconds, in the same units as [Time].
type Duration int64

// Convenience constants for expressing timeouts in [Duration] without importing
// the time package.
const (
	Millisecond Duration = 1_000_000
	Second      Duration = 1000 * Millisecond
)

// Role is a node's current role in the Raft protocol.
type Role uint8

const (
	// Follower is passive: it responds to leaders and candidates and starts an
	// election if it hears from neither before its election timeout elapses.
	Follower Role = iota
	// PreCandidate polls for votes without incrementing its term (§9.6). It
	// prevents a node returning from a partition from forcing the cluster
	// through a needless election with an inflated term.
	PreCandidate
	// Candidate has incremented its term and is soliciting votes.
	Candidate
	// Leader accepts proposals and replicates them.
	Leader
)

// String returns the role name. Written as a switch rather than via fmt because
// this package may not import fmt; see the purity contract in the package doc.
func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown-role"
	}
}

// EntryType distinguishes log entries the state machine applies from entries
// that exist only for the protocol's own purposes.
type EntryType uint8

const (
	// EntryNormal carries an opaque client command for the state machine.
	EntryNormal EntryType = iota
	// EntryNoOp is the empty entry a new leader appends on election. It is what
	// lets a leader commit entries from previous terms without violating §5.4.2,
	// and what lets it learn its true commit index — which ReadIndex depends on.
	EntryNoOp
	// EntryConfChange carries a single-server membership change (§4).
	EntryConfChange
)

// Entry is one record in the replicated log.
//
// The (Index, Term) pair is the identity Raft's Log Matching Property is stated
// over: if two logs contain an entry with the same index and term, then the logs
// are identical in all preceding entries.
type Entry struct {
	Index Index
	Term  Term
	Type  EntryType
	Data  []byte
}

// HardState is the subset of a node's state that MUST survive a crash. Losing
// it is not a performance problem, it is a correctness problem: a node that
// forgets its vote can vote twice in one term after restarting, and two leaders
// can then coexist in the same term.
type HardState struct {
	Term     Term
	VotedFor NodeID
	Commit   Index
}

// IsEmpty reports whether hs carries no information worth persisting.
func (hs HardState) IsEmpty() bool {
	return hs.Term == 0 && hs.VotedFor == None && hs.Commit == 0
}

// SnapshotMeta describes a point-in-time snapshot of the state machine. The
// (LastIncludedIndex, LastIncludedTerm) pair replaces the log prefix the
// snapshot covers, and LastIncludedTerm must be retained so that an
// AppendEntries consistency check landing exactly on the snapshot boundary can
// still be answered.
type SnapshotMeta struct {
	LastIncludedIndex Index
	LastIncludedTerm  Term
	// Config is the cluster membership as of the snapshot. A restarting node
	// recovers its view of the cluster from here, not from a config file.
	Config Config
}

// Config is a cluster membership configuration.
//
// Learners are non-voting members: they receive the log but are not counted
// toward any quorum. New nodes join as learners and catch up before being
// promoted, so that adding a node to a three-node cluster does not put
// availability at risk while it replays the log.
type Config struct {
	Voters   []NodeID
	Learners []NodeID
}

// Quorum returns the number of voters that constitutes a majority.
func (c Config) Quorum() int { return len(c.Voters)/2 + 1 }

// IsVoter reports whether id is a voting member of c.
func (c Config) IsVoter(id NodeID) bool {
	for _, v := range c.Voters {
		if v == id {
			return true
		}
	}
	return false
}

// Sentinel errors. These are errors.New values rather than fmt.Errorf because
// this package may not import fmt; callers should compare with errors.Is.
var (
	// ErrCompacted means the requested index is below the log's compaction
	// boundary and is only recoverable via a snapshot.
	ErrCompacted = errors.New("raft: requested index is unavailable due to log compaction")

	// ErrUnavailable means the requested index is beyond the end of the log.
	ErrUnavailable = errors.New("raft: requested index is beyond the last log entry")

	// ErrNotLeader means a proposal was made to a node that is not the leader.
	// The caller should redirect using the leader hint.
	ErrNotLeader = errors.New("raft: node is not the leader")

	// ErrProposalDropped means the proposal was discarded — typically because
	// leadership was lost, or a transfer is in progress.
	ErrProposalDropped = errors.New("raft: proposal dropped")

	// ErrConfChangeInFlight means a membership change is already uncommitted.
	// Only one may be in flight at a time; see the §4 safety note in membership.go.
	ErrConfChangeInFlight = errors.New("raft: a configuration change is already in flight")
)
