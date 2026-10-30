package raft

// Effect describes something the caller must do on the core's behalf. The core
// never performs I/O; it returns effects and the runtime executes them. This is
// the seam that lets the deterministic simulator and the real node runtime drive
// the same implementation.
//
// # The ordering rule
//
// Effects are returned in a strict order and MUST be executed in that order.
// Every Persist* and Truncate* effect is a durability barrier: it must be
// durable (fsync'd) before any *later* [SendMessage] in the same batch leaves
// the process.
//
// That one rule encodes the three durability obligations Raft's safety actually
// rests on, and it is where essentially every real-world Raft data-loss bug lives:
//
//   - A. Term and vote durable before granting a vote. Otherwise a node that
//     crashes after replying votes again for a different candidate in the same
//     term after restart, and two leaders coexist.
//   - B. Entries durable before replying MsgAppResp with Reject=false. Otherwise
//     a node acknowledges an entry into a commit quorum and then loses it, and a
//     committed entry disappears.
//   - C. The leader's own match index advances only after its own fsync. A leader
//     must not count itself in a quorum for data it has not durably stored.
//
// Obligation C governs *quorum counting*, not transmission. The leader may
// therefore transmit MsgApp before its own fsync completes — a real throughput
// win, and a precise relaxation rather than a shortcut. See Phase 8 in PLAN.md.
type Effect interface {
	isEffect()
}

// PersistHardState requires the term, vote and commit index to be made durable.
// Durability barrier (obligation A).
type PersistHardState struct {
	HardState HardState
}

// PersistEntries requires entries to be appended to the log and made durable.
// Durability barrier (obligations B and C).
type PersistEntries struct {
	Entries []Entry
}

// TruncateSuffix requires the log to be truncated from FromIndex onward,
// discarding conflicting entries. Durability barrier.
//
// This is only ever emitted for a *genuine* term conflict. A delayed or
// duplicated MsgApp must never cause truncation of entries the follower already
// holds — doing so is the most common way a from-scratch Raft loses committed
// data, and it is exactly what the reorder and duplicate faults in the simulator
// exist to catch.
type TruncateSuffix struct {
	FromIndex Index
}

// CompactLog permits the log prefix up to and including UpToIndex to be
// discarded, following a snapshot. Not a durability barrier: it may be applied
// lazily, and discarding less than permitted is always safe.
type CompactLog struct {
	UpToIndex Index
}

// PersistSnapshot requires a snapshot to be made durable. The write must be
// atomic — write to a temporary file, fsync it, rename, then fsync the parent
// directory — because a half-installed snapshot is unrecoverable state rather
// than a merely incomplete one. Durability barrier.
type PersistSnapshot struct {
	Meta SnapshotMeta
	Data []byte
}

// SendMessage requires a message to be transmitted. Delivery is best-effort:
// the transport may drop, delay, duplicate or reorder it, and the protocol
// tolerates all four. It must not be transmitted before every preceding
// durability barrier in the same effect batch has completed.
type SendMessage struct {
	Message Message
}

// RestoreStateMachine requires the state machine to be replaced wholesale by a
// snapshot, discarding whatever it held.
//
// It is always preceded by a [PersistSnapshot] barrier in the same batch, so a
// crash midway leaves either the old snapshot or the new one, never a state
// machine restored from a snapshot that is not on disk.
type RestoreStateMachine struct {
	Meta SnapshotMeta
	Data []byte
}

// ApplyEntries requires entries to be applied to the state machine. Entries are
// always contiguous, strictly ordered by index, never above the commit index,
// and never delivered twice.
type ApplyEntries struct {
	Entries []Entry
}

// ReadIndexReady reports that a linearizable read may now be served once the
// state machine has applied through Index. The runtime waits for its applied
// index to reach Index, then reads locally.
type ReadIndexReady struct {
	ReqID uint64
	Index Index
}

// StateChanged reports a role, term or leader transition. Observability only —
// it carries no obligation, and dropping it cannot affect correctness. Both
// harnesses feed it to the invariant checker, and the real runtime also logs
// and counts it.
type StateChanged struct {
	Role   Role
	Term   Term
	Leader NodeID
}

func (PersistHardState) isEffect()    {}
func (PersistEntries) isEffect()      {}
func (TruncateSuffix) isEffect()      {}
func (CompactLog) isEffect()          {}
func (PersistSnapshot) isEffect()     {}
func (SendMessage) isEffect()         {}
func (RestoreStateMachine) isEffect() {}
func (ApplyEntries) isEffect()        {}
func (ReadIndexReady) isEffect()      {}
func (StateChanged) isEffect()        {}

// IsBarrier reports whether e is a durability barrier: an effect that must be
// fsync'd before any later SendMessage in the same batch is transmitted. The
// runtime's effect executor is built around this predicate, and the storage
// fault injector targets exactly the window it defines.
func IsBarrier(e Effect) bool {
	switch e.(type) {
	case PersistHardState, PersistEntries, TruncateSuffix, PersistSnapshot:
		return true
	default:
		return false
	}
}
