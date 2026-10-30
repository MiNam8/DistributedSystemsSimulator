package raft

// MessageType tags a [Message]. A single tagged struct is used rather than an
// interface per message kind because the simulator must record, replay, mutate
// and compare messages by value, and the gRPC transport must serialise them —
// both of which a flat struct makes trivial.
type MessageType uint8

const (
	_ MessageType = iota

	// MsgPreVote polls for support without incrementing the candidate's term.
	MsgPreVote
	// MsgPreVoteResp answers a pre-vote poll.
	MsgPreVoteResp
	// MsgVote solicits a vote in the candidate's new term.
	MsgVote
	// MsgVoteResp answers a vote request.
	MsgVoteResp

	// MsgApp replicates log entries; with no entries it serves as a heartbeat.
	MsgApp
	// MsgAppResp acknowledges or rejects replication.
	MsgAppResp

	// MsgSnap installs a snapshot on a follower the leader has compacted past.
	MsgSnap
	// MsgSnapResp acknowledges a snapshot install.
	MsgSnapResp

	// MsgHeartbeat confirms leadership for a ReadIndex round without carrying
	// log data.
	MsgHeartbeat
	// MsgHeartbeatResp confirms receipt, carrying the read request id so the
	// leader can count a quorum for that specific read.
	MsgHeartbeatResp

	// MsgTimeoutNow instructs a fully caught-up follower to start an election
	// immediately, for leadership transfer.
	MsgTimeoutNow
)

// String returns the message type name. Hand-written; this package may not
// import fmt (see the package doc).
func (t MessageType) String() string {
	switch t {
	case MsgPreVote:
		return "MsgPreVote"
	case MsgPreVoteResp:
		return "MsgPreVoteResp"
	case MsgVote:
		return "MsgVote"
	case MsgVoteResp:
		return "MsgVoteResp"
	case MsgApp:
		return "MsgApp"
	case MsgAppResp:
		return "MsgAppResp"
	case MsgSnap:
		return "MsgSnap"
	case MsgSnapResp:
		return "MsgSnapResp"
	case MsgHeartbeat:
		return "MsgHeartbeat"
	case MsgHeartbeatResp:
		return "MsgHeartbeatResp"
	case MsgTimeoutNow:
		return "MsgTimeoutNow"
	default:
		return "MsgUnknown"
	}
}

// Message is a Raft protocol message. Not every field is meaningful for every
// [MessageType]; the field comments say which types use them.
//
// Messages are values. The transport may drop, delay, duplicate or reorder them
// arbitrarily — the protocol is responsible for tolerating all four, and the
// simulator injects all four deliberately.
type Message struct {
	Type MessageType
	From NodeID
	To   NodeID

	// Term is the sender's term. A receiver seeing a higher term steps down and
	// adopts it before any other processing; a receiver seeing a lower term
	// rejects the message. This check comes first for every message type.
	Term Term

	// LastLogIndex and LastLogTerm carry the candidate's log tail on MsgVote and
	// MsgPreVote. They implement the election restriction (§5.4.1): a voter
	// refuses a candidate whose log is not at least as up to date as its own,
	// comparing term first and only then index. Reversing that comparison order
	// yields a cluster that works perfectly until it silently loses committed data.
	LastLogIndex Index
	LastLogTerm  Term

	// PrevLogIndex and PrevLogTerm are the AppendEntries consistency check on
	// MsgApp: the follower accepts only if it has this exact entry.
	PrevLogIndex Index
	PrevLogTerm  Term

	// Entries are the entries to replicate on MsgApp. Empty means heartbeat.
	Entries []Entry

	// Commit is the leader's commitIndex on MsgApp, allowing followers to
	// advance their own (bounded by the index of the last entry they hold).
	Commit Index

	// Reject reports failure on MsgAppResp, MsgVoteResp, MsgPreVoteResp and
	// MsgSnapResp.
	Reject bool

	// MatchIndex is the highest index the follower is known to hold, returned on
	// a successful MsgAppResp. It must be derived from the message the follower
	// actually accepted rather than from its own log tail, so that a delayed
	// response cannot advance the leader's view past what it acknowledged.
	MatchIndex Index

	// RejectHintIndex and RejectHintTerm implement fast backtracking on a
	// rejected MsgApp. Naive nextIndex-- costs one round trip per entry; these
	// hints reduce recovery to one round trip per *term*.
	RejectHintIndex Index
	RejectHintTerm  Term

	// ReadReqID correlates MsgHeartbeat/MsgHeartbeatResp for a ReadIndex round,
	// so a leader counts a quorum for one specific read rather than for reads in
	// general.
	ReadReqID uint64

	// Snapshot is set on MsgSnap.
	//
	// On the *send* side the core fills in metadata only: it decides when a
	// follower needs a snapshot, and the runtime loads the bytes from storage
	// and streams them. A consensus core that held multi-megabyte payloads
	// would put them in every effect batch and in every simulator trace.
	//
	// On the *receive* side the transport has already reassembled the stream, so
	// SnapshotData is populated and the core can emit the install as ordinary,
	// ordered effects.
	Snapshot     *SnapshotMeta
	SnapshotData []byte
}
