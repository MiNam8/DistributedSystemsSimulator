package raft

import (
	"errors"
	"slices"
)

// Options configures a Raft node. It is separate from [Config], which describes
// cluster membership and is itself replicated through the log.
type Options struct {
	// ID is this node's identity. Must be non-zero.
	ID NodeID

	// Cluster is the starting membership. On restart it comes from the recovered
	// snapshot rather than from a config file, because membership is replicated
	// state and a stale config file would silently resurrect a removed node.
	Cluster Config

	// ElectionTimeout is the base timeout before a follower starts an election.
	// The effective timeout is drawn uniformly from [T, 2T); that randomisation
	// is the entire mechanism that breaks vote splits.
	ElectionTimeout Duration

	// HeartbeatInterval is how often a leader asserts authority. It must be
	// comfortably below ElectionTimeout or the cluster cannot hold a stable
	// leader — see the validation in [New].
	HeartbeatInterval Duration

	// Rand is the source of election jitter. Required, and required to be
	// seeded: a global source would make simulated runs irreproducible.
	Rand Rand

	// Log is the recovered log. Nil means a fresh node.
	Log *Log

	// HardState is the recovered term, vote and commit index. Zero for a fresh node.
	HardState HardState
}

// Raft is the consensus state machine.
//
// It is not safe for concurrent use, deliberately: all concurrency lives in the
// node runtime, which owns exactly one Raft and drives it from a single
// goroutine. Adding a mutex here would permit interleavings the simulator could
// not reproduce.
type Raft struct {
	id      NodeID
	cluster Config
	rand    Rand

	electionTimeout   Duration
	heartbeatInterval Duration

	// Persistent state. Any change to term or votedFor sets hsDirty, and a dirty
	// hard state is flushed as a PersistHardState effect before any message that
	// depends on it is sent — see [Raft.send].
	term     Term
	votedFor NodeID
	log      *Log
	hsDirty  bool

	// Volatile state.
	role         Role
	leader       NodeID
	commitIndex  Index
	appliedIndex Index

	// Leader state: one progress per voter, including self. Iterated through
	// cluster.Voters rather than by ranging the map, for the same reason as votes.
	prs map[NodeID]*progress

	// Candidate state. Counted by walking cluster.Voters rather than by ranging
	// this map: Go randomises map iteration order, which would make effect order
	// vary between runs of the same seed and quietly destroy reproducibility.
	votes map[NodeID]bool

	// Timing. Absolute deadlines rather than counters, so the core never needs
	// to know how often it is ticked.
	now               Time
	electionDeadline  Time
	heartbeatDeadline Time
}

// New constructs a Raft node.
func New(opts Options) (*Raft, error) {
	switch {
	case opts.ID == None:
		return nil, errors.New("raft: node ID must be non-zero")
	case opts.Rand == nil:
		return nil, errors.New("raft: a seeded Rand is required; a global source would make runs irreproducible")
	case opts.ElectionTimeout <= 0:
		return nil, errors.New("raft: ElectionTimeout must be positive")
	case opts.HeartbeatInterval <= 0:
		return nil, errors.New("raft: HeartbeatInterval must be positive")
	case opts.HeartbeatInterval >= opts.ElectionTimeout:
		// With heartbeats no more frequent than the election timeout, followers
		// time out before the leader can reassert itself and the cluster churns
		// through elections forever. This is a configuration mistake that
		// presents as a mysterious liveness failure, so it is rejected here.
		return nil, errors.New("raft: HeartbeatInterval must be well below ElectionTimeout")
	case len(opts.Cluster.Voters) == 0:
		return nil, errors.New("raft: cluster must have at least one voter")
	}

	cluster := Config{
		Voters:   slices.Clone(opts.Cluster.Voters),
		Learners: slices.Clone(opts.Cluster.Learners),
	}
	// Sorted so that every node iterates peers in the same order regardless of
	// how it was configured, making effect traces comparable across nodes.
	slices.Sort(cluster.Voters)
	slices.Sort(cluster.Learners)

	log := opts.Log
	if log == nil {
		log = NewLog()
	}

	r := &Raft{
		id:                opts.ID,
		cluster:           cluster,
		rand:              opts.Rand,
		electionTimeout:   opts.ElectionTimeout,
		heartbeatInterval: opts.HeartbeatInterval,
		term:              opts.HardState.Term,
		votedFor:          opts.HardState.VotedFor,
		commitIndex:       opts.HardState.Commit,
		log:               log,
		role:              Follower,
		votes:             make(map[NodeID]bool),
	}
	r.resetElectionDeadline()
	return r, nil
}

// ID returns this node's identity.
func (r *Raft) ID() NodeID { return r.id }

// Role returns the current role.
func (r *Raft) Role() Role { return r.role }

// Term returns the current term.
func (r *Raft) Term() Term { return r.term }

// Leader returns the currently known leader, or [None].
func (r *Raft) Leader() NodeID { return r.leader }

// CommitIndex returns the highest index known to be committed.
func (r *Raft) CommitIndex() Index { return r.commitIndex }

// AppliedIndex returns the highest index handed to the state machine.
func (r *Raft) AppliedIndex() Index { return r.appliedIndex }

// Log returns the replicated log. Exposed for the invariant checker and the
// runtime's startup path; callers must not mutate it.
func (r *Raft) Log() *Log { return r.log }

// HardState returns the state that must be durable across a crash.
func (r *Raft) HardState() HardState {
	return HardState{Term: r.term, VotedFor: r.votedFor, Commit: r.commitIndex}
}

// Tick advances the node's notion of time to now and returns the resulting
// effects. now is monotonic nanoseconds; the core cannot tell whether it is
// virtual or real, which is exactly what lets the simulator run an hour of
// cluster behaviour in a millisecond.
func (r *Raft) Tick(now Time) []Effect {
	if now < r.now {
		panic("raft: time moved backwards; Tick requires a monotonic clock")
	}
	r.now = now

	var effs []Effect
	if r.role == Leader {
		if now >= r.heartbeatDeadline {
			r.heartbeatDeadline = now + Time(r.heartbeatInterval)
			// The periodic heartbeat is a full AppendEntries carrying whatever
			// each follower is missing, so a follower that dropped a message
			// recovers on the next beat rather than stalling until a client
			// happens to write again.
			effs = r.broadcastAppend(effs)
		}
		return effs
	}
	if now >= r.electionDeadline {
		effs = r.startElection(effs)
	}
	return effs
}

// Step processes an incoming message and returns the resulting effects.
func (r *Raft) Step(m Message) []Effect {
	var effs []Effect

	switch {
	case m.Term > r.term:
		// A higher term always wins. Step down and adopt it before any other
		// processing — this single rule is what keeps terms acting as a logical
		// clock, and skipping it for even one message type breaks election safety.
		//
		// The vote is cleared because it belonged to the old term; this node has
		// not yet voted in the new one.
		r.becomeFollower(m.Term, None)

	case m.Term < r.term:
		// Stale message. Vote requests get an explicit rejection carrying our
		// term so the stale candidate learns it has fallen behind and steps
		// down, rather than retrying until its own election timeout.
		if m.Type == MsgVote || m.Type == MsgPreVote {
			return r.send(effs, Message{Type: respType(m.Type), To: m.From, Reject: true})
		}
		return nil
	}

	switch m.Type {
	case MsgVote:
		return r.handleVote(effs, m)
	case MsgVoteResp:
		return r.handleVoteResp(effs, m)
	case MsgApp:
		return r.handleApp(effs, m)
	case MsgAppResp:
		return r.handleAppResp(effs, m)
	default:
		return effs
	}
}

// respType maps a request type to its response type.
func respType(t MessageType) MessageType {
	if t == MsgPreVote {
		return MsgPreVoteResp
	}
	return MsgVoteResp
}

// send appends a SendMessage effect, first flushing any dirty hard state.
//
// This is the enforcement point for durability obligation A: a message whose
// meaning depends on the current term or vote must not leave the process before
// that term and vote are durable. Routing every send through here makes the
// ordering rule a property of the code rather than something the author has to
// remember at each call site.
func (r *Raft) send(effs []Effect, m Message) []Effect {
	effs = r.flushHardState(effs)
	m.From = r.id
	m.Term = r.term
	return append(effs, SendMessage{Message: m})
}

// flushHardState emits a PersistHardState effect if term or vote have changed
// since the last flush.
func (r *Raft) flushHardState(effs []Effect) []Effect {
	if !r.hsDirty {
		return effs
	}
	r.hsDirty = false
	return append(effs, PersistHardState{HardState: r.HardState()})
}

// peers returns the other voters, in sorted order. Deterministic by
// construction; see the note on Raft.votes.
func (r *Raft) peers() []NodeID {
	out := make([]NodeID, 0, len(r.cluster.Voters))
	for _, v := range r.cluster.Voters {
		if v != r.id {
			out = append(out, v)
		}
	}
	return out
}

// resetElectionDeadline draws a fresh randomised election timeout from
// [T, 2T). Randomisation is what breaks vote splits: with a fixed timeout,
// followers that lost the same leader would all become candidates at the same
// instant, split the vote, and repeat indefinitely.
func (r *Raft) resetElectionDeadline() {
	jitter := Duration(r.rand.Intn(int(r.electionTimeout)))
	r.electionDeadline = r.now + Time(r.electionTimeout) + Time(jitter)
}
