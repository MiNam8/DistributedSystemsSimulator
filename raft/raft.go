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

	// MaxInflight bounds pipelining: AppendEntries messages that may be
	// outstanding to one follower between heartbeats. One disables pipelining,
	// which is kept only so its absence can be measured.
	MaxInflight int

	// PreVote makes a node poll for support before consuming a term. It costs a
	// little failover latency and prevents a partitioned or restarting node from
	// disrupting a healthy cluster. Off by default so its effect is measurable.
	PreVote bool

	// CheckQuorum makes a leader step down when it can no longer reach a
	// majority. It is pre-vote's necessary partner: without it, an isolated
	// leader's heartbeats keep its reachable followers refusing every pre-vote.
	CheckQuorum bool

	// LeaseDuration enables leader-lease reads: a leader may serve a read from
	// local state for this long after a quorum last acknowledged it, without a
	// confirmation round. Zero disables leases.
	//
	// # The clock assumption, stated plainly
	//
	// This is the only place in the implementation where correctness depends on
	// clocks rather than on messages. It is safe if and only if
	//
	//	LeaseDuration < ElectionTimeout - (maximum clock rate error between nodes)
	//
	// because the argument is: no follower will start an election until its own
	// election timeout elapses, so within that window no other node can have
	// become leader. If this node's clock runs *slow* relative to a follower's,
	// the follower's timeout fires before this node believes its lease expired,
	// a new leader is elected, and this node keeps serving reads from a term it
	// has already lost.
	//
	// Go's garbage collector can also pause a process for longer than a lease,
	// with the same effect and no clock error at all. The DST harness
	// demonstrates both — see the lease tests, which set the bound deliberately
	// wrong and show the stale read appear.
	LeaseDuration Duration

	// Log is the recovered log. Nil means a fresh node.
	Log *Log

	// HardState is the recovered term, vote and commit index. Zero for a fresh node.
	HardState HardState

	// Now seeds the node's notion of the current time. A restarting node must
	// pass the current clock reading, or its first Tick would look like a jump
	// of the entire uptime and fire an election immediately.
	Now Time
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
	maxInflight       int
	leaseDuration     Duration
	preVote           bool
	checkQuorumOn     bool

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

	// Read state. readReqs holds in-flight ReadIndex confirmations; deferredReads
	// are reads waiting for this leader's no-op to commit; leaseUntil is the
	// instant the current leadership lease expires.
	// prevConfig and pendingConfIndex support applying configuration changes on
	// append: the previous configuration must be restorable if the entry that
	// introduced the current one is truncated away.
	prevConfig       Config
	pendingConfIndex Index

	// transferee is the target of an in-progress leadership handover, or None.
	// While set, the leader refuses proposals so the target's log can converge
	// on a fixed endpoint rather than chasing a moving one.
	transferee       NodeID
	transferDeadline Time

	readRound     *readRound
	readRoundSeq  uint64
	readQueue     []uint64
	deferredReads []uint64
	leaseUntil    Time

	leaseReads     int
	confirmedReads int

	// Candidate state. Counted by walking cluster.Voters rather than by ranging
	// this map: Go randomises map iteration order, which would make effect order
	// vary between runs of the same seed and quietly destroy reproducibility.
	votes map[NodeID]bool

	// Timing. Absolute deadlines rather than counters, so the core never needs
	// to know how often it is ticked.
	now              Time
	electionDeadline Time
	// lastLeaderContact is when a leader was last heard from. Pre-vote consults
	// it rather than electionDeadline, because the two mean different things: a
	// node that is not a voter resets its deadline every tick without ever
	// campaigning, and reading contentment off the deadline made such a node
	// refuse every pre-vote forever — one learner could deadlock the cluster.
	lastLeaderContact Time
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
		maxInflight:       max(opts.MaxInflight, 1),
		leaseDuration:     opts.LeaseDuration,
		preVote:           opts.PreVote,
		checkQuorumOn:     opts.CheckQuorum,
		term:              opts.HardState.Term,
		votedFor:          opts.HardState.VotedFor,
		commitIndex:       opts.HardState.Commit,
		log:               log,
		role:              Follower,
		votes:             make(map[NodeID]bool),
		now:               opts.Now,
	}
	// Rebuild the configuration by replaying every configuration entry the
	// recovered log holds.
	//
	// Without this a restarting node comes back with its *bootstrap* membership
	// and silently forgets every change since — counting quorums against a voter
	// set that no longer exists, and in a cluster whose membership has shifted,
	// electing leaders that a real majority never endorsed. The starting point is
	// the snapshot's configuration (already in cluster), because entries below
	// the snapshot boundary are folded into it.
	r.replayConfig()

	r.resetElectionDeadline()
	return r, nil
}

// replayConfig folds every configuration entry in the log into the current
// membership, in order.
func (r *Raft) replayConfig() {
	first, last := r.log.FirstIndex(), r.log.LastIndex()
	if last < first {
		return
	}
	entries, err := r.log.Slice(first, last+1)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.Type != EntryConfChange {
			continue
		}
		cc, err := DecodeConfChange(e.Data)
		if err != nil {
			continue
		}
		r.prevConfig = r.cluster
		r.cluster = applyConfChange(r.cluster, cc)
		// Only a change above the commit index is still revocable; anything at
		// or below it is settled and can never be truncated away.
		if e.Index > r.commitIndex {
			r.pendingConfIndex = e.Index
		}
	}
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

	// After a restart the state machine is empty while commitIndex was recovered
	// from durable state, so the log prefix below it must be replayed. Doing it
	// here rather than only on the next commit advance means a restarted node
	// catches up promptly instead of waiting for the next client write.
	effs := r.applyCommitted(nil)

	if r.role == Leader {
		if now >= r.heartbeatDeadline {
			r.heartbeatDeadline = now + Time(r.heartbeatInterval)
			// The heartbeat is also the pipeline's resynchronisation point:
			// the window is cleared and next is pulled back to what is actually
			// known replicated, so anything lost in flight is retransmitted.
			// Between heartbeats the leader runs ahead optimistically; once per
			// interval it reconciles with reality.
			for _, peer := range r.peers() {
				pr := r.prs[peer]
				pr.inflight = 0
				pr.next = max(pr.match+1, 1)
			}
			r.abortStalledTransfer()
			effs = r.broadcastAppend(effs)
			effs = r.checkQuorum(effs)
		}
		return effs
	}
	if now >= r.electionDeadline {
		// A learner, or a node removed from the configuration, must never
		// campaign: it cannot win, and under pre-vote it would not even be
		// polled. Letting it try only burns terms.
		if !r.cluster.IsVoter(r.id) {
			r.resetElectionDeadline()
			return effs
		}
		if r.preVote {
			effs = r.startPreVote(effs)
		} else {
			effs = r.startElection(effs)
		}
	}
	return effs
}

// Step processes an incoming message and returns the resulting effects.
func (r *Raft) Step(m Message) []Effect {
	var effs []Effect

	// Pre-vote traffic bypasses the term rules entirely. The term it carries is
	// the one the sender *would* campaign in, not one it holds — adopting it
	// would recreate the exact disruption pre-vote exists to prevent.
	switch m.Type {
	case MsgPreVote:
		return r.handlePreVote(effs, m)
	case MsgPreVoteResp:
		return r.handlePreVoteResp(effs, m)
	}

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
		// Stale message. Every *request* gets an explicit rejection carrying our
		// term, because that reply is the only way the sender learns it has
		// fallen behind.
		//
		// Dropping these silently is safe but costs liveness in a way that is
		// easy to miss: a leader deposed while partitioned keeps heartbeating
		// its dead term forever, never steps down, and goes on telling clients
		// it is the leader. Only a reply carrying a higher term makes it yield
		// (§5.1).
		//
		// Responses are still dropped. Replying to a reply would bounce a pair
		// of stale nodes off each other indefinitely.
		switch m.Type {
		case MsgVote:
			return r.send(effs, Message{Type: MsgVoteResp, To: m.From, Reject: true})
		case MsgApp:
			return r.send(effs, Message{Type: MsgAppResp, To: m.From, Reject: true})
		case MsgSnap:
			return r.send(effs, Message{Type: MsgSnapResp, To: m.From, Reject: true})
		case MsgHeartbeat:
			return r.send(effs, Message{Type: MsgHeartbeatResp, To: m.From, Reject: true})
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
	case MsgSnap:
		return r.handleSnap(effs, m)
	case MsgSnapResp:
		return r.handleSnapResp(effs, m)
	case MsgHeartbeat:
		return r.handleHeartbeat(effs, m)
	case MsgHeartbeatResp:
		return r.handleHeartbeatResp(effs, m)
	case MsgTimeoutNow:
		return r.handleTimeoutNow(effs, m)
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

// sendWithTerm is send with an explicit term, for pre-vote traffic whose term is
// hypothetical rather than this node's own.
func (r *Raft) sendWithTerm(effs []Effect, m Message, term Term) []Effect {
	effs = r.flushHardState(effs)
	m.From = r.id
	m.Term = term
	return append(effs, SendMessage{Message: m})
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

// peers returns every other cluster member — voters *and* learners — in sorted
// order. Learners receive the log like anyone else; they simply do not count
// toward a quorum.
func (r *Raft) peers() []NodeID {
	out := make([]NodeID, 0, len(r.cluster.Voters)+len(r.cluster.Learners))
	for _, v := range r.cluster.Voters {
		if v != r.id {
			out = append(out, v)
		}
	}
	out = append(out, r.cluster.Learners...)
	return out
}

// votingPeers returns the other voters only, for elections.
func (r *Raft) votingPeers() []NodeID {
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
