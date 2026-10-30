package raft

import "slices"

// readRound is one in-flight leadership confirmation, shared by every read
// waiting when it started.
//
// Confirmation is per *round*, not per read: a single heartbeat quorum proves
// leadership at one instant, and that one proof serves every read invoked before
// it. Issuing a round per read — the obvious implementation — costs a message to
// every peer per request, and measured 2x *slower* than reading through the log
// at 64 concurrent clients, because reads through the log at least get group
// commit for free. This is the same amortisation, applied to the read path.
type readRound struct {
	id      uint64
	index   Index
	acks    map[NodeID]bool
	waiters []uint64
}

// ReadIndex begins a linearizable read that does not go through the log.
//
// # Why a read needs anything at all
//
// Serving a read from the leader's state machine looks obviously correct and is
// obviously wrong: a leader that has been partitioned away does not know it has
// been deposed, and will happily answer with state that is arbitrarily stale.
// Nothing in Raft tells a leader it has lost its mandate — it finds out only by
// hearing from someone with a higher term, which by definition it cannot hear
// while partitioned.
//
// So a read must establish that this node was still the leader at some instant
// at or after the read was invoked. There are three ways, and this package
// implements all three so they can be compared:
//
//   - through the log — the read becomes a log entry and consensus orders it.
//     Obviously correct, one round trip *and one fsync*.
//   - ReadIndex — record the commit index, confirm leadership with a heartbeat
//     quorum, wait for the state machine to catch up. One round trip, no disk.
//   - a leader lease — serve locally while inside a window in which no election
//     could have succeeded. No round trip, and correct only under a clock
//     assumption; see [Options.LeaseDuration].
//
// The returned effects may be empty: if this leader has not yet committed an
// entry from its own term it does not know its true commit index, so the read is
// deferred until the no-op appended on election commits (§8).
func (r *Raft) ReadIndex(reqID uint64) ([]Effect, error) {
	if r.role != Leader {
		return nil, ErrNotLeader
	}
	if !r.committedInCurrentTerm() {
		// A fresh leader's commit index is a lower bound, not the truth: entries
		// from a previous term may be committed on a quorum without this node
		// knowing. Serving a read against that index could miss a committed
		// write. The no-op from becomeLeader resolves this within one round.
		r.deferredReads = append(r.deferredReads, reqID)
		return nil, nil
	}
	return r.startRead(nil, reqID), nil
}

// startRead serves the read under a valid lease, or queues it for the next
// confirmation round.
func (r *Raft) startRead(effs []Effect, reqID uint64) []Effect {
	// Under a valid lease no confirmation is needed: within this window no other
	// node could have completed an election, so this node is still leader.
	if r.leaseDuration > 0 && r.now < r.leaseUntil {
		r.leaseReads++
		return append(effs, ReadIndexReady{ReqID: reqID, Index: r.commitIndex})
	}
	// A single-voter cluster is its own quorum.
	if r.cluster.Quorum() == 1 {
		return append(effs, ReadIndexReady{ReqID: reqID, Index: r.commitIndex})
	}

	r.confirmedReads++
	r.readQueue = append(r.readQueue, reqID)
	if r.readRound == nil {
		effs = r.startReadRound(effs)
	}
	return effs
}

// startReadRound opens a confirmation round covering every queued read.
func (r *Raft) startReadRound(effs []Effect) []Effect {
	if len(r.readQueue) == 0 {
		return effs
	}
	r.readRoundSeq++
	r.readRound = &readRound{
		id: r.readRoundSeq,
		// Recorded *before* the round is confirmed. Any write acknowledged
		// before these reads were invoked is at or below this index, so serving
		// once the state machine reaches it cannot miss one.
		index:   r.commitIndex,
		acks:    map[NodeID]bool{r.id: true},
		waiters: r.readQueue,
	}
	r.readQueue = nil

	for _, peer := range r.peers() {
		effs = r.send(effs, Message{Type: MsgHeartbeat, To: peer, ReadReqID: r.readRound.id})
	}
	return effs
}

// committedInCurrentTerm reports whether this leader has committed an entry of
// its own term, and therefore knows its true commit index.
func (r *Raft) committedInCurrentTerm() bool {
	t, err := r.log.Term(r.commitIndex)
	return err == nil && t == r.term
}

// flushDeferredReads starts any reads that were waiting for the leader's no-op.
func (r *Raft) flushDeferredReads(effs []Effect) []Effect {
	if len(r.deferredReads) == 0 || !r.committedInCurrentTerm() {
		return effs
	}
	pending := r.deferredReads
	r.deferredReads = nil
	for _, reqID := range pending {
		effs = r.startRead(effs, reqID)
	}
	return effs
}

// handleHeartbeat answers a leadership confirmation probe.
//
// It carries no entries and no commit index, so it can never advance a
// follower's state — it exists only so the leader can prove, to itself, that a
// quorum still recognises it.
func (r *Raft) handleHeartbeat(effs []Effect, m Message) []Effect {
	if r.role != Follower {
		r.becomeFollower(m.Term, m.From)
	}
	r.leader = m.From
	r.lastLeaderContact = r.now
	r.resetElectionDeadline()
	return r.send(effs, Message{Type: MsgHeartbeatResp, To: m.From, ReadReqID: m.ReadReqID})
}

// handleHeartbeatResp counts a confirmation and renews the lease.
func (r *Raft) handleHeartbeatResp(effs []Effect, m Message) []Effect {
	if r.role != Leader {
		return effs
	}
	if pr, ok := r.prs[m.From]; ok {
		pr.lastAck = r.now
	}
	r.renewLease()

	round := r.readRound
	if round == nil || round.id != m.ReadReqID {
		return effs // a response to a round already finished, or long superseded
	}
	round.acks[m.From] = true

	// Counted over the sorted voter list, never by ranging the acks map.
	n := 0
	for _, v := range r.cluster.Voters {
		if round.acks[v] {
			n++
		}
	}
	if n < r.cluster.Quorum() {
		return effs
	}

	// One quorum proves leadership for every read that was waiting.
	r.readRound = nil
	for _, reqID := range round.waiters {
		effs = append(effs, ReadIndexReady{ReqID: reqID, Index: round.index})
	}
	// Reads that arrived while this round was in flight go in the next one.
	return r.startReadRound(effs)
}

// renewLease extends the window during which this node may serve reads without
// confirming leadership.
//
// The window ends one lease duration after the moment a *quorum* last
// acknowledged this leader — not after the most recent single acknowledgement,
// which would let one responsive follower keep a deposed leader's lease alive.
func (r *Raft) renewLease() {
	if r.leaseDuration <= 0 {
		return
	}
	acks := make([]Time, 0, len(r.cluster.Voters))
	for _, v := range r.cluster.Voters {
		if v == r.id {
			acks = append(acks, r.now)
			continue
		}
		if pr, ok := r.prs[v]; ok {
			acks = append(acks, pr.lastAck)
		} else {
			acks = append(acks, 0)
		}
	}
	slices.Sort(acks)
	quorumAck := acks[len(acks)-r.cluster.Quorum()]

	if until := quorumAck + Time(r.leaseDuration); until > r.leaseUntil {
		r.leaseUntil = until
	}
}

// clearReadState drops in-flight read confirmations. Called on stepping down:
// a node that is no longer leader cannot answer them, and the client will
// time out and retry against whoever won.
func (r *Raft) clearReadState() {
	r.readRound = nil
	r.readQueue = nil
	r.deferredReads = nil
	r.leaseUntil = 0
}

// ReadStats returns how many reads were served under a lease versus by an
// explicit quorum confirmation. The ratio is the whole argument for leases.
func (r *Raft) ReadStats() (lease, confirmed int) { return r.leaseReads, r.confirmedReads }
