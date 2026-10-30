package raft

// Leadership transfer (Ongaro §3.10).
//
// Raft has no way to hand leadership over: a leader can only stop and let the
// cluster notice, which costs an election timeout of unavailability every time a
// node is drained for maintenance. Transfer removes that wait — the outgoing
// leader brings the target fully up to date and then tells it to campaign
// immediately, so the gap is one round trip instead of one timeout.

// TransferLeadership asks this leader to hand leadership to target.
//
// It returns [ErrNotLeader] if this node is not the leader, and an error if the
// target is not a voter — a learner cannot win an election, so instructing one
// to campaign would simply strand the cluster until the next timeout.
func (r *Raft) TransferLeadership(target NodeID) ([]Effect, error) {
	if r.role != Leader {
		return nil, ErrNotLeader
	}
	if target == r.id {
		return nil, nil // already here
	}
	if !r.cluster.IsVoter(target) {
		return nil, ErrTransferTarget
	}
	if _, ok := r.prs[target]; !ok {
		return nil, ErrTransferTarget
	}

	r.transferee = target
	// Bounded so a target that never catches up — because it is partitioned, or
	// simply slow — does not leave this leader refusing proposals forever.
	r.transferDeadline = r.now + Time(r.electionTimeout)

	return r.maybeTimeoutNow(nil, target), nil
}

// maybeTimeoutNow hands over as soon as the target holds the whole log.
//
// The wait matters: a target elected while behind would have to be caught up by
// the *next* leader anyway, and in the meantime it cannot commit anything,
// which turns a transfer meant to avoid downtime into a cause of it.
func (r *Raft) maybeTimeoutNow(effs []Effect, target NodeID) []Effect {
	pr, ok := r.prs[target]
	if !ok || r.transferee != target {
		return effs
	}
	if pr.match < r.log.LastIndex() {
		// Not caught up yet. Push the remaining entries; handleAppResp will call
		// back here when the acknowledgement lands.
		return r.sendAppend(effs, target)
	}
	r.transferee = None
	return r.send(effs, Message{Type: MsgTimeoutNow, To: target})
}

// handleTimeoutNow campaigns immediately at the outgoing leader's request.
//
// Pre-vote is deliberately skipped. Its purpose is to stop a node campaigning
// when it would lose, and the leader has just established the opposite: this
// node holds the entire log and the leader is standing aside. Polling first
// would only add a round trip to the handover it exists to make fast.
func (r *Raft) handleTimeoutNow(effs []Effect, m Message) []Effect {
	if !r.cluster.IsVoter(r.id) {
		return effs // a learner cannot win; ignore
	}
	return r.startElection(effs)
}

// abortStalledTransfer gives up on a handover whose target never caught up, so
// the leader resumes accepting proposals.
func (r *Raft) abortStalledTransfer() {
	if r.transferee != None && r.now >= r.transferDeadline {
		r.transferee = None
	}
}
