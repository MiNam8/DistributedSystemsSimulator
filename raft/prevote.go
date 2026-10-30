package raft

// Pre-vote and CheckQuorum (Ongaro §9.6).
//
// # The disruption pre-vote prevents
//
// A node that has been partitioned away keeps timing out and campaigning, and
// each campaign increments its term. When the partition heals it announces that
// term, every healthy node steps down to adopt it, and a perfectly good leader
// is deposed by a node that could not possibly win — its log is behind, so it
// loses the election it just forced. The cluster pays an election for nothing.
//
// The same thing happens on an ordinary restart: a node comes back as a
// follower, times out before it hears a heartbeat, and campaigns against a
// healthy leader. Phase 5's demo showed the term jumping 2 → 4 when a restarted
// node rejoined.
//
// Pre-vote fixes it by asking first. A candidate polls for support *without*
// incrementing its term or persisting anything; only if a quorum says it would
// win does it start a real election. A node whose log is behind, or whose peers
// still hear from a leader, is refused — and its term never moves.

// startPreVote polls peers without incrementing the term or touching disk.
func (r *Raft) startPreVote(effs []Effect) []Effect {
	r.role = PreCandidate
	r.leader = None
	// A pre-vote for oneself is free: nothing is persisted, because nothing has
	// been promised. That is the entire point — a pre-vote round leaves no
	// trace, so losing it costs nothing.
	r.votes = map[NodeID]bool{r.id: true}
	r.resetElectionDeadline()

	effs = append(effs, StateChanged{Role: r.role, Term: r.term, Leader: r.leader})

	if r.tally() >= r.cluster.Quorum() {
		return r.startElection(effs)
	}
	for _, peer := range r.votingPeers() {
		effs = r.sendWithTerm(effs, Message{
			Type:         MsgPreVote,
			To:           peer,
			LastLogIndex: r.log.LastIndex(),
			LastLogTerm:  r.log.LastTerm(),
			// The term this node *would* campaign in. Hypothetical: neither
			// sender nor receiver adopts it.
		}, r.term+1)
	}
	return effs
}

// handlePreVote answers a poll.
//
// Support is granted only if this node would itself be willing to start an
// election — that is, it has stopped hearing from a leader — and the candidate's
// log is at least as up to date as its own. Nothing is persisted and the term is
// not changed, because nothing has been promised.
func (r *Raft) handlePreVote(effs []Effect, m Message) []Effect {
	// Refuse anyone campaigning in a term this node has already passed.
	stale := m.Term < r.term
	// Refuse while a leader is still being heard from. This is what stops a
	// partitioned node from disrupting a healthy cluster: its peers are content,
	// so they decline, and its term never moves.
	//
	// Measured against actual leader contact, not against this node's own
	// election deadline. A node that is not a voter resets that deadline every
	// tick without ever campaigning, so reading contentment off it left such a
	// node permanently content — and permanently refusing. One learner was
	// enough to deadlock a five-node cluster.
	contented := r.leader != None &&
		r.now-r.lastLeaderContact < Time(r.electionTimeout)
	upToDate := r.log.IsUpToDate(m.LastLogIndex, m.LastLogTerm)

	grant := !stale && !contented && upToDate
	return r.sendWithTerm(effs, Message{
		Type:   MsgPreVoteResp,
		To:     m.From,
		Reject: !grant,
	}, m.Term)
}

// handlePreVoteResp counts support and, on a quorum, starts the real election.
func (r *Raft) handlePreVoteResp(effs []Effect, m Message) []Effect {
	if r.role != PreCandidate {
		return effs // already resolved, or a stale reply
	}
	if !r.cluster.IsVoter(m.From) {
		return effs
	}
	r.votes[m.From] = !m.Reject

	if r.tally() >= r.cluster.Quorum() {
		// Support confirmed. Only now is a term consumed.
		return r.startElection(effs)
	}
	if r.rejections() >= r.cluster.Quorum() {
		// A quorum says this node would lose. Stand down without having cost the
		// cluster anything — no term bump, no election, no deposed leader.
		r.becomeFollower(r.term, r.leader)
	}
	return effs
}

// checkQuorum steps a leader down when it can no longer reach a majority.
//
// It is pre-vote's necessary partner. Pre-vote makes peers refuse a candidate
// while they still hear from a leader; without CheckQuorum, a leader that has
// been cut off keeps heartbeating into the void, and on the majority side its
// followers keep refusing pre-votes from each other for as long as those
// heartbeats keep arriving. CheckQuorum makes the isolated leader notice and
// resign, which is what lets the majority elect a successor.
func (r *Raft) checkQuorum(effs []Effect) []Effect {
	if r.role != Leader || !r.checkQuorumOn {
		return effs
	}
	active := 1 // this node counts itself
	for _, v := range r.cluster.Voters {
		if v == r.id {
			continue
		}
		if pr, ok := r.prs[v]; ok && r.now-pr.lastAck < Time(r.electionTimeout) {
			active++
		}
	}
	if active >= r.cluster.Quorum() {
		return effs
	}
	// Step down rather than keep asserting a leadership no quorum recognises.
	r.becomeFollower(r.term, None)
	return append(effs, StateChanged{Role: r.role, Term: r.term, Leader: r.leader})
}
