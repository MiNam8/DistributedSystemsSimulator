package raft

// becomeFollower transitions to follower in the given term.
//
// lead may be None when the leader is not yet known — a node that adopts a
// higher term from a vote request knows the term is current but not who won it.
func (r *Raft) becomeFollower(term Term, lead NodeID) {
	if term > r.term {
		r.term = term
		r.votedFor = None
		r.hsDirty = true
	}
	r.role = Follower
	r.leader = lead
	r.votes = make(map[NodeID]bool)
	r.resetElectionDeadline()
}

// startElection increments the term and solicits votes.
func (r *Raft) startElection(effs []Effect) []Effect {
	r.term++
	r.role = Candidate
	r.leader = None
	r.votedFor = r.id
	r.hsDirty = true
	r.votes = map[NodeID]bool{r.id: true}
	r.resetElectionDeadline()

	effs = append(effs, StateChanged{Role: r.role, Term: r.term, Leader: r.leader})

	// A single-node cluster wins immediately: it is its own quorum, and waiting
	// for a response that can never arrive would leave it permanently leaderless.
	if r.tally() >= r.cluster.Quorum() {
		return r.becomeLeader(effs)
	}

	for _, peer := range r.peers() {
		effs = r.send(effs, Message{
			Type:         MsgVote,
			To:           peer,
			LastLogIndex: r.log.LastIndex(),
			LastLogTerm:  r.log.LastTerm(),
		})
	}
	return effs
}

// handleVote decides whether to grant a vote.
//
// Two conditions, both required (§5.2, §5.4.1):
//
//   - This node has not already voted in this term, or already voted for this
//     same candidate. The second half matters because a duplicated or retried
//     request must get the same answer, not a rejection.
//   - The candidate's log is at least as up to date as this node's. This is the
//     election restriction, and it is what guarantees a new leader already holds
//     every committed entry.
func (r *Raft) handleVote(effs []Effect, m Message) []Effect {
	canVote := r.votedFor == None || r.votedFor == m.From
	upToDate := r.log.IsUpToDate(m.LastLogIndex, m.LastLogTerm)

	if !canVote || !upToDate {
		return r.send(effs, Message{Type: MsgVoteResp, To: m.From, Reject: true})
	}

	r.votedFor = m.From
	r.hsDirty = true
	// Granting a vote resets this node's own election timer. Without this, a
	// follower that just endorsed a candidate could time out and campaign
	// against it before the votes finish arriving, turning every election into a
	// split.
	r.resetElectionDeadline()

	// send flushes the PersistHardState barrier first: the vote must be durable
	// before it is promised, or a crash here lets this node vote again in the
	// same term after restart and elect a second leader (obligation A).
	return r.send(effs, Message{Type: MsgVoteResp, To: m.From, Reject: false})
}

// handleVoteResp counts a vote and promotes to leader on a quorum.
func (r *Raft) handleVoteResp(effs []Effect, m Message) []Effect {
	if r.role != Candidate {
		return effs // stale response from an election already resolved
	}
	// Ignore votes from non-voters: a learner's opinion must never count toward
	// a quorum, or membership changes could elect a leader a majority never
	// endorsed.
	if !r.cluster.IsVoter(m.From) {
		return effs
	}
	r.votes[m.From] = !m.Reject

	if r.tally() >= r.cluster.Quorum() {
		return r.becomeLeader(effs)
	}
	// A rejection quorum means this election is lost. Waiting out the full
	// timeout would be correct but slow; stepping down immediately lets the
	// winner's heartbeat land sooner.
	if r.rejections() >= r.cluster.Quorum() {
		r.becomeFollower(r.term, None)
	}
	return effs
}

// tally counts granted votes by walking the sorted voter list rather than
// ranging the votes map, whose iteration order Go randomises.
func (r *Raft) tally() int {
	n := 0
	for _, v := range r.cluster.Voters {
		if r.votes[v] {
			n++
		}
	}
	return n
}

// rejections counts explicit refusals, ignoring voters yet to answer.
func (r *Raft) rejections() int {
	n := 0
	for _, v := range r.cluster.Voters {
		if granted, answered := r.votes[v]; answered && !granted {
			n++
		}
	}
	return n
}

// becomeLeader promotes this node, appends its term's no-op entry, and
// immediately asserts authority.
//
// The no-op is not a formality. Until a leader commits an entry from its *own*
// term it may not mark any earlier entry committed (§5.4.2), so without it a
// leader elected into a quiet cluster could never commit the backlog it
// inherited. It is also how the leader learns its true commit index, which
// ReadIndex depends on in Phase 9.
func (r *Raft) becomeLeader(effs []Effect) []Effect {
	r.role = Leader
	r.leader = r.id
	r.votes = make(map[NodeID]bool)
	r.heartbeatDeadline = r.now + Time(r.heartbeatInterval)

	r.prs = make(map[NodeID]*progress, len(r.cluster.Voters))
	for _, v := range r.cluster.Voters {
		r.prs[v] = &progress{next: r.log.LastIndex() + 1}
	}

	effs = append(effs, StateChanged{Role: r.role, Term: r.term, Leader: r.leader})

	noop := Entry{Index: r.log.LastIndex() + 1, Term: r.term, Type: EntryNoOp}
	r.log.Append(noop)
	// The durability barrier precedes every send below, satisfying obligation C:
	// this node counts itself in the quorum for the no-op, so it must have the
	// entry on disk before that count can influence anyone.
	effs = append(effs, PersistEntries{Entries: []Entry{noop}})
	r.prs[r.id].match = noop.Index
	r.prs[r.id].next = noop.Index + 1

	effs = r.maybeCommit(effs)
	// Replicate at once rather than waiting out the first heartbeat interval:
	// every follower is already counting down to its own election, and the gap
	// is pure exposure to a needless term bump.
	return r.broadcastAppend(effs)
}
