package raft

import "slices"

// progress is the leader's view of one follower's log.
type progress struct {
	// next is the index of the next entry to send. It is a guess, refined by
	// rejections, and may run ahead of reality.
	next Index
	// match is the highest index known to be replicated on that follower. It is
	// a fact, only ever advanced by an acknowledgement, and it is what quorum
	// counting is allowed to use.
	match Index
}

// Propose submits a command to the replicated log. It returns [ErrNotLeader] if
// this node is not the leader; the caller should redirect using [Raft.Leader].
func (r *Raft) Propose(data []byte) ([]Effect, error) {
	if r.role != Leader {
		return nil, ErrNotLeader
	}
	e := Entry{Index: r.log.LastIndex() + 1, Term: r.term, Type: EntryNormal, Data: data}
	r.log.Append(e)

	// Barrier first: this node counts itself toward the commit quorum, so the
	// entry must be durable before that count can affect anyone (obligation C).
	effs := []Effect{PersistEntries{Entries: []Entry{e}}}
	r.prs[r.id].match = e.Index
	r.prs[r.id].next = e.Index + 1

	// Commit before broadcasting so the outgoing messages carry the newest
	// commit index — followers learn it a full round trip earlier. In a
	// single-node cluster this is also what commits the entry at all.
	effs = r.maybeCommit(effs)
	return r.broadcastAppend(effs), nil
}

// broadcastAppend replicates to every peer.
func (r *Raft) broadcastAppend(effs []Effect) []Effect {
	for _, peer := range r.peers() {
		effs = r.sendAppend(effs, peer)
	}
	return effs
}

// sendAppend sends the entries a follower is believed to be missing.
//
// Phase 2 resends everything from next on every heartbeat, which is correct
// (AppendEntries is idempotent) but wasteful. Flow control — bounding the
// in-flight window so a slow follower is not repeatedly sent the same megabytes
// — is Phase 8, where it is measured rather than guessed at.
func (r *Raft) sendAppend(effs []Effect, to NodeID) []Effect {
	pr := r.prs[to]
	prevIndex := pr.next - 1

	prevTerm, err := r.log.Term(prevIndex)
	if err != nil {
		// The follower is behind the compaction boundary and needs a snapshot.
		// InstallSnapshot is Phase 6; until then the leader simply cannot help
		// this follower, which is visible rather than silent because the
		// follower stays at its old match index.
		return effs
	}
	entries, err := r.log.Slice(pr.next, r.log.LastIndex()+1)
	if err != nil {
		return effs
	}

	return r.send(effs, Message{
		Type:         MsgApp,
		To:           to,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		Commit:       r.commitIndex,
	})
}

// handleApp is the follower's side of replication.
func (r *Raft) handleApp(effs []Effect, m Message) []Effect {
	// A candidate that hears from a legitimate leader in the same term has lost.
	if r.role != Follower {
		r.becomeFollower(m.Term, m.From)
	}
	r.leader = m.From
	r.resetElectionDeadline()

	// The consistency check (§5.3): accept only if this node holds the exact
	// entry the leader claims precedes the batch. Everything Raft guarantees
	// about log agreement follows inductively from this one test.
	if !r.log.Match(m.PrevLogIndex, m.PrevLogTerm) {
		return r.send(effs, r.rejectApp(m))
	}

	if len(m.Entries) > 0 {
		effs = r.appendFromLeader(effs, m.Entries)
	}

	// Advance commit, bounded by the last entry this message actually conveyed.
	//
	// Bounding by m.PrevLogIndex+len(m.Entries) rather than by the local log's
	// last index matters: a delayed message carrying a high commit index must
	// not mark entries committed that this node holds only speculatively from
	// some other leader.
	if last := m.PrevLogIndex + Index(len(m.Entries)); m.Commit > r.commitIndex {
		if c := min(m.Commit, last); c > r.commitIndex {
			r.commitIndex = c
			r.hsDirty = true
			effs = r.applyCommitted(effs)
		}
	}

	// MatchIndex is derived from what this message conveyed, never from the
	// local log's tail.
	//
	// Replying with LastIndex() looks equivalent and is a genuine bug: a bare
	// heartbeat (PrevLogIndex=4, no entries) arriving at a follower holding
	// speculative entries 5..20 from a deposed leader would report match=20, and
	// the new leader would count entries it does not even have toward a quorum.
	return r.send(effs, Message{
		Type:       MsgAppResp,
		To:         m.From,
		Reject:     false,
		MatchIndex: m.PrevLogIndex + Index(len(m.Entries)),
	})
}

// appendFromLeader splices the leader's entries into the log, truncating only
// where the terms genuinely disagree.
//
// The temptation is to truncate at PrevLogIndex+1 and append the whole batch.
// That is the most common way a from-scratch Raft loses committed data: an
// AppendEntries that was delayed or duplicated carries an older, shorter view of
// the log, and blind truncation discards entries a quorum has already
// acknowledged. Entries whose terms match are left exactly where they are.
func (r *Raft) appendFromLeader(effs []Effect, entries []Entry) []Effect {
	for i, e := range entries {
		existing, err := r.log.Term(e.Index)
		if err == nil && existing == e.Term {
			continue // already present and identical: leave it alone
		}
		if err == nil {
			// A genuine term conflict. Everything from here is wrong and must go.
			r.log.TruncateSuffix(e.Index)
			effs = append(effs, TruncateSuffix{FromIndex: e.Index})
		}
		rest := entries[i:]
		r.log.Append(rest...)
		return append(effs, PersistEntries{Entries: rest})
	}
	return effs // every entry was already present
}

// rejectApp builds a rejection carrying enough information for the leader to
// skip a whole term's worth of entries at once.
//
// Plain nextIndex-- converges in one round trip per *entry*, so a follower a
// million entries behind needs a million round trips. The hints below make it
// one round trip per *term*.
func (r *Raft) rejectApp(m Message) Message {
	out := Message{Type: MsgAppResp, To: m.From, Reject: true}

	if m.PrevLogIndex > r.log.LastIndex() {
		// Log too short: tell the leader exactly where it ends.
		out.RejectHintIndex = r.log.LastIndex() + 1
		out.RejectHintTerm = 0
		return out
	}

	conflict, err := r.log.Term(m.PrevLogIndex)
	if err != nil {
		// Below the compaction boundary; the leader must send a snapshot (Phase 6).
		out.RejectHintIndex = r.log.FirstIndex()
		out.RejectHintTerm = 0
		return out
	}

	// Walk back to the first index of the conflicting term, so the leader can
	// discard the entire term in one step.
	first := m.PrevLogIndex
	for first > r.log.FirstIndex() {
		t, err := r.log.Term(first - 1)
		if err != nil || t != conflict {
			break
		}
		first--
	}
	out.RejectHintTerm = conflict
	out.RejectHintIndex = first
	return out
}

// handleAppResp is the leader's side of replication.
func (r *Raft) handleAppResp(effs []Effect, m Message) []Effect {
	if r.role != Leader {
		return effs
	}
	pr, ok := r.prs[m.From]
	if !ok {
		return effs // a learner or a node no longer in the configuration
	}

	if m.Reject {
		r.backtrack(pr, m)
		return r.sendAppend(effs, m.From)
	}

	// match only ever moves forward. A delayed acknowledgement carrying an older,
	// lower index must not walk it backwards, or the leader could un-commit.
	if m.MatchIndex <= pr.match {
		return effs
	}
	pr.match = m.MatchIndex
	pr.next = pr.match + 1

	effs = r.maybeCommit(effs)
	// If this follower is still behind, keep it moving rather than waiting for
	// the next heartbeat.
	if pr.next <= r.log.LastIndex() {
		effs = r.sendAppend(effs, m.From)
	}
	return effs
}

// backtrack lowers next using the follower's rejection hints.
func (r *Raft) backtrack(pr *progress, m Message) {
	next := m.RejectHintIndex

	if m.RejectHintTerm > 0 {
		// If this leader also has entries in the conflicting term, resume just
		// past its last one: everything up to there already agrees.
		for i := r.log.LastIndex(); i >= r.log.FirstIndex(); i-- {
			t, err := r.log.Term(i)
			if err != nil {
				break
			}
			if t == m.RejectHintTerm {
				next = i + 1
				break
			}
			if t < m.RejectHintTerm {
				break // walked past the term entirely; the hint stands
			}
		}
	}

	// Never rewind past what is already known replicated, and never below 1. A
	// stale rejection arriving after progress was made would otherwise undo it.
	pr.next = max(next, pr.match+1, 1)
}

// maybeCommit advances the commit index if a quorum has caught up.
func (r *Raft) maybeCommit(effs []Effect) []Effect {
	if r.role != Leader {
		return effs
	}

	matches := make([]Index, 0, len(r.cluster.Voters))
	for _, v := range r.cluster.Voters { // sorted slice, never the map
		if pr, ok := r.prs[v]; ok {
			matches = append(matches, pr.match)
		} else {
			matches = append(matches, 0)
		}
	}
	slices.Sort(matches)
	// With matches ascending, the highest index present on a majority sits at
	// len-quorum: every element from there up is at least that large.
	n := matches[len(matches)-r.cluster.Quorum()]

	if n <= r.commitIndex {
		return effs
	}

	// §5.4.2 — the Figure-8 rule, and the single most commonly botched line in
	// Raft implementations.
	//
	// A leader may NOT commit an entry from an earlier term merely because a
	// majority stores it. Such an entry can still be overwritten by a future
	// leader that never saw it. Only committing an entry from the *current*
	// term makes the whole prefix safe, which is what the no-op appended on
	// election guarantees will eventually happen.
	if term, err := r.log.Term(n); err != nil || term != r.term {
		return effs
	}

	r.commitIndex = n
	r.hsDirty = true
	return r.applyCommitted(effs)
}

// applyCommitted emits the entries newly eligible for the state machine.
//
// Entries are contiguous, strictly ordered, never above the commit index, and
// never emitted twice — the guarantees ApplyEntries documents.
func (r *Raft) applyCommitted(effs []Effect) []Effect {
	// After a snapshot install the applied index can sit below the compaction
	// boundary; the snapshot already covers that prefix.
	if b := r.log.FirstIndex() - 1; r.appliedIndex < b {
		r.appliedIndex = b
	}
	if r.commitIndex <= r.appliedIndex {
		return effs
	}
	entries, err := r.log.Slice(r.appliedIndex+1, r.commitIndex+1)
	if err != nil || len(entries) == 0 {
		return effs
	}
	r.appliedIndex = r.commitIndex
	return append(effs, ApplyEntries{Entries: entries})
}
