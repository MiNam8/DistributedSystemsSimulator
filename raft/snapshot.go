package raft

// Snapshot records that the state machine has been captured through index, and
// permits the log prefix up to there to be discarded.
//
// The caller supplies the serialised state; the core forwards it in a
// [PersistSnapshot] effect and does not retain it. index must have been applied
// — snapshotting state the state machine has not reached would produce an image
// of a moment that never existed.
func (r *Raft) Snapshot(index Index, data []byte) ([]Effect, error) {
	switch {
	case index <= r.log.FirstIndex()-1:
		return nil, ErrCompacted // already covered by an earlier snapshot
	case index > r.appliedIndex:
		return nil, ErrUnavailable
	}
	term, err := r.log.Term(index)
	if err != nil {
		return nil, err
	}

	// Membership travels with the snapshot, but it must be the *committed*
	// configuration, not the current one.
	//
	// A configuration takes effect on append, so r.cluster can already include a
	// change that is not committed and may yet be truncated away. Baking that
	// into a snapshot makes it permanent and unrevocable: a node restoring it has
	// no earlier configuration to roll back to, and goes on counting quorums
	// against a membership that no log contains.
	cfg := r.cluster
	if r.pendingConfIndex > r.commitIndex {
		cfg = r.prevConfig
	}
	meta := SnapshotMeta{
		LastIncludedIndex: index,
		LastIncludedTerm:  term,
		Config:            cfg,
	}
	r.log.Compact(index, term)

	return []Effect{
		PersistSnapshot{Meta: meta, Data: data},
		CompactLog{UpToIndex: index},
	}, nil
}

// handleSnap installs a snapshot sent by the leader.
func (r *Raft) handleSnap(effs []Effect, m Message) []Effect {
	if m.Snapshot == nil {
		return effs
	}
	meta := *m.Snapshot

	// Recognise the leader and stop campaigning, exactly as for MsgApp: a
	// snapshot is evidence of an active leader.
	if r.role != Follower {
		r.becomeFollower(m.Term, m.From)
	}
	r.leader = m.From
	r.lastLeaderContact = r.now
	r.resetElectionDeadline()

	// A stale snapshot must be ignored rather than installed. Snapshots can
	// arrive late — they are large, streamed out of band, and overtaken by
	// ordinary replication — and installing one that predates this node's commit
	// index would roll the state machine *backwards*, discarding entries that
	// were already applied and acknowledged.
	if meta.LastIncludedIndex <= r.commitIndex {
		return r.send(effs, snapResp(m.From, meta.LastIncludedIndex))
	}

	// If this node already holds the entry at the snapshot boundary with a
	// matching term, its log is a superset of the snapshot: everything below is
	// identical by the Log Matching Property. Fast-forward the commit index
	// instead of wiping a state machine that is already correct — restoring
	// would be safe but would throw away entries the node would then have to
	// fetch again.
	if r.log.Match(meta.LastIncludedIndex, meta.LastIncludedTerm) {
		r.commitIndex = meta.LastIncludedIndex
		r.hsDirty = true
		effs = r.applyCommitted(effs)
		return r.send(effs, snapResp(m.From, meta.LastIncludedIndex))
	}

	// Full install: discard the entire log (§7), not just the prefix.
	//
	// Compacting instead would retain whatever this node held above the
	// boundary. Those entries came from a history that demonstrably diverges
	// here — that is why the Match check above failed — so keeping them grafts a
	// divergent suffix onto a prefix it never followed. If this node later wins
	// an election it replicates that fabrication over entries a quorum
	// committed, and a committed entry is lost.
	r.log.Reset(meta.LastIncludedIndex, meta.LastIncludedTerm)
	r.commitIndex = meta.LastIncludedIndex
	r.appliedIndex = meta.LastIncludedIndex
	r.cluster = meta.Config
	// The snapshot's configuration supersedes any uncommitted change: it is
	// committed state by construction, so there is nothing left to roll back to.
	r.prevConfig, r.pendingConfIndex = meta.Config, 0
	r.syncProgress()
	r.hsDirty = true

	// Order matters and is enforced by the list: the snapshot is durable before
	// the state machine is replaced, and both happen before the acknowledgement
	// leaves the process.
	// Storage must mirror the Reset above, and that takes *two* truncations.
	// CompactLog alone only drops the prefix, leaving durable storage holding
	// the divergent tail the core just discarded — after which the core's next
	// append lands at boundary+1 while storage still ends at the old tail, and
	// the two silently disagree about what this node has on disk.
	effs = append(effs,
		PersistSnapshot{Meta: meta, Data: m.SnapshotData},
		TruncateSuffix{FromIndex: meta.LastIncludedIndex + 1},
		RestoreStateMachine{Meta: meta, Data: m.SnapshotData},
		CompactLog{UpToIndex: meta.LastIncludedIndex},
	)
	return r.send(effs, snapResp(m.From, meta.LastIncludedIndex))
}

// snapResp acknowledges a snapshot, reporting the boundary the exchange
// established rather than this node's log tail.
//
// This is the same rule as MsgAppResp, and getting it wrong here is the same
// bug: a follower holding uncommitted entries from a deposed leader would
// otherwise report holding them, the leader would count them toward a commit
// quorum no node actually formed, and an entry could be reported committed that
// a majority never stored. The tail above the boundary is not what a snapshot
// acknowledges — it is exactly what the leader must still discover by
// replication.
func snapResp(to NodeID, boundary Index) Message {
	return Message{Type: MsgSnapResp, To: to, MatchIndex: boundary}
}

// handleSnapResp updates a follower's progress after a snapshot install.
func (r *Raft) handleSnapResp(effs []Effect, m Message) []Effect {
	if r.role != Leader {
		return effs
	}
	pr, ok := r.prs[m.From]
	if !ok {
		return effs
	}
	pr.pendingSnapshot = 0

	if m.MatchIndex > pr.match {
		pr.match = m.MatchIndex
		pr.next = pr.match + 1
		effs = r.maybeCommit(effs)
	}
	// Resume ordinary replication from wherever the follower now stands.
	if pr.next <= r.log.LastIndex() {
		effs = r.sendAppend(effs, m.From)
	}
	return effs
}

// sendSnapshot tells the runtime to stream this node's latest snapshot to a
// follower that has fallen behind the compaction boundary.
//
// The message carries metadata only; the runtime loads the payload from storage.
func (r *Raft) sendSnapshot(effs []Effect, to NodeID) []Effect {
	pr := r.prs[to]
	index := r.log.FirstIndex() - 1
	term, err := r.log.Term(index)
	if err != nil {
		return effs
	}
	// One snapshot in flight per follower. Without this the leader would resend
	// the whole image on every heartbeat while the first transfer was still
	// running, and a slow follower would saturate the link that its own recovery
	// depends on.
	if pr.pendingSnapshot == index {
		return effs
	}
	pr.pendingSnapshot = index

	meta := SnapshotMeta{LastIncludedIndex: index, LastIncludedTerm: term, Config: r.cluster}
	return r.send(effs, Message{Type: MsgSnap, To: to, Snapshot: &meta})
}
