package raft

import "testing"

// leaderWithLog builds a leader in the given term holding the given entry
// terms, bypassing an election so the commit rule can be tested in isolation.
func leaderWithLog(t *testing.T, voters int, term Term, entryTerms ...Term) *Raft {
	t.Helper()
	var ids []NodeID
	for i := 1; i <= voters; i++ {
		ids = append(ids, NodeID(i))
	}
	r, err := New(Options{
		ID:                1,
		Cluster:           Config{Voters: ids},
		ElectionTimeout:   300 * Millisecond,
		HeartbeatInterval: 50 * Millisecond,
		Rand:              NewRand(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, et := range entryTerms {
		r.log.Append(Entry{Index: Index(i + 1), Term: et, Type: EntryNormal})
	}
	r.term = term
	r.role = Leader
	r.leader = r.id
	r.prs = make(map[NodeID]*progress, voters)
	for _, id := range ids {
		r.prs[id] = &progress{next: r.log.LastIndex() + 1}
	}
	return r
}

// replicatedOn marks index as present on the first n voters.
func (r *Raft) replicatedOn(n int, index Index) {
	for i := 0; i < n; i++ {
		pr := r.prs[r.cluster.Voters[i]]
		pr.match = index
		pr.next = index + 1
	}
}

// TestLeaderDoesNotCommitPreviousTermByCounting is the Figure-8 regression test.
//
// This is the single most commonly botched rule in Raft (§5.4.2). A leader that
// commits an entry from an *earlier* term merely because a majority stores it
// can have that entry overwritten by a future leader that never saw it — the
// entry is reported committed to a client and then vanishes.
//
// Deleting the `term != r.term` guard in maybeCommit makes this test fail, which
// is the point: without it, everything else still passes.
func TestLeaderDoesNotCommitPreviousTermByCounting(t *testing.T) {
	// Five nodes. The log holds index 1 (term 1) and index 2 (term 2), both
	// inherited. This node has just won the election for term 4.
	r := leaderWithLog(t, 5, 4, 1, 2)

	// Three of five — a clear majority — now store index 2.
	r.replicatedOn(3, 2)
	r.maybeCommit(nil)

	if r.commitIndex != 0 {
		t.Fatalf("commitIndex = %d, want 0.\n"+
			"An entry from term 2 was committed by replica count alone while the leader is in "+
			"term 4. A future leader that never saw it can still overwrite it (Raft Figure 8), "+
			"so a client was told a write was durable that can still be lost.", r.commitIndex)
	}

	// Now the leader appends an entry of its *own* term and replicates it.
	// Committing that entry retroactively makes the whole prefix safe.
	r.log.Append(Entry{Index: 3, Term: 4, Type: EntryNoOp})
	r.replicatedOn(3, 3)
	r.maybeCommit(nil)

	if r.commitIndex != 3 {
		t.Errorf("commitIndex = %d, want 3 — committing a current-term entry must carry the prefix with it", r.commitIndex)
	}
}

// TestCommitRequiresQuorumNotJustAny guards the quorum arithmetic itself.
func TestCommitRequiresQuorumNotJustAny(t *testing.T) {
	r := leaderWithLog(t, 5, 1, 1, 1, 1)

	r.replicatedOn(2, 3) // 2 of 5: short of the 3 needed
	r.maybeCommit(nil)
	if r.commitIndex != 0 {
		t.Errorf("commitIndex = %d after 2/5 replication, want 0", r.commitIndex)
	}

	r.replicatedOn(3, 3) // 3 of 5: quorum
	r.maybeCommit(nil)
	if r.commitIndex != 3 {
		t.Errorf("commitIndex = %d after 3/5 replication, want 3", r.commitIndex)
	}
}

// TestCommitIndexNeverRegresses guards against a stale acknowledgement walking
// the commit index backwards, which would let an entry be un-committed after a
// client was told it was durable.
func TestCommitIndexNeverRegresses(t *testing.T) {
	r := leaderWithLog(t, 3, 1, 1, 1, 1)
	r.replicatedOn(3, 3)
	r.maybeCommit(nil)
	if r.commitIndex != 3 {
		t.Fatalf("setup: commitIndex = %d, want 3", r.commitIndex)
	}

	// A delayed MsgAppResp reporting an older, lower match index.
	r.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, MatchIndex: 1})
	if r.commitIndex != 3 {
		t.Errorf("commitIndex regressed to %d after a stale acknowledgement", r.commitIndex)
	}
	if got := r.prs[2].match; got != 3 {
		t.Errorf("match for node 2 regressed to %d, want 3", got)
	}
}

// TestApplyEmitsEachEntryExactlyOnce guards the ApplyEntries contract: entries
// are contiguous, ordered, and never delivered twice. A state machine that sees
// a duplicate has silently diverged from its peers.
func TestApplyEmitsEachEntryExactlyOnce(t *testing.T) {
	r := leaderWithLog(t, 3, 1, 1, 1, 1)

	seen := map[Index]int{}
	collect := func(effs []Effect) {
		for _, e := range effs {
			if ap, ok := e.(ApplyEntries); ok {
				var want Index
				for i, ent := range ap.Entries {
					if i == 0 {
						want = ent.Index
					}
					if ent.Index != want {
						t.Errorf("ApplyEntries not contiguous: got index %d, want %d", ent.Index, want)
					}
					want++
					seen[ent.Index]++
				}
			}
		}
	}

	r.replicatedOn(2, 2)
	collect(r.maybeCommit(nil))
	r.replicatedOn(2, 3)
	collect(r.maybeCommit(nil))
	// A repeat call with nothing new must emit nothing at all.
	collect(r.maybeCommit(nil))

	for i := Index(1); i <= 3; i++ {
		if seen[i] != 1 {
			t.Errorf("entry %d applied %d times, want exactly 1", i, seen[i])
		}
	}
}
