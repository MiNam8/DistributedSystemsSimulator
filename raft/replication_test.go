package raft_test

import (
	"testing"

	"github.com/MiNam8/kvstore/raft"
)

func node(t *testing.T, id raft.NodeID, voters int, log *raft.Log, hs raft.HardState) *raft.Raft {
	t.Helper()
	var ids []raft.NodeID
	for i := 1; i <= voters; i++ {
		ids = append(ids, raft.NodeID(i))
	}
	r, err := raft.New(raft.Options{
		ID:                id,
		Cluster:           raft.Config{Voters: ids},
		ElectionTimeout:   300 * raft.Millisecond,
		HeartbeatInterval: 50 * raft.Millisecond,
		Rand:              raft.NewRand(uint64(id)),
		Log:               log,
		HardState:         hs,
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// logOf builds a log whose entry at index i+1 has terms[i].
func logOf(terms ...raft.Term) *raft.Log {
	l := raft.NewLog()
	for i, tm := range terms {
		l.Append(raft.Entry{Index: raft.Index(i + 1), Term: tm, Type: raft.EntryNormal})
	}
	return l
}

func findMsg(effs []raft.Effect, typ raft.MessageType) (raft.Message, bool) {
	for _, e := range effs {
		if sm, ok := e.(raft.SendMessage); ok && sm.Message.Type == typ {
			return sm.Message, true
		}
	}
	return raft.Message{}, false
}

func hasEffect[T raft.Effect](effs []raft.Effect) bool {
	for _, e := range effs {
		if _, ok := e.(T); ok {
			return true
		}
	}
	return false
}

// TestDelayedAppendDoesNotTruncateMatchingEntries is the blind-truncation
// regression test.
//
// Truncating at PrevLogIndex+1 on every accepted AppendEntries looks correct and
// passes every happy-path test. It is the most common way a from-scratch Raft
// loses committed data: a delayed or duplicated message carrying an older,
// shorter view of the log discards entries a quorum already acknowledged.
func TestDelayedAppendDoesNotTruncateMatchingEntries(t *testing.T) {
	f := node(t, 2, 3, logOf(1, 1, 1), raft.HardState{Term: 1})

	// A stale duplicate of an old message: it re-sends entry 1, which the
	// follower already holds identically, while the follower has since accepted
	// entries 2 and 3.
	effs := f.Step(raft.Message{
		Type: raft.MsgApp, From: 1, To: 2, Term: 1,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []raft.Entry{{Index: 1, Term: 1, Type: raft.EntryNormal}},
	})

	if got := f.Log().LastIndex(); got != 3 {
		t.Fatalf("log truncated to %d by a stale duplicate; entries 2 and 3 were destroyed "+
			"even though their terms matched. Truncate only on a genuine term conflict.", got)
	}
	if hasEffect[raft.TruncateSuffix](effs) {
		t.Error("a TruncateSuffix effect was emitted for a message that conflicted with nothing")
	}
}

// TestMatchIndexReflectsMessageNotLocalLog guards the acknowledgement.
//
// Replying with the local log's last index looks equivalent to replying with
// what the message conveyed, and is a real bug: a follower holding speculative
// entries from a deposed leader would report holding entries the *current*
// leader has never seen, and the leader would count them toward a quorum.
func TestMatchIndexReflectsMessageNotLocalLog(t *testing.T) {
	// The follower holds 20 entries, 5..20 speculative from a deposed leader.
	f := node(t, 2, 3, logOf(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1), raft.HardState{Term: 1})

	// A bare heartbeat from the new leader, which knows only about entries 1..4.
	effs := f.Step(raft.Message{
		Type: raft.MsgApp, From: 1, To: 2, Term: 2,
		PrevLogIndex: 4, PrevLogTerm: 1,
	})

	resp, ok := findMsg(effs, raft.MsgAppResp)
	if !ok {
		t.Fatal("no MsgAppResp emitted")
	}
	if resp.Reject {
		t.Fatal("heartbeat was rejected despite a matching prefix")
	}
	if resp.MatchIndex != 4 {
		t.Errorf("MatchIndex = %d, want 4.\n"+
			"The follower reported its local log tail rather than what this message conveyed, "+
			"so the leader would count 16 entries it has never seen toward a commit quorum.",
			resp.MatchIndex)
	}
}

// TestConflictingEntriesAreTruncated is the other half: a genuine term conflict
// must discard the divergent suffix.
func TestConflictingEntriesAreTruncated(t *testing.T) {
	f := node(t, 2, 3, logOf(1, 2, 2), raft.HardState{Term: 2})

	effs := f.Step(raft.Message{
		Type: raft.MsgApp, From: 1, To: 2, Term: 3,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []raft.Entry{{Index: 2, Term: 3, Type: raft.EntryNormal}},
	})

	if !hasEffect[raft.TruncateSuffix](effs) {
		t.Error("a genuine term conflict emitted no TruncateSuffix effect")
	}
	if got := f.Log().LastIndex(); got != 2 {
		t.Errorf("LastIndex = %d, want 2 — the divergent suffix was not discarded", got)
	}
	if tm, _ := f.Log().Term(2); tm != 3 {
		t.Errorf("Term(2) = %d, want 3 — the leader's entry did not replace the conflict", tm)
	}
}

func TestConsistencyCheckRejectsGap(t *testing.T) {
	f := node(t, 2, 3, logOf(1, 1), raft.HardState{Term: 1})

	effs := f.Step(raft.Message{
		Type: raft.MsgApp, From: 1, To: 2, Term: 1,
		PrevLogIndex: 9, PrevLogTerm: 1, // far beyond this follower's log
		Entries: []raft.Entry{{Index: 10, Term: 1}},
	})

	resp, ok := findMsg(effs, raft.MsgAppResp)
	if !ok || !resp.Reject {
		t.Fatal("an AppendEntries leaving a gap must be rejected")
	}
	if resp.RejectHintIndex != 3 {
		t.Errorf("RejectHintIndex = %d, want 3 (one past this log's end)", resp.RejectHintIndex)
	}
	if f.Log().LastIndex() != 2 {
		t.Errorf("a rejected message still modified the log (LastIndex = %d)", f.Log().LastIndex())
	}
}

// TestFollowerCommitIsBoundedByTheMessage prevents a follower from marking
// entries committed that the sending leader never claimed to have.
func TestFollowerCommitIsBoundedByTheMessage(t *testing.T) {
	f := node(t, 2, 3, logOf(1, 1, 1, 1, 1), raft.HardState{Term: 1})

	// A heartbeat about the first two entries, but carrying a high commit index.
	f.Step(raft.Message{
		Type: raft.MsgApp, From: 1, To: 2, Term: 1,
		PrevLogIndex: 2, PrevLogTerm: 1,
		Commit: 5,
	})

	if got := f.CommitIndex(); got != 2 {
		t.Errorf("commitIndex = %d, want 2 — commit must be bounded by what the message conveyed, "+
			"not by what this node happens to hold speculatively", got)
	}
}

// elect drives r to leadership by ticking it into candidacy and granting a quorum.
func elect(t *testing.T, r *raft.Raft, voters int) []raft.Effect {
	t.Helper()
	tick := raft.Time(0)
	for r.Role() != raft.Candidate {
		tick += raft.Time(10 * raft.Millisecond)
		r.Tick(tick)
		if tick > raft.Time(10*raft.Second) {
			t.Fatal("node never became a candidate")
		}
	}
	var effs []raft.Effect
	for id := 2; id <= voters/2+1; id++ {
		effs = r.Step(raft.Message{
			Type: raft.MsgVoteResp, From: raft.NodeID(id), To: r.ID(), Term: r.Term(),
		})
	}
	if r.Role() != raft.Leader {
		t.Fatalf("expected leader after a quorum of votes, got %v", r.Role())
	}
	return effs
}

// TestFastBacktrackingSkipsWholeTerms measures convergence in round trips.
//
// Plain nextIndex-- costs one round trip per *entry*, so a follower a million
// entries behind needs a million round trips. The conflict hints reduce it to
// one per *term*. This test asserts the difference is real rather than assuming it.
func TestFastBacktrackingSkipsWholeTerms(t *testing.T) {
	// The leader's log: 30 entries across terms 1, 2 and 3.
	var leaderTerms []raft.Term
	for i := 0; i < 30; i++ {
		leaderTerms = append(leaderTerms, raft.Term(i/10+1))
	}
	l := node(t, 1, 3, logOf(leaderTerms...), raft.HardState{Term: 3})
	effs := elect(t, l, 3)

	// The follower diverged early: 25 entries, all from a stale term 1.
	var followerTerms []raft.Term
	for i := 0; i < 25; i++ {
		followerTerms = append(followerTerms, 1)
	}
	f := node(t, 2, 3, logOf(followerTerms...), raft.HardState{Term: 1})

	rounds := 0
	for rounds < 40 {
		app, ok := findMsg(effs, raft.MsgApp)
		if !ok {
			break
		}
		rounds++
		resp, ok := findMsg(f.Step(app), raft.MsgAppResp)
		if !ok {
			t.Fatal("follower produced no response")
		}
		effs = l.Step(resp)
		if !resp.Reject {
			break
		}
	}

	// The follower must end up holding exactly the leader's log.
	if got, want := f.Log().LastIndex(), l.Log().LastIndex(); got != want {
		t.Errorf("follower converged to LastIndex %d, want %d", got, want)
	}
	for i := raft.Index(1); i <= l.Log().LastIndex(); i++ {
		lt, _ := l.Log().Term(i)
		ft, err := f.Log().Term(i)
		if err != nil || lt != ft {
			t.Fatalf("logs diverge at index %d: leader term %d, follower term %d (%v)", i, lt, ft, err)
		}
	}

	// Naive backtracking would need ~25 round trips to walk back entry by entry.
	if rounds > 6 {
		t.Errorf("converged in %d round trips; term hints should need roughly one per term, "+
			"not one per entry", rounds)
	}
	t.Logf("converged in %d round trips (naive nextIndex-- would need ~25)", rounds)
}
