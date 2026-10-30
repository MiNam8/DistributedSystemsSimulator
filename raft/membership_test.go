package raft_test

import (
	"errors"
	"testing"

	"github.com/MiNam8/kvstore/raft"
)

// leaderOf elects node 1 in a cluster of the given voters and learners.
func leaderOf(t *testing.T, voters []raft.NodeID, learners []raft.NodeID) *raft.Raft {
	t.Helper()
	r, err := raft.New(raft.Options{
		ID:                1,
		Cluster:           raft.Config{Voters: voters, Learners: learners},
		ElectionTimeout:   300 * raft.Millisecond,
		HeartbeatInterval: 50 * raft.Millisecond,
		Rand:              raft.NewRand(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	elect(t, r, len(voters))
	// Commit the term's no-op, which every configuration change depends on.
	for _, id := range voters[1:] {
		r.Step(raft.Message{
			Type: raft.MsgAppResp, From: id, To: 1, Term: r.Term(),
			MatchIndex: r.Log().LastIndex(),
		})
	}
	return r
}

func TestConfChangeCodecRoundTrip(t *testing.T) {
	for _, want := range []raft.ConfChange{
		{Type: raft.ConfChangeAddLearner, NodeID: 4},
		{Type: raft.ConfChangeRemoveNode, NodeID: 0xFFFFFFFFFFFFFFFF},
		{Type: raft.ConfChangePromoteLearner, NodeID: 1},
	} {
		got, err := raft.DecodeConfChange(want.Encode())
		if err != nil || got != want {
			t.Errorf("round trip: got %+v (%v), want %+v", got, err, want)
		}
	}
	if _, err := raft.DecodeConfChange([]byte{1, 2, 3}); err == nil {
		t.Error("a malformed change decoded without error")
	}
}

// TestOnlyOneConfChangeInFlight guards the first half of the §4 safety fix.
//
// Overlapping single-server changes can produce two disjoint majorities that
// each elect a leader. Serialising them is what keeps every new configuration
// an increment of one the cluster has agreed on.
func TestOnlyOneConfChangeInFlight(t *testing.T) {
	r := leaderOf(t, []raft.NodeID{1, 2, 3}, nil)

	if _, err := r.ProposeConfChange(raft.ConfChange{Type: raft.ConfChangeAddLearner, NodeID: 4}); err != nil {
		t.Fatalf("first change: %v", err)
	}
	_, err := r.ProposeConfChange(raft.ConfChange{Type: raft.ConfChangeAddLearner, NodeID: 5})
	if !errors.Is(err, raft.ErrConfChangeInFlight) {
		t.Errorf("second change while the first is uncommitted: err = %v, want ErrConfChangeInFlight", err)
	}
}

// TestConfChangeRequiresACommittedCurrentTermEntry guards the second half.
//
// A leader that has not yet committed anything of its own term does not know
// which configuration is actually agreed, so building a change on top of it can
// produce exactly the overlap the first guard prevents.
func TestConfChangeRequiresACommittedCurrentTermEntry(t *testing.T) {
	r, err := raft.New(raft.Options{
		ID:                1,
		Cluster:           raft.Config{Voters: []raft.NodeID{1, 2, 3}},
		ElectionTimeout:   300 * raft.Millisecond,
		HeartbeatInterval: 50 * raft.Millisecond,
		Rand:              raft.NewRand(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	elect(t, r, 3) // leader, but its no-op is not yet committed

	if _, err := r.ProposeConfChange(raft.ConfChange{Type: raft.ConfChangeAddLearner, NodeID: 4}); !errors.Is(err, raft.ErrConfChangeInFlight) {
		t.Errorf("change before committing a current-term entry: err = %v, want ErrConfChangeInFlight", err)
	}
}

// TestConfChangeTakesEffectOnAppend checks the counter-intuitive rule: the
// configuration changes before the entry is committed, because the entry adding
// a node must be replicated under the configuration that includes it.
func TestConfChangeTakesEffectOnAppend(t *testing.T) {
	r := leaderOf(t, []raft.NodeID{1, 2, 3}, nil)
	before := r.CommitIndex()

	if _, err := r.ProposeConfChange(raft.ConfChange{Type: raft.ConfChangeAddLearner, NodeID: 4}); err != nil {
		t.Fatal(err)
	}
	if r.CommitIndex() != before {
		t.Fatalf("setup: the change committed immediately (%d -> %d); this test needs it uncommitted",
			before, r.CommitIndex())
	}
	if !containsNode(r.Config().Learners, 4) {
		t.Error("the configuration did not take effect on append; the new node could not be " +
			"counted toward the quorum that commits its own addition")
	}
}

// TestConfChangeRollsBackWhenTruncated is the part applying-on-append makes
// necessary, and the part most implementations forget.
//
// A follower adopts a configuration from an uncommitted entry. A new leader
// overwrites that entry. The follower must revert — otherwise it counts quorums
// against a membership that exists in no log anywhere.
func TestConfChangeRollsBackWhenTruncated(t *testing.T) {
	f := node(t, 2, 3, logOf(1), raft.HardState{Term: 1})

	// A configuration entry arrives at index 2 and takes effect.
	cc := raft.ConfChange{Type: raft.ConfChangeAddVoter, NodeID: 9}
	f.Step(raft.Message{
		Type: raft.MsgApp, From: 1, To: 2, Term: 1,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []raft.Entry{{Index: 2, Term: 1, Type: raft.EntryConfChange, Data: cc.Encode()}},
	})
	if !containsNode(f.Config().Voters, 9) {
		t.Fatal("setup: the configuration entry did not take effect on append")
	}

	// A new leader overwrites index 2 with an ordinary entry of a higher term.
	f.Step(raft.Message{
		Type: raft.MsgApp, From: 3, To: 2, Term: 2,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []raft.Entry{{Index: 2, Term: 2, Type: raft.EntryNormal, Data: []byte("x")}},
	})

	if containsNode(f.Config().Voters, 9) {
		t.Error("the configuration survived truncation of the entry that introduced it; " +
			"this node now counts quorums against a membership no log contains")
	}
}

// TestLearnersDoNotCountTowardQuorum is the property that makes learners worth
// having: a new node can replay the whole log without ever putting availability
// at risk.
func TestLearnersDoNotCountTowardQuorum(t *testing.T) {
	cfg := raft.Config{Voters: []raft.NodeID{1, 2, 3}, Learners: []raft.NodeID{4, 5}}
	if got := cfg.Quorum(); got != 2 {
		t.Errorf("quorum with 3 voters and 2 learners = %d, want 2", got)
	}
	if cfg.IsVoter(4) {
		t.Error("a learner was reported as a voter")
	}

	// A learner's vote must never be counted, or a leader could be elected by a
	// majority that no majority of voters endorsed.
	r := leaderOf(t, []raft.NodeID{1, 2, 3}, []raft.NodeID{4, 5})
	if r.Role() != raft.Leader {
		t.Fatal("setup: not leader")
	}
}

// TestLeaderStepsDownAfterRemovingItself checks the ordering: the leader must
// keep serving until the change is committed — the entry removing it is
// committed by the *old* configuration, which still includes it — and resign
// immediately afterwards.
func TestLeaderStepsDownAfterRemovingItself(t *testing.T) {
	r := leaderOf(t, []raft.NodeID{1, 2, 3}, nil)

	if _, err := r.ProposeConfChange(raft.ConfChange{Type: raft.ConfChangeRemoveNode, NodeID: 1}); err != nil {
		t.Fatal(err)
	}
	if r.Role() != raft.Leader {
		t.Error("the leader resigned before its own removal was committed; nothing would then " +
			"be able to commit it")
	}

	// The remaining voters acknowledge, committing the removal.
	idx := r.Log().LastIndex()
	for _, id := range []raft.NodeID{2, 3} {
		r.Step(raft.Message{Type: raft.MsgAppResp, From: id, To: 1, Term: r.Term(), MatchIndex: idx})
	}
	if r.Role() == raft.Leader {
		t.Error("the leader kept leading after its removal was committed")
	}
}

func containsNode(s []raft.NodeID, id raft.NodeID) bool {
	for _, v := range s {
		if v == id {
			return true
		}
	}
	return false
}
