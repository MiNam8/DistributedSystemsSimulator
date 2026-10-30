package sim_test

import (
	"testing"

	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/sim"
)

// TestMembershipChurnUnderChaos points the fault injector at the subtlest
// correctness case in Raft.
//
// A configuration takes effect when it is *appended*, not when it is committed —
// the entry adding a node must be replicated under the configuration that
// includes it. The consequence is that an uncommitted configuration can be
// truncated away and must roll back, and two overlapping changes can produce
// disjoint majorities that each elect a leader.
//
// Those are precisely the failures that need a partition, a crash and a
// truncation to line up, which is to say the ones hand-written tests do not
// reach.
func TestMembershipChurnUnderChaos(t *testing.T) {
	var changes int
	for seed := uint64(1); seed <= 120; seed++ {
		c := mustNew(t, sim.MembershipOptions(5, seed))
		c.RunFor(20 * raft.Second)
		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d under chaos: %v\n\nchaos log:\n%v", seed, err, c.ChaosLog)
		}

		c.StopChaos()
		c.HealAll()
		c.RunFor(6 * raft.Second)
		c.StopWorkload()
		c.RunFor(10 * raft.Second)

		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d after heal: %v", seed, err)
		}
		changes += c.ConfChanges
	}
	if changes == 0 {
		t.Fatal("no membership change was ever accepted; this test exercised nothing")
	}
	t.Logf("%d membership changes accepted across 120 chaotic runs", changes)
}

// TestMembershipWithSnapshots combines the two hardest paths: a configuration
// that changed while the log was being compacted underneath it.
//
// A snapshot carries the membership, so it must carry the *committed* one — a
// snapshot that bakes in an uncommitted change makes it permanent, and a node
// restoring it has no earlier configuration to roll back to.
func TestMembershipWithSnapshots(t *testing.T) {
	for seed := uint64(1); seed <= 60; seed++ {
		opts := sim.MembershipOptions(5, seed)
		opts.SnapshotThreshold = 30
		c := mustNew(t, opts)

		c.RunFor(20 * raft.Second)
		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		c.StopChaos()
		c.HealAll()
		c.RunFor(6 * raft.Second)
		c.StopWorkload()
		c.RunFor(10 * raft.Second)

		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d after heal: %v", seed, err)
		}
	}
}
