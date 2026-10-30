package sim_test

import (
	"testing"

	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/sim"
)

// TestChaosPreservesSafety is the Phase 3 headline: under continuous packet
// loss, duplication, reordering, partitions, crashes, restarts, stalls and clock
// drift, every safety invariant must hold at every instant.
//
// Liveness is deliberately not asserted here. A partitioned Raft cluster is
// *supposed* to stop making progress, so demanding progress during chaos would
// be demanding a bug.
func TestChaosPreservesSafety(t *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		c := mustNew(t, sim.ChaosOptions(5, seed))
		c.RunFor(20 * raft.Second)

		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d: %v\n\nReproduce: go run ./cmd/dst -chaos -seed=%d\n\nchaos log:\n%v",
				seed, err, seed, c.ChaosLog)
		}
	}
}

// TestChaosThenHealConverges checks liveness where it is legitimate to: once the
// network is repaired and every node is back, the cluster must resume and every
// replica must agree.
func TestChaosThenHealConverges(t *testing.T) {
	healed := 0
	for seed := uint64(1); seed <= 50; seed++ {
		c := mustNew(t, sim.ChaosOptions(5, seed))
		c.RunFor(15 * raft.Second)

		// Retire the adversary before asserting progress. Healing alone is not
		// enough: the chaos driver would break the cluster again on its next roll.
		c.StopChaos()
		c.HealAll()
		c.RunFor(5 * raft.Second)
		c.StopWorkload()
		c.RunFor(10 * raft.Second)

		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d after heal: %v", seed, err)
		}
		if len(c.Leaders()) != 1 {
			t.Fatalf("seed %d: %d leaders after healing; the cluster did not recover",
				seed, len(c.Leaders()))
		}
		requireConvergence(t, c, c.IDs())
		healed++
	}
	t.Logf("%d chaotic runs healed and converged", healed)
}

// TestMinorityPartitionCannotCommit is the property that makes Raft worth using:
// a leader cut off from a majority must stop committing rather than serve a
// divergent view.
func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := mustNew(t, sim.DefaultOptions(5, 5))
	c.RunFor(2 * raft.Second)

	leader, _ := soleLeader(t, c)

	// Isolate the leader with one companion: 2 of 5 is short of a quorum.
	var minority, majority []raft.NodeID
	minority = append(minority, leader)
	for _, id := range c.IDs() {
		if id == leader {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	c.Partition(minority, majority)

	before := c.Node(leader).CommitIndex()
	c.RunFor(5 * raft.Second)
	after := c.Node(leader).CommitIndex()

	if after != before {
		t.Errorf("isolated leader %d advanced its commit index from %d to %d with only %d of %d nodes",
			leader, before, after, len(minority), len(c.IDs()))
	}
	// The majority side must elect its own leader and carry on.
	var majLeader bool
	for _, id := range majority {
		if c.Node(id).Role() == raft.Leader {
			majLeader = true
		}
	}
	if !majLeader {
		t.Error("the majority partition never elected a leader")
	}
	if err := c.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

// TestOneWayPartitionIsSafe covers the asymmetric case: a node that can send but
// not receive looks alive to itself while being invisible to everyone else.
func TestOneWayPartitionIsSafe(t *testing.T) {
	for seed := uint64(1); seed <= 30; seed++ {
		c := mustNew(t, sim.DefaultOptions(5, seed))
		c.RunFor(2 * raft.Second)

		leader, _ := soleLeader(t, c)
		// Everyone can still hear the leader, but the leader hears no one — so
		// it keeps asserting authority while learning nothing.
		for _, id := range c.IDs() {
			if id != leader {
				c.PartitionOneWay(id, leader)
			}
		}
		c.RunFor(5 * raft.Second)
		c.HealAll()
		c.RunFor(5 * raft.Second)
		c.StopWorkload()
		c.RunFor(5 * raft.Second)

		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}

// TestStalledNodeIsSafe freezes nodes for longer than an election timeout, the
// way a garbage collection pause or a SIGSTOP does, and requires safety to hold
// when they wake into a burst of stale buffered messages.
func TestStalledNodeIsSafe(t *testing.T) {
	for seed := uint64(1); seed <= 30; seed++ {
		c := mustNew(t, sim.DefaultOptions(5, seed))
		c.RunFor(2 * raft.Second)

		leader, _ := soleLeader(t, c)
		c.Stall(leader, 2*raft.Second) // far beyond the 300ms election timeout
		c.RunFor(4 * raft.Second)

		c.StopWorkload()
		c.RunFor(5 * raft.Second)
		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
	}
}

// TestRestartRecoversFromDurableStateAlone verifies the crash is real: every
// volatile structure is discarded and the node is rebuilt from storage only.
func TestRestartRecoversFromDurableStateAlone(t *testing.T) {
	c := mustNew(t, sim.DefaultOptions(5, 17))
	c.RunFor(3 * raft.Second)

	victim := c.IDs()[1]
	termBefore := c.Node(victim).Term()
	if termBefore == 0 {
		t.Fatal("setup: node never advanced past term 0")
	}

	c.Stop(victim)
	if len(c.Applied(victim)) != 0 {
		t.Error("crash did not discard the state machine")
	}

	c.Start(victim)
	// Term and vote are durable, so they must survive; anything less would let
	// the node vote twice in one term and elect two leaders.
	if got := c.Node(victim).Term(); got < termBefore {
		t.Errorf("term after restart = %d, want >= %d — durable state was lost", got, termBefore)
	}

	c.RunFor(5 * raft.Second)
	c.StopWorkload()
	c.RunFor(5 * raft.Second)

	if len(c.Applied(victim)) == 0 {
		t.Error("restarted node never rebuilt its state machine from the log")
	}
	requireConvergence(t, c, c.IDs())
}

// TestChaosIsReproducible extends the determinism contract to the full fault
// model: the faults a seed produces are part of what that seed replays.
func TestChaosIsReproducible(t *testing.T) {
	run := func(seed uint64) (uint64, int) {
		c := mustNew(t, sim.ChaosOptions(5, seed))
		c.RunFor(15 * raft.Second)
		c.StopChaos()
		c.HealAll()
		c.RunFor(5 * raft.Second)
		return c.TraceHash(), c.Events()
	}
	for seed := uint64(1); seed <= 15; seed++ {
		wantHash, wantEvents := run(seed)
		for rep := range 3 {
			gotHash, gotEvents := run(seed)
			if gotHash != wantHash || gotEvents != wantEvents {
				t.Fatalf("seed %d diverged on repetition %d: hash %#x/%#x, events %d/%d",
					seed, rep, gotHash, wantHash, gotEvents, wantEvents)
			}
		}
	}
}
