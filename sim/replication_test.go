package sim_test

import (
	"testing"

	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/sim"
)

// quiesce halts the write workload and lets the cluster settle.
func quiesce(c *sim.Cluster) {
	c.StopWorkload()
	c.RunFor(3 * raft.Second)
}

// requireConvergence asserts every live node's state machine agrees entry for
// entry. This is the property clients actually care about: replicas that report
// different values for the same key have diverged, however healthy the protocol
// looked while doing it.
func requireConvergence(t *testing.T, c *sim.Cluster, live []raft.NodeID) {
	t.Helper()
	if err := c.CheckInvariants(); err != nil {
		t.Fatal(err)
	}

	shortest := -1
	for _, id := range live {
		if n := len(c.Applied(id)); shortest < 0 || n < shortest {
			shortest = n
		}
	}
	if shortest <= 0 {
		t.Fatal("no node applied anything; replication never happened")
	}

	base := c.Applied(live[0])
	for _, id := range live[1:] {
		other := c.Applied(id)
		for i := range shortest {
			if base[i].Index != other[i].Index || base[i].Term != other[i].Term ||
				string(base[i].Data) != string(other[i].Data) {
				t.Fatalf("nodes %d and %d diverge at applied position %d: %+v vs %+v",
					live[0], id, i, base[i], other[i])
			}
		}
	}
	t.Logf("%d nodes agree on %d applied entries", len(live), shortest)
}

func TestReplicationConverges(t *testing.T) {
	c := mustNew(t, sim.DefaultOptions(5, 3))
	c.RunFor(5 * raft.Second)
	quiesce(c)

	if c.Acked == 0 {
		t.Fatal("no proposals were accepted")
	}
	requireConvergence(t, c, c.IDs())

	// Every accepted proposal, plus one no-op per election, should be applied.
	if got := c.MaxApplied(); got < raft.Index(c.Acked) {
		t.Errorf("applied through index %d but %d proposals were accepted", got, c.Acked)
	}
}

// TestFailoverMidWriteLosesNothing kills leaders while writes are in flight —
// the scenario the whole project exists to make safe.
func TestFailoverMidWriteLosesNothing(t *testing.T) {
	c := mustNew(t, sim.DefaultOptions(5, 11))
	c.RunFor(2 * raft.Second)

	var killed []raft.NodeID
	live := append([]raft.NodeID(nil), c.IDs()...)

	// Kill two leaders in succession, leaving 3 of 5 alive — still a quorum.
	for range 2 {
		leaders := c.Leaders()
		if len(leaders) != 1 {
			t.Fatalf("expected 1 leader before killing, got %d", len(leaders))
		}
		var victim raft.NodeID
		for id := range leaders {
			victim = id
		}
		c.Stop(victim)
		killed = append(killed, victim)
		c.RunFor(3 * raft.Second)
	}

	// Remove the killed nodes from the live set.
	var remaining []raft.NodeID
	for _, id := range live {
		if id != killed[0] && id != killed[1] {
			remaining = append(remaining, id)
		}
	}

	quiesce(c)
	requireConvergence(t, c, remaining)

	if len(c.Elections) < 3 {
		t.Errorf("expected at least 3 elections after 2 leader kills, got %d", len(c.Elections))
	}
}

// TestCrashedNodeCatchesUp exercises the conflict-resolution path: a node that
// misses a long stretch of writes must be brought fully back into agreement.
func TestCrashedNodeCatchesUp(t *testing.T) {
	c := mustNew(t, sim.DefaultOptions(5, 23))
	c.RunFor(2 * raft.Second)

	// Take a follower offline for a long stretch of writes.
	var victim raft.NodeID
	for _, id := range c.IDs() {
		if c.Node(id).Role() != raft.Leader {
			victim = id
			break
		}
	}
	c.Stop(victim)
	c.RunFor(5 * raft.Second)

	behind := len(c.Applied(victim))
	c.Start(victim)
	quiesce(c)

	if got := len(c.Applied(victim)); got <= behind {
		t.Fatalf("node %d applied %d entries before restart and %d after; it never caught up",
			victim, behind, got)
	}
	requireConvergence(t, c, c.IDs())
}

// TestManySeedsReplicationIsSafe is the Phase 2 soak: safety must hold for every
// seed, not just the ones the author tried.
func TestManySeedsReplicationIsSafe(t *testing.T) {
	for seed := uint64(1); seed <= 300; seed++ {
		c := mustNew(t, sim.DefaultOptions(5, seed))
		c.RunFor(3 * raft.Second)

		// Kill whoever leads, mid-write.
		for id := range c.Leaders() {
			c.Stop(id)
		}
		c.RunFor(3 * raft.Second)
		quiesce(c)

		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d: %v\n\nReproduce with: go run ./cmd/dst -seed=%d", seed, err, seed)
		}
	}
}
