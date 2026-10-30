package sim_test

import (
	"testing"

	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/sim"
)

// TestChaosWithRealWAL runs the adversarial scenario against real segmented
// write-ahead logs on disk instead of in-memory storage.
//
// This is what stops the simulator from only proving things about the
// simulator. Every simulated crash now goes through real fsync, real segment
// replay and real torn-write recovery, and every restart rebuilds the node from
// bytes that actually survived on a filesystem.
//
// It is far slower than the in-memory suite — real fsync is the point — so it
// runs few seeds. The in-memory soak covers breadth; this covers fidelity.
func TestChaosWithRealWAL(t *testing.T) {
	if testing.Short() {
		t.Skip("real fsync: skipped under -short")
	}
	for seed := uint64(1); seed <= 10; seed++ {
		opts := sim.ChaosOptions(5, seed)
		opts.WALDir = t.TempDir()

		c, err := sim.New(opts)
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		c.RunFor(8 * raft.Second)
		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d under chaos: %v", seed, err)
		}

		c.StopChaos()
		c.HealAll()
		c.RunFor(4 * raft.Second)
		c.StopWorkload()
		c.RunFor(6 * raft.Second)

		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d after heal: %v", seed, err)
		}
		if n := len(c.Leaders()); n != 1 {
			t.Fatalf("seed %d: %d leaders after healing", seed, n)
		}
		requireConvergence(t, c, c.IDs())
		c.Close()
	}
}

// TestRestartFromRealWALRecoversState is the durability claim, end to end: a
// node crashed and rebuilt from bytes on disk must come back with its term and
// vote intact and rejoin agreement.
func TestRestartFromRealWALRecoversState(t *testing.T) {
	opts := sim.DefaultOptions(5, 3)
	opts.WALDir = t.TempDir()
	c, err := sim.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.RunFor(3 * raft.Second)
	victim := c.IDs()[1]
	termBefore := c.Node(victim).Term()
	if termBefore == 0 {
		t.Fatal("setup: node never advanced past term 0")
	}

	c.Stop(victim)
	c.Start(victim)

	// Term and vote are the two things that must survive a crash. Losing them
	// lets a node vote twice in one term, which elects two leaders.
	if got := c.Node(victim).Term(); got < termBefore {
		t.Fatalf("term after WAL recovery = %d, want >= %d — durable state was lost", got, termBefore)
	}

	c.RunFor(5 * raft.Second)
	c.StopWorkload()
	c.RunFor(5 * raft.Second)
	requireConvergence(t, c, c.IDs())
}
