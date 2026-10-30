package sim_test

import (
	"testing"

	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/sim"
)

// TestSnapshotsUnderChaos runs the full adversarial scenario with compaction so
// aggressive that snapshots, not replication, become the normal way a lagging
// node catches up.
//
// That aggressiveness is the point. With a production-sized threshold the
// InstallSnapshot path runs perhaps once an hour and is effectively untested;
// at thirty entries it runs constantly, so every partition, crash and stall in
// the chaos driver gets thrown at it.
func TestSnapshotsUnderChaos(t *testing.T) {
	var snaps, restores int
	for seed := uint64(1); seed <= 150; seed++ {
		c := mustNew(t, sim.SnapshotOptions(5, seed))
		c.RunFor(20 * raft.Second)
		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d under chaos: %v\n\nchaos log:\n%v", seed, err, c.ChaosLog)
		}

		c.StopChaos()
		c.HealAll()
		c.RunFor(5 * raft.Second)
		c.StopWorkload()
		c.RunFor(10 * raft.Second)

		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d after heal: %v", seed, err)
		}
		snaps += c.Snapshots
		restores += c.Restores
	}
	if snaps == 0 {
		t.Fatal("no snapshots were taken: the compaction path was never exercised")
	}
	if restores == 0 {
		t.Fatal("no snapshots were installed: the InstallSnapshot path was never exercised")
	}
	t.Logf("%d snapshots taken, %d installed on followers across 150 seeds", snaps, restores)
}

// TestFollowerCatchesUpViaSnapshot closes the gap Phase 2 left open: a follower
// that falls behind the compaction boundary cannot be helped by replication,
// because the entries it needs no longer exist. Before InstallSnapshot the
// leader silently gave up and the follower stayed stuck forever.
func TestFollowerCatchesUpViaSnapshot(t *testing.T) {
	opts := sim.SnapshotOptions(5, 21)
	opts.Faults = sim.NoFaults()
	opts.ChaosInterval = 0
	c := mustNew(t, opts)

	c.RunFor(2 * raft.Second)
	leader, _ := soleLeader(t, c)

	var victim raft.NodeID
	for _, id := range c.IDs() {
		if id != leader {
			victim = id
			break
		}
	}

	// Take the follower down long enough that the leader compacts past
	// everything it is missing.
	c.Stop(victim)
	c.RunFor(10 * raft.Second)

	leaderFirst := c.Node(leader).Log().FirstIndex()
	if leaderFirst <= 1 {
		t.Fatalf("the leader never compacted (FirstIndex = %d); the test proves nothing", leaderFirst)
	}
	t.Logf("leader compacted past index %d while node %d was down", leaderFirst-1, victim)

	before := c.Restores
	c.Start(victim)
	c.RunFor(5 * raft.Second)
	c.StopWorkload()
	c.RunFor(5 * raft.Second)

	if c.Restores == before {
		t.Error("the follower caught up without installing a snapshot, so this test did not exercise InstallSnapshot")
	}
	if err := c.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
	requireConvergence(t, c, c.IDs())
}

// TestSnapshotsWithRealWAL runs the same path against real files, so the
// atomic snapshot write (temp, fsync, rename, fsync dir) is exercised rather
// than assumed.
func TestSnapshotsWithRealWAL(t *testing.T) {
	if testing.Short() {
		t.Skip("real fsync: skipped under -short")
	}
	for seed := uint64(1); seed <= 5; seed++ {
		opts := sim.SnapshotOptions(5, seed)
		opts.WALDir = t.TempDir()

		c, err := sim.New(opts)
		if err != nil {
			t.Fatal(err)
		}
		c.RunFor(10 * raft.Second)
		c.StopChaos()
		c.HealAll()
		c.RunFor(5 * raft.Second)
		c.StopWorkload()
		c.RunFor(8 * raft.Second)

		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if c.Snapshots == 0 {
			t.Fatalf("seed %d: no snapshots taken", seed)
		}
		requireConvergence(t, c, c.IDs())
		c.Close()
	}
}
