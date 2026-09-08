package sim_test

import (
	"testing"

	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/sim"
)

func mustNew(t *testing.T, opts sim.Options) *sim.Cluster {
	t.Helper()
	c, err := sim.New(opts)
	if err != nil {
		t.Fatalf("sim.New: %v", err)
	}
	return c
}

// soleLeader asserts exactly one node claims leadership and returns it.
func soleLeader(t *testing.T, c *sim.Cluster) (raft.NodeID, raft.Term) {
	t.Helper()
	leaders := c.Leaders()
	if len(leaders) != 1 {
		t.Fatalf("expected exactly 1 leader, got %d: %v", len(leaders), leaders)
	}
	for id, term := range leaders {
		return id, term
	}
	return raft.None, 0
}

// checkElectionSafety asserts Raft's core guarantee: at most one leader per
// term, over the entire run.
func checkElectionSafety(t *testing.T, c *sim.Cluster) {
	t.Helper()
	byTerm := make(map[raft.Term]raft.NodeID)
	for _, e := range c.Elections {
		if prev, seen := byTerm[e.Term]; seen && prev != e.Leader {
			t.Fatalf("ELECTION SAFETY VIOLATED: nodes %d and %d were both leader in term %d",
				prev, e.Leader, e.Term)
		}
		byTerm[e.Term] = e.Leader
	}
}

func TestElectsSingleLeader(t *testing.T) {
	for _, n := range []int{1, 3, 5, 7} {
		t.Run(strconvItoa(n)+"-nodes", func(t *testing.T) {
			c := mustNew(t, sim.DefaultOptions(n, 1))
			c.RunFor(2 * raft.Second)

			id, term := soleLeader(t, c)
			checkElectionSafety(t, c)
			if term == 0 {
				t.Errorf("leader %d is in term 0; no election actually happened", id)
			}
			// Every follower should agree on who won, or the cluster is
			// leaderless from the clients' point of view even though a leader exists.
			for _, peer := range c.IDs() {
				if got := c.Node(peer).Leader(); got != id {
					t.Errorf("node %d thinks the leader is %d, want %d", peer, got, id)
				}
			}
		})
	}
}

// TestFailoverElectsNewLeader is the headline behaviour: kill the leader and the
// cluster elects another one on its own.
func TestFailoverElectsNewLeader(t *testing.T) {
	c := mustNew(t, sim.DefaultOptions(5, 7))
	c.RunFor(2 * raft.Second)

	first, firstTerm := soleLeader(t, c)
	c.Stop(first)
	c.RunFor(3 * raft.Second)

	leaders := c.Leaders()
	if len(leaders) != 1 {
		t.Fatalf("after killing leader %d, expected 1 leader, got %d: %v", first, len(leaders), leaders)
	}
	for id, term := range leaders {
		if id == first {
			t.Fatalf("stopped node %d is still leader", id)
		}
		// A new leader must be in a strictly higher term. Equal terms would mean
		// two leaders coexisted in one term, which is the safety violation.
		if term <= firstTerm {
			t.Errorf("new leader %d is in term %d, want > %d", id, term, firstTerm)
		}
	}
	checkElectionSafety(t, c)
}

// TestFailoverIsBounded measures how long failover actually takes, so the
// resume claim is a measurement rather than an assertion. Phase 11 turns this
// into a distribution over many seeds; this is the floor it must already clear.
func TestFailoverIsBounded(t *testing.T) {
	const budget = 2 * raft.Second

	for seed := uint64(1); seed <= 50; seed++ {
		c := mustNew(t, sim.DefaultOptions(5, seed))
		c.RunFor(2 * raft.Second)
		first, _ := soleLeader(t, c)

		killedAt := c.Now()
		c.Stop(first)
		c.RunFor(budget)

		var elected raft.Time
		for _, e := range c.Elections {
			if e.At > killedAt && e.Leader != first {
				elected = e.At
				break
			}
		}
		if elected == 0 {
			t.Fatalf("seed %d: no new leader within %v ms of killing node %d",
				seed, budget/raft.Millisecond, first)
		}
		if took := raft.Duration(elected - killedAt); took > budget {
			t.Errorf("seed %d: failover took %d ms, budget %d ms",
				seed, took/raft.Millisecond, budget/raft.Millisecond)
		}
		checkElectionSafety(t, c)
	}
}

// TestSameSeedReplaysIdentically is the contract the entire project rests on.
//
// It is also the most sensitive detector of accidental nondeterminism we have:
// a ranged map anywhere in the core or the harness produces a different effect
// order between runs, and the trace hash diverges immediately. Several
// repetitions are used because Go's map iteration randomisation is per-range,
// so a single comparison can pass by luck.
func TestSameSeedReplaysIdentically(t *testing.T) {
	run := func(seed uint64) uint64 {
		c := mustNew(t, sim.DefaultOptions(5, seed))
		c.RunFor(3 * raft.Second)
		c.Stop(c.IDs()[0])
		c.RunFor(3 * raft.Second)
		return c.TraceHash()
	}

	for seed := uint64(1); seed <= 20; seed++ {
		want := run(seed)
		for rep := range 5 {
			if got := run(seed); got != want {
				t.Fatalf("seed %d diverged on repetition %d: trace %#x != %#x\n"+
					"Something is reading a nondeterministic source — most likely a ranged map "+
					"in the core or the harness.", seed, rep, got, want)
			}
		}
	}
}

// TestDifferentSeedsExploreDifferentSchedules confirms the seed actually
// changes the execution. A harness where every seed produced the same trace
// would pass every test above and explore nothing.
func TestDifferentSeedsExploreDifferentSchedules(t *testing.T) {
	seen := make(map[uint64]bool)
	for seed := uint64(1); seed <= 20; seed++ {
		c := mustNew(t, sim.DefaultOptions(5, seed))
		c.RunFor(2 * raft.Second)
		seen[c.TraceHash()] = true
	}
	if len(seen) < 15 {
		t.Errorf("20 seeds produced only %d distinct traces; the harness is barely exploring", len(seen))
	}
}

// TestManySeedsAllElectALeader is a miniature soak: liveness must hold for
// every seed, not just the one the author happened to try.
func TestManySeedsAllElectALeader(t *testing.T) {
	for seed := uint64(1); seed <= 500; seed++ {
		c := mustNew(t, sim.DefaultOptions(3, seed))
		c.RunFor(3 * raft.Second)
		if leaders := c.Leaders(); len(leaders) != 1 {
			t.Fatalf("seed %d: expected 1 leader, got %d: %v", seed, len(leaders), leaders)
		}
		checkElectionSafety(t, c)
	}
}

func strconvItoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
