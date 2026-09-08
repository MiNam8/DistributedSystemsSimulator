package raft_test

import (
	"testing"

	"github.com/MiNam8/kvstore/raft"
)

// TestRandIsReproducible is the project's foundational guarantee in miniature:
// a seed must replay exactly. If this ever fails, every claim about reproducing
// a simulated failure from a seed is void.
func TestRandIsReproducible(t *testing.T) {
	a, b := raft.NewRand(42), raft.NewRand(42)
	for i := range 1000 {
		if x, y := a.Uint64(), b.Uint64(); x != y {
			t.Fatalf("draw %d diverged: %d != %d", i, x, y)
		}
	}
}

// TestRandStreamsAreIndependent checks that Fork gives each node and fault
// source its own stream. Without this, adding a fault source would reshuffle
// every unrelated random decision and destroy seed stability across runs.
func TestRandStreamsAreIndependent(t *testing.T) {
	parent := raft.NewRand(7)
	x, y := parent.Fork(), parent.Fork()

	same := 0
	for range 100 {
		if x.Uint64() == y.Uint64() {
			same++
		}
	}
	if same > 1 {
		t.Errorf("forked streams produced %d identical draws in 100; they are not independent", same)
	}
}

// TestIntnRespectsBounds guards the rejection-sampling arithmetic. An off-by-one
// there would either panic or silently return out-of-range election timeouts.
func TestIntnRespectsBounds(t *testing.T) {
	r := raft.NewRand(99)
	for _, n := range []int{1, 2, 3, 7, 150, 1 << 20} {
		for range 10_000 {
			v := r.Intn(n)
			if v < 0 || v >= n {
				t.Fatalf("Intn(%d) returned %d, out of range", n, v)
			}
		}
	}
}

// TestIntnIsUnbiased checks the rejection sampling actually produces a uniform
// distribution. Election timeout jitter is the mechanism that breaks vote
// splits; a biased source would make split votes measurably more likely and the
// resulting flakiness extremely hard to attribute.
func TestIntnIsUnbiased(t *testing.T) {
	const buckets, draws = 10, 200_000
	counts := make([]int, buckets)
	r := raft.NewRand(1234)
	for range draws {
		counts[r.Intn(buckets)]++
	}
	expected := draws / buckets
	for i, c := range counts {
		// ±5% is loose enough to never flake on a fixed seed and tight enough
		// to catch a genuine modulo bias.
		if c < expected*95/100 || c > expected*105/100 {
			t.Errorf("bucket %d got %d draws, expected ~%d (>5%% deviation)", i, c, expected)
		}
	}
}

// TestIntnPanicsOnNonPositive documents the contract rather than leaving it to
// a division-by-zero panic deep inside the generator.
func TestIntnPanicsOnNonPositive(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Intn(0) should panic")
		}
	}()
	raft.NewRand(1).Intn(0)
}
