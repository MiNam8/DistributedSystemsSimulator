package raft

// Rand is the source of randomness the core uses for election timeout jitter.
// It is injected rather than imported: the core may not import math/rand, and
// more importantly a global source would make simulated runs irreproducible,
// which would silently destroy the determinism the whole project rests on.
type Rand interface {
	// Uint64 returns a pseudo-random 64-bit value.
	Uint64() uint64
	// Intn returns a pseudo-random int in [0, n). It panics if n <= 0.
	Intn(n int) int
}

// SplitMix64 is a small, fast, well-distributed PRNG with 64 bits of state.
//
// It is implemented here rather than taken from math/rand for two reasons: the
// core may not import math/rand, and an in-tree algorithm guarantees that a
// given seed replays identically across Go releases. A seed that reproduces a
// bug today must still reproduce it after a toolchain upgrade, or the
// reproducibility contract is worthless.
type SplitMix64 struct {
	state uint64
}

// NewRand returns a SplitMix64 seeded with seed. Any seed value is valid,
// including zero.
func NewRand(seed uint64) *SplitMix64 { return &SplitMix64{state: seed} }

// Uint64 implements [Rand].
func (r *SplitMix64) Uint64() uint64 {
	r.state += 0x9E3779B97F4A7C15
	z := r.state
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// Intn implements [Rand]. It uses rejection sampling rather than a plain
// modulo, so the distribution is exactly uniform. Election timeout jitter is
// the mechanism that breaks vote splits; biasing it would make split votes
// measurably more likely and the resulting flakiness very hard to attribute.
func (r *SplitMix64) Intn(n int) int {
	if n <= 0 {
		panic("raft: Intn requires n > 0")
	}
	un := uint64(n)
	// Reject the short final bucket so every value in [0, n) is equally likely.
	threshold := (uint64(0) - un) % un
	for {
		v := r.Uint64()
		if v >= threshold {
			return int(v % un)
		}
	}
}

// Fork returns an independent generator derived from r, advancing r. The
// simulator uses this to give each node and each fault source its own stream,
// so that adding a fault source does not perturb the sequence every other
// source sees — without it, changing one simulation parameter would reshuffle
// every unrelated random decision and destroy seed stability.
func (r *SplitMix64) Fork() *SplitMix64 { return NewRand(r.Uint64()) }
