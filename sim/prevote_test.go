package sim_test

import (
	"testing"

	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/sim"
)

// TestPreVoteReducesTermChurn measures what pre-vote is for.
//
// Every term consumed is an election that happened. Under chaos most of them are
// elections that did not need to: a node returns from a partition or a restart,
// times out before hearing a heartbeat, and campaigns against a leader it cannot
// beat — depositing the healthy leader on the way. Pre-vote makes it ask first,
// and a node whose peers are content, or whose log is behind, is refused without
// its term ever moving.
func TestPreVoteReducesTermChurn(t *testing.T) {
	const seeds = 60
	var withPre, withoutPre int

	for seed := uint64(1); seed <= seeds; seed++ {
		plain := mustNew(t, sim.ChaosOptions(5, seed))
		plain.RunFor(20 * raft.Second)
		if err := plain.CheckInvariants(); err != nil {
			t.Fatalf("seed %d without pre-vote: %v", seed, err)
		}
		withoutPre += int(plain.MaxTerm())

		opts := sim.ChaosOptions(5, seed)
		opts.PreVote, opts.CheckQuorum = true, true
		pre := mustNew(t, opts)
		pre.RunFor(20 * raft.Second)
		if err := pre.CheckInvariants(); err != nil {
			t.Fatalf("seed %d with pre-vote: %v", seed, err)
		}
		withPre += int(pre.MaxTerm())
	}

	avgWithout := float64(withoutPre) / seeds
	avgWith := float64(withPre) / seeds
	t.Logf("mean final term over %d chaotic runs: %.1f without pre-vote, %.1f with (%.1fx fewer elections)",
		seeds, avgWithout, avgWith, avgWithout/max(avgWith, 1))

	if avgWith >= avgWithout {
		t.Errorf("pre-vote did not reduce term churn (%.1f with vs %.1f without); "+
			"it is meant to stop nodes campaigning when they cannot win", avgWith, avgWithout)
	}
}

// TestPreVoteStillElectsALeader checks the obvious risk: a mechanism that makes
// nodes reluctant to campaign could make a cluster that never elects anyone.
func TestPreVoteStillElectsALeader(t *testing.T) {
	for seed := uint64(1); seed <= 100; seed++ {
		opts := sim.DefaultOptions(5, seed)
		opts.PreVote, opts.CheckQuorum = true, true
		c := mustNew(t, opts)
		c.RunFor(5 * raft.Second)
		if n := len(c.Leaders()); n != 1 {
			t.Fatalf("seed %d: %d leaders with pre-vote enabled; the cluster cannot elect", seed, n)
		}
	}
}

// TestPreVoteSurvivesChaosAndRecovers is the safety and liveness check together:
// pre-vote must not break either.
func TestPreVoteSurvivesChaosAndRecovers(t *testing.T) {
	for seed := uint64(1); seed <= 60; seed++ {
		opts := sim.ChaosOptions(5, seed)
		opts.PreVote, opts.CheckQuorum = true, true
		c := mustNew(t, opts)
		c.RunFor(20 * raft.Second)
		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d under chaos: %v", seed, err)
		}
		c.StopChaos()
		c.HealAll()
		c.RunFor(6 * raft.Second)
		c.StopWorkload()
		c.RunFor(10 * raft.Second)

		if err := c.CheckInvariants(); err != nil {
			t.Fatalf("seed %d after heal: %v", seed, err)
		}
		if n := len(c.Leaders()); n != 1 {
			t.Fatalf("seed %d: %d leaders after healing with pre-vote", seed, n)
		}
		requireConvergence(t, c, c.IDs())
	}
}
