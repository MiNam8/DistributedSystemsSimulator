package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/MiNam8/kvstore/raft"
)

// TestLeadershipTransferIsFasterThanAnElection is the point of §3.10.
//
// Without transfer, draining a node for maintenance means stopping it and
// waiting for the cluster to notice — an election timeout of unavailability
// every time. Transfer brings the target fully up to date and tells it to
// campaign immediately, so the gap is a round trip.
func TestLeadershipTransferIsFasterThanAnElection(t *testing.T) {
	h := start(t, 3)
	old := h.leader(5 * time.Second)

	for range 20 {
		if _, err := h.put("k", "v"); err != nil {
			t.Fatal(err)
		}
	}

	var target raft.NodeID
	for _, id := range h.ids {
		if id != old {
			target = id
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	if err := h.nodes[old].TransferLeadership(ctx, target); err != nil {
		t.Fatalf("transfer: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.nodes[target].IsLeader() {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	elapsed := time.Since(start)

	if !h.nodes[target].IsLeader() {
		t.Fatalf("node %d did not take leadership within 3s", target)
	}
	// The harness uses a 250ms election timeout, so an ordinary failover costs
	// at least that. A handover should land in a small multiple of a round trip.
	if elapsed > 150*time.Millisecond {
		t.Errorf("handover took %v, which is no better than waiting for an election", elapsed)
	}
	t.Logf("leadership moved from node %d to node %d in %v (an election would cost >250ms)",
		old, target, elapsed.Round(time.Millisecond))

	// And the cluster must still work afterwards.
	if _, err := h.put("after", "transfer"); err != nil {
		t.Fatalf("write after handover: %v", err)
	}
}

// TestLearnerJoinsAndIsPromoted walks the safe way to grow a cluster: join as a
// non-voting learner, catch up, then promote.
//
// Adding a node straight in as a voter raises the quorum immediately — three of
// four instead of two of three — while the new node holds nothing, so a single
// further failure costs availability. A learner replays the log without ever
// counting toward a quorum.
func TestLearnerJoinsAndIsPromoted(t *testing.T) {
	h := start(t, 3)
	leader := h.leader(5 * time.Second)

	for range 10 {
		if _, err := h.put("k", "v"); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Demote an existing voter to a learner: the harness has no spare node, and
	// the interesting transitions are the same.
	var victim raft.NodeID
	for _, id := range h.ids {
		if id != leader {
			victim = id
			break
		}
	}
	if err := h.nodes[leader].ProposeConfChange(ctx,
		raft.ConfChange{Type: raft.ConfChangeAddLearner, NodeID: victim}); err != nil {
		t.Fatalf("demote to learner: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		cfg := h.nodes[leader].Config()
		if len(cfg.Learners) == 1 && cfg.Learners[0] == victim {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cfg := h.nodes[leader].Config()
	if len(cfg.Learners) != 1 || cfg.Learners[0] != victim {
		t.Fatalf("configuration = %+v, want node %d as a learner", cfg, victim)
	}
	if cfg.Quorum() != 2 {
		t.Errorf("quorum with 2 voters = %d, want 2", cfg.Quorum())
	}

	// The cluster keeps serving with the reduced voter set.
	if _, err := h.put("during", "membership-change"); err != nil {
		t.Fatalf("write while a node is a learner: %v", err)
	}

	// Promote it back.
	if err := h.nodes[leader].ProposeConfChange(ctx,
		raft.ConfChange{Type: raft.ConfChangePromoteLearner, NodeID: victim}); err != nil {
		t.Fatalf("promote: %v", err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.nodes[leader].Config().Learners) == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := h.nodes[leader].Config(); len(got.Voters) != 3 || len(got.Learners) != 0 {
		t.Errorf("configuration after promotion = %+v, want 3 voters and no learners", got)
	}
	if _, err := h.put("after", "promotion"); err != nil {
		t.Fatalf("write after promotion: %v", err)
	}
}
