package server_test

import (
	"errors"
	"testing"
	"time"

	"github.com/MiNam8/kvstore/kv"
	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/server"
)

// startRead builds a cluster serving reads through the given path.
func startRead(t *testing.T, n int, mode server.ReadMode, lease time.Duration) *harness {
	t.Helper()
	h := startWith(t, n, 0)
	// Rebuild every node with the read mode set; startWith launched them with
	// defaults.
	cluster := raft.Config{Voters: h.ids}
	for _, id := range h.ids {
		h.kill(id)
	}
	h.readMode, h.lease = mode, lease
	for _, id := range h.ids {
		h.net.SetDown(id, false)
		h.launch(id, cluster)
	}
	return h
}

// TestAllReadPathsAreCorrect runs the same read-your-writes check through each
// of the three paths.
//
// They differ only in how leadership is established, so they must be
// indistinguishable to a client. Any divergence means one of them is not
// linearizable.
func TestAllReadPathsAreCorrect(t *testing.T) {
	modes := []struct {
		name  string
		mode  server.ReadMode
		lease time.Duration
	}{
		{"through-log", server.ReadThroughLog, 0},
		{"readindex", server.ReadIndex, 0},
		{"lease", server.ReadLease, 100 * time.Millisecond}, // well under the 250ms election timeout
	}
	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			h := startRead(t, 3, tc.mode, tc.lease)
			h.leader(5 * time.Second)

			for i := range 25 {
				val := string(rune('a' + i%26))
				if _, err := h.put("k", val); err != nil {
					t.Fatalf("put %d: %v", i, err)
				}
				res, err := h.get("k")
				if err != nil {
					t.Fatalf("get %d: %v", i, err)
				}
				if string(res.Value) != val {
					t.Fatalf("read-your-write violated at %d: got %q, want %q", i, res.Value, val)
				}
			}
		})
	}
}

// TestReadPathsSurviveFailover checks each path still serves correct data after
// the leader is killed — including that a *new* leader defers reads until it has
// committed an entry of its own term, without which its commit index is only a
// lower bound and a read could miss an acknowledged write.
func TestReadPathsSurviveFailover(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mode  server.ReadMode
		lease time.Duration
	}{
		{"readindex", server.ReadIndex, 0},
		{"lease", server.ReadLease, 100 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := startRead(t, 5, tc.mode, tc.lease)
			h.leader(5 * time.Second)

			if _, err := h.put("before", "written"); err != nil {
				t.Fatal(err)
			}
			h.kill(h.leader(5 * time.Second))
			h.leader(5 * time.Second)

			res, err := h.get("before")
			if err != nil {
				t.Fatal(err)
			}
			if !res.Found || string(res.Value) != "written" {
				t.Errorf("a write acknowledged before the failover was not visible to a read "+
					"on the new leader: %q found=%v", res.Value, res.Found)
			}
		})
	}
}

// TestLeaseAvoidsTheConfirmationRound checks the lease is doing what it claims:
// serving reads without a quorum round trip. If every read still confirmed, the
// lease would be correct but pointless.
func TestLeaseAvoidsTheConfirmationRound(t *testing.T) {
	h := startRead(t, 3, server.ReadLease, 100*time.Millisecond)
	leader := h.leader(5 * time.Second)

	if _, err := h.put("k", "v"); err != nil {
		t.Fatal(err)
	}
	for range 40 {
		if _, err := h.get("k"); err != nil {
			t.Fatal(err)
		}
	}
	st := h.nodes[leader].Stats()
	if st.LeaseReads == 0 {
		t.Errorf("no read was served under a lease (%d confirmed); the lease path is inert",
			st.ConfirmedReads)
	}
	t.Logf("%d reads served under lease, %d by quorum confirmation", st.LeaseReads, st.ConfirmedReads)
}

// writeVia writes through a specific node, following leader hints within a
// partition. It returns false if no reachable node accepted the write.
func (h *harness) writeVia(from raft.NodeID, key, val string, timeout time.Duration) bool {
	h.t.Helper()
	h.seq++
	cmd := kv.Command{Op: kv.OpPut, Key: key, Value: []byte(val), ClientID: 77, Seq: h.seq}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := h.issue(from, cmd); err == nil {
			return true
		} else {
			var nl server.NotLeaderError
			if errors.As(err, &nl) && nl.Leader != raft.None {
				if _, err := h.issue(nl.Leader, cmd); err == nil {
					return true
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// TestLeaseBoundIsWhatMakesLeaseReadsSafe is the honest test of the one place in
// this system where correctness rests on clocks rather than messages.
//
// The lease argument is: no follower starts an election before its own election
// timeout elapses, so within that window nobody else can have become leader. It
// holds only while
//
//	lease duration < election timeout - maximum clock rate error
//
// This test sets the lease *longer* than the election timeout — the exact
// misconfiguration the bound forbids — partitions the leader away from a quorum,
// lets the majority elect a successor and accept a write, and then reads from the
// deposed leader. It must return stale data.
//
// Demonstrating precisely when your own fastest path is unsafe is worth more
// than asserting that it is safe.
func TestLeaseBoundIsWhatMakesLeaseReadsSafe(t *testing.T) {
	// The harness uses a 250ms election timeout; 3s is far outside the bound.
	h := startRead(t, 5, server.ReadLease, 3*time.Second)
	old := h.leader(5 * time.Second)

	if _, err := h.put("k", "v1"); err != nil {
		t.Fatal(err)
	}

	// Cut the leader off from everyone else. It cannot learn it has been
	// deposed — nothing in Raft tells a leader that, and it certainly cannot
	// hear it through a partition.
	var majority []raft.NodeID
	for _, id := range h.ids {
		if id != old {
			majority = append(majority, id)
		}
	}
	h.net.Partition([]raft.NodeID{old}, majority)

	// The majority elects a successor and accepts a write the old leader will
	// never see.
	if !h.writeVia(majority[0], "k", "v2", 6*time.Second) {
		t.Fatal("the majority partition never accepted a write")
	}

	// Read from the deposed leader. Its lease has not expired, so it answers
	// from local state — with a value that was superseded before the read began.
	res, err := h.issue(old, kv.Command{Op: kv.OpGet, Key: "k", ClientID: 99, Seq: 1})
	if err != nil {
		t.Fatalf("the deposed leader refused the read (%v); the point of this test is that "+
			"an over-long lease makes it answer", err)
	}
	if string(res.Value) == "v2" {
		t.Fatal("the deposed leader somehow had the newer value; the partition did not take effect")
	}
	if string(res.Value) != "v1" {
		t.Fatalf("unexpected value %q", res.Value)
	}
	t.Logf("STALE READ as designed: the deposed leader served %q while the cluster held %q",
		res.Value, "v2")
}

// TestSafeLeaseRefusesRatherThanLies is the counterpart, and the reason the
// bound is worth respecting.
//
// With the lease inside its bound, the deposed leader's lease expires before any
// successor can be elected. It then cannot serve the read locally, and the
// confirmation round it falls back to never reaches a quorum — so the read
// *fails* instead of returning a stale value.
//
// Unavailable is a correct answer. Wrong is not.
func TestSafeLeaseRefusesRatherThanLies(t *testing.T) {
	h := startRead(t, 5, server.ReadLease, 80*time.Millisecond) // well inside the 250ms timeout
	old := h.leader(5 * time.Second)

	if _, err := h.put("k", "v1"); err != nil {
		t.Fatal(err)
	}

	var majority []raft.NodeID
	for _, id := range h.ids {
		if id != old {
			majority = append(majority, id)
		}
	}
	h.net.Partition([]raft.NodeID{old}, majority)

	if !h.writeVia(majority[0], "k", "v2", 6*time.Second) {
		t.Fatal("the majority partition never accepted a write")
	}

	res, err := h.issue(old, kv.Command{Op: kv.OpGet, Key: "k", ClientID: 99, Seq: 1})
	if err == nil && string(res.Value) == "v1" {
		t.Fatalf("a lease inside its bound still served stale data (%q) — the bound is not "+
			"actually being enforced", res.Value)
	}
	if err != nil {
		t.Logf("the deposed leader correctly refused the read: %v", err)
	}
}
