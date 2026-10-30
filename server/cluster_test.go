package server_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/MiNam8/kvstore/kv"
	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/server"
	"github.com/MiNam8/kvstore/storage/mem"
	"github.com/MiNam8/kvstore/transport/inproc"
)

// harness is a cluster of nodes wired through an in-process network.
type harness struct {
	t     *testing.T
	net   *inproc.Network
	nodes map[raft.NodeID]*server.Node
	ids   []raft.NodeID
	stops map[raft.NodeID]context.CancelFunc
	store map[raft.NodeID]*mem.Storage
	seq   uint64

	// snapshotThreshold is set before launching to exercise compaction.
	snapshotThreshold int
	readMode          server.ReadMode
	lease             time.Duration
}

func start(t *testing.T, n int) *harness { return startWith(t, n, 0) }

func startWith(t *testing.T, n, snapshotThreshold int) *harness {
	t.Helper()
	h := &harness{
		t: t, net: inproc.NewNetwork(),
		nodes: map[raft.NodeID]*server.Node{},
		stops: map[raft.NodeID]context.CancelFunc{},
		store: map[raft.NodeID]*mem.Storage{}, snapshotThreshold: snapshotThreshold,
	}
	cluster := raft.Config{}
	for i := 1; i <= n; i++ {
		h.ids = append(h.ids, raft.NodeID(i))
		cluster.Voters = append(cluster.Voters, raft.NodeID(i))
	}
	for _, id := range h.ids {
		h.store[id] = mem.New(cluster)
		h.launch(id, cluster)
	}
	t.Cleanup(h.stopAll)
	return h
}

func (h *harness) launch(id raft.NodeID, cluster raft.Config) {
	h.t.Helper()
	node, err := server.New(server.Config{
		ID:                id,
		Cluster:           cluster,
		Storage:           h.store[id],
		Transport:         h.net.Transport(id),
		ElectionTimeout:   250 * time.Millisecond,
		HeartbeatInterval: 40 * time.Millisecond,
		TickInterval:      5 * time.Millisecond,
		Seed:              uint64(id) * 7919,
		SnapshotThreshold: h.snapshotThreshold,
		ReadMode:          h.readMode,
		LeaseDuration:     h.lease,
	})
	if err != nil {
		h.t.Fatalf("node %d: %v", id, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.nodes[id] = node
	h.stops[id] = cancel
	h.net.Register(id, node)
	go node.Run(ctx)
}

func (h *harness) stopAll() {
	for _, cancel := range h.stops {
		cancel()
	}
}

// kill stops a node and cuts it off the network, as a crashed process is.
func (h *harness) kill(id raft.NodeID) {
	h.stops[id]()
	h.nodes[id].Wait()
	h.net.SetDown(id, true)
}

// restart brings a killed node back from its durable state alone.
func (h *harness) restart(id raft.NodeID, cluster raft.Config) {
	h.net.SetDown(id, false)
	h.launch(id, cluster)
}

// leader waits for exactly one node to claim leadership.
func (h *harness) leader(timeout time.Duration) raft.NodeID {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var found raft.NodeID
		count := 0
		for _, id := range h.ids {
			if h.net.IsDown(id) {
				continue
			}
			if h.nodes[id].IsLeader() {
				found, count = id, count+1
			}
		}
		if count == 1 {
			return found
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("no single leader within %v", timeout)
	return raft.None
}

// do runs a command against the cluster, following leader hints and retrying
// with the *same* sequence number — which is exactly what a real client must do,
// and what the session table makes safe.
func (h *harness) do(cmd kv.Command, timeout time.Duration) (kv.Result, error) {
	h.t.Helper()
	if cmd.Seq == 0 {
		h.seq++
		cmd.Seq = h.seq
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		for _, id := range h.ids {
			if h.net.IsDown(id) {
				continue
			}
			res, err := h.issue(id, cmd)
			if err == nil {
				return res, nil
			}
			lastErr = err
			var nl server.NotLeaderError
			if errors.As(err, &nl) && nl.Leader != raft.None && !h.net.IsDown(nl.Leader) {
				// Follow the hint rather than polling every node.
				res, err := h.issue(nl.Leader, cmd)
				if err == nil {
					return res, nil
				}
				lastErr = err
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return kv.Result{}, lastErr
}

// issue sends one command to one node.
//
// Reads go through Node.Read, not Node.Apply. Routing them through Apply would
// put every read through the log regardless of the configured read mode — which
// is exactly what this harness did at first, leaving the ReadIndex and lease
// tests green while exercising neither.
func (h *harness) issue(id raft.NodeID, cmd kv.Command) (kv.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	if cmd.Op == kv.OpGet {
		return h.nodes[id].Read(ctx, cmd.Key, cmd.ClientID, cmd.Seq)
	}
	return h.nodes[id].Apply(ctx, cmd)
}

func (h *harness) put(k, v string) (kv.Result, error) {
	return h.do(kv.Command{Op: kv.OpPut, Key: k, Value: []byte(v), ClientID: 1}, 5*time.Second)
}

func (h *harness) get(k string) (kv.Result, error) {
	return h.do(kv.Command{Op: kv.OpGet, Key: k, ClientID: 1}, 5*time.Second)
}

func TestClusterServesReadsAndWrites(t *testing.T) {
	h := start(t, 3)
	h.leader(5 * time.Second)

	for _, kvp := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		if _, err := h.put(kvp[0], kvp[1]); err != nil {
			t.Fatalf("put %s: %v", kvp[0], err)
		}
	}
	for _, kvp := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		res, err := h.get(kvp[0])
		if err != nil {
			t.Fatalf("get %s: %v", kvp[0], err)
		}
		if !res.Found || string(res.Value) != kvp[1] {
			t.Errorf("get %s = %q (found=%v), want %q", kvp[0], res.Value, res.Found, kvp[1])
		}
	}
}

// TestReadYourOwnWrite is the minimum linearizability requirement: a client told
// its write succeeded must never then read a stale value.
func TestReadYourOwnWrite(t *testing.T) {
	h := start(t, 3)
	h.leader(5 * time.Second)

	for i := range 20 {
		val := string(rune('a' + i%26))
		if _, err := h.put("k", val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		res, err := h.get("k")
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if string(res.Value) != val {
			t.Fatalf("read-your-write violated at iteration %d: got %q, want %q", i, res.Value, val)
		}
	}
}

// TestClientSurvivesLeaderFailure is the Phase 5 checkpoint: kill the leader and
// the client transparently continues.
func TestClientSurvivesLeaderFailure(t *testing.T) {
	h := start(t, 5)
	old := h.leader(5 * time.Second)

	if _, err := h.put("before", "1"); err != nil {
		t.Fatal(err)
	}
	h.kill(old)

	// The client keeps working without knowing an election happened.
	if _, err := h.put("after", "2"); err != nil {
		t.Fatalf("write after leader failure: %v", err)
	}
	res, err := h.get("before")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Found || string(res.Value) != "1" {
		t.Errorf("a write acknowledged before the leader died was lost: %q found=%v", res.Value, res.Found)
	}
	if next := h.leader(5 * time.Second); next == old {
		t.Errorf("the killed node %d is still leader", old)
	}
}

// TestRetryAcrossFailoverIsNotAppliedTwice is why the session table exists.
//
// The command is proposed, the leader is killed before the client hears back,
// and the client retries with the same sequence number. Without deduplication
// the CAS would be evaluated a second time against changed state and report
// failure for a command that succeeded.
func TestRetryAcrossFailoverIsNotAppliedTwice(t *testing.T) {
	h := start(t, 5)
	h.leader(5 * time.Second)

	claim := kv.Command{Op: kv.OpCAS, Key: "lock", Value: []byte("client-9"), ClientID: 9, Seq: 1}
	first, err := h.do(claim, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !first.OK {
		t.Fatal("setup: the initial claim failed")
	}

	h.kill(h.leader(5 * time.Second))
	h.leader(5 * time.Second)

	// Someone else takes the lock after the failover.
	if _, err := h.do(kv.Command{Op: kv.OpPut, Key: "lock", Value: []byte("other"), ClientID: 2, Seq: 1}, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	// Client 9 retries its original request, as a real client would after a timeout.
	retry, err := h.do(claim, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.OK {
		t.Error("the retry reported failure for a command that had already succeeded")
	}
	res, err := h.get("lock")
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Value) != "other" {
		t.Errorf("the retry was applied a second time and clobbered the key: %q", res.Value)
	}
}

// TestRestartedNodeRejoins checks a node rebuilt from durable state alone
// catches up and serves correct data.
func TestRestartedNodeRejoins(t *testing.T) {
	h := start(t, 3)
	h.leader(5 * time.Second)
	cluster := raft.Config{Voters: h.ids}

	if _, err := h.put("k", "v1"); err != nil {
		t.Fatal(err)
	}

	// Kill a follower, write more, bring it back.
	victim := h.ids[0]
	if victim == h.leader(5*time.Second) {
		victim = h.ids[1]
	}
	h.kill(victim)

	for i := range 10 {
		if _, err := h.put("k", string(rune('A'+i))); err != nil {
			t.Fatal(err)
		}
	}
	h.restart(victim, cluster)
	h.leader(5 * time.Second)

	// The restarted node must catch up and agree.
	deadline := time.Now().Add(5 * time.Second)
	want := h.nodes[h.leader(5*time.Second)].Stats().AppliedIndex
	for time.Now().Before(deadline) {
		if h.nodes[victim].Stats().AppliedIndex >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("restarted node %d applied through %d, leader is at %d",
		victim, h.nodes[victim].Stats().AppliedIndex, want)
}

// TestSnapshotCompactsAndFollowerCatchesUp exercises the whole snapshot path on
// a real cluster: the leader compacts, a follower that was down falls behind the
// compaction boundary, and the only way back is InstallSnapshot.
//
// It also covers the detail that only shows up here: the session table must
// survive the round trip, or a client retrying after the follower becomes leader
// has its command applied twice.
func TestSnapshotCompactsAndFollowerCatchesUp(t *testing.T) {
	h := startWith(t, 3, 20) // compact aggressively
	h.leader(5 * time.Second)
	cluster := raft.Config{Voters: h.ids}

	// A client claims a key, so there is session state worth preserving.
	claim := kv.Command{Op: kv.OpCAS, Key: "lock", Value: []byte("client-9"), ClientID: 9, Seq: 1}
	if res, err := h.do(claim, 5*time.Second); err != nil || !res.OK {
		t.Fatalf("setup claim: %v %+v", err, res)
	}

	victim := h.ids[0]
	if victim == h.leader(5*time.Second) {
		victim = h.ids[1]
	}
	h.kill(victim)

	// Write well past the compaction threshold while it is down.
	for i := range 120 {
		if _, err := h.put("k", fmt.Sprintf("v%d", i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	leader := h.leader(5 * time.Second)
	if snaps := h.nodes[leader].Stats().Snapshots; snaps == 0 {
		t.Fatalf("the leader never snapshotted; this test would not exercise InstallSnapshot")
	}

	h.restart(victim, cluster)

	// The follower must come back through a snapshot install, not replication.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if h.nodes[victim].Stats().Restores > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	st := h.nodes[victim].Stats()
	if st.Restores == 0 {
		t.Fatalf("node %d caught up without installing a snapshot (applied %d)", victim, st.AppliedIndex)
	}

	want := h.nodes[h.leader(5*time.Second)].Stats().AppliedIndex
	for time.Now().Before(deadline) {
		if h.nodes[victim].Stats().AppliedIndex >= want {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := h.nodes[victim].Stats().AppliedIndex; got < want {
		t.Fatalf("restored node applied through %d, leader is at %d", got, want)
	}

	// The session table travelled with the snapshot: the original claim must
	// still be recognised as a duplicate rather than re-executed.
	if _, err := h.do(kv.Command{Op: kv.OpPut, Key: "lock", Value: []byte("other"), ClientID: 2, Seq: 1}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	retry, err := h.do(claim, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Duplicate {
		t.Error("the session table did not survive snapshot and restore: a retry was re-executed")
	}
	res, err := h.get("lock")
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Value) != "other" {
		t.Errorf("the retry clobbered the key after a snapshot round trip: %q", res.Value)
	}
}
