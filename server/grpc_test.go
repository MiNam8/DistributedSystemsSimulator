package server_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/MiNam8/kvstore/client"
	"github.com/MiNam8/kvstore/proto/kvstorepb"
	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/server"
	"github.com/MiNam8/kvstore/storage/mem"
	grpctransport "github.com/MiNam8/kvstore/transport/grpc"
)

// grpcCluster is a real cluster: real listeners, real gRPC, real sockets.
// It is the fidelity counterpart to the in-process harness — slower, but it
// exercises serialisation, connection handling and the client's redirect logic,
// none of which the in-process transport touches.
type grpcCluster struct {
	t      *testing.T
	ids    []raft.NodeID
	addrs  []string
	nodes  map[raft.NodeID]*server.Node
	stops  map[raft.NodeID]context.CancelFunc
	grpcs  map[raft.NodeID]*grpc.Server
	client *client.Client
}

func startGRPC(t *testing.T, n int) *grpcCluster {
	t.Helper()
	c := &grpcCluster{
		t:     t,
		nodes: map[raft.NodeID]*server.Node{}, stops: map[raft.NodeID]context.CancelFunc{},
		grpcs: map[raft.NodeID]*grpc.Server{},
	}

	// Bind ports first so every node knows the full address map before starting.
	listeners := map[raft.NodeID]net.Listener{}
	addrMap := map[raft.NodeID]string{}
	cluster := raft.Config{}
	for i := 1; i <= n; i++ {
		id := raft.NodeID(i)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id] = lis
		addrMap[id] = lis.Addr().String()
		cluster.Voters = append(cluster.Voters, id)
		c.ids = append(c.ids, id)
		c.addrs = append(c.addrs, lis.Addr().String())
	}

	for _, id := range c.ids {
		tr, err := grpctransport.New(id, addrMap)
		if err != nil {
			t.Fatal(err)
		}
		node, err := server.New(server.Config{
			ID:                id,
			Cluster:           cluster,
			Storage:           mem.New(cluster),
			Transport:         tr,
			ElectionTimeout:   250 * time.Millisecond,
			HeartbeatInterval: 40 * time.Millisecond,
			TickInterval:      8 * time.Millisecond,
			Seed:              uint64(id) * 7919,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		gs := grpc.NewServer()
		kvstorepb.RegisterRaftServiceServer(gs, grpctransport.NewService(node))
		kvstorepb.RegisterKVServer(gs, server.NewService(node))

		c.nodes[id], c.stops[id], c.grpcs[id] = node, cancel, gs
		go node.Run(ctx)
		go gs.Serve(listeners[id])
	}

	cl, err := client.New(c.addrs)
	if err != nil {
		t.Fatal(err)
	}
	c.client = cl

	t.Cleanup(func() {
		cl.Close()
		for _, id := range c.ids {
			c.stops[id]()
			c.grpcs[id].Stop()
		}
	})
	return c
}

func (c *grpcCluster) leader(timeout time.Duration) raft.NodeID {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var found raft.NodeID
		count := 0
		for _, id := range c.ids {
			if _, stopped := c.stops[id]; stopped && c.nodes[id].IsLeader() {
				found, count = id, count+1
			}
		}
		if count == 1 {
			return found
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no single leader within %v", timeout)
	return raft.None
}

// TestGRPCEndToEnd is the Phase 5 checkpoint over real sockets: a cluster serves
// reads and writes through gRPC, the leader is killed, and the client continues
// without being told anything happened.
func TestGRPCEndToEnd(t *testing.T) {
	c := startGRPC(t, 3)
	c.leader(10 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for i := range 10 {
		if err := c.client.Put(ctx, fmt.Sprintf("k%d", i), fmt.Appendf(nil, "v%d", i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	for i := range 10 {
		v, found, err := c.client.Get(ctx, fmt.Sprintf("k%d", i))
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if !found || string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("get k%d = %q found=%v", i, v, found)
		}
	}

	// Kill the leader. The client must find the new one on its own.
	old := c.leader(10 * time.Second)
	c.stops[old]()
	c.grpcs[old].Stop()
	c.nodes[old].Wait()

	if err := c.client.Put(ctx, "after", []byte("failover")); err != nil {
		t.Fatalf("write after leader failure: %v", err)
	}
	v, found, err := c.client.Get(ctx, "k5")
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(v) != "v5" {
		t.Errorf("a write acknowledged before the failover was lost: %q found=%v", v, found)
	}
}

// TestGRPCCompareAndSwap exercises the operation that makes duplicate
// application observable, over the wire.
func TestGRPCCompareAndSwap(t *testing.T) {
	c := startGRPC(t, 3)
	c.leader(10 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ok, err := c.client.CompareAndSwap(ctx, "lock", nil, []byte("mine"), false)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("CAS expecting absence failed on an absent key")
	}
	// The same expectation must now fail: the key exists.
	ok, err = c.client.CompareAndSwap(ctx, "lock", nil, []byte("other"), false)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("CAS expecting absence succeeded on a present key")
	}
	v, _, err := c.client.Get(ctx, "lock")
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "mine" {
		t.Errorf("value = %q, want mine", v)
	}
}

// TestClientRedirectsToLeader checks a request sent to a follower is answered
// with a redirect rather than an error, so the client retries immediately
// instead of backing off as it would for an unreachable node.
func TestClientRedirectsToLeader(t *testing.T) {
	c := startGRPC(t, 3)
	leader := c.leader(10 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var follower int
	for i, id := range c.ids {
		if id != leader {
			follower = i
			break
		}
	}
	stub := kvstorepb.NewKVClient(mustDial(t, c.addrs[follower]))
	resp, err := stub.Put(ctx, &kvstorepb.PutRequest{
		Key: "k", Value: []byte("v"),
		Session: &kvstorepb.Session{ClientId: 42, Seq: 1},
	})
	if err != nil {
		t.Fatalf("a follower returned a transport error instead of a redirect: %v", err)
	}
	if !resp.NotLeader {
		t.Fatal("a follower accepted a write")
	}
	if raft.NodeID(resp.LeaderHint) != leader {
		t.Errorf("leader hint = %d, want %d", resp.LeaderHint, leader)
	}
}

func mustDial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecureCreds()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}
