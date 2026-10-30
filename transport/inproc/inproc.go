// Package inproc is an in-process [transport.Transport] for running a whole
// cluster inside one test binary.
//
// It is real concurrency — real goroutines, real races if any exist — but no
// sockets, so a three-node cluster starts in microseconds. That makes it the
// right tool for integration tests of the node runtime, sitting between the
// deterministic simulator (no concurrency at all) and the gRPC transport (real
// sockets, real processes).
package inproc

import (
	"sync"

	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/transport"
)

type link struct{ from, to raft.NodeID }

// Network routes messages between nodes registered with it.
type Network struct {
	mu        sync.RWMutex
	receivers map[raft.NodeID]transport.Receiver
	blocked   map[link]bool
	down      map[raft.NodeID]bool
}

// NewNetwork returns an empty network.
func NewNetwork() *Network {
	return &Network{
		receivers: make(map[raft.NodeID]transport.Receiver),
		blocked:   make(map[link]bool),
		down:      make(map[raft.NodeID]bool),
	}
}

// Register attaches a node's receiver.
func (n *Network) Register(id raft.NodeID, r transport.Receiver) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.receivers[id] = r
}

// Partition blocks traffic between the two groups, in both directions.
func (n *Network) Partition(a, b []raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, x := range a {
		for _, y := range b {
			n.blocked[link{x, y}] = true
			n.blocked[link{y, x}] = true
		}
	}
}

// Heal removes every partition.
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = make(map[link]bool)
}

// SetDown makes a node unreachable in both directions, as a crashed process is.
func (n *Network) SetDown(id raft.NodeID, down bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.down[id] = down
}

// Transport returns the transport for one node.
func (n *Network) Transport(id raft.NodeID) transport.Transport {
	return &nodeTransport{net: n, id: id}
}

type nodeTransport struct {
	net *Network
	id  raft.NodeID
}

// Send delivers a message, or silently drops it.
//
// Dropping is not an error path. Delivery is best-effort by contract, and Raft's
// own retries are the recovery mechanism; surfacing an error here would tempt a
// caller into a retry loop that duplicates what the protocol already does.
func (t *nodeTransport) Send(msg raft.Message) {
	t.net.mu.RLock()
	r, ok := t.net.receivers[msg.To]
	blocked := t.net.blocked[link{msg.From, msg.To}] ||
		t.net.down[msg.From] || t.net.down[msg.To]
	t.net.mu.RUnlock()

	if !ok || blocked {
		return
	}
	// Deliver directly: the receiver only enqueues onto its own goroutine, so
	// this neither blocks nor re-enters the sender.
	r.Deliver(msg)
}

func (t *nodeTransport) SendSnapshot(to raft.NodeID, term raft.Term, meta raft.SnapshotMeta, data []byte) {
	t.net.mu.RLock()
	r, ok := t.net.receivers[to]
	blocked := t.net.blocked[link{t.id, to}] || t.net.down[t.id] || t.net.down[to]
	t.net.mu.RUnlock()

	if !ok || blocked {
		return
	}
	r.DeliverSnapshot(t.id, term, meta, data)
}

func (t *nodeTransport) Close() error { return nil }

// IsDown reports whether a node has been marked unreachable.
func (n *Network) IsDown(id raft.NodeID) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.down[id]
}
