// Package transport carries Raft messages between nodes.
//
// The interface lives outside the raft package because the core may not touch
// a network. Two implementations exist: a simulated one that applies seeded
// drop, delay, duplicate, reorder and partition faults, and a gRPC one.
package transport

import "github.com/MiNam8/kvstore/raft"

// Transport delivers Raft messages to peers.
//
// Delivery is explicitly best-effort and unreliable: an implementation may drop,
// delay, duplicate or reorder any message, and the protocol tolerates all four.
// Send therefore returns no error worth acting on — a failed send is
// indistinguishable from a dropped packet, and Raft's own retry logic (heartbeats
// and nextIndex backtracking) is the recovery mechanism. Adding delivery
// guarantees here would hide protocol bugs rather than prevent them, which is why
// the simulator injects all four faults deliberately.
type Transport interface {
	// Send transmits msg to msg.To. It must not block the caller.
	Send(msg raft.Message)

	// SendSnapshot transmits a snapshot payload out of band, since snapshots
	// are far larger than protocol messages and must not stall replication.
	SendSnapshot(to raft.NodeID, meta raft.SnapshotMeta, data []byte)

	// Close releases resources.
	Close() error
}

// Receiver handles messages arriving from peers. The node runtime implements
// this and funnels every message into the single goroutine that owns the core.
type Receiver interface {
	Deliver(msg raft.Message)
	DeliverSnapshot(meta raft.SnapshotMeta, data []byte)
}
