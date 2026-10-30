// Package grpc is the gRPC [transport.Transport] used between real nodes.
package grpc

import (
	"context"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/MiNam8/kvstore/proto/kvstorepb"
	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/transport"
)

// sendQueue bounds how many messages may be waiting for one peer.
//
// When it fills, messages are dropped rather than queued. That is the correct
// behaviour, not a compromise: an unbounded queue in front of a slow peer turns
// memory pressure into an outage, and every message here is one Raft will resend
// on its next heartbeat anyway. Blocking would be worse still — it would let one
// unreachable follower stall the leader's entire event loop.
const sendQueue = 512

// Transport sends Raft messages to peers over gRPC.
type Transport struct {
	self  raft.NodeID
	peers map[raft.NodeID]*peer

	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup
}

var _ transport.Transport = (*Transport)(nil)

type peer struct {
	id   raft.NodeID
	addr string
	out  chan raft.Message
	snap chan snapshotJob

	conn   *grpc.ClientConn
	client kvstorepb.RaftServiceClient
}

type snapshotJob struct {
	term raft.Term
	meta raft.SnapshotMeta
	data []byte
}

// New builds a transport for self, with addrs mapping every peer to a host:port.
func New(self raft.NodeID, addrs map[raft.NodeID]string) (*Transport, error) {
	t := &Transport{self: self, peers: make(map[raft.NodeID]*peer)}
	for id, addr := range addrs {
		if id == self {
			continue
		}
		// grpc.NewClient connects lazily and reconnects on its own, so a peer
		// that is down at startup is not a startup failure — it is just a peer
		// that is down, which Raft already tolerates.
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Close()
			return nil, err
		}
		p := &peer{
			id: id, addr: addr, conn: conn,
			client: kvstorepb.NewRaftServiceClient(conn),
			out:    make(chan raft.Message, sendQueue),
			snap:   make(chan snapshotJob, 2),
		}
		t.peers[id] = p
		t.wg.Add(1)
		go t.run(p)
	}
	return t, nil
}

// run is one peer's sender goroutine. Keeping the network off the caller's
// goroutine is what lets Send be non-blocking.
func (t *Transport) run(p *peer) {
	defer t.wg.Done()
	for {
		select {
		case m, ok := <-p.out:
			if !ok {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			// The error is deliberately ignored: a failed send is
			// indistinguishable from a dropped packet, which the protocol
			// already handles.
			_, _ = p.client.Step(ctx, toProto(m))
			cancel()

		case job := <-p.snap:
			t.sendSnapshot(p, job)
		}
	}
}

func (t *Transport) sendSnapshot(p *peer, job snapshotJob) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := p.client.InstallSnapshot(ctx)
	if err != nil {
		return
	}
	const chunkSize = 1 << 20
	meta := metaToProto(job.meta)
	for off := 0; off < len(job.data) || off == 0; off += chunkSize {
		end := min(off+chunkSize, len(job.data))
		if err := stream.Send(&kvstorepb.SnapshotChunk{
			From: uint64(t.self),
			Term: uint64(job.term),
			Meta: meta,
			Data: job.data[off:end],
		}); err != nil {
			return
		}
		if end == len(job.data) {
			break
		}
	}
	_, _ = stream.CloseAndRecv()
}

// Send queues a message for its destination, dropping it if the queue is full.
func (t *Transport) Send(msg raft.Message) {
	p, ok := t.peers[msg.To]
	if !ok {
		return
	}
	select {
	case p.out <- msg:
	default: // queue full: drop, and let Raft's retries recover
	}
}

// SendSnapshot queues a snapshot for out-of-band transfer, so a multi-megabyte
// payload never sits in front of the heartbeats that keep leadership alive.
func (t *Transport) SendSnapshot(to raft.NodeID, term raft.Term, meta raft.SnapshotMeta, data []byte) {
	p, ok := t.peers[to]
	if !ok {
		return
	}
	select {
	case p.snap <- snapshotJob{term: term, meta: meta, data: data}:
	default:
	}
}

// Close shuts down every peer connection.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.mu.Unlock()

	for _, p := range t.peers {
		close(p.out)
	}
	t.wg.Wait()
	for _, p := range t.peers {
		_ = p.conn.Close()
	}
	return nil
}

// Service is the server half: it receives peer messages and hands them to a
// [transport.Receiver].
type Service struct {
	kvstorepb.UnimplementedRaftServiceServer
	recv transport.Receiver
}

// NewService wraps a receiver as a gRPC service.
func NewService(recv transport.Receiver) *Service { return &Service{recv: recv} }

// Step delivers one Raft message.
func (s *Service) Step(_ context.Context, m *kvstorepb.RaftMessage) (*kvstorepb.Ack, error) {
	s.recv.Deliver(fromProto(m))
	return &kvstorepb.Ack{}, nil
}

// InstallSnapshot reassembles a streamed snapshot and delivers it.
func (s *Service) InstallSnapshot(stream kvstorepb.RaftService_InstallSnapshotServer) error {
	var (
		meta raft.SnapshotMeta
		data []byte
		from raft.NodeID
		term raft.Term
		seen bool
	)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if chunk.Meta != nil {
			meta, seen = metaFromProto(chunk.Meta), true
			from, term = raft.NodeID(chunk.From), raft.Term(chunk.Term)
		}
		data = append(data, chunk.Data...)
	}
	if seen {
		s.recv.DeliverSnapshot(from, term, meta, data)
	}
	return stream.SendAndClose(&kvstorepb.Ack{})
}
