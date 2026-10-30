package server

import (
	"context"
	"errors"

	"github.com/MiNam8/kvstore/kv"
	"github.com/MiNam8/kvstore/proto/kvstorepb"
	"github.com/MiNam8/kvstore/raft"
)

// Service exposes a [Node] over gRPC.
//
// A request to a follower is answered, not failed: the response carries
// not_leader and a hint, and the client redirects. Returning a transport-level
// error instead would make "you asked the wrong node" indistinguishable from
// "the node is down", and the client would back off when it should retry
// immediately.
type Service struct {
	kvstorepb.UnimplementedKVServer
	node *Node
}

// NewService wraps a node as a gRPC KV service.
func NewService(n *Node) *Service { return &Service{node: n} }

func session(s *kvstorepb.Session) (uint64, uint64) {
	if s == nil {
		return 0, 0
	}
	return s.ClientId, s.Seq
}

// respond converts an apply result into a wire response, translating a
// not-leader rejection into a redirect rather than an error.
func respond(res kv.Result, err error) (*kvstorepb.Response, error) {
	if err == nil {
		return &kvstorepb.Response{
			Ok:        res.OK,
			Value:     res.Value,
			Found:     res.Found,
			Duplicate: res.Duplicate,
		}, nil
	}
	var nl NotLeaderError
	if errors.As(err, &nl) {
		return &kvstorepb.Response{NotLeader: true, LeaderHint: uint64(nl.Leader)}, nil
	}
	// Anything else — most often a context deadline — is genuinely
	// indeterminate and must surface as an error so the client retries with the
	// same sequence number rather than assuming failure.
	return nil, err
}

func (s *Service) Put(ctx context.Context, req *kvstorepb.PutRequest) (*kvstorepb.Response, error) {
	id, seq := session(req.Session)
	return respond(s.node.Apply(ctx, kv.Command{
		Op: kv.OpPut, Key: req.Key, Value: req.Value, ClientID: id, Seq: seq,
	}))
}

func (s *Service) Get(ctx context.Context, req *kvstorepb.GetRequest) (*kvstorepb.Response, error) {
	id, seq := session(req.Session)
	return respond(s.node.Read(ctx, req.Key, id, seq))
}

func (s *Service) Delete(ctx context.Context, req *kvstorepb.DeleteRequest) (*kvstorepb.Response, error) {
	id, seq := session(req.Session)
	return respond(s.node.Apply(ctx, kv.Command{
		Op: kv.OpDelete, Key: req.Key, ClientID: id, Seq: seq,
	}))
}

func (s *Service) CompareAndSwap(ctx context.Context, req *kvstorepb.CASRequest) (*kvstorepb.Response, error) {
	id, seq := session(req.Session)
	return respond(s.node.Apply(ctx, kv.Command{
		Op: kv.OpCAS, Key: req.Key, Value: req.Value,
		Expected: req.Expected, HasExpected: req.HasExpected,
		ClientID: id, Seq: seq,
	}))
}

func (s *Service) Status(context.Context, *kvstorepb.StatusRequest) (*kvstorepb.StatusResponse, error) {
	st := s.node.Stats()
	return &kvstorepb.StatusResponse{
		Id:             uint64(st.ID),
		Role:           st.Role.String(),
		Term:           uint64(st.Term),
		Leader:         uint64(st.Leader),
		CommitIndex:    uint64(st.CommitIndex),
		AppliedIndex:   uint64(st.AppliedIndex),
		Elections:      uint64(st.Elections),
		Applied:        uint64(st.Applied),
		Batches:        uint64(st.Batches),
		BatchedEntries: uint64(st.BatchedEntries),
		MaxBatch:       uint64(st.MaxBatchSeen),
		Proposals:      uint64(st.Proposals),
	}, nil
}

// LeaderHint exposes the node's current leader belief for the HTTP shim.
func (s *Service) LeaderHint() raft.NodeID { return s.node.Leader() }
