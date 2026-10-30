package grpc

import (
	"github.com/MiNam8/kvstore/proto/kvstorepb"
	"github.com/MiNam8/kvstore/raft"
)

// The wire format is a separate type from raft.Message on purpose: the core may
// not import protobuf (it may not import anything that reaches os), and keeping
// the conversion explicit means a field added to the protocol fails to compile
// here rather than being silently dropped on the wire.

func toProto(m raft.Message) *kvstorepb.RaftMessage {
	out := &kvstorepb.RaftMessage{
		Type:            uint32(m.Type),
		From:            uint64(m.From),
		To:              uint64(m.To),
		Term:            uint64(m.Term),
		LastLogIndex:    uint64(m.LastLogIndex),
		LastLogTerm:     uint64(m.LastLogTerm),
		PrevLogIndex:    uint64(m.PrevLogIndex),
		PrevLogTerm:     uint64(m.PrevLogTerm),
		Commit:          uint64(m.Commit),
		Reject:          m.Reject,
		MatchIndex:      uint64(m.MatchIndex),
		RejectHintIndex: uint64(m.RejectHintIndex),
		RejectHintTerm:  uint64(m.RejectHintTerm),
		ReadReqId:       m.ReadReqID,
	}
	if len(m.Entries) > 0 {
		out.Entries = make([]*kvstorepb.Entry, len(m.Entries))
		for i, e := range m.Entries {
			out.Entries[i] = &kvstorepb.Entry{
				Index: uint64(e.Index),
				Term:  uint64(e.Term),
				Type:  uint32(e.Type),
				Data:  e.Data,
			}
		}
	}
	return out
}

func fromProto(p *kvstorepb.RaftMessage) raft.Message {
	m := raft.Message{
		Type:            raft.MessageType(p.Type),
		From:            raft.NodeID(p.From),
		To:              raft.NodeID(p.To),
		Term:            raft.Term(p.Term),
		LastLogIndex:    raft.Index(p.LastLogIndex),
		LastLogTerm:     raft.Term(p.LastLogTerm),
		PrevLogIndex:    raft.Index(p.PrevLogIndex),
		PrevLogTerm:     raft.Term(p.PrevLogTerm),
		Commit:          raft.Index(p.Commit),
		Reject:          p.Reject,
		MatchIndex:      raft.Index(p.MatchIndex),
		RejectHintIndex: raft.Index(p.RejectHintIndex),
		RejectHintTerm:  raft.Term(p.RejectHintTerm),
		ReadReqID:       p.ReadReqId,
	}
	if len(p.Entries) > 0 {
		m.Entries = make([]raft.Entry, len(p.Entries))
		for i, e := range p.Entries {
			m.Entries[i] = raft.Entry{
				Index: raft.Index(e.Index),
				Term:  raft.Term(e.Term),
				Type:  raft.EntryType(e.Type),
				Data:  e.Data,
			}
		}
	}
	return m
}

func metaToProto(m raft.SnapshotMeta) *kvstorepb.SnapshotMeta {
	out := &kvstorepb.SnapshotMeta{
		LastIncludedIndex: uint64(m.LastIncludedIndex),
		LastIncludedTerm:  uint64(m.LastIncludedTerm),
	}
	for _, v := range m.Config.Voters {
		out.Voters = append(out.Voters, uint64(v))
	}
	for _, l := range m.Config.Learners {
		out.Learners = append(out.Learners, uint64(l))
	}
	return out
}

func metaFromProto(p *kvstorepb.SnapshotMeta) raft.SnapshotMeta {
	m := raft.SnapshotMeta{
		LastIncludedIndex: raft.Index(p.LastIncludedIndex),
		LastIncludedTerm:  raft.Term(p.LastIncludedTerm),
	}
	for _, v := range p.Voters {
		m.Config.Voters = append(m.Config.Voters, raft.NodeID(v))
	}
	for _, l := range p.Learners {
		m.Config.Learners = append(m.Config.Learners, raft.NodeID(l))
	}
	return m
}
