package raft

import (
	"errors"
	"slices"
)

// Single-server membership changes (Ongaro §4.1).
//
// # Why one server at a time
//
// Changing membership by more than one server at once can split the cluster into
// two disjoint majorities — old and new — that can each elect a leader without
// the other's knowledge. Adding or removing a single server makes that
// impossible: any old majority and any new majority must overlap in at least one
// node, and that node cannot vote for two leaders in one term.
//
// # Applied on append, not on commit
//
// A configuration entry takes effect the moment it is *appended*, before it is
// committed. That is counter-intuitive and load-bearing: the entry that adds a
// node must be replicated using the new configuration, or the node being added
// would not be counted toward the quorum that commits its own addition.
//
// The consequence is that an uncommitted configuration can be *truncated away*,
// and the membership must then roll back. See [Raft.rollbackConfIfTruncated] —
// this is the part most implementations forget, and it is why some prefer to
// apply on commit and accept the availability cost instead.

// ConfChangeType is the kind of membership change.
type ConfChangeType uint8

const (
	// ConfChangeAddLearner adds a non-voting member. New nodes should join this
	// way: a learner replays the log without counting toward any quorum, so
	// adding one to a three-node cluster cannot put availability at risk while
	// it catches up. Adding it as a voter immediately would raise the quorum to
	// three of four while the fourth is empty.
	ConfChangeAddLearner ConfChangeType = iota + 1
	// ConfChangePromoteLearner makes a caught-up learner a voter.
	ConfChangePromoteLearner
	// ConfChangeAddVoter adds a member directly as a voter.
	ConfChangeAddVoter
	// ConfChangeRemoveNode removes a member entirely.
	ConfChangeRemoveNode
)

// ConfChange is one membership change.
type ConfChange struct {
	Type   ConfChangeType
	NodeID NodeID
}

var errBadConfChange = errors.New("raft: malformed configuration change")

// Encode serialises a change for the log.
//
// Hand-rolled rather than using encoding/binary, which reaches reflect and sync
// and would break the core's purity contract for the sake of nine bytes.
func (c ConfChange) Encode() []byte {
	b := make([]byte, 9)
	b[0] = byte(c.Type)
	v := uint64(c.NodeID)
	for i := range 8 {
		b[1+i] = byte(v >> (8 * i))
	}
	return b
}

// DecodeConfChange parses a change from the log.
func DecodeConfChange(b []byte) (ConfChange, error) {
	if len(b) != 9 {
		return ConfChange{}, errBadConfChange
	}
	var v uint64
	for i := range 8 {
		v |= uint64(b[1+i]) << (8 * i)
	}
	return ConfChange{Type: ConfChangeType(b[0]), NodeID: NodeID(v)}, nil
}

// applyConfChange returns the configuration that results from applying cc.
func applyConfChange(c Config, cc ConfChange) Config {
	out := Config{Voters: slices.Clone(c.Voters), Learners: slices.Clone(c.Learners)}
	drop := func(s []NodeID, id NodeID) []NodeID {
		return slices.DeleteFunc(s, func(v NodeID) bool { return v == id })
	}
	add := func(s []NodeID, id NodeID) []NodeID {
		if slices.Contains(s, id) {
			return s
		}
		return append(s, id)
	}

	switch cc.Type {
	case ConfChangeAddLearner:
		out.Voters = drop(out.Voters, cc.NodeID)
		out.Learners = add(out.Learners, cc.NodeID)
	case ConfChangePromoteLearner, ConfChangeAddVoter:
		out.Learners = drop(out.Learners, cc.NodeID)
		out.Voters = add(out.Voters, cc.NodeID)
	case ConfChangeRemoveNode:
		out.Voters = drop(out.Voters, cc.NodeID)
		out.Learners = drop(out.Learners, cc.NodeID)
	}
	slices.Sort(out.Voters)
	slices.Sort(out.Learners)
	return out
}

// ProposeConfChange appends a membership change.
func (r *Raft) ProposeConfChange(cc ConfChange) ([]Effect, error) {
	if r.role != Leader {
		return nil, ErrNotLeader
	}
	if r.transferee != None {
		return nil, ErrProposalDropped
	}

	// Guard 1: only one uncommitted change at a time.
	//
	// Guard 2: this leader must have committed an entry of its own term.
	//
	// Both are required. Single-server changes as originally published are
	// unsafe when changes overlap: a leader can begin a second change against a
	// configuration it inherited but has not yet committed, and the two can
	// produce disjoint majorities. Together these restore the invariant that
	// every configuration a leader builds on is one it knows is committed.
	if r.pendingConfIndex > r.commitIndex {
		return nil, ErrConfChangeInFlight
	}
	if !r.committedInCurrentTerm() {
		return nil, ErrConfChangeInFlight
	}

	e := Entry{
		Index: r.log.LastIndex() + 1,
		Term:  r.term,
		Type:  EntryConfChange,
		Data:  cc.Encode(),
	}
	r.log.Append(e)
	effs := []Effect{PersistEntries{Entries: []Entry{e}}}

	// Applied on append: the entry adding a node must be replicated under the
	// configuration that includes it.
	r.applyConfChangeEntry(e)

	if pr, ok := r.prs[r.id]; ok {
		pr.match = r.log.LastIndex()
		pr.next = r.log.LastIndex() + 1
	}
	effs = r.maybeCommit(effs)
	return r.broadcastAppend(effs), nil
}

// applyConfChangeEntry adopts the configuration in e, remembering the previous
// one so it can be restored if the entry is later truncated away.
func (r *Raft) applyConfChangeEntry(e Entry) {
	cc, err := DecodeConfChange(e.Data)
	if err != nil {
		return
	}
	r.prevConfig = r.cluster
	r.pendingConfIndex = e.Index
	r.cluster = applyConfChange(r.cluster, cc)
	r.syncProgress()
}

// rollbackConfIfTruncated restores the previous configuration when the entry
// that introduced the current one is discarded.
//
// Without this a node keeps a membership that no longer exists in any log —
// counting quorums against phantom members, or omitting real ones. It is the
// price of applying on append, and the reason that choice has to be made
// deliberately rather than by accident.
func (r *Raft) rollbackConfIfTruncated(from Index) {
	if r.pendingConfIndex == 0 || r.pendingConfIndex < from {
		return
	}
	r.cluster = r.prevConfig
	r.pendingConfIndex = 0
	r.syncProgress()
}

// syncProgress adds replication state for new members and drops it for departed
// ones. Only meaningful on a leader.
func (r *Raft) syncProgress() {
	if r.role != Leader || r.prs == nil {
		return
	}
	member := func(id NodeID) bool {
		return slices.Contains(r.cluster.Voters, id) || slices.Contains(r.cluster.Learners, id)
	}
	for _, id := range append(slices.Clone(r.cluster.Voters), r.cluster.Learners...) {
		if _, ok := r.prs[id]; !ok {
			r.prs[id] = &progress{next: r.log.LastIndex() + 1}
		}
	}
	// Collected and sorted before deleting, to keep the "never range a map for
	// anything order-dependent" discipline uniform even where it does not matter.
	existing := make([]NodeID, 0, len(r.prs))
	for id := range r.prs {
		existing = append(existing, id)
	}
	slices.Sort(existing)
	for _, id := range existing {
		// A leader removing *itself* keeps its own progress until it steps down.
		// It must go on replicating the entry that removes it — that entry is
		// committed by a quorum of the new configuration, which this node is
		// driving but no longer part of.
		if id == r.id {
			continue
		}
		if !member(id) {
			delete(r.prs, id)
		}
	}
}

// stepDownIfRemoved makes a leader resign once the change that removed it is
// committed.
//
// It must serve until then: the entry removing the leader is committed by the
// *old* configuration, which still includes it, so resigning early would leave
// nobody able to commit its own removal.
func (r *Raft) stepDownIfRemoved(effs []Effect) []Effect {
	if r.role != Leader || r.cluster.IsVoter(r.id) {
		return effs
	}
	if r.pendingConfIndex > r.commitIndex {
		return effs // the removal is not durable yet
	}
	r.becomeFollower(r.term, None)
	return append(effs, StateChanged{Role: r.role, Term: r.term, Leader: r.leader})
}

// Config returns the current cluster configuration.
func (r *Raft) Config() Config { return r.cluster }
