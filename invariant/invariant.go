// Package invariant checks Raft's safety properties.
//
// It is deliberately not part of the raft package: these checks need a view
// across *every* node at once, which no single node has, and they need error
// formatting, which the core's purity contract forbids.
//
// Both harnesses feed the same checker. The simulator can call it continuously
// because it holds every node's state directly; the real-cluster driver in
// Phase 7 replays the same checks over structured logs. That the two agree is
// the point — a property proven only in simulation proves only that the
// simulation is self-consistent.
package invariant

import (
	"fmt"

	"github.com/MiNam8/kvstore/raft"
)

// LogView is the read-only view of a node's log the checker needs.
// *raft.Log satisfies it.
type LogView interface {
	FirstIndex() raft.Index
	LastIndex() raft.Index
	Term(raft.Index) (raft.Term, error)
}

// Violation is a broken safety property.
type Violation struct {
	At       raft.Time
	Property string
	Detail   string
}

func (v Violation) Error() string {
	return fmt.Sprintf("%s violated at t=%dms: %s", v.Property, v.At/raft.Time(raft.Millisecond), v.Detail)
}

// Checker accumulates observations and reports the first violation of each
// property. It is not safe for concurrent use; the simulator drives it from its
// single goroutine.
type Checker struct {
	// ElectionSafety: at most one leader per term.
	leaderOfTerm map[raft.Term]raft.NodeID

	// StateMachineSafety: the entry each index was first applied with, by anyone.
	// Every later apply at that index, on any node, must match it exactly.
	appliedAt map[raft.Index]raft.Entry

	// committedBy[idx] bounds the term in which idx became committed: the
	// lowest term any node held while applying it. Leader completeness is
	// stated over *higher-numbered* terms only, so this bound is what makes the
	// check sound — see ObserveLeader.
	committedBy map[raft.Index]raft.Term

	// Per-node apply cursor, for contiguity. Reset on restart, since a
	// restarting node legitimately rebuilds its state machine from index 1.
	nextApply map[raft.NodeID]raft.Index

	// Monotonicity: a node's commit index never decreases *within one
	// incarnation*. It may legitimately regress across a restart — the Raft
	// paper lists commitIndex as volatile state (Figure 2), recovered by
	// re-learning it from the leader — so the cursor is reset on crash. Holding
	// it across a restart would report a violation for correct behaviour.
	lastCommit map[raft.NodeID]raft.Index

	// LeaderAppendOnly: what each leader's log held when last observed. A leader
	// may only extend its log; changing an entry it already had means it
	// overwrote its own history.
	leaderLog map[raft.NodeID]map[raft.Index]raft.Term

	Violations []Violation
}

// New returns an empty checker.
func New() *Checker {
	return &Checker{
		leaderOfTerm: make(map[raft.Term]raft.NodeID),
		appliedAt:    make(map[raft.Index]raft.Entry),
		committedBy:  make(map[raft.Index]raft.Term),
		nextApply:    make(map[raft.NodeID]raft.Index),
		lastCommit:   make(map[raft.NodeID]raft.Index),
		leaderLog:    make(map[raft.NodeID]map[raft.Index]raft.Term),
	}
}

func (c *Checker) fail(at raft.Time, property, format string, args ...any) error {
	v := Violation{At: at, Property: property, Detail: fmt.Sprintf(format, args...)}
	c.Violations = append(c.Violations, v)
	return v
}

// Err returns the first violation recorded, or nil.
func (c *Checker) Err() error {
	if len(c.Violations) == 0 {
		return nil
	}
	return c.Violations[0]
}

// ObserveLeader records a node claiming leadership, checking two properties.
func (c *Checker) ObserveLeader(at raft.Time, id raft.NodeID, term raft.Term, log LogView) error {
	// Election safety.
	if prev, seen := c.leaderOfTerm[term]; seen && prev != id {
		return c.fail(at, "ElectionSafety",
			"nodes %d and %d were both leader in term %d", prev, id, term)
	}
	c.leaderOfTerm[term] = id

	// Leader completeness (§5.4.3): "if a log entry is committed in a given
	// term, then that entry will be present in the logs of the leaders for all
	// higher-numbered terms."
	//
	// The "higher-numbered" clause is load-bearing and easy to drop. A node can
	// legitimately become leader in a *stale* term: stall it past an election,
	// let the cluster move on, and its buffered vote responses can still arrive
	// and carry it to leadership in a term nobody is in any more. Such a leader
	// is harmless — every follower rejects its lower term, so it can never
	// commit, and it steps down on the first reply — but it has no obligation to
	// hold entries committed after it fell behind.
	//
	// So the check applies only to entries committed strictly *below* this
	// leader's term. Without that guard it reports a violation for correct
	// behaviour; with it, this remains the check that catches a broken §5.4.2
	// commit rule or a broken election restriction, at the moment of election
	// rather than later when the data has already vanished.
	for idx, want := range c.appliedAt {
		if committedIn, ok := c.committedBy[idx]; !ok || committedIn >= term {
			continue // not committed in a lower-numbered term: no obligation
		}
		got, err := log.Term(idx)
		if err != nil {
			if idx < log.FirstIndex() {
				continue // covered by a snapshot
			}
			return c.fail(at, "LeaderCompleteness",
				"node %d became leader in term %d without entry %d, committed in term %d (%v)",
				id, term, idx, c.committedBy[idx], err)
		}
		if got != want.Term {
			return c.fail(at, "LeaderCompleteness",
				"node %d became leader in term %d holding term %d at index %d, but term %d was committed there",
				id, term, got, idx, want.Term)
		}
	}

	c.leaderLog[id] = snapshotLog(log)
	return nil
}

// ObserveApply records entries handed to a node's state machine. term is the
// applying node's current term, which bounds the term in which those entries
// became committed.
func (c *Checker) ObserveApply(at raft.Time, id raft.NodeID, term raft.Term, entries []raft.Entry) error {
	for _, e := range entries {
		// Contiguity and exactly-once, per node.
		if want, ok := c.nextApply[id]; ok && e.Index != want {
			return c.fail(at, "ApplyOrder",
				"node %d applied index %d where %d was expected", id, e.Index, want)
		}
		c.nextApply[id] = e.Index + 1

		// State machine safety: the heart of it. Two nodes applying different
		// commands at the same index means the replicas have silently diverged,
		// and every read after that point is a coin flip.
		if prev, seen := c.appliedAt[e.Index]; seen {
			if prev.Term != e.Term || string(prev.Data) != string(e.Data) {
				return c.fail(at, "StateMachineSafety",
					"index %d applied as (term %d, %q) and also as (term %d, %q) on node %d",
					e.Index, prev.Term, prev.Data, e.Term, e.Data, id)
			}
		} else {
			c.appliedAt[e.Index] = e
		}

		// An entry applied by a node in term T was committed by a leader of some
		// term <= T. Keeping the lowest such observation gives the tightest
		// sound bound on when it became committed.
		if prev, ok := c.committedBy[e.Index]; !ok || term < prev {
			c.committedBy[e.Index] = term
		}
	}
	return nil
}

// ObserveCommit records a node's commit index.
func (c *Checker) ObserveCommit(at raft.Time, id raft.NodeID, commit raft.Index) error {
	if prev, ok := c.lastCommit[id]; ok && commit < prev {
		return c.fail(at, "CommitMonotonicity",
			"node %d commit index went backwards, %d to %d, without an intervening restart",
			id, prev, commit)
	}
	c.lastCommit[id] = commit
	return nil
}

// ObserveLeaderLog checks that a leader only ever extends its log.
func (c *Checker) ObserveLeaderLog(at raft.Time, id raft.NodeID, log LogView) error {
	prev, ok := c.leaderLog[id]
	if !ok {
		c.leaderLog[id] = snapshotLog(log)
		return nil
	}
	for idx, term := range prev {
		got, err := log.Term(idx)
		if err != nil {
			if idx < log.FirstIndex() {
				continue // compacted, not deleted
			}
			return c.fail(at, "LeaderAppendOnly",
				"leader %d no longer holds index %d, which it held earlier", id, idx)
		}
		if got != term {
			return c.fail(at, "LeaderAppendOnly",
				"leader %d changed index %d from term %d to term %d — a leader may only append",
				id, idx, term, got)
		}
	}
	c.leaderLog[id] = snapshotLog(log)
	return nil
}

// ForgetLeaderLog drops the append-only baseline for a node that has stepped
// down. A former leader may legitimately have its log truncated by the new one.
func (c *Checker) ForgetLeaderLog(id raft.NodeID) { delete(c.leaderLog, id) }

// ResetApplyCursor clears a node's contiguity expectation, for a restart that
// legitimately rebuilds the state machine from the beginning.
func (c *Checker) ResetApplyCursor(id raft.NodeID) { delete(c.nextApply, id) }

// ObserveRestore records a state machine replaced wholesale by a snapshot.
//
// The restored contents are checked against what was applied elsewhere, so a
// snapshot carrying the wrong state is caught at install time rather than as a
// divergence much later. The apply cursor is then set to continue above the
// snapshot boundary — a restored node legitimately skips the prefix.
func (c *Checker) ObserveRestore(at raft.Time, id raft.NodeID, upTo raft.Index, entries []raft.Entry) error {
	for _, e := range entries {
		if prev, seen := c.appliedAt[e.Index]; seen {
			if prev.Term != e.Term || string(prev.Data) != string(e.Data) {
				return c.fail(at, "SnapshotIntegrity",
					"node %d restored index %d as (term %d, %q) but (term %d, %q) was applied elsewhere",
					id, e.Index, e.Term, e.Data, prev.Term, prev.Data)
			}
		} else {
			c.appliedAt[e.Index] = e
		}
	}
	c.nextApply[id] = upTo + 1
	return nil
}

// ResetCommitCursor clears a node's commit-monotonicity baseline. A harness
// calls this when a node crashes: commitIndex is volatile state, so a restarted
// node legitimately comes back with a lower one and re-learns the rest from the
// leader. What must *not* regress is anything durable — the term, the vote, and
// the log itself — and those are checked separately.
func (c *Checker) ResetCommitCursor(id raft.NodeID) { delete(c.lastCommit, id) }

// CheckLogMatching verifies Raft's Log Matching Property across every pair of
// nodes: if two logs hold the same term at the same index, every preceding entry
// is identical.
//
// Equivalently — and this is how it is checked — once two logs diverge at some
// index they may never agree again at a higher one. A pair that diverges and
// then re-converges means one of them spliced in entries out of order.
func (c *Checker) CheckLogMatching(at raft.Time, logs map[raft.NodeID]LogView, order []raft.NodeID) error {
	for i, a := range order {
		for _, b := range order[i+1:] {
			la, lb := logs[a], logs[b]
			if la == nil || lb == nil {
				continue
			}
			last := min(la.LastIndex(), lb.LastIndex())
			from := max(la.FirstIndex(), lb.FirstIndex())

			diverged := false
			for idx := from; idx <= last; idx++ {
				ta, ea := la.Term(idx)
				tb, eb := lb.Term(idx)
				if ea != nil || eb != nil {
					continue
				}
				if ta != tb {
					diverged = true
					continue
				}
				if diverged {
					return c.fail(at, "LogMatching",
						"nodes %d and %d diverge below index %d but agree at it (term %d)",
						a, b, idx, ta)
				}
			}
		}
	}
	return nil
}

func snapshotLog(log LogView) map[raft.Index]raft.Term {
	out := make(map[raft.Index]raft.Term)
	for idx := log.FirstIndex(); idx <= log.LastIndex(); idx++ {
		if t, err := log.Term(idx); err == nil {
			out[idx] = t
		}
	}
	return out
}
