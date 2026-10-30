// Package sim is the deterministic simulation harness.
//
// A whole cluster runs inside one goroutine against a virtual clock. Time
// advances only by popping the next event from a priority queue ordered by
// (time, insertion sequence), so a run is a pure function of its seed: no wall
// clock, no real concurrency, no scheduler involvement. The same seed replays
// the same execution, event for event — which is what makes a failure here a
// complete bug report rather than a story about something that happened once.
//
// Safety is checked continuously; liveness is only checked after the cluster is
// healed. That split is deliberate. Under an arbitrary partition a Raft cluster
// is *supposed* to stop making progress, so demanding progress during chaos
// would be demanding a bug. Safety, by contrast, must hold at every instant no
// matter what the network does.
package sim

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"strconv"

	"github.com/MiNam8/kvstore/invariant"
	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/storage"
	"github.com/MiNam8/kvstore/storage/mem"
	"github.com/MiNam8/kvstore/storage/wal"
)

// Options configures a simulated cluster.
type Options struct {
	Nodes int
	Seed  uint64

	ElectionTimeout   raft.Duration
	HeartbeatInterval raft.Duration
	// TickInterval is the runtime's timer granularity. It must be well below
	// ElectionTimeout, or timeouts are detected so coarsely that the election
	// jitter is quantised away and vote splits become artificially common.
	TickInterval raft.Duration
	// Latency is the base one-way message delay.
	Latency raft.Duration
	// ProposeInterval, if positive, drives a background write workload.
	ProposeInterval raft.Duration
	// AuditInterval is how often the expensive cross-node invariants — log
	// matching and leader-append-only — are swept. The cheap ones are checked on
	// every relevant event.
	AuditInterval raft.Duration

	Faults Faults

	// ChaosInterval, if positive, rolls one adversarial action that often:
	// partition, one-way partition, heal, crash, restart or stall.
	ChaosInterval raft.Duration

	// MaxClockDriftPPM gives each node a constant clock rate error of up to this
	// many parts per million, in either direction. Rate error, not offset:
	// Raft only ever measures elapsed time, so a constant offset is invisible to
	// it while a rate difference genuinely changes who times out first.
	MaxClockDriftPPM int64

	// Learners starts this many additional nodes as non-voting members, so the
	// chaos driver can promote and demote them. Membership changes are the
	// subtlest correctness case in Raft, which makes them the one most worth
	// running under the fault injector rather than only under hand-written tests.
	Learners int

	// PreVote and CheckQuorum enable §9.6. They are options rather than defaults
	// so their effect on term churn can be measured.
	PreVote     bool
	CheckQuorum bool

	// SnapshotThreshold, if positive, makes a node snapshot and compact once its
	// in-memory log exceeds this many entries. Set it low and most follower
	// catch-up goes through InstallSnapshot rather than replication, which is
	// the only way that path gets exercised often enough to trust.
	SnapshotThreshold int

	// WALDir, if set, backs every node with a real segmented write-ahead log in
	// a subdirectory rather than in-memory storage. A simulated crash then goes
	// through the same fsync, replay and torn-write recovery a real node uses,
	// which is what stops the simulation from only proving things about the
	// simulation. Much slower, so it is opt-in.
	WALDir string
}

// DefaultOptions returns a quiet, fault-free configuration.
func DefaultOptions(n int, seed uint64) Options {
	return Options{
		Nodes:             n,
		Seed:              seed,
		ElectionTimeout:   300 * raft.Millisecond,
		HeartbeatInterval: 50 * raft.Millisecond,
		TickInterval:      5 * raft.Millisecond,
		Latency:           10 * raft.Millisecond,
		ProposeInterval:   20 * raft.Millisecond,
		AuditInterval:     500 * raft.Millisecond,
	}
}

// ChaosOptions returns a configuration under continuous network and node faults.
func ChaosOptions(n int, seed uint64) Options {
	o := DefaultOptions(n, seed)
	o.Faults = DefaultFaults()
	o.ChaosInterval = 400 * raft.Millisecond
	o.MaxClockDriftPPM = 50_000 // ±5%
	return o
}

// MembershipOptions is ChaosOptions plus learners and pre-vote, so membership
// changes churn continuously alongside every other fault.
//
// Membership changes are the subtlest correctness case in Raft — a configuration
// takes effect on append, so it can be truncated away and must roll back, and
// two overlapping changes can produce disjoint majorities. That is exactly the
// class of bug hand-written tests miss and a fault injector finds.
func MembershipOptions(n int, seed uint64) Options {
	o := ChaosOptions(n, seed)
	o.Learners = 2
	o.PreVote, o.CheckQuorum = true, true
	return o
}

// SnapshotOptions is ChaosOptions with compaction turned up so aggressively
// that snapshots, not replication, become the normal way a lagging node catches
// up.
func SnapshotOptions(n int, seed uint64) Options {
	o := ChaosOptions(n, seed)
	o.SnapshotThreshold = 30
	return o
}

type eventKind uint8

const (
	evTick eventKind = iota
	evDeliver
	evPropose
	evChaos
	evAudit
)

type event struct {
	at   raft.Time
	seq  uint64 // insertion order: makes the ordering total, and therefore replayable
	kind eventKind
	to   raft.NodeID
	msg  raft.Message
}

type queue []*event

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)   { *q = append(*q, x.(*event)) }
func (q *queue) Pop() any     { old := *q; n := len(old); it := old[n-1]; *q = old[:n-1]; return it }

type node struct {
	id raft.NodeID

	// rf is the volatile consensus state. A crash discards it entirely; a
	// restart builds a fresh one from store and nothing else.
	rf *raft.Raft

	// store is the durable state. It is the *only* thing that survives a crash,
	// which is what makes a simulated restart exercise the real recovery path.
	store storage.Storage

	// sm is the state machine, rebuilt from the log on restart.
	sm []raft.Entry

	crashed      bool
	stalledUntil raft.Time
	driftPPM     int64
	rnd          *raft.SplitMix64
}

// Cluster is a simulated Raft cluster on a virtual clock.
type Cluster struct {
	opts  Options
	ids   []raft.NodeID
	nodes map[raft.NodeID]*node

	q      queue
	now    raft.Time
	seq    uint64
	events int

	netRnd   *raft.SplitMix64
	chaosRnd *raft.SplitMix64

	blocked map[link]bool

	check     *invariant.Checker
	violation error

	trace uint64

	// Elections records every leadership change observed.
	Elections []Election
	// Acked counts proposals a leader accepted.
	Acked int
	// ChaosLog records the adversarial actions taken, for reading a failure.
	ChaosLog []string

	// ConfChanges counts membership changes accepted.
	ConfChanges int

	// Snapshots counts snapshots taken; Restores counts installs on a follower.
	// Tests assert these are non-zero: a snapshot path that never runs is a
	// snapshot path that is not being tested.
	Snapshots int
	Restores  int

	proposals uint64
}

// Election records one node claiming leadership in a term.
type Election struct {
	At     raft.Time
	Leader raft.NodeID
	Term   raft.Term
}

// New builds a cluster of opts.Nodes nodes with ids 1..n.
func New(opts Options) (*Cluster, error) {
	if opts.AuditInterval <= 0 {
		opts.AuditInterval = 500 * raft.Millisecond
	}
	c := &Cluster{
		opts:    opts,
		nodes:   make(map[raft.NodeID]*node, opts.Nodes),
		blocked: make(map[link]bool),
		check:   invariant.New(),
		trace:   fnv.New64a().Sum64(),
	}

	var voters, learners []raft.NodeID
	for i := 1; i <= opts.Nodes; i++ {
		voters = append(voters, raft.NodeID(i))
	}
	for i := opts.Nodes + 1; i <= opts.Nodes+opts.Learners; i++ {
		learners = append(learners, raft.NodeID(i))
	}
	c.ids = append(append([]raft.NodeID{}, voters...), learners...)
	cluster := raft.Config{Voters: voters, Learners: learners}

	// Streams are forked in a fixed order so that adding a fault source does not
	// reshuffle every unrelated random draw, keeping seeds stable as the
	// harness grows.
	root := raft.NewRand(opts.Seed)
	c.netRnd = root.Fork()
	c.chaosRnd = root.Fork()

	// Learners get nodes too: they run the protocol and receive the log, they
	// simply do not count toward a quorum.
	for _, id := range c.ids {
		store, err := newStore(opts, id, cluster)
		if err != nil {
			return nil, err
		}
		n := &node{id: id, store: store, rnd: root.Fork()}
		if opts.MaxClockDriftPPM > 0 {
			n.driftPPM = int64(n.rnd.Intn(int(2*opts.MaxClockDriftPPM+1))) - opts.MaxClockDriftPPM
		}
		rf, err := raft.New(c.raftOptions(n))
		if err != nil {
			return nil, err
		}
		n.rf = rf
		c.nodes[id] = n
	}

	// Stagger first ticks so nodes do not run in lockstep, which would quantise
	// away the election jitter the protocol depends on.
	for _, id := range c.ids {
		c.schedule(raft.Time(root.Intn(int(opts.TickInterval))), &event{kind: evTick, to: id})
	}
	if opts.ProposeInterval > 0 {
		c.schedule(raft.Time(opts.ProposeInterval), &event{kind: evPropose})
	}
	if opts.ChaosInterval > 0 {
		c.schedule(raft.Time(opts.ChaosInterval), &event{kind: evChaos})
	}
	c.schedule(raft.Time(opts.AuditInterval), &event{kind: evAudit})
	return c, nil
}

// raftOptions builds the constructor arguments for a node, recovering whatever
// its storage holds. This is the restart path.
// newStore builds a node's durable storage.
func newStore(opts Options, id raft.NodeID, cluster raft.Config) (storage.Storage, error) {
	if opts.WALDir == "" {
		return mem.New(cluster), nil
	}
	// A small segment size so compaction and rollover are exercised without
	// writing megabytes per node.
	return wal.OpenSize(filepath.Join(opts.WALDir, "node"+strconv.FormatUint(uint64(id), 10)),
		cluster, 64<<10)
}

func (c *Cluster) raftOptions(n *node) raft.Options {
	hs, cluster, log, err := storage.RecoverState(n.store)
	if err != nil {
		panic("sim: recovery failed: " + err.Error())
	}
	return raft.Options{
		ID:                n.id,
		Cluster:           cluster,
		ElectionTimeout:   c.opts.ElectionTimeout,
		HeartbeatInterval: c.opts.HeartbeatInterval,
		PreVote:           c.opts.PreVote,
		CheckQuorum:       c.opts.CheckQuorum,
		Rand:              n.rnd,
		Log:               log,
		HardState:         hs,
		Now:               c.nodeTime(n),
	}
}

// nodeTime is the clock as this node reads it, including its rate drift.
func (c *Cluster) nodeTime(n *node) raft.Time {
	if n.driftPPM == 0 {
		return c.now
	}
	return c.now + raft.Time(int64(c.now)*n.driftPPM/1_000_000)
}

func (c *Cluster) schedule(delay raft.Time, ev *event) {
	ev.at = c.now + delay
	ev.seq = c.seq
	c.seq++
	heap.Push(&c.q, ev)
}

// Now returns the current virtual time.
func (c *Cluster) Now() raft.Time { return c.now }

// TraceHash returns the running hash of every observable event so far.
func (c *Cluster) TraceHash() uint64 { return c.trace }

// Events returns how many events have been processed.
func (c *Cluster) Events() int { return c.events }

// Node returns a node's current Raft instance.
func (c *Cluster) Node(id raft.NodeID) *raft.Raft { return c.nodes[id].rf }

// IDs returns node ids in sorted order.
func (c *Cluster) IDs() []raft.NodeID { return c.ids }

// Checker exposes the invariant checker.
func (c *Cluster) Checker() *invariant.Checker { return c.check }

// Violation returns the first safety violation seen, or nil.
func (c *Cluster) Violation() error { return c.violation }

// Stop crashes a node: all volatile state is discarded, durable state is kept.
func (c *Cluster) Stop(id raft.NodeID) {
	n := c.nodes[id]
	if n.crashed {
		return
	}
	n.crashed = true
	// The state machine is volatile: it is rebuilt by replaying the log. Its
	// apply cursor resets with it, which the checker is told about explicitly so
	// a legitimate rebuild is not mistaken for a gap.
	n.sm = nil
	c.check.ResetApplyCursor(id)
	c.check.ResetCommitCursor(id)
	c.check.ForgetLeaderLog(id)
	c.hash("crash", uint64(id))
}

// Start restarts a crashed node, rebuilding it from durable state alone.
func (c *Cluster) Start(id raft.NodeID) {
	n := c.nodes[id]
	if !n.crashed {
		return
	}
	n.crashed = false
	n.stalledUntil = 0
	rf, err := raft.New(c.raftOptions(n))
	if err != nil {
		panic("sim: restart failed: " + err.Error())
	}
	n.rf = rf

	// Rebuild the state machine from the durable snapshot before replaying the
	// log above it. Recovering the log alone would leave a node whose applied
	// index claims a prefix its state machine never saw.
	if meta, data, err := n.store.LoadSnapshot(); err == nil {
		n.sm = decodeEntries(data)
		c.record(c.check.ObserveRestore(c.now, id, meta.LastIncludedIndex, n.sm))
	}
	c.hash("restart", uint64(id))
	c.schedule(raft.Time(c.opts.TickInterval), &event{kind: evTick, to: id})
}

// Stall freezes a node for d, as a long garbage collection pause or a SIGSTOP
// would. Messages sent to it queue up and arrive in a burst when it wakes.
//
// This is the fault most simulators omit and the one that breaks leader leases
// in Phase 9: a node can be frozen well past the point where it believes its
// lease is still valid.
func (c *Cluster) Stall(id raft.NodeID, d raft.Duration) {
	n := c.nodes[id]
	if n.crashed {
		return
	}
	if until := c.now + raft.Time(d); until > n.stalledUntil {
		n.stalledUntil = until
	}
	c.hash("stall", uint64(id), uint64(d))
}

// Partition splits the cluster into groups that cannot reach each other.
func (c *Cluster) Partition(groups ...[]raft.NodeID) {
	where := make(map[raft.NodeID]int, len(c.ids))
	for gi, g := range groups {
		for _, id := range g {
			where[id] = gi
		}
	}
	for _, a := range c.ids {
		for _, b := range c.ids {
			if a != b && where[a] != where[b] {
				c.blocked[link{a, b}] = true
			}
		}
	}
	c.hash("partition", uint64(len(groups)))
}

// PartitionOneWay blocks traffic from → to but leaves the reverse direction
// working. A node that can send but not receive still looks alive to itself
// while being invisible to everyone else, and it is the asymmetry that breaks
// naive leader-liveness assumptions.
func (c *Cluster) PartitionOneWay(from, to raft.NodeID) {
	c.blocked[link{from, to}] = true
	c.hash("oneway", uint64(from), uint64(to))
}

// Heal removes every partition.
func (c *Cluster) Heal() {
	c.blocked = make(map[link]bool)
	c.hash("heal")
}

// Close releases every node's storage. Only meaningful for WAL-backed clusters.
func (c *Cluster) Close() {
	for _, id := range c.ids {
		_ = c.nodes[id].store.Close()
	}
}

// StopChaos retires the adversary. Liveness can only be asserted after this:
// HealAll repairs the cluster once, but a still-running chaos driver will break
// it again on its next roll, and a test that heals without retiring the
// adversary is asserting progress *during* chaos — which for a partitioned Raft
// cluster is asserting a bug.
func (c *Cluster) StopChaos() { c.opts.ChaosInterval = 0 }

// Crashed reports whether a node is currently down. A crashed node's Raft
// instance is retained for inspection but is not being driven, so its volatile
// state is frozen at the moment of the crash.
func (c *Cluster) Crashed(id raft.NodeID) bool { return c.nodes[id].crashed }

// LiveIDs returns the ids of nodes that are not crashed.
func (c *Cluster) LiveIDs() []raft.NodeID {
	var out []raft.NodeID
	for _, id := range c.ids {
		if !c.nodes[id].crashed {
			out = append(out, id)
		}
	}
	return out
}

// HealAll removes every partition, restarts every crashed node, and clears
// every stall. It does not retire the chaos driver — see [Cluster.StopChaos].
func (c *Cluster) HealAll() {
	c.Heal()
	for _, id := range c.ids {
		c.nodes[id].stalledUntil = 0
		c.Start(id)
	}
}

// Leaders returns every live node claiming leadership, with its term.
func (c *Cluster) Leaders() map[raft.NodeID]raft.Term {
	out := make(map[raft.NodeID]raft.Term)
	for _, id := range c.ids { // sorted: never range the map
		n := c.nodes[id]
		if !n.crashed && n.rf.Role() == raft.Leader {
			out[id] = n.rf.Term()
		}
	}
	return out
}

// RunFor advances virtual time by d. It stops early if a safety violation is
// detected, so the reproduction is as short as possible.
func (c *Cluster) RunFor(d raft.Duration) {
	deadline := c.now + raft.Time(d)
	for len(c.q) > 0 && c.q[0].at <= deadline && c.violation == nil {
		c.step()
	}
	if c.violation == nil {
		c.now = deadline
	}
}

func (c *Cluster) step() {
	ev := heap.Pop(&c.q).(*event)
	c.now = ev.at
	c.events++
	n := c.nodes[ev.to] // nil for events targeting no particular node

	switch ev.kind {
	case evTick:
		if n.crashed {
			return // a crashed node is rescheduled by Start
		}
		if c.now < n.stalledUntil {
			// Frozen: skip the work but keep the timer alive so it resumes.
			c.schedule(raft.Time(c.opts.TickInterval), &event{kind: evTick, to: ev.to})
			return
		}
		c.hash("tick", uint64(ev.to))
		c.deliver(n, n.rf.Tick(c.nodeTime(n)))
		c.schedule(raft.Time(c.opts.TickInterval), &event{kind: evTick, to: ev.to})

	case evDeliver:
		if n.crashed {
			return // a crash drops in-flight messages, as a real one does
		}
		if c.now < n.stalledUntil {
			// Buffered by the kernel while the process is frozen, then delivered
			// in a burst — a pile of stale messages arriving at once, which is
			// exactly the adversary a stalled node faces on waking.
			c.schedule(n.stalledUntil-c.now, ev)
			return
		}
		c.hash("recv", uint64(ev.to), uint64(ev.msg.Type), uint64(ev.msg.From), uint64(ev.msg.Term))
		c.deliver(n, n.rf.Step(ev.msg))

	case evPropose:
		if c.opts.ProposeInterval <= 0 {
			return
		}
		c.proposals++
		c.Propose(uint64Bytes(c.proposals))
		c.schedule(raft.Time(c.opts.ProposeInterval), &event{kind: evPropose})

	case evChaos:
		if c.opts.ChaosInterval <= 0 {
			return // adversary retired
		}
		c.rollChaos()
		c.schedule(raft.Time(c.opts.ChaosInterval), &event{kind: evChaos})

	case evAudit:
		c.audit()
		c.schedule(raft.Time(c.opts.AuditInterval), &event{kind: evAudit})
	}
}

// deliver executes an effect batch on behalf of a node, strictly in order.
//
// Durability barriers are applied to the node's storage synchronously, before
// any later SendMessage is transmitted — the ordering rule the whole design
// rests on. Phase 4 puts a real write-ahead log behind the same calls and starts
// injecting torn writes at exactly this seam.
func (c *Cluster) deliver(n *node, effs []raft.Effect) {
	for _, e := range effs {
		switch eff := e.(type) {
		case raft.PersistHardState:
			c.mustPersist(n, "SetHardState", n.store.SetHardState(eff.HardState))
			c.hash("hard", uint64(n.id), uint64(eff.HardState.Term), uint64(eff.HardState.VotedFor))

		case raft.PersistEntries:
			c.mustPersist(n, "Append", n.store.Append(eff.Entries))
			c.hash("append", uint64(n.id), uint64(len(eff.Entries)))

		case raft.TruncateSuffix:
			c.mustPersist(n, "TruncateSuffix", n.store.TruncateSuffix(eff.FromIndex))
			c.hash("truncate", uint64(n.id), uint64(eff.FromIndex))

		case raft.PersistSnapshot:
			c.mustPersist(n, "SaveSnapshot", n.store.SaveSnapshot(eff.Meta, eff.Data))

		case raft.CompactLog:
			c.mustPersist(n, "TruncatePrefix", n.store.TruncatePrefix(eff.UpToIndex))

		case raft.SendMessage:
			m := eff.Message
			// The core names a snapshot but does not carry it; the runtime loads
			// the bytes from storage. Keeping payloads out of the core keeps
			// them out of every effect batch and every simulator trace.
			if m.Type == raft.MsgSnap {
				meta, data, err := n.store.LoadSnapshot()
				if err != nil {
					continue // nothing to send yet
				}
				m.Snapshot = &meta
				m.SnapshotData = data
			}
			c.hash("send", uint64(n.id), uint64(m.Type), uint64(m.To), uint64(m.Term))
			c.transmit(m)

		case raft.RestoreStateMachine:
			restored := decodeEntries(eff.Data)
			n.sm = restored
			c.Restores++
			c.hash("restore", uint64(n.id), uint64(eff.Meta.LastIncludedIndex))
			c.record(c.check.ObserveRestore(c.now, n.id, eff.Meta.LastIncludedIndex, restored))

		case raft.ApplyEntries:
			n.sm = append(n.sm, eff.Entries...)
			for _, ent := range eff.Entries {
				c.hash("apply", uint64(n.id), uint64(ent.Index), uint64(ent.Term))
			}
			c.record(c.check.ObserveApply(c.now, n.id, n.rf.Term(), eff.Entries))

		case raft.StateChanged:
			c.hash("state", uint64(n.id), uint64(eff.Role), uint64(eff.Term))
			if eff.Role == raft.Leader {
				c.Elections = append(c.Elections, Election{At: c.now, Leader: n.id, Term: eff.Term})
				c.record(c.check.ObserveLeader(c.now, n.id, eff.Term, n.rf.Log()))
			} else {
				c.check.ForgetLeaderLog(n.id)
			}
		}
	}
	c.record(c.check.ObserveCommit(c.now, n.id, n.rf.CommitIndex()))
	c.maybeSnapshot(n)
}

// maybeSnapshot compacts a node whose log has grown past the threshold.
func (c *Cluster) maybeSnapshot(n *node) {
	if c.opts.SnapshotThreshold <= 0 || n.crashed {
		return
	}
	if n.rf.Log().Len() < c.opts.SnapshotThreshold {
		return
	}
	applied := n.rf.AppliedIndex()
	if applied <= n.rf.Log().FirstIndex()-1 {
		return // nothing new has been applied since the last snapshot
	}
	effs, err := n.rf.Snapshot(applied, encodeEntries(n.sm))
	if err != nil {
		return
	}
	c.Snapshots++
	c.hash("snapshot", uint64(n.id), uint64(applied))
	for _, e := range effs {
		switch eff := e.(type) {
		case raft.PersistSnapshot:
			_ = n.store.SaveSnapshot(eff.Meta, eff.Data)
		case raft.CompactLog:
			_ = n.store.TruncatePrefix(eff.UpToIndex)
		}
	}
}

// The simulated state machine is the applied entry list, so a snapshot is just
// that list serialised. It is a test double, but it round-trips through the same
// PersistSnapshot / RestoreStateMachine path a real state machine uses.
func encodeEntries(entries []raft.Entry) []byte {
	buf := make([]byte, 0, len(entries)*24)
	for _, e := range entries {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Index))
		buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Term))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Data)))
		buf = append(buf, e.Data...)
	}
	return buf
}

func decodeEntries(b []byte) []raft.Entry {
	var out []raft.Entry
	for len(b) >= 20 {
		e := raft.Entry{
			Index: raft.Index(binary.LittleEndian.Uint64(b[0:8])),
			Term:  raft.Term(binary.LittleEndian.Uint64(b[8:16])),
		}
		n := int(binary.LittleEndian.Uint32(b[16:20]))
		if 20+n > len(b) {
			return out
		}
		if n > 0 {
			e.Data = append([]byte(nil), b[20:20+n]...)
		}
		out = append(out, e)
		b = b[20+n:]
	}
	return out
}

// transmit applies the network fault model and schedules delivery.
func (c *Cluster) transmit(m raft.Message) {
	if c.blocked[link{m.From, m.To}] {
		return
	}
	f := c.opts.Faults
	if f.DropPerMille > 0 && c.netRnd.Intn(1000) < f.DropPerMille {
		return
	}

	c.scheduleDelivery(m)
	// A duplicate is drawn its own independent delay, so the copies can arrive
	// in either order — which is what makes duplication a real test of the
	// truncation rule rather than a formality.
	if f.DuplicatePerMille > 0 && c.netRnd.Intn(1000) < f.DuplicatePerMille {
		c.scheduleDelivery(m)
	}
}

func (c *Cluster) scheduleDelivery(m raft.Message) {
	f := c.opts.Faults
	delay := int64(c.opts.Latency)

	if f.JitterPerMille > 0 {
		// Uniform in [base*(1-j), base*(1+j)]. Varying delay is what produces
		// reordering; no separate reorder fault is needed.
		span := delay * int64(f.JitterPerMille) / 1000
		delay += int64(c.netRnd.Intn(int(2*span+1))) - span
	}
	if f.SlowPerMille > 0 && f.SlowFactor > 1 && c.netRnd.Intn(1000) < f.SlowPerMille {
		delay *= int64(f.SlowFactor)
	}
	if delay < 1 {
		delay = 1
	}
	c.schedule(raft.Time(delay), &event{kind: evDeliver, to: m.To, msg: m})
}

// rollChaos performs one adversarial action.
func (c *Cluster) rollChaos() {
	switch chaosAction(c.chaosRnd.Intn(int(chaosActionCount))) {
	case chaosPartition:
		// Split at a random point, so both majority and minority partitions occur.
		cut := 1 + c.chaosRnd.Intn(len(c.ids)-1)
		c.Partition(c.ids[:cut], c.ids[cut:])
		c.logChaos("partition %v | %v", c.ids[:cut], c.ids[cut:])

	case chaosOneWayPartition:
		a := c.ids[c.chaosRnd.Intn(len(c.ids))]
		b := c.ids[c.chaosRnd.Intn(len(c.ids))]
		if a != b {
			c.PartitionOneWay(a, b)
			c.logChaos("one-way partition %d -> %d", a, b)
		}

	case chaosHeal:
		c.Heal()
		c.logChaos("heal")

	case chaosCrash:
		id := c.ids[c.chaosRnd.Intn(len(c.ids))]
		c.Stop(id)
		c.logChaos("crash %d", id)

	case chaosRestart:
		id := c.ids[c.chaosRnd.Intn(len(c.ids))]
		if c.nodes[id].crashed {
			c.Start(id)
			c.logChaos("restart %d", id)
		}

	case chaosConfChange:
		c.rollConfChange()

	case chaosStall:
		id := c.ids[c.chaosRnd.Intn(len(c.ids))]
		// Up to twice the election timeout: long enough that the cluster gives
		// up on this node entirely and elects around it.
		d := raft.Duration(c.chaosRnd.Intn(int(2*c.opts.ElectionTimeout))) + c.opts.ElectionTimeout/10
		c.Stall(id, d)
		c.logChaos("stall %d for %dms", id, d/raft.Duration(raft.Millisecond))
	}
}

// rollConfChange proposes one membership change on whichever node leads.
//
// It keeps at least three voters, so a quorum stays meaningful and the run can
// still be checked for liveness after healing. Everything else — promoting a
// learner that is far behind, demoting a voter mid-partition — is fair game,
// because those are exactly the cases the guards exist for.
func (c *Cluster) rollConfChange() {
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.crashed || c.now < n.stalledUntil || n.rf.Role() != raft.Leader {
			continue
		}
		cfg := n.rf.Config()
		var cc raft.ConfChange
		switch {
		case len(cfg.Learners) > 0 && c.chaosRnd.Intn(2) == 0:
			cc = raft.ConfChange{
				Type:   raft.ConfChangePromoteLearner,
				NodeID: cfg.Learners[c.chaosRnd.Intn(len(cfg.Learners))],
			}
		case len(cfg.Voters) > 3:
			// Never demote the leader itself here: that is covered by a
			// dedicated test, and doing it at random would mostly measure how
			// often the cluster is leaderless.
			var candidates []raft.NodeID
			for _, v := range cfg.Voters {
				if v != id {
					candidates = append(candidates, v)
				}
			}
			cc = raft.ConfChange{
				Type:   raft.ConfChangeAddLearner,
				NodeID: candidates[c.chaosRnd.Intn(len(candidates))],
			}
		default:
			return
		}

		effs, err := n.rf.ProposeConfChange(cc)
		if err != nil {
			return // guarded: a change is already in flight, or the term is fresh
		}
		c.ConfChanges++
		c.hash("confchange", uint64(id), uint64(cc.Type), uint64(cc.NodeID))
		c.logChaos("conf change on %d: type %d node %d", id, cc.Type, cc.NodeID)
		c.deliver(n, effs)
		return
	}
}

func (c *Cluster) logChaos(format string, args ...any) {
	c.ChaosLog = append(c.ChaosLog,
		fmt.Sprintf("t=%dms "+format, append([]any{int64(c.now) / int64(raft.Millisecond)}, args...)...))
}

// audit runs the expensive cross-node invariants.
func (c *Cluster) audit() {
	logs := make(map[raft.NodeID]invariant.LogView, len(c.ids))
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.crashed {
			continue
		}
		logs[id] = n.rf.Log()
		if n.rf.Role() == raft.Leader {
			c.record(c.check.ObserveLeaderLog(c.now, id, n.rf.Log()))
		}
	}
	c.record(c.check.CheckLogMatching(c.now, logs, c.ids))
}

// mustPersist records a storage failure as a safety violation.
//
// The simulator used to discard these. That was worse than a missing check: the
// real runtime treats a storage error as fatal and panics, so a simulator that
// shrugs one off lets durable state drift away from the core's — and every
// restart it then performs is rebuilding from a state no real node could ever
// have. The property the whole harness exists to verify quietly stops being
// verified.
func (c *Cluster) mustPersist(n *node, op string, err error) {
	if err != nil && c.violation == nil {
		c.violation = fmt.Errorf("StorageFailure violated at t=%dms: node %d %s: %w",
			int64(c.now)/int64(raft.Millisecond), n.id, op, err)
	}
}

func (c *Cluster) record(err error) {
	if err != nil && c.violation == nil {
		c.violation = err
	}
}

// Propose submits a value to whichever node is currently leader.
func (c *Cluster) Propose(data []byte) bool {
	for _, id := range c.ids { // sorted: never range the map
		n := c.nodes[id]
		if n.crashed || c.now < n.stalledUntil || n.rf.Role() != raft.Leader {
			continue
		}
		effs, err := n.rf.Propose(data)
		if err != nil {
			continue
		}
		c.hash("propose", uint64(id))
		c.deliver(n, effs)
		c.Acked++
		return true
	}
	return false
}

// StopWorkload halts the background proposal stream, so the cluster can be run
// to quiescence and compared for convergence.
func (c *Cluster) StopWorkload() { c.opts.ProposeInterval = 0 }

// Applied returns a node's state machine contents.
func (c *Cluster) Applied(id raft.NodeID) []raft.Entry { return c.nodes[id].sm }

// MaxTerm returns the highest term any node has reached. It is the direct
// measure of election churn: every term consumed is an election that happened,
// and most of them under chaos are elections that did not need to.
func (c *Cluster) MaxTerm() raft.Term {
	var maxT raft.Term
	for _, id := range c.ids {
		if t := c.nodes[id].rf.Term(); t > maxT {
			maxT = t
		}
	}
	return maxT
}

// MaxApplied returns the highest index any live node has applied.
func (c *Cluster) MaxApplied() raft.Index {
	var maxIdx raft.Index
	for _, id := range c.ids {
		if a := c.nodes[id].sm; len(a) > 0 {
			if last := a[len(a)-1].Index; last > maxIdx {
				maxIdx = last
			}
		}
	}
	return maxIdx
}

// CheckInvariants returns the first safety violation observed during the run,
// then runs a final audit. Safety is checked continuously as the run proceeds;
// this is the closing sweep, not the only check.
func (c *Cluster) CheckInvariants() error {
	if c.violation != nil {
		return c.violation
	}
	c.audit()
	return c.violation
}

func (c *Cluster) hash(tag string, vals ...uint64) {
	h := fnv.New64a()
	var buf [8]byte
	putUint64(&buf, c.trace)
	_, _ = h.Write(buf[:])
	_, _ = h.Write([]byte(tag))
	for _, v := range vals {
		_, _ = h.Write([]byte(strconv.FormatUint(v, 10)))
	}
	c.trace = h.Sum64()
}

func putUint64(b *[8]byte, v uint64) {
	for i := range b {
		b[i] = byte(v >> (8 * i))
	}
}

func uint64Bytes(v uint64) []byte {
	var b [8]byte
	putUint64(&b, v)
	return b[:]
}
