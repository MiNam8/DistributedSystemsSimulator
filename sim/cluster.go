// Package sim is the deterministic simulation harness.
//
// It runs a whole cluster inside one goroutine against a virtual clock. Time
// advances only by popping the next event from a priority queue, so a run is a
// pure function of its seed: no wall clock, no real concurrency, no scheduler
// involvement. The same seed replays the same execution, event for event.
//
// Phase 1 is the skeleton: correct virtual time, deterministic ordering, and
// node stop/start. Fault injection — drop, delay, duplicate, reorder,
// asymmetric partition, process stall, clock skew — arrives in Phase 3, along
// with the invariant checker.
package sim

import (
	"container/heap"
	"fmt"
	"hash/fnv"
	"strconv"

	"github.com/MiNam8/kvstore/raft"
)

// Options configures a simulated cluster.
type Options struct {
	Nodes             int
	Seed              uint64
	ElectionTimeout   raft.Duration
	HeartbeatInterval raft.Duration
	// TickInterval is how often each node is ticked. It stands in for the
	// runtime's timer granularity and must be well below ElectionTimeout, or
	// timeouts are detected so coarsely that the election jitter is quantised
	// away and vote splits become artificially common.
	TickInterval raft.Duration
	// Latency is the one-way message delay. Fixed in Phase 1; Phase 3 draws it
	// from a distribution with a heavy tail.
	Latency raft.Duration
	// ProposeInterval, if positive, drives a background write workload: a value
	// is proposed to whichever node is leader at that instant. Proposals are
	// scheduled as events so they stay inside the deterministic timeline rather
	// than depending on when a test happens to call in.
	ProposeInterval raft.Duration
}

// DefaultOptions returns a usable configuration for a cluster of n nodes.
func DefaultOptions(n int, seed uint64) Options {
	return Options{
		Nodes:             n,
		Seed:              seed,
		ElectionTimeout:   300 * raft.Millisecond,
		HeartbeatInterval: 50 * raft.Millisecond,
		TickInterval:      5 * raft.Millisecond,
		Latency:           10 * raft.Millisecond,
		ProposeInterval:   20 * raft.Millisecond,
	}
}

type eventKind uint8

const (
	evTick eventKind = iota
	evDeliver
	evPropose
)

type event struct {
	at   raft.Time
	seq  uint64 // insertion order: breaks ties so the ordering is total
	kind eventKind
	to   raft.NodeID
	msg  raft.Message
}

// queue is a min-heap over (at, seq).
//
// The seq tiebreaker is what makes the simulation deterministic. Without a
// total order, two events scheduled for the same virtual instant would be
// popped in whatever order the heap happened to arrange them, and that order
// can shift with unrelated changes — silently breaking seed reproducibility.
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
	rf      *raft.Raft
	stopped bool

	// Durable state, kept outside the node so that a Phase 3 crash can discard
	// everything else and rebuild from exactly this.
	hard raft.HardState

	// applied is this node's state machine: every entry handed to it by an
	// ApplyEntries effect, in order. Two nodes disagreeing at the same index is
	// a state-machine-safety violation.
	applied []raft.Entry
}

// Cluster is a simulated Raft cluster on a virtual clock.
type Cluster struct {
	opts  Options
	ids   []raft.NodeID
	nodes map[raft.NodeID]*node

	q   queue
	now raft.Time
	seq uint64

	// trace is a running hash of every observable event, in order. Two runs of
	// the same seed must produce the same value; if they do not, something in
	// the core or the harness is reading a source of nondeterminism — most
	// commonly a ranged map.
	trace uint64

	// Elections records every leadership change, for assertions and for the
	// Phase 3 invariant checker.
	Elections []Election

	// Acked counts proposals the leader accepted. Every acked proposal must
	// eventually appear in every live node's state machine.
	Acked int

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
	c := &Cluster{
		opts:  opts,
		nodes: make(map[raft.NodeID]*node, opts.Nodes),
		trace: fnv.New64a().Sum64(),
	}

	var voters []raft.NodeID
	for i := 1; i <= opts.Nodes; i++ {
		voters = append(voters, raft.NodeID(i))
	}
	c.ids = voters

	// Each node gets its own PRNG stream, forked from the seed. Per-node streams
	// mean that changing one node's behaviour does not reshuffle every other
	// node's random draws, which keeps seeds stable as the project grows.
	root := raft.NewRand(opts.Seed)
	for _, id := range voters {
		rf, err := raft.New(raft.Options{
			ID:                id,
			Cluster:           raft.Config{Voters: voters},
			ElectionTimeout:   opts.ElectionTimeout,
			HeartbeatInterval: opts.HeartbeatInterval,
			Rand:              root.Fork(),
		})
		if err != nil {
			return nil, err
		}
		c.nodes[id] = &node{rf: rf}
	}

	// Stagger the first tick of each node so they do not run in lockstep, which
	// would quantise away the election jitter the protocol depends on.
	for _, id := range voters {
		c.schedule(raft.Time(root.Intn(int(opts.TickInterval))), &event{kind: evTick, to: id})
	}
	if opts.ProposeInterval > 0 {
		c.schedule(raft.Time(opts.ProposeInterval), &event{kind: evPropose})
	}
	return c, nil
}

// schedule enqueues ev to fire delay from now.
func (c *Cluster) schedule(delay raft.Time, ev *event) {
	ev.at = c.now + delay
	ev.seq = c.seq
	c.seq++
	heap.Push(&c.q, ev)
}

// Now returns the current virtual time.
func (c *Cluster) Now() raft.Time { return c.now }

// TraceHash returns the running hash of the run so far.
func (c *Cluster) TraceHash() uint64 { return c.trace }

// Node returns a node's Raft instance.
func (c *Cluster) Node(id raft.NodeID) *raft.Raft { return c.nodes[id].rf }

// IDs returns the node ids in sorted order.
func (c *Cluster) IDs() []raft.NodeID { return c.ids }

// Stop halts a node: it stops ticking and stops receiving messages. Its durable
// state is retained, so Phase 3 can restart it from exactly that state.
func (c *Cluster) Stop(id raft.NodeID) { c.nodes[id].stopped = true }

// Start resumes a stopped node and schedules its next tick.
func (c *Cluster) Start(id raft.NodeID) {
	n := c.nodes[id]
	if !n.stopped {
		return
	}
	n.stopped = false
	c.schedule(raft.Time(c.opts.TickInterval), &event{kind: evTick, to: id})
}

// Leaders returns every node currently claiming to be leader, with its term.
// More than one entry in the same term is an election-safety violation.
func (c *Cluster) Leaders() map[raft.NodeID]raft.Term {
	out := make(map[raft.NodeID]raft.Term)
	for _, id := range c.ids { // sorted: never range the map
		n := c.nodes[id]
		if !n.stopped && n.rf.Role() == raft.Leader {
			out[id] = n.rf.Term()
		}
	}
	return out
}

// RunFor advances virtual time by d, processing every event that falls due.
func (c *Cluster) RunFor(d raft.Duration) {
	deadline := c.now + raft.Time(d)
	for len(c.q) > 0 && c.q[0].at <= deadline {
		c.step()
	}
	c.now = deadline
}

// step processes exactly one event.
func (c *Cluster) step() {
	ev := heap.Pop(&c.q).(*event)
	c.now = ev.at
	n := c.nodes[ev.to] // nil for events that target no particular node

	switch ev.kind {
	case evTick:
		if n.stopped {
			return // a stopped node is not rescheduled; Start does that
		}
		c.hash("tick", uint64(ev.to), uint64(c.now))
		c.apply(ev.to, n.rf.Tick(c.now))
		c.schedule(raft.Time(c.opts.TickInterval), &event{kind: evTick, to: ev.to})

	case evDeliver:
		if n.stopped {
			return // delivery to a stopped node is a drop, as on a real network
		}
		c.hash("recv", uint64(ev.to), uint64(ev.msg.Type), uint64(ev.msg.From), uint64(ev.msg.Term))
		c.apply(ev.to, n.rf.Step(ev.msg))

	case evPropose:
		if c.opts.ProposeInterval <= 0 {
			return // workload halted
		}
		c.proposals++
		c.Propose(uint64Bytes(c.proposals))
		c.schedule(raft.Time(c.opts.ProposeInterval), &event{kind: evPropose})
	}
}

// apply executes an effect batch on behalf of a node.
//
// Effects are executed strictly in order. Phase 1 has no real storage, so the
// durability barriers are recorded rather than fsync'd — but they are recorded
// *in order*, so the trace already proves that a PersistHardState precedes every
// send that depends on it. Phase 4 replaces this with a real write-ahead log
// and starts injecting torn writes.
func (c *Cluster) apply(id raft.NodeID, effs []raft.Effect) {
	n := c.nodes[id]
	for _, e := range effs {
		switch eff := e.(type) {
		case raft.PersistHardState:
			n.hard = eff.HardState
			c.hash("hard", uint64(id), uint64(eff.HardState.Term), uint64(eff.HardState.VotedFor))

		case raft.SendMessage:
			c.hash("send", uint64(id), uint64(eff.Message.Type), uint64(eff.Message.To), uint64(eff.Message.Term))
			c.schedule(raft.Time(c.opts.Latency), &event{
				kind: evDeliver,
				to:   eff.Message.To,
				msg:  eff.Message,
			})

		case raft.PersistEntries:
			c.hash("append", uint64(id), uint64(len(eff.Entries)))

		case raft.TruncateSuffix:
			c.hash("truncate", uint64(id), uint64(eff.FromIndex))

		case raft.ApplyEntries:
			for _, e := range eff.Entries {
				n.applied = append(n.applied, e)
				c.hash("apply", uint64(id), uint64(e.Index), uint64(e.Term))
			}

		case raft.StateChanged:
			c.hash("state", uint64(id), uint64(eff.Role), uint64(eff.Term))
			if eff.Role == raft.Leader {
				c.Elections = append(c.Elections, Election{At: c.now, Leader: id, Term: eff.Term})
			}

		default:
			c.hash("effect", uint64(id))
		}
	}
}

// hash folds one observable event into the running trace hash.
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

// Propose submits a value to whichever node is currently leader. It reports
// whether any node accepted it.
func (c *Cluster) Propose(data []byte) bool {
	for _, id := range c.ids { // sorted: never range the map
		n := c.nodes[id]
		if n.stopped || n.rf.Role() != raft.Leader {
			continue
		}
		effs, err := n.rf.Propose(data)
		if err != nil {
			continue
		}
		c.hash("propose", uint64(id))
		c.apply(id, effs)
		c.Acked++
		return true
	}
	return false
}

// StopWorkload halts the background proposal stream, so the cluster can be run
// to quiescence and its nodes compared for convergence.
func (c *Cluster) StopWorkload() { c.opts.ProposeInterval = 0 }

// Applied returns a node's state machine contents.
func (c *Cluster) Applied(id raft.NodeID) []raft.Entry { return c.nodes[id].applied }

// CheckInvariants evaluates the safety properties that must hold at every
// instant, over every node. Phase 3 extends this set and runs it continuously;
// here it is called at the end of a run.
func (c *Cluster) CheckInvariants() error {
	// Election safety: at most one leader per term.
	byTerm := make(map[raft.Term]raft.NodeID)
	for _, e := range c.Elections {
		if prev, seen := byTerm[e.Term]; seen && prev != e.Leader {
			return errf("ELECTION SAFETY: nodes %d and %d were both leader in term %d", uint64(prev), uint64(e.Leader), uint64(e.Term))
		}
		byTerm[e.Term] = e.Leader
	}

	// State machine safety: no two nodes apply different entries at the same
	// index, and each node applies a contiguous sequence exactly once.
	agreed := make(map[raft.Index]raft.Entry)
	for _, id := range c.ids {
		var want raft.Index
		for i, e := range c.nodes[id].applied {
			if i == 0 {
				want = e.Index
			}
			if e.Index != want {
				return errf("APPLY ORDER: node %d applied index %d where %d was expected", uint64(id), uint64(e.Index), uint64(want))
			}
			want++

			if prev, seen := agreed[e.Index]; seen {
				if prev.Term != e.Term || string(prev.Data) != string(e.Data) {
					return errf("STATE MACHINE SAFETY: divergence at index %d (terms %d vs %d)", uint64(e.Index), uint64(prev.Term), uint64(e.Term))
				}
			} else {
				agreed[e.Index] = e
			}
		}
	}

	// Commit monotonicity: a node's applied index never exceeds its commit index.
	for _, id := range c.ids {
		n := c.nodes[id]
		if n.rf.AppliedIndex() > n.rf.CommitIndex() {
			return errf("APPLIED PAST COMMIT: node %d applied %d, commit %d", uint64(id), uint64(n.rf.AppliedIndex()), uint64(n.rf.CommitIndex()))
		}
	}
	return nil
}

// MaxApplied returns the highest index any live node has applied.
func (c *Cluster) MaxApplied() raft.Index {
	var maxIdx raft.Index
	for _, id := range c.ids {
		if a := c.nodes[id].applied; len(a) > 0 {
			if last := a[len(a)-1].Index; last > maxIdx {
				maxIdx = last
			}
		}
	}
	return maxIdx
}

func uint64Bytes(v uint64) []byte {
	var b [8]byte
	putUint64(&b, v)
	return b[:]
}

func errf(format string, args ...uint64) error {
	as := make([]any, len(args))
	for i, a := range args {
		as[i] = a
	}
	return fmt.Errorf(format, as...)
}
