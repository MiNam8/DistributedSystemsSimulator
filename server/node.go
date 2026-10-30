// Package server is the node runtime: it wires the pure Raft core to real I/O.
//
// # Where the concurrency lives
//
// All of it lives here, and only here. Exactly one goroutine owns the Raft core,
// the state machine and the storage; every other goroutine — gRPC handlers,
// the transport's receive loop, the ticker — submits a closure to that goroutine
// and waits. The core has no locks because it is never touched concurrently, and
// that is what lets the simulator reproduce a run exactly.
//
// # The effect executor
//
// [Node.execute] is the whole durability contract in one function. Effects are
// executed strictly in order, and every Persist* effect is fsync'd before the
// loop continues — so no message that depends on durable state can be
// transmitted before that state is durable. Batching those fsyncs is Phase 8's
// job, and it will have to preserve exactly this ordering.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MiNam8/kvstore/clock"
	"github.com/MiNam8/kvstore/kv"
	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/storage"
	"github.com/MiNam8/kvstore/transport"
)

// Config configures a node.
type Config struct {
	ID      raft.NodeID
	Cluster raft.Config

	Storage   storage.Storage
	Transport transport.Transport
	Clock     clock.Clock

	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	// TickInterval is the timer granularity. It must be well below
	// ElectionTimeout or the jitter that breaks vote splits is quantised away.
	TickInterval time.Duration

	// Seed makes election jitter reproducible. Nodes must not share one.
	Seed uint64

	// MaxInflight bounds pipelining: unacknowledged AppendEntries per follower
	// between heartbeats.
	//
	// It defaults to 1 — pipelining effectively off — because it was measured
	// and did not help. Group commit and pipelining are in tension: batch size
	// is arrival rate times service time, so letting a proposal start its own
	// replication round instead of queueing behind the current fsync makes every
	// batch smaller. With an fsync-bound leader, batching wins. Raising this to
	// 16 cost 5-50% of throughput at every latency measured.
	//
	// The mechanism is implemented and exercised by the fault suites, so it is
	// ready if the balance ever changes — a faster durable device, or a link
	// slow enough that round trips dominate fsync. It is off because there is no
	// evidence for it, not because it is unfinished.
	MaxInflight int

	// MaxBatch bounds group commit: the number of client proposals that may be
	// appended and fsync'd together. One is the naive behaviour — an fsync per
	// proposal — and is kept only so the cost of not batching can be measured.
	//
	// An fsync costs about the same for one entry as for a thousand, so
	// collecting the proposals that arrived while the previous fsync was in
	// flight converts N fsyncs into one. Nothing is weakened: every entry in the
	// batch is durable before any of them is acknowledged.
	MaxBatch int

	// SnapshotThreshold is the number of in-memory log entries above which the
	// node snapshots and compacts. Zero disables compaction, and the log then
	// grows without bound — fine for a short-lived test, never for a real node.
	SnapshotThreshold int

	// PreVote and CheckQuorum enable §9.6: a node polls for support before
	// consuming a term, and a leader that cannot reach a majority resigns.
	PreVote     bool
	CheckQuorum bool

	// ReadMode selects how linearizable reads are served. See [ReadMode].
	ReadMode ReadMode

	// LeaseDuration is how long a leader may serve lease reads after a quorum
	// last acknowledged it. Only meaningful with [ReadLease].
	//
	// Safe only while LeaseDuration < ElectionTimeout - maximum clock rate error
	// between nodes. Setting it at or above the election timeout is the
	// deliberate misconfiguration the lease tests use to produce a stale read.
	LeaseDuration time.Duration

	// UnsafeLocalReads makes any node serve reads straight from its local state
	// machine, with no consensus round.
	//
	// It is deliberately, knowingly broken, and it exists to prove the
	// linearizability checker can fail. This is the most tempting optimisation
	// in any replicated store — reads are cheap, replicas are right there — and
	// it produces stale reads exactly when it matters: a follower that has not
	// yet applied the latest commit, or a deposed leader that does not yet know
	// it lost. Phase 9 adds the correct fast paths, ReadIndex and leader leases.
	//
	// Never set this outside a test.
	UnsafeLocalReads bool

	// HTTPPeers maps node ids to their JSON-shim addresses. When set, a request
	// arriving at a follower is answered with a 307 redirect to the leader, so
	// `curl -L` against any node just works. Without it the shim returns 421
	// with an X-Raft-Leader header and the caller redirects itself.
	HTTPPeers map[raft.NodeID]string
}

func (c *Config) setDefaults() {
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 300 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.TickInterval == 0 {
		c.TickInterval = 10 * time.Millisecond
	}
	if c.Clock == nil {
		c.Clock = clock.NewReal()
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 10000
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = 256
	}
	if c.MaxInflight <= 0 {
		c.MaxInflight = 1
	}
}

// ReadMode selects how a linearizable read is served.
type ReadMode uint8

const (
	// ReadThroughLog turns the read into a log entry, so consensus orders it
	// against every write. Obviously correct, and pays a full round trip *and*
	// an fsync for a request that changes nothing.
	ReadThroughLog ReadMode = iota

	// ReadIndex records the commit index, confirms leadership with a heartbeat
	// quorum, waits for the state machine to catch up, then reads locally. One
	// round trip, no disk write. This is what etcd does by default.
	ReadIndex

	// ReadLease serves locally while inside a window in which no other node
	// could have completed an election. No round trip at all — and the only
	// place in this system where correctness rests on clocks rather than
	// messages. See [Config.LeaseDuration].
	ReadLease
)

func (m ReadMode) String() string {
	switch m {
	case ReadThroughLog:
		return "log"
	case ReadIndex:
		return "readindex"
	case ReadLease:
		return "lease"
	default:
		return "unknown"
	}
}

// ParseReadMode maps a flag value to a [ReadMode].
func ParseReadMode(s string) (ReadMode, error) {
	switch s {
	case "log":
		return ReadThroughLog, nil
	case "readindex":
		return ReadIndex, nil
	case "lease":
		return ReadLease, nil
	default:
		return 0, fmt.Errorf("server: unknown read mode %q (want log, readindex or lease)", s)
	}
}

// ErrNoLeader is returned when this node does not know who the leader is.
var ErrNoLeader = errors.New("server: no leader known")

// NotLeaderError says this node is not the leader, and names the one it
// believes is. The client library follows the hint rather than polling.
type NotLeaderError struct{ Leader raft.NodeID }

func (e NotLeaderError) Error() string {
	if e.Leader == raft.None {
		return "server: not the leader; leader unknown"
	}
	return fmt.Sprintf("server: not the leader; try node %d", e.Leader)
}

// waiter is a client blocked on a command reaching the state machine. Waiters
// are keyed by (client, sequence) rather than by log index: a proposal can be
// truncated away by a new leader and re-proposed at a different index, and the
// client only cares that *its* command was applied.
type waitKey struct {
	client uint64
	seq    uint64
}

// proposal is one client command waiting to be appended.
type proposal struct {
	cmd []byte
	// err carries the propose-time outcome — nil, or a not-leader rejection.
	// Applying the command is reported separately, through the waiter map.
	err chan error
}

// pendingRead is a read waiting to be served.
type pendingRead struct {
	key    string
	index  raft.Index
	ready  bool
	result chan kv.Result
}

// Node is one running Raft node.
type Node struct {
	cfg Config
	rf  *raft.Raft
	sm  *kv.Store

	// ops carries work onto the owning goroutine. Every mutation of rf, sm or
	// storage happens inside a closure sent here.
	ops chan func()

	// proposals is separate from ops precisely so it can be *drained in bulk*.
	// A closure per proposal would force one append and one fsync each, which is
	// the whole cost group commit exists to remove.
	proposals chan *proposal

	done chan struct{}

	mu      sync.Mutex
	waiters map[waitKey]chan kv.Result

	// pendingReads are ReadIndex reads awaiting leadership confirmation and then
	// state machine catch-up. Owned by the Run goroutine.
	pendingReads map[uint64]*pendingRead
	readSeq      atomic.Uint64

	// Observability, guarded by mu so status can be read from any goroutine.
	stats   Stats
	metrics *nodeMetrics
}

// Stats is a snapshot of a node's state.
type Stats struct {
	ID           raft.NodeID
	Role         raft.Role
	Term         raft.Term
	Leader       raft.NodeID
	CommitIndex  raft.Index
	AppliedIndex raft.Index
	Elections    int
	Proposals    int
	Applied      int
	Snapshots    int
	Restores     int

	// Batches and BatchedEntries give the mean group-commit size, which is the
	// direct measure of how many fsyncs were saved.
	Batches        int
	BatchedEntries int
	MaxBatchSeen   int

	// LeaseReads and ConfirmedReads split ReadIndex reads by how leadership was
	// established: a lease that was still valid, or an explicit quorum round.
	LeaseReads     int
	ConfirmedReads int
}

// New builds a node, recovering whatever its storage holds.
func New(cfg Config) (*Node, error) {
	cfg.setDefaults()
	if cfg.Storage == nil {
		return nil, errors.New("server: Storage is required")
	}
	if cfg.Transport == nil {
		return nil, errors.New("server: Transport is required")
	}

	nm := newNodeMetrics(cfg.ID)
	// Every durable write is timed. Phase 8 measured fsync as the write path's
	// ceiling, which makes this the first number to look at when throughput
	// disappoints.
	cfg.Storage = &timedStorage{Storage: cfg.Storage, hist: nm.fsync}

	// Membership comes from durable state, not from the config file: a stale
	// file would silently resurrect a node that was removed from the cluster.
	hs, cluster, log, err := storage.RecoverState(cfg.Storage)
	if err != nil {
		return nil, err
	}
	if len(cluster.Voters) == 0 {
		cluster = cfg.Cluster
	}

	rf, err := raft.New(raft.Options{
		ID:                cfg.ID,
		Cluster:           cluster,
		ElectionTimeout:   raft.Duration(cfg.ElectionTimeout),
		HeartbeatInterval: raft.Duration(cfg.HeartbeatInterval),
		Rand:              raft.NewRand(cfg.Seed),
		MaxInflight:       cfg.MaxInflight,
		LeaseDuration:     leaseFor(cfg),
		PreVote:           cfg.PreVote,
		CheckQuorum:       cfg.CheckQuorum,
		Log:               log,
		HardState:         hs,
		Now:               cfg.Clock.Now(),
	})
	if err != nil {
		return nil, err
	}

	return &Node{
		cfg:          cfg,
		rf:           rf,
		sm:           kv.New(),
		ops:          make(chan func(), 1024),
		proposals:    make(chan *proposal, 4096),
		done:         make(chan struct{}),
		waiters:      make(map[waitKey]chan kv.Result),
		pendingReads: make(map[uint64]*pendingRead),
		stats:        Stats{ID: cfg.ID},
		metrics:      nm,
	}, nil
}

// Run drives the node until ctx is cancelled. It must be called exactly once,
// and it is the goroutine that owns everything.
func (n *Node) Run(ctx context.Context) {
	defer close(n.done)
	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case op := <-n.ops:
			op()
		case p := <-n.proposals:
			n.proposeBatch(p)
		case <-ticker.C:
			n.execute(n.rf.Tick(n.cfg.Clock.Now()))
		}
	}
}

// submit runs fn on the owning goroutine and waits for it.
func (n *Node) submit(ctx context.Context, fn func()) error {
	done := make(chan struct{})
	select {
	case n.ops <- func() { fn(); close(done) }:
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return errors.New("server: node stopped")
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return errors.New("server: node stopped")
	}
}

// Deliver hands an incoming peer message to the owning goroutine. It implements
// [transport.Receiver] and must not block the transport.
func (n *Node) Deliver(msg raft.Message) {
	select {
	case n.ops <- func() { n.execute(n.rf.Step(msg)) }:
	case <-n.done:
	default:
		// The queue is full: drop it. Raft's own retries — heartbeats and
		// nextIndex backtracking — are the recovery mechanism, and blocking the
		// transport here would turn a slow node into a stalled cluster.
	}
}

// DeliverSnapshot implements [transport.Receiver]. The transport has already
// reassembled the stream, so the payload is handed to the core as an ordinary
// message and the install becomes ordered effects like anything else.
func (n *Node) DeliverSnapshot(from raft.NodeID, term raft.Term, meta raft.SnapshotMeta, data []byte) {
	msg := raft.Message{
		Type:         raft.MsgSnap,
		From:         from,
		To:           n.cfg.ID,
		Term:         term,
		Snapshot:     &meta,
		SnapshotData: data,
	}
	select {
	case n.ops <- func() { n.execute(n.rf.Step(msg)) }:
	case <-n.done:
	}
}

// proposeBatch appends first together with everything already queued behind it.
//
// The drain is non-blocking: it takes what has already arrived and never waits
// for more. Waiting would trade latency for throughput on a guess, whereas
// draining what is already there is free — those proposals were going to sit
// behind this fsync regardless.
func (n *Node) proposeBatch(first *proposal) {
	batch := make([]*proposal, 1, n.cfg.MaxBatch)
	batch[0] = first

drain:
	for len(batch) < n.cfg.MaxBatch {
		select {
		case p := <-n.proposals:
			batch = append(batch, p)
		default:
			break drain
		}
	}

	cmds := make([][]byte, len(batch))
	for i, p := range batch {
		cmds[i] = p.cmd
	}
	effs, err := n.rf.ProposeBatch(cmds)

	if err != nil {
		err = NotLeaderError{Leader: n.rf.Leader()}
	}
	for _, p := range batch {
		p.err <- err
	}
	if err != nil {
		return
	}

	n.metrics.proposals.Add(uint64(len(batch)))
	n.metrics.batches.Inc()
	n.mu.Lock()
	n.stats.Proposals += len(batch)
	if len(batch) > n.stats.MaxBatchSeen {
		n.stats.MaxBatchSeen = len(batch)
	}
	n.stats.BatchedEntries += len(batch)
	n.stats.Batches++
	n.mu.Unlock()

	n.execute(effs)
}

// execute performs an effect batch. THIS FUNCTION IS THE DURABILITY CONTRACT.
//
// Effects run strictly in order, and each Persist* effect is fsync'd before the
// loop moves on. That ordering is what guarantees:
//
//   - a vote is durable before it is promised (or a crashed node votes twice in
//     one term and elects two leaders);
//   - entries are durable before replication is acknowledged (or a node acks an
//     entry into a commit quorum and then loses it);
//   - the leader's own entries are durable before it counts itself in a quorum.
//
// A storage error here is not recoverable: continuing would mean acting on state
// the disk does not hold. Panicking is the honest response — the node dies, the
// cluster elects around it, and no lie is told to any peer.
func (n *Node) execute(effs []raft.Effect) {
	for _, e := range effs {
		switch eff := e.(type) {
		case raft.PersistHardState:
			n.must(n.cfg.Storage.SetHardState(eff.HardState))

		case raft.PersistEntries:
			n.must(n.cfg.Storage.Append(eff.Entries))

		case raft.TruncateSuffix:
			n.must(n.cfg.Storage.TruncateSuffix(eff.FromIndex))

		case raft.PersistSnapshot:
			n.must(n.cfg.Storage.SaveSnapshot(eff.Meta, eff.Data))

		case raft.CompactLog:
			n.must(n.cfg.Storage.TruncatePrefix(eff.UpToIndex))

		case raft.SendMessage:
			n.send(eff.Message)

		case raft.RestoreStateMachine:
			// The PersistSnapshot barrier above has already returned, so the
			// image is durable before the state machine is replaced by it.
			if err := n.sm.Restore(eff.Data); err != nil {
				panic("server: snapshot restore failed: " + err.Error())
			}
			n.mu.Lock()
			n.stats.Restores++
			n.metrics.restores.Inc()
			n.mu.Unlock()

		case raft.ReadIndexReady:
			if pr, ok := n.pendingReads[eff.ReqID]; ok {
				pr.index, pr.ready = eff.Index, true
			}

		case raft.ApplyEntries:
			n.apply(eff.Entries)

		case raft.StateChanged:
			n.mu.Lock()
			if eff.Role == raft.Leader {
				n.stats.Elections++
				n.metrics.elections.Inc()
			}
			n.mu.Unlock()
		}
	}
	// A read may become servable either because leadership was confirmed or
	// because the state machine caught up, and both happen inside an effect
	// batch — so resolve once, here, rather than at each of those points.
	n.resolveReads()
	n.refreshStats()
	n.maybeSnapshot()
}

// resolveReads answers every read whose index the state machine has reached.
//
// Waiting for *applied*, not merely committed, is the point: a read must observe
// every write that was acknowledged before it was invoked, and an entry that is
// committed but not yet applied is not visible to the state machine.
func (n *Node) resolveReads() {
	if len(n.pendingReads) == 0 {
		return
	}
	applied := n.rf.AppliedIndex()
	for id, pr := range n.pendingReads {
		if !pr.ready || pr.index > applied {
			continue
		}
		v, found := n.sm.Get(pr.key)
		pr.result <- kv.Result{OK: true, Value: v, Found: found}
		delete(n.pendingReads, id)
	}
}

// leaseFor returns the core's lease duration, which is zero unless lease reads
// are actually selected.
func leaseFor(cfg Config) raft.Duration {
	if cfg.ReadMode != ReadLease {
		return 0
	}
	if cfg.LeaseDuration > 0 {
		return raft.Duration(cfg.LeaseDuration)
	}
	// Default to half the election timeout: comfortably inside the bound for
	// any plausible clock error, and still removing the round trip.
	return raft.Duration(cfg.ElectionTimeout / 2)
}

// send transmits one message, loading a snapshot payload from storage when the
// core asks for one.
//
// Every durability barrier earlier in the batch has already returned, so this is
// the ordering rule in action — enforced by nothing more exotic than executing
// the list in order.
func (n *Node) send(m raft.Message) {
	if m.Type != raft.MsgSnap {
		n.cfg.Transport.Send(m)
		return
	}
	// The core names the snapshot; the runtime supplies the bytes and streams
	// them out of band, so a multi-megabyte transfer never queues ahead of the
	// heartbeats that keep leadership alive.
	meta, data, err := n.cfg.Storage.LoadSnapshot()
	if err != nil {
		return
	}
	n.cfg.Transport.SendSnapshot(m.To, m.Term, meta, data)
}

// maybeSnapshot compacts once the in-memory log grows past the threshold.
func (n *Node) maybeSnapshot() {
	if n.rf.Log().Len() < n.cfg.SnapshotThreshold {
		return
	}
	applied := n.rf.AppliedIndex()
	if applied <= n.rf.Log().FirstIndex()-1 {
		return // nothing new applied since the last snapshot
	}
	effs, err := n.rf.Snapshot(applied, n.sm.Snapshot())
	if err != nil {
		return
	}
	n.mu.Lock()
	n.stats.Snapshots++
	n.metrics.snapshots.Inc()
	n.mu.Unlock()
	// Executed directly rather than recursively through execute: a snapshot
	// produces only storage effects, and re-entering would risk snapshotting
	// again from inside a snapshot.
	for _, e := range effs {
		switch eff := e.(type) {
		case raft.PersistSnapshot:
			n.must(n.cfg.Storage.SaveSnapshot(eff.Meta, eff.Data))
		case raft.CompactLog:
			n.must(n.cfg.Storage.TruncatePrefix(eff.UpToIndex))
		}
	}
}

func (n *Node) must(err error) {
	if err != nil {
		panic("server: durable storage failed, cannot continue safely: " + err.Error())
	}
}

// apply hands committed entries to the state machine and wakes any client
// waiting on them.
func (n *Node) apply(entries []raft.Entry) {
	for _, e := range entries {
		if e.Type != raft.EntryNormal || len(e.Data) == 0 {
			continue // no-ops and configuration changes are not state machine commands
		}
		cmd, err := kv.DecodeCommand(e.Data)
		if err != nil {
			continue
		}
		res := n.sm.ApplyCommand(cmd)

		n.metrics.applied.Inc()
		n.mu.Lock()
		n.stats.Applied++
		key := waitKey{cmd.ClientID, cmd.Seq}
		if ch, ok := n.waiters[key]; ok {
			delete(n.waiters, key)
			ch <- res
			close(ch)
		}
		n.mu.Unlock()
	}
}

func (n *Node) refreshStats() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stats.Role = n.rf.Role()
	n.stats.Term = n.rf.Term()
	n.stats.Leader = n.rf.Leader()
	n.stats.CommitIndex = n.rf.CommitIndex()
	n.stats.AppliedIndex = n.rf.AppliedIndex()
	n.stats.LeaseReads, n.stats.ConfirmedReads = n.rf.ReadStats()
	n.metrics.observe(n.stats, n.rf.Config(), n.rf.Log().Len())
}

// Metrics renders this node's metrics in the Prometheus text format.
func (n *Node) Metrics(w io.Writer) error {
	_, err := n.metrics.reg.WriteTo(w)
	return err
}

// Stats returns a snapshot of the node's state. Safe from any goroutine.
func (n *Node) Stats() Stats {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.stats
}

// Apply proposes a command and waits for it to reach the state machine.
//
// Returning only after the command is *applied* — not merely committed — is what
// lets the caller be told the actual result, and it is required for
// linearizability: a client that is told "ok" must be able to read its own write.
func (n *Node) Apply(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	key := waitKey{cmd.ClientID, cmd.Seq}
	ch := make(chan kv.Result, 1)

	n.mu.Lock()
	n.waiters[key] = ch
	n.mu.Unlock()

	cleanup := func() {
		n.mu.Lock()
		delete(n.waiters, key)
		n.mu.Unlock()
	}

	p := &proposal{cmd: kv.EncodeCommand(cmd), err: make(chan error, 1)}
	select {
	case n.proposals <- p:
	case <-ctx.Done():
		cleanup()
		return kv.Result{}, ctx.Err()
	case <-n.done:
		cleanup()
		return kv.Result{}, errors.New("server: node stopped")
	}

	select {
	case err := <-p.err:
		if err != nil {
			cleanup()
			return kv.Result{}, err
		}
	case <-ctx.Done():
		cleanup()
		return kv.Result{}, ctx.Err()
	case <-n.done:
		cleanup()
		return kv.Result{}, errors.New("server: node stopped")
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		// Indeterminate, not failed: the command may still commit. The client
		// must retry with the same sequence number, and the session table makes
		// that safe. Reporting this as a failure is what makes a
		// linearizability checker report false violations.
		cleanup()
		return kv.Result{}, ctx.Err()
	case <-n.done:
		cleanup()
		return kv.Result{}, errors.New("server: node stopped")
	}
}

// Read performs a linearizable read.
//
// Phase 5 reads through the log: the read becomes a log entry, so it is ordered
// against every write by consensus itself. Obviously correct and obviously
// expensive — one full round trip and one fsync per read. Phase 9 adds ReadIndex
// and lease reads and measures all three against each other.
func (n *Node) Read(ctx context.Context, key string, clientID, seq uint64) (kv.Result, error) {
	if n.cfg.ReadMode == ReadIndex || n.cfg.ReadMode == ReadLease {
		return n.readIndex(ctx, key)
	}
	n.metrics.readsLog.Inc()
	if n.cfg.UnsafeLocalReads {
		var res kv.Result
		if err := n.submit(ctx, func() {
			v, found := n.sm.Get(key)
			res = kv.Result{OK: true, Value: v, Found: found}
		}); err != nil {
			return kv.Result{}, err
		}
		return res, nil
	}
	return n.Apply(ctx, kv.Command{Op: kv.OpGet, Key: key, ClientID: clientID, Seq: seq})
}

// readIndex serves a read without appending to the log.
func (n *Node) readIndex(ctx context.Context, key string) (kv.Result, error) {
	reqID := n.readSeq.Add(1)
	ch := make(chan kv.Result, 1)

	var proposeErr error
	if err := n.submit(ctx, func() {
		effs, err := n.rf.ReadIndex(reqID)
		if err != nil {
			proposeErr = NotLeaderError{Leader: n.rf.Leader()}
			return
		}
		// Registered before the effects run: under a valid lease the core
		// answers synchronously, and the entry must already exist.
		n.pendingReads[reqID] = &pendingRead{key: key, result: ch}
		n.execute(effs)
	}); err != nil {
		return kv.Result{}, err
	}
	if proposeErr != nil {
		return kv.Result{}, proposeErr
	}

	select {
	case res := <-ch:
		return res, nil
	case <-ctx.Done():
		n.abandonRead(reqID)
		return kv.Result{}, ctx.Err()
	case <-n.done:
		return kv.Result{}, errors.New("server: node stopped")
	}
}

// abandonRead drops a read whose caller gave up, so a leadership change does not
// leak an entry per abandoned request.
func (n *Node) abandonRead(reqID uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = n.submit(ctx, func() { delete(n.pendingReads, reqID) })
}

// TransferLeadership hands leadership to target without waiting for an election
// timeout, for a zero-downtime restart or drain.
func (n *Node) TransferLeadership(ctx context.Context, target raft.NodeID) error {
	var err error
	if serr := n.submit(ctx, func() {
		effs, terr := n.rf.TransferLeadership(target)
		if terr != nil {
			err = terr
			return
		}
		n.execute(effs)
	}); serr != nil {
		return serr
	}
	return err
}

// ProposeConfChange submits a membership change. It returns once the change is
// appended, not once it is committed; callers that need the latter should watch
// the configuration.
func (n *Node) ProposeConfChange(ctx context.Context, cc raft.ConfChange) error {
	var err error
	if serr := n.submit(ctx, func() {
		effs, cerr := n.rf.ProposeConfChange(cc)
		if cerr != nil {
			err = cerr
			return
		}
		n.execute(effs)
	}); serr != nil {
		return serr
	}
	return err
}

// Config returns this node's view of the cluster configuration.
func (n *Node) Config() raft.Config {
	var cfg raft.Config
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = n.submit(ctx, func() { cfg = n.rf.Config() })
	return cfg
}

// Leader returns the node this one believes is leader.
func (n *Node) Leader() raft.NodeID { return n.Stats().Leader }

// IsLeader reports whether this node is currently leader.
func (n *Node) IsLeader() bool { return n.Stats().Role == raft.Leader }

// Wait blocks until the node's Run loop has exited.
func (n *Node) Wait() { <-n.done }
