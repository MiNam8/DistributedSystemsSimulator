package server

import (
	"strconv"
	"time"

	"github.com/MiNam8/kvstore/metrics"
	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/storage"
)

// nodeMetrics is everything worth watching on a running node.
//
// The selection is driven by what the benchmarks in Phases 8 and 9 showed
// actually determines behaviour: fsync latency (the write path's ceiling), group
// commit batch size (how many fsyncs were saved), commit-to-apply lag (whether
// the state machine is keeping up), and elections (whether the cluster is
// stable). A metric nobody would act on is a metric not worth exporting.
type nodeMetrics struct {
	reg *metrics.Registry

	// Durability is the write path's ceiling, so it is the first thing to look
	// at when throughput disappoints.
	fsync *metrics.Histogram

	proposals *metrics.Counter
	batches   *metrics.Counter
	applied   *metrics.Counter
	elections *metrics.Counter
	snapshots *metrics.Counter
	restores  *metrics.Counter

	readsLog       *metrics.Counter
	readsLease     *metrics.Counter
	readsConfirmed *metrics.Counter

	term     *metrics.Gauge
	isLeader *metrics.Gauge
	commit   *metrics.Gauge
	applyIdx *metrics.Gauge
	// applyLag is commit minus applied: entries agreed by the cluster that this
	// node's state machine has not caught up to. A lag that grows is the earliest
	// warning that reads will start blocking.
	applyLag *metrics.Gauge
	logLen   *metrics.Gauge
	voters   *metrics.Gauge
	learners *metrics.Gauge
}

func newNodeMetrics(id raft.NodeID) *nodeMetrics {
	reg := metrics.New(map[string]string{"node": strconv.FormatUint(uint64(id), 10)})
	return &nodeMetrics{
		reg: reg,
		fsync: reg.Histogram("raft_storage_sync_seconds",
			"Time spent making log writes durable.", metrics.LatencyBuckets()),
		proposals: reg.Counter("raft_proposals_total", "Client commands appended."),
		batches:   reg.Counter("raft_append_batches_total", "Group-commit batches appended."),
		applied:   reg.Counter("raft_entries_applied_total", "Entries handed to the state machine."),
		elections: reg.Counter("raft_elections_won_total", "Times this node became leader."),
		snapshots: reg.Counter("raft_snapshots_taken_total", "Snapshots created."),
		restores:  reg.Counter("raft_snapshots_installed_total", "Snapshots installed from a leader."),
		readsLog:  reg.Counter("raft_reads_through_log_total", "Reads served by appending a log entry."),
		readsLease: reg.Counter("raft_reads_lease_total",
			"Reads served under a leader lease, with no confirmation round."),
		readsConfirmed: reg.Counter("raft_reads_confirmed_total",
			"Reads served after confirming leadership with a heartbeat quorum."),
		term:     reg.Gauge("raft_term", "Current Raft term."),
		isLeader: reg.Gauge("raft_is_leader", "1 when this node is the leader."),
		commit:   reg.Gauge("raft_commit_index", "Highest committed log index."),
		applyIdx: reg.Gauge("raft_applied_index", "Highest applied log index."),
		applyLag: reg.Gauge("raft_apply_lag_entries",
			"Committed entries the state machine has not yet applied."),
		logLen:   reg.Gauge("raft_log_entries", "Entries held in memory above the snapshot boundary."),
		voters:   reg.Gauge("raft_config_voters", "Voting members."),
		learners: reg.Gauge("raft_config_learners", "Non-voting members."),
	}
}

// observe refreshes the gauges from a stats snapshot.
func (m *nodeMetrics) observe(st Stats, cfg raft.Config, logLen int) {
	// Mirrored from the core, which counts these itself. Both are monotonic, so
	// exporting them as counters is honest.
	m.readsLease.Set(uint64(st.LeaseReads))
	m.readsConfirmed.Set(uint64(st.ConfirmedReads))

	m.term.Set(int64(st.Term))
	m.commit.Set(int64(st.CommitIndex))
	m.applyIdx.Set(int64(st.AppliedIndex))
	m.applyLag.Set(int64(st.CommitIndex) - int64(st.AppliedIndex))
	m.logLen.Set(int64(logLen))
	m.voters.Set(int64(len(cfg.Voters)))
	m.learners.Set(int64(len(cfg.Learners)))
	if st.Role == raft.Leader {
		m.isLeader.Set(1)
	} else {
		m.isLeader.Set(0)
	}
}

// timedStorage records how long each durable write takes.
//
// It wraps the Storage rather than instrumenting the write-ahead log directly,
// so the same measurement covers the in-memory implementation and any future
// one — and so the timing includes everything the node actually waits for, not
// just the syscall.
type timedStorage struct {
	storage.Storage
	hist *metrics.Histogram
}

func (s *timedStorage) time(f func() error) error {
	start := time.Now()
	err := f()
	s.hist.Observe(time.Since(start).Seconds())
	return err
}

func (s *timedStorage) SetHardState(hs raft.HardState) error {
	return s.time(func() error { return s.Storage.SetHardState(hs) })
}

func (s *timedStorage) Append(entries []raft.Entry) error {
	return s.time(func() error { return s.Storage.Append(entries) })
}

func (s *timedStorage) TruncateSuffix(i raft.Index) error {
	return s.time(func() error { return s.Storage.TruncateSuffix(i) })
}

func (s *timedStorage) SaveSnapshot(meta raft.SnapshotMeta, data []byte) error {
	return s.time(func() error { return s.Storage.SaveSnapshot(meta, data) })
}

func (s *timedStorage) Sync() error {
	return s.time(s.Storage.Sync)
}
