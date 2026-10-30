# Roadmap: A Distributed Key-Value Store on Raft Consensus

**Theme:** *Proving a consensus implementation correct under adversarial failure — by construction, not by demo.*

This project is not "an implementation of Raft." It is an argument that a particular implementation of
Raft is **correct when the network partitions, the leader dies mid-write, and the disk tears a record
in half** — and that the argument is *reproducible*. The engineering loop is:

> **model → simulate → break it → prove the invariant held → make it fast → prove it again.**

The centerpiece is a design decision made on line one and never relaxed: **the Raft core is a pure,
I/O-free state machine.** It has no goroutines, no channels, no `time.Now()`, no unseeded randomness,
and it never performs I/O. It accepts events and returns *effects as data*. One consequence pays for
everything else in this document: the **same core binary-identical logic** runs under a deterministic
simulator and under a real multi-process cluster, so a bug found in simulation is a bug in production,
and a bug found in production can be reproduced in simulation by recreating its fault pattern — after
which a single seed replays it exactly. (A real-cluster run itself is not replayable: real sockets and
the Go scheduler are nondeterministic.)

---

## The four groups

1. **The core** — a pure `Step`/`Tick`/`Propose` state machine: leader election, log replication,
   commit rules, snapshots, membership changes. Zero I/O.
2. **The harnesses** — deterministic simulation (virtual clock, seeded event queue, fault-injecting
   network) *and* a real cluster (gRPC, file-backed WAL, `kill -9`, `iptables`). Both drive the same core.
3. **The oracles** — a continuously-evaluated Raft invariant checker, plus a linearizability checker
   over recorded client histories. These are what turn "it seemed to work" into a claim.
4. **The measurements** — failover-time distribution, the fsync/batching/pipelining throughput arc,
   and a three-way comparison of linearizable read strategies.

Each phase below is a **stoppable checkpoint**: the project is presentable at the end of every one.
The core's purity is preserved at every phase — it is the invariant of the *project*, not just the code.

---

## Architecture: the one decision everything hangs off

```
                      ┌──────────────────────────────────────┐
   events in          │            raft (pure core)          │         effects out
  ───────────────────▶│  Step(Message)  Tick(now)  Propose() │────────────────────▶
                      │  no time · no net · no disk · no go  │
                      └──────────────────────────────────────┘
                                   ▲            ▲
                 ┌─────────────────┘            └──────────────────┐
                 │                                                 │
      ┌──────────┴───────────┐                        ┌────────────┴───────────┐
      │   sim harness (DST)  │                        │  real node runtime     │
      │  virtual clock       │                        │  monotonic clock       │
      │  seeded event queue  │                        │  gRPC transport        │
      │  in-mem storage      │                        │  segmented WAL + fsync │
      │  fault injector      │                        │  chaos: kill -9, tc    │
      └──────────┬───────────┘                        └────────────┬───────────┘
                 │                                                 │
                 └──────────────► shared oracles ◄─────────────────┘
                        invariant checker · linearizability checker
```

### The effect protocol, and the ordering rule that makes it safe

The core never acts; it *describes* what must happen:

```go
type Effect interface{ isEffect() }

PersistHardState{Term, VotedFor}   // durability barrier
PersistEntries{Entries}            // durability barrier
TruncateSuffix{FromIndex}          // durability barrier
CompactLog{UpToIndex}
PersistSnapshot{Meta, Data}
SendMessage{To, Message}
ApplyEntries{Entries}
ReadIndexReady{ReqID, Index}
StateChanged{Role, Term, Leader}   // observability only
```

**Effects are returned in strict order and MUST be executed in that order. Every `Persist*` /
`Truncate*` effect is an fsync barrier: it must be durable before any *later* `SendMessage` in the
same batch leaves the process.** This single rule encodes the three durability obligations that Raft
safety actually rests on, and it is where nearly every real-world Raft data-loss bug lives:

- **A.** `currentTerm` + `votedFor` durable **before** granting a vote — otherwise a crashed node
  votes twice in one term after restart, and two leaders coexist.
- **B.** Entries durable **before** replying `AppendEntriesResponse{success:true}` — otherwise a node
  acks an entry into a commit quorum and then loses it.
- **C.** The leader's own `matchIndex[self]` advances **only** after its own fsync — it must not count
  itself in a quorum for data it has not durably stored.

(Phase 8 relaxes exactly one of these safely: the leader may *transmit* `AppendEntries` before its own
fsync completes, because rule C governs quorum counting, not transmission. That is a real throughput
win and it is a precise, defensible relaxation — not a shortcut.)

### Enforcing purity mechanically

A test asserts the `raft` package's transitive import graph contains **none of**
`time`, `net`, `os`, `math/rand`, `sync`, `syscall`, or `context`, via `go list -deps ./raft`.
Purity stops being a convention you have to remember and becomes a build failure. This is a two-line
test and one of the strongest things in the repo to point at in an interview.

### Layout

```
kvstore/
├── raft/            # THE PURE CORE. Import-restricted by test.
│   ├── raft.go          Step / Tick / Propose, role transitions
│   ├── log.go           in-memory log view, compaction boundary
│   ├── replication.go   leader replication state, commit advance
│   ├── election.go      vote, pre-vote, election restriction
│   ├── membership.go    single-server config changes, learners
│   ├── read.go          read-through-log / ReadIndex / lease
│   ├── effect.go        message.go  config.go
│   └── invariant.go     exported invariant checks (used by BOTH harnesses)
├── storage/         Storage interface · wal/ (segmented) · snap/ · mem/ (DST)
├── kv/              state machine: Apply, Snapshot, Restore, dedup table
├── server/          node runtime: wires core → gRPC + WAL + real clock
├── transport/       grpc/ · sim/
├── sim/             DST: virtual clock, seeded event queue, network model, faults
├── chaos/           real-cluster driver: process supervision, kill -9, iptables/tc
├── lincheck/        history recorder + linearizability checking
├── cmd/             kvserver/ · kvctl/ · dst/ (seeded sim runner) · chaosctl/
├── proto/  bench/  analysis/
```

---

## Progress

- [x] **Phase 0 — Foundations** *(DONE)*
  - Go 1.27.1 installed to `~/.local/go` (no root needed); module `github.com/MiNam8/kvstore`.
  - Core types, `Message`, the `Effect` protocol with its documented fsync-barrier rule, and an
    in-core `SplitMix64` PRNG (no `math/rand` — an in-tree algorithm guarantees a seed replays
    identically across Go releases, which an imported one does not).
  - `Storage`, `Transport`, `Clock` interfaces defined *outside* the core.
  - **Purity contract enforced by four tests, each verified to actually fail on a deliberate
    violation:** transitive import graph, direct-import allowlist, no goroutines/channels/select,
    no mutable globals.
  - **Result: the core's entire transitive dependency graph is `errors`, `math/bits`, `runtime`,
    `unsafe`.** No `time`, `os`, `net`, `sync`, or `fmt` anywhere in it — the clock is not merely
    unused by the core, it is not linked into it. This is the claim `make deps` prints on demand.
  - Cost of that guarantee: the core may not import `fmt` (it reaches `time` via `os`), so errors are
    `errors.New` sentinels and `String` methods are hand-written switches. Load-bearing, not aesthetic.
  - `make check` (fmt + vet + purity + tests) green; CI wired with the purity contract as its own
    step so a violation is legible in the summary rather than buried in test output.
- [x] **Phase 1 — Core: leader election** *(DONE)*
  - `raft/log.go` (in-memory log with a snapshot sentinel), `raft/raft.go` (Options, New, Tick, Step
    dispatch), `raft/election.go` (vote, election restriction, heartbeats). `sim/` skeleton:
    single-goroutine virtual clock, `(at, seq)`-ordered event queue, node stop/start. `cmd/dst`.
  - **Design deviation from etcd, deliberate:** the core owns the entire log in memory rather than
    reading stable entries back through a `Storage` interface. etcd's design is why its `Storage` has
    a "must not block" caveat — the core reaches toward a disk it is not allowed to wait for. Memory
    is bounded by compaction, which is the mechanism that already exists for it.
  - **The `send` helper is the ordering rule made structural:** every outgoing message routes through
    `Raft.send`, which flushes a dirty hard state as a `PersistHardState` barrier first. Obligation A
    cannot be forgotten at a call site because there is no other way to send.
  - **Result — 40,000 seeds, zero failures, zero election-safety violations.** Failover p50/p99/max:
    3-node 401/1004/1539 ms · 5-node 355/536/1143 ms · 7-node 341/468/790 ms.
  - **Finding: larger clusters fail over *faster*.** Election timeout is drawn from `[T, 2T)` per
    node, so the cluster's time-to-first-candidate is the *minimum* of N draws, and that minimum
    falls as N grows. The 3-node tail is worst (p99 1004 ms) because it has the fewest draws and
    because a split vote there costs a full extra round. Worth a graph in Phase 11.
  - **Verified the determinism test can fail:** replacing the sorted peer walk with a ranged map
    diverged the trace hash on the first repetition. Go randomises map iteration per-range, so the
    test repeats each seed five times rather than comparing once.
- [x] **Phase 2 — Core: log replication and the commit rules** *(DONE)*
  - `raft/replication.go`: `Propose`, per-follower `progress`, the consistency check, conflict
    resolution with term hints, the §5.4.2 commit rule, `applyCommitted`. `becomeLeader` now appends
    its term's no-op. Heartbeats became full `AppendEntries`, so a follower that dropped a message
    recovers on the next beat instead of stalling until the next client write.
  - **Both traps have regression tests that were verified to fail against the naive version:**
    - Deleting the `term != r.term` guard in `maybeCommit` → Figure-8 test fails immediately
      (`commitIndex = 2, want 0`). Every other test still passes, which is exactly why the guard
      gets dropped in the first place.
    - Blind truncation at `PrevLogIndex+1` → a stale duplicate destroys entries 2 and 3.
    - Replying `MatchIndex = log.LastIndex()` instead of `PrevLogIndex + len(Entries)` → a bare
      heartbeat makes a follower claim 16 entries the leader has never seen. This one was actually
      present in the Phase 1 code and caught while writing the test.
  - **Fast backtracking measured: 3 round trips vs ~25 for naive `nextIndex--`** on a follower
    diverged 25 entries back across 3 terms.
  - **Result — 30,000 seeds under a continuous write workload with the leader killed mid-write:
    zero failures, zero safety violations, full convergence.** Failover p50/p99:
    3-node 422/1010 ms · 5-node 377/549 ms · 7-node 362/490 ms. (Slightly above Phase 1's numbers
    because a new leader now has a real log to catch up on before it can serve.)
  - Invariant checker now covers election safety, state-machine safety, apply-order contiguity and
    applied-never-exceeds-commit. Phase 3 makes it continuous rather than end-of-run.
  - **Known cost, deferred to Phase 8:** every commit advance marks the hard state dirty, so commits
    fsync. And `sendAppend` resends everything from `next` on every heartbeat — correct because
    AppendEntries is idempotent, but wasteful without an in-flight window. Both are Phase 8's
    measurements to make, not guesses to act on now.
- [x] **Phase 3 — DST harness: the bug-finding engine** *(centerpiece #1 — DONE)*
  - `sim/`: seeded network faults (drop, duplicate, jitter-driven reordering, heavy-tail delay),
    symmetric **and one-way** partitions, real crash/restart, process stalls, per-node clock **rate**
    drift. `storage/mem` makes a crash real: volatile state is discarded, the node is rebuilt from
    durable state alone. `invariant/` holds the shared checker. `cmd/dst -chaos`.
  - **Result — 30,000 chaos seeds, 1.17 billion simulated events, zero violations.**
    (3-node 215M · 5-node 386M · 7-node 570M events.)
  - **Rates are integers (per mille), not floats**, so a seed replays identically across platforms.
  - **Three bugs the harness found — two of them in my own invariant checker, not the core:**
    1. `CommitMonotonicity` fired on a restart. Correct behaviour: Figure 2 of the paper lists
       `commitIndex` as *volatile* state, legitimately lost and re-learned. The cursor now resets on
       crash.
    2. `LeaderCompleteness` fired on a node becoming leader in a **stale** term. Reproduction from the
       trace: node stalled at t=400ms → node 1 led term 2 at t=755ms → the stalled node woke at
       t=1022ms, processed its *buffered term-1 vote responses*, and took leadership in term 1. Such a
       leader is harmless (every follower rejects its lower term). §5.4.3 says "leaders for all
       **higher-numbered** terms" — I had dropped that clause. The checker now tracks the term each
       entry was committed in.
    3. A liveness assertion in my own test that healed the cluster but left the chaos driver running,
       so it asserted progress *during* chaos. Added `StopChaos`; the package doc warned about exactly
       this and I did it anyway.
  - **The honest finding, and the most important one in this phase:** injected bugs were caught at
    very different rates. Reversing the election restriction → **16 failures in 500 seeds**. Blind
    truncation → **310 in 500**. But **reverting the §5.4.2 commit rule was NOT found in 10,000 seeds
    and 300M events**, on 3- and 5-node clusters, with or without the no-op that was my first
    hypothesis for hiding it. The hand-written Figure-8 unit test catches it in microseconds.
    **Uniform random fault injection explores schedules, not scenarios**; a bug whose manifestation
    needs one precise interleaving is astronomically unlikely to be hit by chance. DST does not
    replace reasoning about the protocol — it complements targeted adversarial tests, and this
    project needs both. A guided/biased adversary is the natural follow-up.
- [x] **Phase 4 — Persistence: segmented WAL + crash recovery** *(DONE)*
  - `storage/wal`: segmented log, CRC32-Castagnoli framing over type+payload, `fallocate`
    preallocation, **directory fsync after every segment create and delete**, snapshot written
    atomically (temp → fsync → rename → fsync dir).
  - **Truncation is append-only.** A conflicting suffix is discarded by appending a *marker*, not by
    rewriting the file. Replay applies markers in order. The write path stays purely sequential, and
    a crash during a truncation is indistinguishable from a crash before it — the marker is either
    durable or it is not.
  - **Checkpoint met exhaustively rather than by sampling: recovery verified at every one of 1,420
    truncation points**, not 1,000 random ones. Every cut yields a clean prefix, never a gap or a
    corrupted entry, and the log stays appendable afterwards. All 49 injected bit flips detected.
  - **DST now runs against real files** (`-wal`): 200 chaos seeds, 7.67M events, real fsync, real
    segment replay, real torn-write recovery — no failures. Roughly 60× slower per seed and
    thoroughly I/O-bound (52s user vs 2m03s sys), which is why the in-memory soak still carries
    breadth and this carries fidelity.
  - **Three bugs the tests found, all of which would have passed a casual review:**
    1. `TruncatePrefix` guarded on the *snapshot boundary*, which `SaveSnapshot` advances first — so
       compaction was a permanent no-op. The log would have grown forever with every test green.
    2. Replay's torn-tail `Truncate` gave back the preallocation, so every restart silently put the
       active segment back on the slow path where each append also rewrites the inode.
    3. Compaction is segment-granular, so a retained segment still holds entries *below* the snapshot
       boundary. The live view trimmed to the requested index while the recovered view did not — and
       `RecoverLog` would then hand the core a log starting below its own boundary, a non-contiguous
       append. Fixed in both places: storage trims only to what actually survives on disk, and
       `RecoverLog` starts above the boundary.
- [x] **Phase 5 — KV state machine + gRPC API + a real cluster** *(DONE)*
  - `kv/` (map + session table, self-contained binary codec), `server/` (node runtime + effect
    executor + gRPC service + HTTP shim), `transport/grpc`, `transport/inproc`, `client/`,
    `cmd/kvserver`, `cmd/kvctl`, `proto/`. Go 1.27.1, protoc 36.1, gRPC 1.83.2.
  - **The core stayed pure through the phase that added the most I/O.** With gRPC, protobuf and
    `net/http` in the module, `make deps` still prints `errors math/bits slices cmp iter runtime
    unsafe`. That is the architecture paying for itself.
  - **`Node.execute` is the durability contract in one function**: effects in order, every `Persist*`
    fsync'd before the loop continues, a storage error panics rather than lying to a peer.
  - **At-most-once is real, not decorative.** Sequence numbers are allocated once per *logical*
    operation and reused across retries; the session table is in the snapshot; `TestSnapshotPreserves
    Sessions` fails if it is omitted. A not-leader reply is a *redirect*, not an error, so a client
    retries immediately rather than backing off as it would for a dead node.
  - **Checkpoint met over real sockets**: `./demo.sh` starts three nodes, writes via gRPC and via
    `curl -L` (307 to the leader), `kill -9`s the leader, shows the client continuing uninterrupted,
    then restarts the dead node from its WAL and watches it catch up. 8 integration tests, clean
    under `-race`.
  - **Finding: a restarting node disrupts a healthy cluster.** In the demo the term jumped 2 → 4 when
    node 3 rejoined — it comes back as a follower, times out, and campaigns even though a perfectly
    good leader exists. Harmless but wasteful, and exactly what pre-vote fixes in Phase 10. Worth
    measuring there rather than asserting.
  - **Honest gap:** reads go through the log (one consensus round and one fsync each), which is
    correct and slow. Phase 9 adds ReadIndex and leases and measures all three. Graceful shutdown
    stops accepting and exits cleanly but does not drain in-flight proposals.
- [x] **Phase 6 — Snapshots, log compaction, InstallSnapshot** *(DONE)*
  - `raft/snapshot.go` (create, install, stale-reject, fast-forward), `Log.Reset`, `MsgSnap`/
    `MsgSnapResp`, `RestoreStateMachine` effect, one-transfer-at-a-time flow control per follower.
    Wired through the simulator, the WAL, and the real server runtime.
  - **The core still never holds a payload.** On the send side it names a snapshot and the runtime
    loads the bytes from storage; on the receive side the transport has already reassembled the
    stream. Payloads stay out of every effect batch and every simulator trace.
  - **Result — 30,000 snapshot-heavy chaos seeds, ~1.15B events, zero violations** (3-node 212M ·
    5-node 378M · 7-node 559M), plus 100 seeds against real fsync'd files. In one 150-seed test run:
    12,858 snapshots taken, 697 installed on followers.
  - **Five real bugs, four of them safety:**
    1. **Full install used `Compact` instead of `Reset`.** §7 says that when the boundary entry does
       not match, *discard the entire log*. Compact keeps the tail — grafting a divergent suffix onto
       a prefix it never followed. A node that later won an election replicated that fabrication over
       committed entries. Caught as `LeaderCompleteness`.
    2. **`MsgSnapResp` reported `log.LastIndex()` instead of the snapshot boundary** — the *same
       class* as the Phase 2 `MatchIndex` bug, reintroduced in a new code path. A follower claimed
       its whole uncommitted tail and the leader counted entries no quorum held.
    3. **The install emitted only `CompactLog`.** The core did `Reset` (discard everything) but
       storage was told only to trim the *prefix*, so it kept the tail the core had thrown away. The
       core's next append then landed at boundary+1 while storage still ended at the old tail, and
       the two silently disagreed about what was on disk.
    4. **`mem.Storage.TruncatePrefix` guarded on the snapshot offset that `SaveSnapshot` had already
       advanced** — the *same class* as the Phase 4 WAL bug, in the implementation I had not fixed.
       Compaction was a permanent no-op.
    5. **A stale leader never stepped down.** `MsgApp` and `MsgSnap` carrying an old term were
       dropped silently instead of rejected with the receiver's term, so a leader deposed while
       partitioned heartbeated its dead term forever and went on telling clients it was the leader
       (§5.1). Found as `2 leaders after healing` on **1 seed in 10,000**.
  - **The meta-finding, and the most useful one: the harness itself had a hole.** The simulator was
    discarding storage errors that the real runtime treats as fatal. That let simulated durable state
    drift away from the core's, so every restart it performed was rebuilding from a state no real
    node could reach — quietly invalidating the property the whole harness exists to check. Bug 3 was
    invisible until storage errors were promoted to violations, at which point it named itself in one
    run. **A checker that ignores a failure is worse than one that never looked.**
- [x] **Phase 7 — Real-cluster chaos + linearizability** *(centerpiece #2 — DONE)*
  - `lincheck/` (Porcupine model + history recorder), `chaos/` (directed TCP proxies, process
    supervisor, fault driver, workload), `cmd/chaosctl`. Real `kvserver` processes, real `kill -9`,
    real `SIGSTOP`, real sockets reset mid-flight.
  - **Partitions without root.** `iptables`/`tc` need privileges that make a suite unrunnable in CI or
    on a laptop. Instead every *directed* link gets its own userspace TCP proxy, which is strictly
    more expressive: A→B can be cut while B→A still carries traffic. Links are cut by *resetting*
    connections rather than blackholing them — a blackholed socket sits in a TCP timeout for minutes,
    which would make every partition look like a stall.
  - **Indeterminate operations, done properly.** Porcupine turned out *not* to support unmatched call
    events (it derives the operation count from `len(history)/2`, so a pending call is misread as a
    return). The fix is better than the original plan: the model is a
    `porcupine.NondeterministicModel`, an abandoned operation gets a synthetic return at the end of
    the history marked indeterminate, and the model then **branches into both futures** — one where
    it took effect and one where it did not. That is Jepsen's `:info` stated exactly. Two unit tests
    pin both directions: a read observing an indeterminate write must be accepted, and an
    indeterminate write that never took effect must also be accepted.
  - **The suite is verified to fail.** `-unsafe-local-reads` starts the cluster serving reads straight
    from each replica's local state machine — the most tempting optimisation in any replicated store —
    and the checker must catch it. It does, and there is a permanent CI test for it.
  - **The measured finding that changed the design: with every read directed at the leader, that bug
    went undetected in 8 of 8 runs. Spreading reads across replicas caught it in 4 of 4.** The fault
    model has to include *where the client sends its request*, not only what the network does — a
    chaos suite that only ever reads from the leader will certify a system that is serving stale data
    from its followers.
  - **Checkpoint met: one hour of continuous chaos against a real 5-node cluster — 222,572
    operations, 99,024 writes acknowledged, 3,330 faults applied, zero linearizability violations,
    zero invariant violations, cluster recovered after healing.** The whole history checked in 707 ms,
    which is per-key partitioning earning its place: unpartitioned, 222k operations would not finish.
  - **6,143 operations (2.8%) came back indeterminate.** That is the number that makes the recorder
    design matter rather than being a pedantic footnote. Treating those as failures would have
    manufactured violations at that scale; treating them as successes would have asserted ~6,000
    effects that may never have happened. Either way the run would have reported confidently about a
    history that never occurred.
  - Reads cost real throughput: ~6,900 ops in 15s with (broken) local reads versus ~1,800 with
    read-through-log. That gap is precisely what Phase 9's ReadIndex and lease paths exist to close
    *without* reintroducing the bug above.
- [x] **Phase 8 — Performance: batching, group commit, pipelining** *(DONE)*
  - `cmd/kvbench` (closed-loop load generator, can spawn its own cluster with injected inter-node
    latency), `bench/run.sh`, `analysis/plot_bench.py`. `raft.ProposeBatch`, group commit in the node
    runtime, a bounded pipelining window, and a measurement-only `-unsafe-no-fsync`.
  - **Result (3 nodes, 64 clients, one machine):**

    | configuration | throughput | p50 | mean batch |
    |---|---|---|---|
    | naive — one fsync per proposal | 632 ops/s | 100.8 ms | 1.0 |
    | **group commit** | **5,856 ops/s (9.3×)** | **10.5 ms** | **16.6** |
    | no fsync *(unsafe, measurement only)* | 28,641 ops/s | 1.9 ms | 2.0 |

    Concurrency sweep with group commit: 227 → 954 → 3,325 → 8,906 ops/s at 1 → 8 → 32 → 128 clients.
    Single-client p50 is 4.4 ms, which is one fsync plus one replication round trip.
  - **The naive baseline was exactly one fsync per operation** — 632 ops/s against a measured ~1.6 ms
    fsync. Not approximately: the throughput ceiling *was* the fsync rate.
  - **Group commit works by converting fsync latency into batching**, and the mean batch size is the
    proof rather than the story: 1.0 → 16.6. Batch size is arrival rate times service time, which is
    also why it needs concurrent load to do anything.
  - **Pipelining was measured, did not help, and is off by default.** Group commit and pipelining are
    in *tension*: letting a proposal start its own replication round instead of queueing behind the
    current fsync makes every batch smaller. At `max-inflight=16` throughput fell 5% on loopback and
    **9× in the no-fsync configuration** (3,198 vs 28,641), because that configuration depends
    entirely on batching. The mechanism is implemented and covered by the fault suites; it is off
    because there is no evidence for it, not because it is unfinished.
  - **Two bugs found by benchmarking, both invisible to every correctness test:**
    1. **A pipelining window-slot leak.** `inflight` was decremented only after the `MatchIndex >
       pr.match` check, so an acknowledgement carrying no new index never released its slot. The
       window jammed until the next heartbeat reset it, capping throughput at one batch per heartbeat
       interval. Invisible on loopback, where the reply arrives before the next send is attempted.
    2. **A bug in my own measurement instrument.** The latency proxy slept inline before each write,
       which serialises the delay and turns a 2 ms "latency" into a 500-message-per-second bandwidth
       cap. The first pipelining benchmark run through it reported that pipelining made throughput
       *worse* — an artefact of the harness, not a property of the system. There is now a test
       (`TestProxyLatencyIsLatencyNotBandwidth`) asserting that 50 pipelined messages through a 20 ms
       link take ~20 ms, not 50 × 20 ms. **Instruments need tests too.**
  - **fsync is ~80% of the cost** at this batch size (5,856 → 28,641 with it disabled). Note the
    no-fsync figure is *not* a clean isolation: removing the fsync also removes what proposals were
    queueing behind, so mean batch collapses from 16.6 to 2.0. It is an upper bound on what durability
    costs, not a measurement of it.
  - **Every safety suite re-run after each optimisation** — full DST, chaos, snapshot-heavy chaos, and
    the real-cluster suite — all clean. An optimisation that breaks an invariant is not an optimisation.
- [x] **Phase 9 — Read paths: log vs ReadIndex vs lease** *(DONE)*
  - `raft/read.go` (ReadIndex with **batched** confirmation rounds, leader leases, deferred reads
    until the term's no-op commits), read modes in the node runtime, `-read-mode` and
    `-lease-duration` flags.
  - **Result (3 nodes, one machine):**

    | read path | 64 clients | p50 @ 1 client | log entries written |
    |---|---|---|---|
    | through the log | 5,737 ops/s | 4.95 ms | 85,290 |
    | **ReadIndex** | **22,408 ops/s (3.9×)** | **1.20 ms (4.1×)** | **0** |
    | **leader lease** | **36,270 ops/s (6.3×)** | **0.27 ms (18.3×)** | **0** |

    The "log entries written" column is the instrumentation confirming the design rather than
    describing it: both fast paths applied exactly one entry — the election no-op — while serving
    tens of thousands of reads per second.
  - **Batching the confirmation round was worth 9.9×.** The first implementation issued one heartbeat
    quorum *per read*, and measured 2,574 ops/s at 64 clients — **slower than reading through the
    log**, because reads through the log at least inherit group commit. One quorum proves leadership
    at one instant, and that single proof serves every read invoked before it: 2,574 → 25,478 ops/s.
    The same amortisation as Phase 8, applied to a different path.
  - **The lease buys latency, not throughput.** At 64 clients batched ReadIndex is within ~35% of the
    lease and needs no clock assumption at all; at one client the lease is 4.4× faster (0.27 ms vs
    1.20 ms). So the clock risk is worth taking for latency-sensitive reads and close to pointless
    under load — which is the opposite of how leases are usually motivated.
  - **The clock assumption is documented and then demonstrated, not asserted.** Two tests pin it:
    - `TestLeaseBoundIsWhatMakesLeaseReadsSafe` sets the lease *longer* than the election timeout,
      partitions the leader from a quorum, lets the majority elect a successor and commit a write,
      then reads from the deposed leader — and requires a **stale read**. It gets one: `v1` while the
      cluster holds `v2`.
    - `TestSafeLeaseRefusesRatherThanLies` repeats it with the lease inside its bound, and the
      deposed leader **refuses** the read instead. Unavailable is a correct answer; wrong is not.
  - **A test bug worth recording: the read-path tests were green while testing nothing.** The harness
    routed reads through `Node.Apply` rather than `Node.Read`, so all three "read modes" went through
    the log. The lease counters reading 0/0 is what exposed it — the correctness assertions could not
    have. Instrumentation caught what assertions could not.
- [x] **Phase 10 — Membership changes, pre-vote, leadership transfer** *(DONE)*
  - `raft/prevote.go` (pre-vote + CheckQuorum), `raft/transfer.go` (§3.10), `raft/membership.go`
    (single-server changes, learners, both §4 guards), membership churn wired into the chaos driver
    (`-membership`).
  - **Pre-vote: 6.1× fewer elections.** Mean final term over 60 identical chaotic runs fell from
    **31.7 to 5.2**. Every term is an election that happened, and under chaos most of them are
    elections that did not need to — a node returns from a partition or a restart, times out before
    hearing a heartbeat, and campaigns against a leader it cannot beat, deposing it on the way. This
    is the same effect seen in Phase 5's demo, where the term jumped 2 → 4 when a node rejoined.
  - **Leadership transfer: 2 ms versus >250 ms.** A handover costs a round trip where waiting for an
    election costs a full timeout — 125× on the harness's settings.
  - **Membership changes apply on append, not on commit**, because the entry adding a node must be
    replicated under the configuration that includes it. The price is that an uncommitted
    configuration can be truncated and must **roll back**, which is the part most implementations skip.
    Both §4 safety guards are enforced: one uncommitted change at a time, and the leader must have
    committed an entry of its own term first.
  - **Result — 24,000 membership-churn seeds, zero violations** (3- and 5-node, with and without
    aggressive compaction; the snapshot run alone was 425M events), plus every earlier soak clean.
  - **Five real bugs, four of them found by the fault injector:**
    1. **`raft.New` never rebuilt the configuration from the log.** A restarting node came back with
       its *bootstrap* membership and silently forgot every change since, counting quorums against a
       voter set that no longer existed. Caught as `LeaderCompleteness` within a few seeds of turning
       membership churn on.
    2. **Snapshots recorded the *current* configuration**, which can include an uncommitted change.
       Baking that in makes it unrevocable — a node restoring the snapshot has no earlier
       configuration to roll back to. Now the committed configuration is stored.
    3. **Restart ignored the snapshot's configuration** and used the bootstrap one. Added
       `storage.RecoverState`, which prefers the snapshot's.
    4. **Pre-vote deadlock, 1 seed in 4,000.** Contentment was measured from a node's own election
       deadline, but a node that is not a voter resets that deadline every tick without ever
       campaigning — so it looked permanently content and refused *every* pre-vote. One learner was
       enough to deadlock a five-node cluster. Now measured from actual leader contact.
    5. `syncProgress` deleted the leader's own replication state when it removed itself, panicking on
       the next append. A leader must keep replicating until its own removal commits — that entry is
       committed by a quorum it is driving but no longer part of.
  - **Pre-vote and CheckQuorum default *on* in `kvserver`** and off in the simulator, so the
    comparison stays measurable and the older soaks stay comparable.
- [x] **Phase 11 — Observability and the write-up** *(DONE)*
  - `metrics/` (Prometheus exposition format, written in-house — the format is a few lines and the
    alternative is a large dependency tree for one endpoint), `GET /metrics`, per-node labels.
    Every durable write is timed through a `Storage` decorator, so the fsync histogram covers whatever
    the node actually waits for rather than one syscall.
  - **Metrics chosen by what the benchmarks showed actually matters:** fsync latency (the write path's
    ceiling, from Phase 8), group-commit batch size (how many fsyncs were saved), commit-to-apply lag
    (the earliest warning that reads will block), and reads split by path (Phase 9). A metric nobody
    would act on is not worth exporting.
  - **Failover measured as a distribution and a trade-off, not a number.** `dst -failover` sweeps the
    election timeout with 500 leader kills per point, and *also* counts elections held while nothing
    was wrong. Reporting only the first half is what makes "failover in under X ms" misleading — it
    can always be made smaller, and the bill arrives as unnecessary elections.

    | election timeout | failover p50 | p99 | unnecessary elections/run |
    |---|---|---|---|
    | 50 ms | 84 ms | 399 ms | 0.02 |
    | 150 ms | 239 ms | 946 ms | 0.00 |
    | 400 ms | 600 ms | 2,846 ms | 0.00 |

  - **Finding: pre-vote's benefit is the tail, not the median.** It adds a poll round, so p50 rises —
    but at a 600 ms timeout it cuts p99 from 4,463 ms to 1,850 ms and the maximum from 7,428 ms to
    2,515 ms. Without it a disruptive candidate can force extra elections *during* a failover.
  - **A measurement-design bug worth recording.** The first sweep used a 25 ms link, which put a
    50-150 ms election timeout *below* the round trip. CheckQuorum then correctly refused to let a
    leader claim authority it could not verify, and 485 of 500 runs got stuck — the sweep was
    measuring an unworkable cluster rather than an aggressively tuned one. Fixed by holding a
    datacenter-like 3 ms link fixed while varying only the timeout, which is the comparison that was
    intended.
  - README restructured around the whole narrative: headline table, the purity contract, both
    harnesses, all the performance results with graphs, **the twenty-one bugs the harnesses found**
    (including three in the tooling itself), what random search did *not* find, and an honest gaps
    section.

---

## Phase 0 — Foundations

Set up so that nothing later has to be undone.

- Go module, `Makefile`, `golangci-lint`, CI that runs `go test -race ./...` plus a short DST soak.
- **Interfaces first**, because they are the seam both harnesses plug into:
  - `Clock` — a monotonic `Now() Time` (int64 nanos). *Never* wall-clock. DST supplies virtual nanos;
    production supplies `time.Since(start)`.
  - `Storage` — `AppendEntries`, `TruncateSuffix`, `TruncatePrefix`, `SetHardState`, `Entries(lo,hi)`,
    `SaveSnapshot`, `LoadSnapshot`, `Sync`. Implemented by `mem/` (Phase 3) and `wal/` (Phase 4).
  - `Transport` — `Send(NodeID, Message)`. Fire-and-forget, may drop. Implemented by `sim/` and `grpc/`.
- Core types: `Term`, `Index`, `NodeID`, `Entry{Index, Term, Type, Data}`, `HardState`, `Message`,
  `Effect`, `Config`.
- The **purity test** (import-graph assertion) — written now, before there is anything to violate it.
- **Deliverable:** `go test ./...` green, purity test passing on an empty core.
- **Done.** See the progress note above. One deviation from the original sketch, and it is an
  improvement: the purity test grew from a direct-import check into four checks, because a direct
  allowlist alone would not have caught a goroutine (which needs no import at all) or a mutable
  global (which breaks seed reproducibility across runs in one process, the most confusing possible
  way for determinism to fail). Each check was verified to fail on a deliberately introduced
  violation before being trusted.

## Phase 1 — Core: leader election

Follower/Candidate/Leader transitions and the vote, with nothing else.

- `Tick(now)` drives randomized election timeouts. **Randomness is a seeded `*rand.Rand` passed into
  the core at construction** — never `math/rand` global, or determinism dies silently.
- RequestVote with the **election restriction (§5.4.1)**: reject unless the candidate's log is at
  least as up to date — compare `lastLogTerm` first, then `lastLogIndex`. Getting this backwards
  produces a cluster that works perfectly until it silently loses committed data.
- Any message with a higher term → step down and adopt the term, *before* any other processing.
- Emit `PersistHardState` **before** `SendMessage(vote granted)` (rule A).
- A minimal single-goroutine driver in `cmd/dst` — no faults yet, just deliver every message in order.
- **Checkpoint:** 3 and 5 nodes elect exactly one leader in virtual time; re-elect after the leader
  stops ticking; two identical seeds produce byte-identical event traces.
- **Done.** All met, across 1/3/5/7-node clusters and 40,000 seeds. Note what is deliberately *not*
  here: `handleApp` implements only the heartbeat half. The consistency check, conflict resolution
  and commit advance are Phase 2, and no node sends entries yet, so ignoring them cannot mask a bug.

## Phase 2 — Core: log replication and the commit rules

- `Propose(cmd)` → leader appends → `AppendEntries` to followers → commit on quorum → `ApplyEntries`.
- **The AppendEntries consistency check and its two traps:**
  - **Do not blindly truncate** at `prevLogIndex+1`. Truncate *only* on an actual term conflict. A
    delayed or duplicated `AppendEntries` will otherwise destroy entries that are already committed.
    This is the most common way a from-scratch Raft loses data, and it is precisely the bug the
    reorder/duplicate faults in Phase 3 exist to catch.
  - **Fast backtracking:** naive `nextIndex--` takes one round trip per entry. The follower returns
    `conflictTerm` + `conflictIndex` so recovery costs one round trip per *term*.
- **The commit rule, stated exactly (§5.4.2):** advance `commitIndex` to `N` only if a majority of
  `matchIndex >= N` **and** `log[N].Term == currentTerm`. A leader may never commit an entry from a
  previous term by counting replicas — this is the Figure-8 scenario, and it is the single most
  commonly botched line in every Raft implementation on GitHub.
  **Mitigation:** on election, immediately append a no-op entry of the current term. This also gives
  the new leader a way to learn its true commit index, which Phase 9's ReadIndex depends on.
- Apply strictly in index order, exactly once, never beyond `commitIndex`.
- **Checkpoint:** entries replicate and commit on 3/5 nodes; a lagging follower catches up in O(terms)
  round trips; the Figure-8 scenario is a hand-written regression test that fails against the naive
  commit rule and passes against the correct one.
- **Done.** All met. Not yet here: `sendAppend` gives up silently when a follower has fallen behind
  the compaction boundary, because `InstallSnapshot` is Phase 6. That is visible rather than silent —
  the follower simply stops advancing — but it is a real gap until then.

## Phase 3 — DST harness: the bug-finding engine *(centerpiece #1)*

Everything above was written to make this phase possible. This is the primary correctness engine.

- **One goroutine. One seeded RNG. One priority queue of `(virtual_time, event)`.** Time advances by
  popping the next event, never by sleeping. No wall-clock anywhere in the loop.
- **Network model**, all parameters drawn from the same seeded RNG:
  drop · delay (configurable distribution, incl. heavy tail) · duplicate · reorder ·
  **asymmetric one-way partitions** (A can reach B but not the reverse — the case that breaks naive
  leader-liveness assumptions and that symmetric-only testing never finds) · heal.
- **Node faults:** clean crash (stop calling `Tick`/`Step`), restart (rebuild *only* from `Storage`),
  and **process pause/stall** — a node frozen for seconds while the world moves on. The stall fault is
  what will break lease reads in Phase 9, and it is the fault most simulators forget.
- **Clock skew** per node, within and beyond the bound lease reads assume.
- **Workload driver:** concurrent virtual clients issuing put/get/CAS, retrying on failure, with every
  call/return recorded into a history for Phase 7's linearizability checker.
- **Invariant checker, evaluated continuously** (it can see every node's full state — a luxury only
  simulation gives):
  1. **Election safety** — at most one leader per term, across all nodes and all time.
  2. **Log matching** — same (index, term) on two nodes ⇒ all preceding entries identical.
  3. **Leader append-only** — a leader never overwrites or deletes an entry in its own log.
  4. **Leader completeness** — an entry committed in term T appears in every leader of term > T.
  5. **State machine safety** — no two nodes apply different commands at the same index.
  6. **Monotonicity** — `commitIndex` and `appliedIndex` never decrease, including across restarts.
  7. **Durability** — an entry that was acked committed is present after any crash/restart.
- **Reproducibility contract:** `dst -seed=N` replays bit-for-bit. A CI failure prints one integer,
  and that integer is a complete bug report.
- **Soak mode:** thousands of seeds in parallel, minimizing any failing seed to a short trace.
- **Checkpoint:** ≥10,000 seeds × ≥100k virtual events, zero invariant violations; a deliberately
  reintroduced bug (revert the §5.4.2 commit rule) is caught in under a second and replays exactly.
  **This checkpoint is the resume bullet.**
- **Done, with one part of the checkpoint failed and worth reporting as a result rather than quietly
  restating.** 30,000 seeds and 1.17B events, zero violations, every seed replaying bit-for-bit — but
  the §5.4.2 revert specifically was *not* found by random search (see above). Two other injected bugs
  were caught within seconds. The checkpoint as written assumed DST would find any real bug; the
  measurement says otherwise, and that is the more useful thing to know.

## Phase 4 — Persistence: segmented WAL + crash recovery

Now that the core is trustworthy, make it survive power loss.

- **Segmented WAL** (~64 MB segments), record framing `[crc32 | length | type | payload]`,
  types: entry · hardstate · snapshot-meta.
- **Torn-write handling:** on replay, stop at the first bad CRC or short record and truncate there.
  This is *correct*, not a hack: anything after an incomplete record was by definition never fsync'd,
  so it was never acknowledged to anyone.
- **Preallocate** segments so appends don't trigger filesystem metadata updates, and **fsync the
  parent directory** after creating a segment — otherwise the file itself can vanish in a crash.
  This is the classic durability bug that survives every test *except* a real power-loss test.
- `TruncatePrefix` (compaction) drops whole segments; `TruncateSuffix` (log conflict) rewrites the
  tail segment.
- **Storage fault injection**, run under the *same* DST harness: torn writes, short writes,
  fsync that silently does nothing, crash *between* the write and the fsync, reordered writes.
- **Checkpoint:** kill the process at a randomized byte offset mid-append, 1,000×; recovery always
  yields a prefix of the log with no acked entry missing. DST suite still green with the WAL swapped
  in for in-memory storage.
- **Done, and strengthened:** every byte offset is tested rather than 1,000 random ones, which is both
  cheaper to run and strictly stronger. Not yet here: fault injection *below* the file API — a Sync
  that silently does nothing, or reordered writes across a barrier — which needs an injectable file
  layer. The torn-write and corruption cases those faults produce are covered; the missing piece is a
  storage stack that lies about durability, and it belongs with Phase 8's fsync work.

## Phase 5 — KV state machine + gRPC API + a real cluster

The first phase that produces something you can `curl`.

- `kv/`: a map behind an `Apply(Entry)` interface, plus `Snapshot()`/`Restore()`.
- **At-most-once client semantics** — `(clientID, seqNo)` with a dedup table in the state machine.
  Without it, a client that retries across a failover applies its write twice, and **linearizability
  is violated by design** no matter how correct Raft is. **The dedup table must be included in the
  snapshot** — omitting it is a bug that only appears after a compaction plus a restart.
- gRPC: `Raft` service (peer RPC) and `KV` service (`Get`/`Put`/`Delete`/`CAS`/`Watch`), plus a thin
  HTTP/JSON shim for demos and `curl`.
- Not-leader responses carry a **leader hint**; the client library follows it and retries.
- `server/`: the node runtime — a single goroutine owning the core, executing effect batches in order
  and honoring the fsync barrier. All concurrency lives *here*, never in `raft/`.
- **Checkpoint:** a 3-node cluster on one machine serves reads and writes through gRPC and HTTP;
  `kill` the leader and the client transparently continues.

## Phase 6 — Snapshots, log compaction, InstallSnapshot

- Trigger compaction on log size; snapshot `(lastIncludedIndex, lastIncludedTerm, state, dedup table)`.
- `InstallSnapshot` for followers the leader has already compacted past.
- **Three details that are each a real bug:** the snapshot boundary must still satisfy the
  `AppendEntries` consistency check (keep `lastIncludedTerm`); a *stale* `InstallSnapshot` arriving
  late must be ignored rather than rolling state backwards; and snapshot install must be atomic
  (write to temp, fsync, rename, fsync dir).
- **Checkpoint:** a follower crashed for long enough to fall behind compaction recovers via snapshot;
  DST runs with an aggressively low compaction threshold so *most* catch-up goes through snapshots.
- **Done.** Both met, in simulation (`-snapshot`) and on a real cluster
  (`TestSnapshotCompactsAndFollowerCatchesUp`), which also checks the session table survives the
  snapshot round trip — otherwise a client retrying after the restored node becomes leader is applied
  twice.

## Phase 7 — Real-cluster chaos + linearizability *(centerpiece #2)*

Prove it outside the simulator.

- `chaos/`: supervises real `kvserver` processes and applies **real** faults — `kill -9` mid-write,
  `SIGSTOP` (a real process stall), `iptables -j DROP` partitions including one-way,
  `tc netem` delay/loss/reorder, disk-full, clock skew via a `Clock` shim.
- **History recorder** with correct call/return timestamps. The critical subtlety: an operation that
  **times out is indeterminate, not failed** — it may or may not have committed. It must be recorded
  as an unreturned call so the checker may linearize it anywhere after its invocation *or not at all*.
  Recording timeouts as failures is the #1 source of both false alarms and false confidence.
- **Checker:** use **Porcupine** (`anishathalye/porcupine`) as the trusted oracle first — it is the
  standard Go linearizability checker and it is well tested, so results are credible immediately.
  *Optional stretch:* write a Wing–Gong checker and **cross-validate against Porcupine** on identical
  histories. Cross-validation is a better story than a checker you wrote and only trust because you
  wrote it.
- The **same invariant checker** from Phase 3 runs over the cluster's structured logs.
- **Checkpoint:** ≥1 hour of continuous chaos against a real 5-node cluster with a concurrent
  workload — zero linearizability violations, zero invariant violations, zero acknowledged writes
  lost. **This is the second resume bullet.**
- **Done.** 60 minutes, 222,572 operations, 3,330 faults, clean. Not done: the plan's optional
  Wing–Gong checker cross-validated against Porcupine. Porcupine is the trusted oracle here, and a
  second checker is worth writing only if there is a reason to doubt the first.

## Phase 8 — Performance: batching, group commit, pipelining

The "find the bottleneck → re-architect → quantify" arc, on a correctness-first codebase where every
optimization is re-validated by Phases 3 and 7.

- **Baseline (deliberately naive):** one entry per `AppendEntries`, one fsync per entry, one in-flight
  RPC per follower. Measure it. It will be humiliating, and that is the point — it is the "before."
- **Group commit:** batch concurrent proposals into one fsync. fsync is the floor; amortizing it is
  the single largest win available.
- **Pipelining:** multiple `AppendEntries` in flight per follower, with correct `nextIndex` bookkeeping
  on out-of-order responses — the change most likely to introduce a subtle bug, which is exactly why
  it lands *after* the DST harness exists.
- **The safe relaxation:** transmit `AppendEntries` before the leader's own fsync completes, while
  still not advancing `matchIndex[self]` until it does (rule C, precisely).
- **Isolate the fsync cost:** measure with fsync disabled (unsafe; measurement only) to quantify what
  durability actually costs, rather than guessing.
- **Deliverable:** a throughput/latency table across all four configurations, and a graph. Then
  **re-run the full DST soak and the chaos suite** — an optimization that breaks an invariant is not
  an optimization. Say so explicitly in the write-up.
- **Done**, with one planned item deliberately not built: transmitting `AppendEntries` before the
  leader's own fsync completes. It would save perhaps 2.7 ms of a 10.5 ms median, but it requires the
  core to learn about *asynchronous* persistence — `matchIndex[self]` may only advance once the write
  lands, so the core would need a `PersistedTo(index)` callback and the effect list would stop being a
  strictly ordered script. That contract is the foundation everything else in this project rests on,
  and trading it for ~25% of median latency, on a path already amortised 16.6× by group commit, is a
  bad exchange. Worth revisiting only if fsync ever stops being amortised.

## Phase 9 — Read paths: log vs ReadIndex vs lease

Three ways to serve a **linearizable** read, implemented and compared. This is the feature most likely
to be probed in an interview, because it is where people quietly cheat.

1. **Read-through-log** — the read becomes a log entry. Obviously correct, one full consensus round.
2. **ReadIndex** — record `commitIndex`, confirm leadership with a heartbeat quorum, wait for
   `appliedIndex >= readIndex`, then serve. No disk write. **Requires the Phase-2 no-op**, because a
   fresh leader does not know its true commit index until it commits an entry of its own term.
3. **Leader lease** — serve locally while within `election_timeout − max_clock_drift` of the last
   successful heartbeat quorum. Fastest, and **unsafe if clock drift exceeds the assumed bound**.
- **Document the lease assumption honestly, then test it:** point the DST harness at it with clock
  skew *beyond* the bound plus process stalls, and show the stale read appear. Demonstrating the exact
  conditions under which your own fastest path is unsafe is a stronger signal than claiming it is safe.
- **Deliverable:** p50/p99 latency and throughput for all three, plus the stale-read demonstration.
- **Done.** All safety suites re-run clean afterwards — the read path touches the core, and ReadIndex
  in particular depends on the §5.4.2 no-op that Phase 2 introduced for an unrelated reason.

## Phase 10 — Membership changes, pre-vote, leadership transfer

- **Single-server changes (§4).** Apply a config entry **on append, not on commit** — and therefore
  handle the ugly consequence: if that entry is later truncated, the configuration must **roll back**.
  This is the subtlety that makes membership changes notorious.
- **The known unsafety, and its fix:** overlapping single-server changes can violate safety. Guard
  with both accepted rules — the leader must have committed an entry from its current term before
  starting a change, and only one uncommitted config change may be in flight.
- **Learners:** new nodes join as non-voting and catch up before counting toward quorum, so adding a
  node to a 3-node cluster does not put availability at risk while it replays the log.
- A leader that removes **itself** must step down after committing the change.
- **Pre-vote:** a candidate polls without incrementing its term, so a node returning from a partition
  cannot force the cluster through a needless election with an inflated term. Pair with **CheckQuorum**.
- **Leadership transfer:** bring the target fully up to date, then `TimeoutNow` — zero-downtime restarts.
- **Point DST specifically at this phase:** membership-change bugs are the subtlest in Raft, and this
  is where a deterministic replay earns its cost. Run config changes *concurrently with* partitions,
  crashes, and compaction.
- **Done, and it paid for itself immediately.** Turning membership churn on in the chaos driver
  surfaced bug 1 within a handful of seeds and bug 4 only at 1-in-4,000 — neither reachable by
  hand-written tests, and the second not reachable by a small soak either.

## Phase 11 — Observability and the write-up

- Structured logs (term, role, index transitions) and Prometheus metrics: elections, term, commit and
  apply lag, **fsync latency histogram**, AE batch size, in-flight RPCs, proposal p50/p99, read
  latency by path.
- **Measure the failover number properly** rather than asserting it: kill the leader 1,000× under
  load, plot the *distribution* of time-to-first-successful-write, and show the real trade-off curve —
  aggressive election timeouts cut failover time but raise the spurious-election rate under load.
  A distribution and a trade-off curve is a much better answer than a single number.
- README as an engineering narrative, matching the four groups. Headline table, graphs, and — in the
  style that made the HTTP server project land — an **honest gaps section**.
- **Resume bullets, filled in only with measured numbers:**
  - *"Implemented Raft consensus from scratch in Go as a pure I/O-free state machine, achieving
    automatic leader failover in <X ms (p99) across a 5-node cluster."*
  - *"Built a deterministic simulation harness (virtual clock, seeded fault injection) plus a real
    `kill -9` / network-partition chaos suite, validating linearizability and Raft's safety invariants
    across N seeds — every failure reproducible from a single integer."*
  - *"Raised throughput Nx via group-commit fsync and AppendEntries pipelining, re-validating all
    safety invariants after each optimization."*
- **Done. The bullets, filled in from measurements rather than estimates:**
  - *"Implemented Raft consensus from scratch in Go as a pure I/O-free state machine — 7 transitive
    dependencies, none of them `os`, `net` or `time`, enforced by a build-failing purity test —
    achieving automatic leader failover with p50 239 ms / p99 946 ms measured over 500 leader kills."*
  - *"Built a deterministic simulation harness (virtual clock, seeded fault injection, exact replay
    from one integer) plus a real `kill -9` / network-partition chaos suite with Porcupine
    linearizability checking: 84,000 seeds and ~2.7 billion simulated events, plus one hour of
    continuous real-cluster chaos (222,572 operations, 3,330 faults), with zero safety violations.
    Found 21 real bugs, including 3 in the test harness itself."*
  - *"Raised write throughput 9.3x with group-commit fsync and read throughput 6.3x with ReadIndex and
    leader leases, re-running the full fault suite after every optimisation; measured and then
    *disabled* AppendEntries pipelining after showing it competes with group commit."*

---

## Verification (per phase — nothing advances until these hold)

1. **Purity:** the import-graph test passes. `raft/` never gains a dependency on time, net, or disk.
2. **Determinism:** the same seed produces a byte-identical trace, on every phase, forever. If this
   ever breaks, fix it before anything else — every other guarantee is downstream of it.
3. **Invariants:** all seven checks green across the full DST soak *and* over real-cluster logs.
4. **Linearizability:** zero violations across DST histories and chaos histories, with indeterminate
   operations recorded correctly.
5. **Durability:** no acknowledged write is ever lost — across simulated crashes, real `kill -9`,
   and torn-write injection.
6. **Regression:** every phase re-runs the full DST soak. Later phases (especially 8 and 10) are the
   ones most likely to break earlier guarantees, which is the entire reason the harness came first.
7. **Race freedom:** `go test -race ./...` green; note that this is a *weak* check here by design —
   the core is single-threaded, so the race detector has almost nothing to say. The DST harness is
   the real check.

## Known risks, stated up front

- **The effect-ordering rule is the whole ballgame.** If the runtime ever executes effects out of
  order or skips an fsync barrier, the core stays provably correct and the *system* silently loses
  data. Mitigation: the executor is one small, heavily-tested function, and Phase 4's storage fault
  injection targets exactly this.
- **DST only explores what you inject.** It cannot find a fault class the network model does not
  express. Mitigation: Phase 7 exists precisely because real `kill -9` and real `iptables` are not a
  model of anything.
- **`Tick`-driven virtual time can mask real scheduling delay.** Mitigation: the pause/stall fault,
  plus the real cluster.
- **Go's GC can stall a real node for longer than a lease assumes.** This is not hypothetical, and it
  is the honest caveat on the lease-read path — measure it in Phase 9 rather than hand-waving it.
