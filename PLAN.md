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
and a bug found in production can be replayed in simulation from a seed.

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
- [ ] **Phase 3 — DST harness: the bug-finding engine** *(centerpiece #1)*
- [ ] **Phase 4 — Persistence: segmented WAL + crash recovery**
- [ ] **Phase 5 — KV state machine + gRPC API + a real cluster**
- [ ] **Phase 6 — Snapshots, log compaction, InstallSnapshot**
- [ ] **Phase 7 — Real-cluster chaos + linearizability** *(centerpiece #2)*
- [ ] **Phase 8 — Performance: batching, group commit, pipelining**
- [ ] **Phase 9 — Read paths: log vs ReadIndex vs lease**
- [ ] **Phase 10 — Membership changes, pre-vote, leadership transfer**
- [ ] **Phase 11 — Observability and the write-up**

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
