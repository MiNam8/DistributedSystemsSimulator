# A Distributed Key-Value Store on Raft Consensus

**Proving a consensus implementation correct under adversarial failure — by construction, not by demo.**

A from-scratch Raft implementation in Go, built around one decision made on line one and never
relaxed: **the consensus core is a pure, I/O-free state machine.** It has no goroutines, no channels,
no clock, and no unseeded randomness. It accepts events and returns *effects as data*; the caller
performs the I/O.

That single constraint is what makes the rest possible. The same core is driven by two harnesses — a
deterministic simulator (virtual clock, seeded fault injection) and a real multi-process cluster
(gRPC, fsync'd write-ahead log, `kill -9`, `iptables`) — so a bug found in simulation is a bug in
production, and a bug seen in production replays in simulation from a single integer seed.

> **Status: Phase 2 of 11 complete.** Leader election and log replication work, verified across
> 70,000 simulated seeds with zero safety violations. Persistence is Phase 4; fault injection is
> Phase 3. See [PLAN.md](PLAN.md) for the full roadmap.

## Where it is

Leader election and log replication, running on a virtual clock under the deterministic simulator.
Each seed writes continuously, kills the leader mid-write, and must converge with no safety violation:

```
$ go run ./cmd/dst -soak -seeds=10000 -nodes=5
ran 10000 seeds on 5-node clusters
failover: p50 377 ms  p90 461 ms  p99 549 ms  max 928 ms
no failures
```

| cluster | failover p50 | p90 | p99 | max |
|---|---|---|---|---|
| 3 nodes | 422 ms | 563 ms | 1010 ms | 1871 ms |
| 5 nodes | 377 ms | 461 ms | 549 ms | 928 ms |
| 7 nodes | 362 ms | 425 ms | 490 ms | 917 ms |

**Larger clusters fail over faster**, which is not the obvious result. Each node draws its election
timeout from `[T, 2T)`, so the cluster's time-to-first-candidate is the *minimum* of N draws — and
that minimum falls as N grows. The three-node tail is the worst of the three (p99 1004 ms) because it
has the fewest draws to take a minimum over, and because a split vote costs a full extra round.

Any failing seed is a complete bug report:

```
$ go run ./cmd/dst -seed=42
seed 42 ok — failover in 392 ms, 211 entries applied and agreed
```

## The three bugs the tests are actually about

Each of these passes every happy-path test and loses committed data only under partition. Each has a
regression test that was **verified to fail against the naive implementation** before being trusted.

| rule | the tempting mistake | what it costs |
|---|---|---|
| §5.4.2 commit rule | commit an entry once a majority stores it | a future leader that never saw the entry overwrites it — a client was told a write was durable that then vanishes (Raft Figure 8) |
| conflict truncation | truncate at `PrevLogIndex+1` on every accepted append | a delayed or duplicated message carries an older view of the log and discards entries a quorum already acknowledged |
| acknowledgement | reply `MatchIndex = log.LastIndex()` | a follower holding speculative entries from a deposed leader reports holding entries the current leader has never seen, and they get counted toward a quorum |

The election restriction has a fourth: comparing log *index* before *term* in `IsUpToDate` lets a node
that missed committed entries win an election and overwrite them.

---

## The purity contract, enforced by the build

The core's constraint is not a convention anyone has to remember. `raft/purity_test.go` asserts four
things on every commit:

| check | what it prevents |
|---|---|
| transitive import graph excludes `time`, `net`, `os`, `sync`, `syscall`, `fmt`, `io`, `reflect`, `math/rand` | the core *cannot* read a clock — it is not linked in |
| direct imports match an explicit allowlist | an impure dependency arriving through a "harmless" helper |
| no `go` statements, channels, or `select` | nondeterministic interleaving that no seed can replay |
| no mutable package-level state | a soak run behaving differently from an isolated replay |

The whole dependency graph of the consensus core, verifiable with `make deps`:

```
errors
math/bits
runtime
unsafe
```

This is why the core uses `errors.New` sentinels and hand-written `String` methods instead of `fmt`:
importing `fmt` pulls in `os`, which pulls in `time`, which would make the transitive guarantee
unprovable. The constraint is load-bearing, not aesthetic.

## The effect protocol

The core never acts; it describes what must happen:

```go
func (r *Raft) Step(Message) []Effect
func (r *Raft) Tick(now Time) []Effect
func (r *Raft) Propose(cmd []byte) []Effect
```

**Effects are returned in strict order and must be executed in that order. Every `Persist*` effect is
an fsync barrier: it must be durable before any later `SendMessage` in the same batch leaves the
process.** That one rule encodes the three durability obligations Raft safety actually rests on —
vote durability before granting a vote, entry durability before acknowledging replication, and the
leader's own fsync before counting itself in a quorum — and it is where essentially every real-world
Raft data-loss bug lives.

## Layout

```
raft/        the pure core — import-restricted by test
storage/     Storage interface; segmented WAL and in-memory implementations
transport/   Transport interface; gRPC and simulated implementations
clock/       monotonic clock (never wall-clock)
sim/         deterministic simulation harness: virtual time, seeded faults
chaos/       real-cluster driver: kill -9, iptables, tc netem
lincheck/    linearizability checking over recorded client histories
kv/          the replicated state machine
server/      node runtime: wires the core to real I/O
```

## Building

Requires Go 1.27+.

```sh
make check    # fmt, vet, purity contract, tests — the gate for every phase
make deps     # print exactly what the core links
make purity   # the purity contract on its own
```

## Roadmap

See [PLAN.md](PLAN.md). Twelve stoppable phases; the deterministic simulation harness lands at
Phase 3, deliberately before persistence and before anything demoable, so that the two most
bug-prone phases — pipelining and membership changes — land on top of a working bug-finder rather
than in front of one.
