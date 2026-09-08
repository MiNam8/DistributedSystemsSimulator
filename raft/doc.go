// Package raft implements the Raft consensus algorithm as a pure, I/O-free
// state machine.
//
// # The purity contract
//
// This package performs no I/O, starts no goroutines, reads no clock, and uses
// no unseeded randomness. It has three entry points — [Raft.Step], [Raft.Tick]
// and [Raft.Propose] — each of which accepts an event and returns a slice of
// [Effect] values *describing* what must happen. The caller performs the I/O.
//
// This is what allows one implementation to be driven by two very different
// harnesses: a deterministic simulator (virtual clock, seeded fault injection,
// in-memory storage) and a real node runtime (monotonic clock, gRPC, an
// fsync'd write-ahead log). A bug found in simulation is a bug in production,
// and a bug seen in production can be replayed in simulation from a seed.
//
// The contract is enforced mechanically, not by convention: purity_test.go
// asserts that this package's *transitive* import graph contains no time, net,
// os, sync, syscall, context, io, fmt, reflect or math/rand — and that no file
// contains a go statement, a channel, or a select. The core cannot read the
// clock because the clock is not linked into it.
//
// That constraint is why this package uses errors.New sentinels and hand-written
// String methods instead of fmt: importing fmt would pull in os, and os pulls in
// time, which would make the transitive guarantee unprovable.
//
// # The effect ordering rule
//
// Effects are returned in a strict order and MUST be executed in that order.
// Every Persist* and Truncate* effect is a durability barrier: it must be
// durable (fsync'd) before any *later* SendMessage in the same batch leaves the
// process. See [Effect] for the three safety obligations this encodes.
package raft
