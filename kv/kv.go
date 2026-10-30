// Package kv is the replicated state machine: a key-value store applied from
// the Raft log.
//
// # Why at-most-once matters more than it looks
//
// Raft guarantees every replica applies the same commands in the same order. It
// does not guarantee a client's command is applied *once*. A client that sends a
// write, times out because the leader died mid-commit, and retries against the
// new leader can have that write applied twice — the first copy was committed
// before the leader fell over, and the retry is a second, distinct log entry.
//
// For an idempotent Put that is invisible. For anything that reads-then-writes
// it is a lost update, and it breaks linearizability no matter how correct the
// consensus layer is. So every command carries (ClientID, Seq), the store keeps
// the last sequence number and *result* per client, and a duplicate returns the
// original answer instead of re-executing.
//
// The session table is part of the state machine and therefore part of the
// snapshot. Omitting it from the snapshot is a bug that only appears after a
// compaction *and* a restart *and* a retry — which is to say, in production.
package kv

import "maps"

// Op is a command's operation.
type Op uint8

const (
	// OpGet reads a key. It is only ever logged on the read-through-log path;
	// the ReadIndex and lease paths in Phase 9 serve reads without a log entry.
	OpGet Op = iota + 1
	OpPut
	OpDelete
	// OpCAS writes only if the current value matches Expected. It is the
	// operation that makes duplicate application observable, and therefore the
	// one the session table exists for.
	OpCAS
)

// Command is one state machine operation.
type Command struct {
	Op    Op
	Key   string
	Value []byte

	// Expected is the compare value for OpCAS. HasExpected distinguishes
	// "expect this value" from "expect the key to be absent", which a nil slice
	// alone cannot express.
	Expected    []byte
	HasExpected bool

	// ClientID and Seq identify the client and its command. Seq must increase
	// per client; a command at or below the last applied Seq is a retry and
	// returns the stored result.
	ClientID uint64
	Seq      uint64
}

// Result is the outcome of applying a [Command].
type Result struct {
	// OK is false for a CAS whose comparison failed. It is not an error.
	OK bool
	// Value and Found describe the key after (or, for OpGet, at) application.
	Value []byte
	Found bool
	// Duplicate reports that this was a retry answered from the session table
	// rather than re-executed. Observability only — the answer is identical.
	Duplicate bool
}

type session struct {
	Seq    uint64
	Result Result
}

// Store is the replicated key-value state.
//
// It is not safe for concurrent use. The node runtime owns it from the single
// goroutine that drives the Raft core, so every replica applies in the same
// order with no locking on the apply path.
type Store struct {
	data     map[string][]byte
	sessions map[uint64]session
}

// New returns an empty store.
func New() *Store {
	return &Store{
		data:     make(map[string][]byte),
		sessions: make(map[uint64]session),
	}
}

// Apply executes an encoded command and returns its result. Commands must be
// applied in log order, exactly once per index.
func (s *Store) Apply(data []byte) Result {
	cmd, err := DecodeCommand(data)
	if err != nil {
		// A command that will not decode is the same on every replica, so
		// skipping it keeps them identical. Diverging here — panicking on one
		// node and not another — would be far worse than ignoring it.
		return Result{}
	}
	return s.ApplyCommand(cmd)
}

// ApplyCommand executes a decoded command.
func (s *Store) ApplyCommand(cmd Command) Result {
	// Reads never mutate state, so they are never deduplicated: replaying a read
	// at a different log position legitimately yields a different answer.
	if cmd.Op == OpGet {
		v, ok := s.data[cmd.Key]
		return Result{OK: true, Value: clone(v), Found: ok}
	}

	if cmd.ClientID != 0 {
		if prev, ok := s.sessions[cmd.ClientID]; ok && cmd.Seq <= prev.Seq {
			r := prev.Result
			r.Duplicate = true
			return r
		}
	}

	var res Result
	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = clone(cmd.Value)
		res = Result{OK: true}

	case OpDelete:
		_, found := s.data[cmd.Key]
		delete(s.data, cmd.Key)
		res = Result{OK: true, Found: found}

	case OpCAS:
		cur, found := s.data[cmd.Key]
		match := (cmd.HasExpected && found && string(cur) == string(cmd.Expected)) ||
			(!cmd.HasExpected && !found)
		if match {
			s.data[cmd.Key] = clone(cmd.Value)
		}
		res = Result{OK: match, Value: clone(cur), Found: found}

	default:
		res = Result{}
	}

	if cmd.ClientID != 0 {
		s.sessions[cmd.ClientID] = session{Seq: cmd.Seq, Result: res}
	}
	return res
}

// Get reads a key directly, for the ReadIndex and lease read paths that serve
// without appending to the log.
func (s *Store) Get(key string) ([]byte, bool) {
	v, ok := s.data[key]
	return clone(v), ok
}

// Len returns the number of keys held.
func (s *Store) Len() int { return len(s.data) }

// Keys returns every key, for tests and debugging.
func (s *Store) Keys() map[string][]byte {
	out := make(map[string][]byte, len(s.data))
	maps.Copy(out, s.data)
	return out
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
