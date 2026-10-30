package lincheck_test

import (
	"testing"
	"time"

	"github.com/MiNam8/kvstore/lincheck"
)

const checkTimeout = 5 * time.Second

func put(k, v string) lincheck.Input {
	return lincheck.Input{Kind: lincheck.OpPut, Key: k, Value: v}
}
func get(k string) lincheck.Input {
	return lincheck.Input{Kind: lincheck.OpGet, Key: k}
}
func found(v string) lincheck.Output { return lincheck.Output{Value: v, Found: true, OK: true} }

var absent = lincheck.Output{OK: true}

func check(t *testing.T, r *lincheck.Recorder) lincheck.Result {
	t.Helper()
	res, err := r.Check(checkTimeout, "")
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// seq runs an operation to completion, with no concurrency.
func seq(r *lincheck.Recorder, client int, in lincheck.Input, out lincheck.Output) {
	id := r.Begin(client, in)
	r.End(client, id, out)
}

func TestSequentialHistoryIsLinearizable(t *testing.T) {
	r := lincheck.NewRecorder()
	seq(r, 0, put("k", "a"), lincheck.Output{OK: true})
	seq(r, 0, get("k"), found("a"))
	seq(r, 0, put("k", "b"), lincheck.Output{OK: true})
	seq(r, 0, get("k"), found("b"))

	if res := check(t, r); !res.Linearizable {
		t.Errorf("a plainly sequential history was rejected: %v", res)
	}
}

// TestStaleReadIsRejected is the violation this whole apparatus exists to catch:
// a read served by a leader that had already lost its mandate, returning a value
// that a completed write had superseded.
func TestStaleReadIsRejected(t *testing.T) {
	r := lincheck.NewRecorder()
	seq(r, 0, put("k", "a"), lincheck.Output{OK: true})
	seq(r, 0, put("k", "b"), lincheck.Output{OK: true})
	seq(r, 1, get("k"), found("a")) // stale: "b" already completed

	if res := check(t, r); res.Linearizable {
		t.Error("a stale read was accepted as linearizable; the checker is not actually checking")
	}
}

// TestLostWriteIsRejected covers the other direction: a write acknowledged and
// then invisible to a later read.
func TestLostWriteIsRejected(t *testing.T) {
	r := lincheck.NewRecorder()
	seq(r, 0, put("k", "a"), lincheck.Output{OK: true})
	seq(r, 1, get("k"), absent)

	if res := check(t, r); res.Linearizable {
		t.Error("an acknowledged write that vanished was accepted as linearizable")
	}
}

// TestIndeterminateOperationMayBeLinearized is the first half of the reason this
// package uses the Event API.
//
// A write times out, so the client never learns whether it committed. A later
// read observes its value. That is entirely legal: the write did commit and the
// acknowledgement was lost. A checker that treated the timeout as a failure
// would report a violation here — a false alarm on correct behaviour.
func TestIndeterminateOperationMayBeLinearized(t *testing.T) {
	r := lincheck.NewRecorder()
	id := r.Begin(0, put("k", "ghost"))
	r.Abandon(id) // no return event: outcome unknown
	seq(r, 1, get("k"), found("ghost"))

	res := check(t, r)
	if !res.Linearizable {
		t.Error("a read observing an indeterminate write was rejected; timeouts are being treated as failures")
	}
	if res.Abandoned != 1 {
		t.Errorf("abandoned = %d, want 1", res.Abandoned)
	}
}

// TestIndeterminateOperationMayBeOmitted is the second half.
//
// The same timed-out write, but nothing ever observes it. Also legal: it never
// committed. A checker that treated the timeout as a success would demand the
// value appear somewhere and report a violation — false confidence in the other
// direction.
func TestIndeterminateOperationMayBeOmitted(t *testing.T) {
	r := lincheck.NewRecorder()
	seq(r, 0, put("k", "real"), lincheck.Output{OK: true})
	id := r.Begin(1, put("k", "ghost"))
	r.Abandon(id)
	seq(r, 0, get("k"), found("real"))

	if res := check(t, r); !res.Linearizable {
		t.Error("an indeterminate write that never took effect was rejected; " +
			"a pending operation must be allowed not to happen at all")
	}
}

// TestConcurrentOperationsMayLinearizeEitherWay checks the checker is not
// simply demanding real-time order for operations that genuinely overlap.
func TestConcurrentOperationsMayLinearizeEitherWay(t *testing.T) {
	r := lincheck.NewRecorder()
	a := r.Begin(0, put("k", "a"))
	b := r.Begin(1, put("k", "b"))
	r.End(0, a, lincheck.Output{OK: true})
	r.End(1, b, lincheck.Output{OK: true})
	// Both writes overlapped, so either may be last.
	seq(r, 2, get("k"), found("a"))

	if res := check(t, r); !res.Linearizable {
		t.Error("overlapping writes were required to linearize in call order")
	}
}

// TestRealTimeOrderIsEnforced is the converse: operations that do *not* overlap
// must linearize in real-time order. Without this the checker would accept
// sequential consistency and quietly stop testing linearizability.
func TestRealTimeOrderIsEnforced(t *testing.T) {
	r := lincheck.NewRecorder()
	seq(r, 0, put("k", "a"), lincheck.Output{OK: true})
	seq(r, 1, put("k", "b"), lincheck.Output{OK: true})
	seq(r, 2, get("k"), found("a")) // "b" strictly followed "a" and strictly preceded this read

	if res := check(t, r); res.Linearizable {
		t.Error("the checker accepted a history that violates real-time order; " +
			"it is checking sequential consistency, not linearizability")
	}
}

func TestCASAndDeleteSemantics(t *testing.T) {
	r := lincheck.NewRecorder()
	seq(r, 0, lincheck.Input{Kind: lincheck.OpCAS, Key: "k", Value: "one"}, lincheck.Output{OK: true})
	seq(r, 0, get("k"), found("one"))
	seq(r, 0, lincheck.Input{Kind: lincheck.OpCAS, Key: "k", Value: "two",
		Expected: "wrong", HasExpected: true}, lincheck.Output{OK: false, Value: "one", Found: true})
	seq(r, 0, get("k"), found("one"))
	seq(r, 0, lincheck.Input{Kind: lincheck.OpDelete, Key: "k"}, lincheck.Output{OK: true, Found: true})
	seq(r, 0, get("k"), absent)

	if res := check(t, r); !res.Linearizable {
		t.Errorf("a valid CAS/delete history was rejected: %v", res)
	}
}

func TestPartitioningKeepsKeysIndependent(t *testing.T) {
	r := lincheck.NewRecorder()
	// Many keys, each with its own short history. Without partitioning by key
	// the search space is exponential in the total operation count and a run of
	// any interesting length never finishes.
	for i := range 200 {
		k := string(rune('a'+i%26)) + string(rune('a'+i/26))
		seq(r, i%8, put(k, "v"), lincheck.Output{OK: true})
		seq(r, i%8, get(k), found("v"))
	}
	res := check(t, r)
	if !res.Linearizable {
		t.Errorf("independent per-key histories were rejected: %v", res)
	}
	if res.Operations != 400 {
		t.Errorf("operations = %d, want 400", res.Operations)
	}
}
