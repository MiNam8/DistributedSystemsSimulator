// Package lincheck records client histories and checks them for
// linearizability.
//
// # Why the recorder matters as much as the checker
//
// The single most consequential detail in linearizability testing is how an
// operation that *timed out* is recorded. It did not fail — it is
// **indeterminate**. The request may have committed and the acknowledgement been
// lost, or it may never have been applied at all, and the client cannot tell
// which.
//
// Recording such an operation as failed produces false violations (a later read
// sees a write that "never happened"). Recording it as succeeded produces false
// confidence. The correct treatment is Jepsen's :info: the operation may be
// placed anywhere after its invocation, or nowhere at all.
//
// Porcupine expresses that as a call event with no matching return event, which
// is why this package uses the Event API rather than the simpler Operation API.
// [Recorder.Abandon] is the whole point of the design.
package lincheck

import (
	"fmt"

	"github.com/anishathalye/porcupine"
)

// OpKind is a client operation.
type OpKind uint8

const (
	OpGet OpKind = iota
	OpPut
	OpDelete
	OpCAS
)

func (k OpKind) String() string {
	switch k {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpDelete:
		return "del"
	case OpCAS:
		return "cas"
	default:
		return "?"
	}
}

// Input is the invocation half of an operation.
type Input struct {
	Kind        OpKind
	Key         string
	Value       string
	Expected    string
	HasExpected bool
}

// Output is what the client observed.
//
// Indeterminate marks an operation whose outcome the client never learned — a
// timeout, a dropped connection, a node that died mid-request. It is not a
// failure. The command may have committed with the acknowledgement lost, or it
// may never have been applied, and no amount of retrying at the client can
// distinguish the two after the fact.
type Output struct {
	Value         string
	Found         bool
	OK            bool
	Indeterminate bool
}

// state is one key's value in the model.
type state struct {
	Present bool
	Value   string
}

// KVModel is the sequential specification the history is checked against.
//
// It is partitioned by key: operations on different keys cannot affect one
// another, so each key's sub-history is checked independently. Without that,
// checking is exponential in the total number of operations rather than in the
// number per key, and a run of any interesting length simply never finishes.
var KVModel = kvNondeterministic.ToModel()

// kvNondeterministic is the real specification.
//
// It is nondeterministic for exactly one reason: an indeterminate operation
// branches into two futures — one where it took effect and one where it did
// not — and the checker explores both. That is the precise meaning of Jepsen's
// :info, and it is what makes a timeout neither a false alarm nor false
// confidence.
//
// Porcupine requires every call to have a matching return, so an abandoned
// operation is given a synthetic return at the end of the history; the branching
// here is what keeps that from asserting an effect that may never have happened.
var kvNondeterministic = porcupine.NondeterministicModel{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var order []string
		for _, op := range history {
			k := op.Input.(Input).Key
			if _, seen := byKey[k]; !seen {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(order))
		for _, k := range order {
			out = append(out, byKey[k])
		}
		return out
	},
	PartitionEvent: func(history []porcupine.Event) [][]porcupine.Event {
		// Events are partitioned by the key recorded on the *call*; a return
		// event carries no key, so calls are indexed by id first.
		keyOf := map[int]string{}
		byKey := map[string][]porcupine.Event{}
		var order []string
		for _, e := range history {
			var k string
			if e.Kind == porcupine.CallEvent {
				k = e.Value.(Input).Key
				keyOf[e.Id] = k
			} else {
				k = keyOf[e.Id]
			}
			if _, seen := byKey[k]; !seen {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], e)
		}
		out := make([][]porcupine.Event, 0, len(order))
		for _, k := range order {
			out = append(out, byKey[k])
		}
		return out
	},

	Init: func() []any { return []any{state{}} },

	Step: func(st, input, output any) []any {
		s := st.(state)
		in := input.(Input)
		out := output.(Output)

		// An indeterminate operation constrains nothing about the output and may
		// or may not have taken effect, so both futures stay open.
		if out.Indeterminate {
			switch in.Kind {
			case OpGet:
				return []any{s} // a read changes nothing either way
			case OpPut:
				return []any{s, state{Present: true, Value: in.Value}}
			case OpDelete:
				return []any{s, state{}}
			case OpCAS:
				if (in.HasExpected && s.Present && s.Value == in.Expected) ||
					(!in.HasExpected && !s.Present) {
					return []any{s, state{Present: true, Value: in.Value}}
				}
				return []any{s} // the comparison could not have matched
			}
			return []any{s}
		}

		switch in.Kind {
		case OpGet:
			// A read must return exactly what the model holds. This is the
			// clause that catches a stale read served by a leader that had
			// already lost its mandate.
			if out.Found == s.Present && (!s.Present || out.Value == s.Value) {
				return []any{s}
			}
			return nil

		case OpPut:
			if !out.OK {
				return nil
			}
			return []any{state{Present: true, Value: in.Value}}

		case OpDelete:
			if out.Found != s.Present {
				return nil
			}
			return []any{state{}}

		case OpCAS:
			match := (in.HasExpected && s.Present && s.Value == in.Expected) ||
				(!in.HasExpected && !s.Present)
			if out.OK != match {
				return nil
			}
			if match {
				return []any{state{Present: true, Value: in.Value}}
			}
			return []any{s}
		}
		return nil
	},

	DescribeOperation: func(input, output any) string {
		in := input.(Input)
		out := output.(Output)
		if out.Indeterminate {
			return fmt.Sprintf("%s(%s) -> <indeterminate>", in.Kind, in.Key)
		}
		switch in.Kind {
		case OpGet:
			if !out.Found {
				return fmt.Sprintf("get(%s) -> <absent>", in.Key)
			}
			return fmt.Sprintf("get(%s) -> %q", in.Key, out.Value)
		case OpPut:
			return fmt.Sprintf("put(%s, %q)", in.Key, in.Value)
		case OpDelete:
			return fmt.Sprintf("del(%s) -> found=%v", in.Key, out.Found)
		case OpCAS:
			exp := "<absent>"
			if in.HasExpected {
				exp = fmt.Sprintf("%q", in.Expected)
			}
			return fmt.Sprintf("cas(%s, %s -> %q) -> %v", in.Key, exp, in.Value, out.OK)
		}
		return "?"
	},

	DescribeState: func(st any) string {
		s := st.(state)
		if !s.Present {
			return "<absent>"
		}
		return fmt.Sprintf("%q", s.Value)
	},
}
