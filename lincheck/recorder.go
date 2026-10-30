package lincheck

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// Recorder builds a history of client operations.
//
// Events are appended under one mutex at the instant they occur, which gives a
// total order consistent with real time — exactly what linearizability is
// defined against. Porcupine's Event representation uses event *order* rather
// than timestamps, so this ordering is the whole input.
type Recorder struct {
	mu     sync.Mutex
	events []porcupine.Event
	nextID int

	// pending tracks operations that have been invoked but not answered.
	pending  map[int]Input
	clientOf map[int]int

	completed int
	abandoned int
}

// NewRecorder returns an empty recorder.
func NewRecorder() *Recorder {
	return &Recorder{pending: make(map[int]Input), clientOf: make(map[int]int)}
}

// Begin records an invocation and returns the operation's id.
func (r *Recorder) Begin(clientID int, in Input) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.nextID
	r.nextID++
	r.pending[id] = in
	r.clientOf[id] = clientID
	r.events = append(r.events, porcupine.Event{
		ClientId: clientID,
		Kind:     porcupine.CallEvent,
		Value:    in,
		Id:       id,
	})
	return id
}

// End records a definitive result.
func (r *Recorder) End(clientID, id int, out Output) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.pending[id]; !ok {
		return
	}
	delete(r.pending, id)
	r.completed++
	r.events = append(r.events, porcupine.Event{
		ClientId: clientID,
		Kind:     porcupine.ReturnEvent,
		Value:    out,
		Id:       id,
	})
}

// Abandon records that an operation's outcome is unknown — a timeout, a dropped
// connection, a node that died mid-request.
//
// The operation is closed out only when the history is read, with a synthetic
// return at the very end marked [Output.Indeterminate]. That placement lets it
// linearize anywhere between its invocation and the end of the run, and the
// nondeterministic model then branches on whether it took effect at all. The two
// together are Jepsen's :info, and getting either wrong is silent:
//
//   - recording it as a failure invents a false violation, because a later read
//     may legitimately observe a write whose acknowledgement was simply lost;
//   - recording it as a success invents false confidence, asserting an effect
//     that may never have happened.
//
// Neither error announces itself. A checker fed a mis-recorded history reports
// with total confidence about a history that never occurred.
func (r *Recorder) Abandon(id int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.pending[id]; ok {
		r.abandoned++
	}
	// The entry stays in pending so Events can close it out at the end of the
	// history — see the note there.
}

// Events returns the recorded history, closing out every still-unresolved
// operation with an indeterminate return at the end.
//
// Porcupine requires every call to have a matching return — it derives the
// operation count from the entry count — so a genuinely unmatched call is
// misread as a return entry rather than treated as pending. Appending the
// returns here, at the end, gives each abandoned operation the widest honest
// linearization window: anywhere from its invocation to the end of the run.
func (r *Recorder) Events() []porcupine.Event {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := append([]porcupine.Event(nil), r.events...)
	for id, in := range r.pending {
		out = append(out, porcupine.Event{
			ClientId: r.clientOf[id],
			Kind:     porcupine.ReturnEvent,
			Value:    Output{Indeterminate: true},
			Id:       id,
		})
		_ = in
	}
	return out
}

// Stats reports how many operations completed and how many were abandoned.
func (r *Recorder) Stats() (completed, abandoned int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.completed, r.abandoned
}

// Result is the outcome of checking a history.
type Result struct {
	Linearizable bool
	TimedOut     bool
	Operations   int
	Completed    int
	Abandoned    int
	Elapsed      time.Duration
}

func (r Result) String() string {
	verdict := "LINEARIZABLE"
	switch {
	case r.TimedOut:
		verdict = "UNKNOWN (checker timed out)"
	case !r.Linearizable:
		verdict = "NOT LINEARIZABLE"
	}
	return fmt.Sprintf("%s — %d operations (%d completed, %d indeterminate) checked in %v",
		verdict, r.Operations, r.Completed, r.Abandoned, r.Elapsed.Round(time.Millisecond))
}

// Check verifies the recorded history against [KVModel].
//
// vizPath, when non-empty, receives an interactive HTML visualization on
// failure. A linearizability violation is close to unreadable as a list of
// operations; the visualization shows exactly which prefix could not be
// extended, which is usually the difference between a fixable bug report and a
// shrug.
func (r *Recorder) Check(timeout time.Duration, vizPath string) (Result, error) {
	events := r.Events()
	completed, abandoned := r.Stats()

	start := time.Now()
	res, info := porcupine.CheckEventsVerbose(KVModel, events, timeout)
	out := Result{
		Linearizable: res == porcupine.Ok,
		TimedOut:     res == porcupine.Unknown,
		Operations:   completed + abandoned,
		Completed:    completed,
		Abandoned:    abandoned,
		Elapsed:      time.Since(start),
	}

	if res != porcupine.Ok && vizPath != "" {
		f, err := os.Create(vizPath)
		if err != nil {
			return out, err
		}
		defer f.Close()
		if err := porcupine.Visualize(KVModel, info, f); err != nil {
			return out, err
		}
	}
	return out, nil
}
