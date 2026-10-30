package chaos

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MiNam8/kvstore/client"
	"github.com/MiNam8/kvstore/lincheck"
)

// Workload drives concurrent clients against the cluster and records what they
// observed.
type Workload struct {
	Recorder *lincheck.Recorder

	// SpreadReads sends each read to a rotating node instead of always to the
	// last-known leader. Against a correct server this changes nothing — the
	// follower redirects — but it is what makes a server that answers reads
	// locally actually observable.
	SpreadReads bool

	addrs   []string
	keys    []string
	timeout time.Duration

	ops         atomic.Int64
	errs        atomic.Int64
	indet       atomic.Int64
	writesAcked atomic.Int64

	mu       sync.Mutex
	lastSeen map[string]string // last value each key was observed or written to hold
}

// NewWorkload prepares a workload over keys, driven by clients dialling addrs.
func NewWorkload(addrs []string, keys int, timeout time.Duration) *Workload {
	w := &Workload{
		Recorder: lincheck.NewRecorder(),
		addrs:    addrs,
		timeout:  timeout,
		lastSeen: make(map[string]string),
	}
	for i := range keys {
		w.keys = append(w.keys, fmt.Sprintf("key%02d", i))
	}
	return w
}

// Run drives n concurrent clients until ctx is cancelled.
func (w *Workload) Run(ctx context.Context, n int) error {
	var wg sync.WaitGroup
	errCh := make(chan error, n)

	for i := range n {
		cl, err := client.New(w.addrs)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func(clientID int, cl *client.Client) {
			defer wg.Done()
			defer cl.Close()
			w.loop(ctx, clientID, cl)
		}(i, cl)
	}
	wg.Wait()
	close(errCh)
	return nil
}

func (w *Workload) loop(ctx context.Context, clientID int, cl *client.Client) {
	rng := rand.New(rand.NewPCG(uint64(clientID)+1, 0x9E3779B9))
	seq := 0

	for ctx.Err() == nil {
		seq++
		key := w.keys[rng.IntN(len(w.keys))]
		value := fmt.Sprintf("c%d-%d", clientID, seq)

		switch rng.IntN(10) {
		case 0, 1, 2, 3, 4: // reads dominate, as they do in practice
			w.doGet(ctx, clientID, cl, key, rng.IntN(len(w.addrs)))
		case 5, 6, 7:
			w.doPut(ctx, clientID, cl, key, value)
		case 8:
			w.doDelete(ctx, clientID, cl, key)
		default:
			w.doCAS(ctx, clientID, cl, key, value)
		}
	}
}

// call wraps one logical operation: record the invocation, run it, then record
// either a definitive result or — crucially — an indeterminate outcome.
func (w *Workload) call(ctx context.Context, clientID int, in lincheck.Input,
	do func(context.Context) (lincheck.Output, error)) {

	opCtx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	id := w.Recorder.Begin(clientID, in)
	out, err := do(opCtx)
	w.ops.Add(1)

	if err != nil {
		// The operation did not fail — its outcome is unknown. The command may
		// have committed with the reply lost. Recording this as a failure is the
		// single most common way to make a linearizability checker report a
		// violation that never happened.
		w.errs.Add(1)
		w.indet.Add(1)
		w.Recorder.Abandon(id)
		return
	}
	w.Recorder.End(clientID, id, out)
}

func (w *Workload) doGet(ctx context.Context, clientID int, cl *client.Client, key string, node int) {
	w.call(ctx, clientID, lincheck.Input{Kind: lincheck.OpGet, Key: key},
		func(c context.Context) (lincheck.Output, error) {
			var (
				v     []byte
				found bool
				err   error
			)
			if w.SpreadReads {
				v, found, err = cl.GetFrom(c, node, key)
			} else {
				v, found, err = cl.Get(c, key)
			}
			if err != nil {
				return lincheck.Output{}, err
			}
			if found {
				w.observe(key, string(v))
			}
			return lincheck.Output{Value: string(v), Found: found, OK: true}, nil
		})
}

func (w *Workload) doPut(ctx context.Context, clientID int, cl *client.Client, key, value string) {
	w.call(ctx, clientID, lincheck.Input{Kind: lincheck.OpPut, Key: key, Value: value},
		func(c context.Context) (lincheck.Output, error) {
			if err := cl.Put(c, key, []byte(value)); err != nil {
				return lincheck.Output{}, err
			}
			w.writesAcked.Add(1)
			w.observe(key, value)
			return lincheck.Output{OK: true}, nil
		})
}

func (w *Workload) doDelete(ctx context.Context, clientID int, cl *client.Client, key string) {
	w.call(ctx, clientID, lincheck.Input{Kind: lincheck.OpDelete, Key: key},
		func(c context.Context) (lincheck.Output, error) {
			found, err := cl.Delete(c, key)
			if err != nil {
				return lincheck.Output{}, err
			}
			w.writesAcked.Add(1)
			return lincheck.Output{OK: true, Found: found}, nil
		})
}

// doCAS compares against the last value this workload saw for the key, so a
// meaningful fraction of compare-and-swaps actually succeed. Compare-and-swap is
// the operation that makes duplicate application observable, which is why it is
// worth making it hit rather than always fail.
func (w *Workload) doCAS(ctx context.Context, clientID int, cl *client.Client, key, value string) {
	expected, hasExpected := w.lastValue(key)
	in := lincheck.Input{
		Kind: lincheck.OpCAS, Key: key, Value: value,
		Expected: expected, HasExpected: hasExpected,
	}
	w.call(ctx, clientID, in, func(c context.Context) (lincheck.Output, error) {
		ok, err := cl.CompareAndSwap(c, key, []byte(expected), []byte(value), hasExpected)
		if err != nil {
			return lincheck.Output{}, err
		}
		if ok {
			w.writesAcked.Add(1)
			w.observe(key, value)
		}
		return lincheck.Output{OK: ok}, nil
	})
}

func (w *Workload) observe(key, value string) {
	w.mu.Lock()
	w.lastSeen[key] = value
	w.mu.Unlock()
}

func (w *Workload) lastValue(key string) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	v, ok := w.lastSeen[key]
	return v, ok
}

// Stats reports progress.
func (w *Workload) Stats() (ops, errs, indeterminate, writes int64) {
	return w.ops.Load(), w.errs.Load(), w.indet.Load(), w.writesAcked.Load()
}

// Keys returns the key space.
func (w *Workload) Keys() []string { return w.keys }
