// Command kvbench measures throughput and latency against a running cluster.
//
//	kvbench -peers=127.0.0.1:7001,... -clients=64 -duration=20s
//
// # What this measures, and what it does not
//
// The load is closed-loop: each client issues one request, waits for the reply,
// then issues the next. Closed-loop latency systematically *under-reports* the
// tail once the server saturates, because a slow server simply receives fewer
// requests — the classic coordinated-omission error. Throughput at saturation
// is measured honestly; the latency percentiles below it are conditional on the
// load actually offered, and are reported as such rather than as service-level
// numbers.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MiNam8/kvstore/chaos"
	"github.com/MiNam8/kvstore/client"
)

func main() {
	var (
		peers    = flag.String("peers", "127.0.0.1:7001", "comma-separated node addresses")
		clients  = flag.Int("clients", 32, "concurrent clients")
		duration = flag.Duration("duration", 15*time.Second, "measurement window")
		warmup   = flag.Duration("warmup", 3*time.Second, "warm-up period, excluded from results")
		valueSz  = flag.Int("value-size", 128, "value size in bytes")
		keys     = flag.Int("keys", 1000, "key space size")
		op       = flag.String("op", "put", "put or get")
		label    = flag.String("label", "", "configuration name, for the CSV row")
		csvPath  = flag.String("csv", "", "append a CSV row here")
		jsonOut  = flag.Bool("json", false, "print the result as JSON")
		spawn    = flag.Int("spawn", 0, "start an N-node cluster instead of using -peers")
		latency  = flag.Duration("latency", 0, "one-way inter-node latency, injected between spawned nodes")
		binary   = flag.String("binary", "./bin/kvserver", "kvserver binary, for -spawn")
		srvFlags = flag.String("server-flags", "", "extra kvserver flags for -spawn, comma-separated")
	)
	flag.Parse()

	addrs := strings.Split(*peers, ",")
	if *spawn > 0 {
		cluster, cleanup, err := spawnCluster(*binary, *spawn, *latency, *srvFlags)
		if err != nil {
			log.Fatal(err)
		}
		defer cleanup()
		addrs = cluster
	}
	value := make([]byte, *valueSz)
	for i := range value {
		value[i] = byte('a' + i%26)
	}

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		latencies []time.Duration
		ops       atomic.Int64
		errs      atomic.Int64
		measuring atomic.Bool
	)

	ctx, cancel := context.WithTimeout(context.Background(), *warmup+*duration)
	defer cancel()

	for c := range *clients {
		cl, err := client.New(addrs)
		if err != nil {
			log.Fatal(err)
		}
		wg.Add(1)
		go func(id int, cl *client.Client) {
			defer wg.Done()
			defer cl.Close()

			rng := rand.New(rand.NewPCG(uint64(id)+1, 42))
			var local []time.Duration

			for ctx.Err() == nil {
				key := fmt.Sprintf("k%06d", rng.IntN(*keys))
				opCtx, opCancel := context.WithTimeout(ctx, 5*time.Second)

				start := time.Now()
				var err error
				if *op == "get" {
					_, _, err = cl.Get(opCtx, key)
				} else {
					err = cl.Put(opCtx, key, value)
				}
				elapsed := time.Since(start)
				opCancel()

				// Only samples taken after warm-up count: the first seconds
				// include leader election, connection setup and an empty log.
				if measuring.Load() {
					if err != nil {
						errs.Add(1)
					} else {
						ops.Add(1)
						local = append(local, elapsed)
					}
				}
			}
			mu.Lock()
			latencies = append(latencies, local...)
			mu.Unlock()
		}(c, cl)
	}

	time.Sleep(*warmup)
	measuring.Store(true)
	windowStart := time.Now()
	<-ctx.Done()
	window := time.Since(windowStart)
	wg.Wait()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pct := func(q float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*q)]
	}

	res := struct {
		Label      string  `json:"label"`
		Op         string  `json:"op"`
		Clients    int     `json:"clients"`
		Ops        int64   `json:"ops"`
		Errors     int64   `json:"errors"`
		Seconds    float64 `json:"seconds"`
		Throughput float64 `json:"throughput_per_sec"`
		P50ms      float64 `json:"p50_ms"`
		P90ms      float64 `json:"p90_ms"`
		P99ms      float64 `json:"p99_ms"`
		MaxMs      float64 `json:"max_ms"`
	}{
		Label: *label, Op: *op, Clients: *clients,
		Ops: ops.Load(), Errors: errs.Load(),
		Seconds:    window.Seconds(),
		Throughput: float64(ops.Load()) / window.Seconds(),
		P50ms:      ms(pct(0.50)), P90ms: ms(pct(0.90)),
		P99ms: ms(pct(0.99)), MaxMs: ms(pct(1.0)),
	}

	// Written before anything is printed: a caller piping this through `head`
	// closes stdout early, the process takes SIGPIPE on its next write, and a
	// CSV appended afterwards is silently never produced.
	if *csvPath != "" {
		appendCSV(*csvPath, res.Label, res.Op, res.Clients, res.Throughput,
			res.P50ms, res.P90ms, res.P99ms, res.MaxMs, res.Errors)
	}

	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(res)
	} else {
		fmt.Printf("%-22s %-4s clients=%-4d %8.0f ops/s   p50 %6.2f ms  p90 %6.2f ms  p99 %7.2f ms  max %8.2f ms  errors=%d\n",
			res.Label, res.Op, res.Clients, res.Throughput, res.P50ms, res.P90ms, res.P99ms, res.MaxMs, res.Errors)
	}

	// Group-commit instrumentation from the leader, which is what explains any
	// throughput result: the mean batch size is the number of fsyncs saved.
	if *spawn > 0 {
		if probe, err := client.New(addrs); err == nil {
			for i := range probe.Len() {
				c, cancel := context.WithTimeout(context.Background(), time.Second)
				st, err := probe.Status(c, i)
				cancel()
				if err == nil && st.Role == "leader" {
					mean := 0.0
					if st.Batches > 0 {
						mean = float64(st.BatchedEntries) / float64(st.Batches)
					}
					fmt.Printf("    leader node %d: %d batches, mean %.1f entries, max %d, applied %d\n",
						st.Id, st.Batches, mean, st.MaxBatch, st.AppliedIndex)
				}
			}
			probe.Close()
		}
	}

}

// spawnCluster starts a self-contained cluster, optionally with injected
// inter-node latency.
//
// Loopback has effectively zero round-trip time, which flatters any design that
// waits for a reply — and hides the entire point of pipelining. Injecting
// latency is what makes the difference between "one message in flight" and
// "several" measurable at all.
func spawnCluster(binary string, n int, latency time.Duration, extra string) ([]string, func(), error) {
	dir, err := os.MkdirTemp("", "kvbench-")
	if err != nil {
		return nil, nil, err
	}
	cluster, err := chaos.NewCluster(binary, dir, n)
	if err != nil {
		os.RemoveAll(dir)
		return nil, nil, err
	}
	if extra != "" {
		cluster.ExtraArgs = strings.Split(extra, ",")
	}
	if err := cluster.Start(); err != nil {
		cluster.Close()
		os.RemoveAll(dir)
		return nil, nil, err
	}
	if latency > 0 {
		cluster.SetAllLatency(latency)
	}

	// Wait for a leader rather than sleeping a guessed amount.
	addrs := cluster.ClientAddrs()
	deadline := time.Now().Add(30 * time.Second)
	probe, err := client.New(addrs)
	if err != nil {
		cluster.Close()
		os.RemoveAll(dir)
		return nil, nil, err
	}
	defer probe.Close()

	for time.Now().Before(deadline) {
		leaders := 0
		for i := range probe.Len() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			st, err := probe.Status(ctx, i)
			cancel()
			if err == nil && st.Role == "leader" {
				leaders++
			}
		}
		if leaders == 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	return addrs, func() { cluster.Close(); os.RemoveAll(dir) }, nil
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func appendCSV(path, label, op string, clients int, tput, p50, p90, p99, max float64, errs int64) {
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("csv: %v", err)
		return
	}
	defer f.Close()
	if os.IsNotExist(statErr) {
		fmt.Fprintln(f, "config,op,clients,throughput,p50_ms,p90_ms,p99_ms,max_ms,errors")
	}
	fmt.Fprintf(f, "%s,%s,%d,%.1f,%.3f,%.3f,%.3f,%.3f,%d\n",
		label, op, clients, tput, p50, p90, p99, max, errs)
}
