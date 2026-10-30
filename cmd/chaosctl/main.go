// Command chaosctl runs a real cluster of kvserver processes under real faults
// and checks the resulting client history for linearizability.
//
//	make build && ./bin/chaosctl -nodes=5 -duration=2m
//
// This is the fidelity half of the project's correctness argument. The
// deterministic simulator explores far more schedules and replays any failure
// exactly; it cannot tell you whether a real process survives SIGKILL, whether
// fsync did what it claimed, or whether the gRPC layer reassembles a snapshot
// correctly. This can.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/MiNam8/kvstore/chaos"
	"github.com/MiNam8/kvstore/client"
)

func main() {
	var (
		nodes     = flag.Int("nodes", 5, "cluster size")
		duration  = flag.Duration("duration", 60*time.Second, "how long to apply chaos")
		clients   = flag.Int("clients", 8, "concurrent workload clients")
		keys      = flag.Int("keys", 12, "size of the key space")
		seed      = flag.Uint64("seed", 1, "fault sequence seed")
		interval  = flag.Duration("fault-interval", 900*time.Millisecond, "time between faults")
		opTimeout = flag.Duration("op-timeout", 3*time.Second, "per-operation client timeout")
		dir       = flag.String("dir", "", "working directory (default: a temp dir)")
		binary    = flag.String("binary", "./bin/kvserver", "path to the kvserver binary")
		viz       = flag.String("viz", "", "write an HTML visualization here if the history is not linearizable")
		keep      = flag.Bool("keep", false, "keep the working directory")
		srvFlags  = flag.String("server-flags", "", "extra kvserver flags, comma-separated (e.g. -read-mode=lease)")
		unsafe    = flag.Bool("unsafe-local-reads", false,
			"start the cluster with deliberately non-linearizable reads, to verify this checker can fail")
	)
	flag.Parse()

	workDir := *dir
	if workDir == "" {
		d, err := os.MkdirTemp("", "chaos-")
		if err != nil {
			log.Fatal(err)
		}
		workDir = d
		if !*keep {
			defer os.RemoveAll(workDir)
		}
	}
	if _, err := os.Stat(*binary); err != nil {
		log.Fatalf("kvserver binary not found at %s (run: make build)", *binary)
	}
	abs, _ := filepath.Abs(*binary)

	cluster, err := chaos.NewCluster(abs, workDir, *nodes)
	if err != nil {
		log.Fatal(err)
	}
	defer cluster.Close()
	cluster.UnsafeLocalReads = *unsafe
	if *srvFlags != "" {
		cluster.ExtraArgs = strings.Split(*srvFlags, ",")
		fmt.Printf("server flags: %v\n", cluster.ExtraArgs)
	}
	if *unsafe {
		fmt.Println("WARNING: -unsafe-local-reads set; a violation is EXPECTED")
	}

	if err := cluster.Start(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("started %d nodes in %s\n", *nodes, workDir)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := waitForLeader(ctx, cluster.ClientAddrs(), 30*time.Second); err != nil {
		log.Fatalf("cluster never elected a leader: %v", err)
	}
	fmt.Println("leader elected; starting workload and chaos")

	work := chaos.NewWorkload(cluster.ClientAddrs(), *keys, *opTimeout)
	// Spread reads across replicas: harmless against a correct server, and the
	// only way a server that answers reads locally becomes observable.
	work.SpreadReads = true
	driver := chaos.NewDriver(cluster, *seed, *interval)

	runCtx, endRun := context.WithTimeout(ctx, *duration)
	workDone := make(chan struct{})
	go func() { defer close(workDone); _ = work.Run(runCtx, *clients) }()
	go driver.Run(runCtx)

	// Progress, so a long run is not a silent one.
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				ops, errs, indet, writes := work.Stats()
				fmt.Printf("  %6d ops  %5d writes acked  %4d indeterminate  %4d errors\n",
					ops, writes, indet, errs)
			}
		}
	}()

	<-runCtx.Done()
	endRun()
	ticker.Stop()

	// Retire the adversary and let the cluster settle before asserting anything
	// about liveness. Under an arbitrary partition a Raft cluster is *supposed*
	// to stop making progress.
	fmt.Println("chaos over; healing and quiescing")
	if err := cluster.HealAll(); err != nil {
		log.Printf("heal: %v", err)
	}
	<-workDone

	if err := waitForLeader(context.Background(), cluster.ClientAddrs(), 30*time.Second); err != nil {
		fmt.Printf("FAIL: cluster did not recover after healing: %v\n", err)
		printFaults(driver)
		os.Exit(1)
	}
	fmt.Println("cluster recovered")

	ops, errs, indet, writes := work.Stats()
	fmt.Printf("\nworkload: %d operations, %d writes acknowledged, %d indeterminate, %d errors\n",
		ops, writes, indet, errs)

	fmt.Println("checking linearizability...")
	res, err := work.Recorder.Check(60*time.Second, *viz)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res)

	if !res.Linearizable {
		fmt.Println("\nFAULTS APPLIED:")
		printFaults(driver)
		if *viz != "" {
			fmt.Printf("\nvisualization written to %s\n", *viz)
		}
		os.Exit(1)
	}
	if res.TimedOut {
		fmt.Println("\nthe checker did not finish; treat this as unknown, not as a pass")
		os.Exit(1)
	}
	fmt.Printf("\n%d faults applied, no linearizability violation\n", len(driver.Log()))
}

func printFaults(d *chaos.Driver) {
	for _, l := range d.Log() {
		fmt.Println("  " + l)
	}
}

// waitForLeader blocks until exactly one node reports itself leader.
func waitForLeader(ctx context.Context, addrs []string, timeout time.Duration) error {
	cl, err := client.New(addrs)
	if err != nil {
		return err
	}
	defer cl.Close()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) && ctx.Err() == nil {
		leaders := 0
		for i := range cl.Len() {
			c, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			st, err := cl.Status(c, i)
			cancel()
			if err != nil {
				lastErr = err
				continue
			}
			if st.Role == "leader" {
				leaders++
			}
		}
		if leaders == 1 {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("timed out")
	}
	return lastErr
}
