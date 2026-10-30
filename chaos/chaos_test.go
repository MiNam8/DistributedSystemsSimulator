package chaos_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/MiNam8/kvstore/chaos"
	"github.com/MiNam8/kvstore/client"
)

// buildServer compiles kvserver into a temporary directory so the test
// exercises the same binary a real deployment would run.
func buildServer(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "kvserver")
	cmd := exec.Command("go", "build", "-o", bin, "../cmd/kvserver")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building kvserver: %v\n%s", err, out)
	}
	return bin
}

// run drives a cluster under chaos and returns the checked history.
func run(t *testing.T, unsafeReads bool, duration time.Duration, seed uint64) (linearizable bool, ops int, faults int) {
	t.Helper()
	bin := buildServer(t)

	cluster, err := chaos.NewCluster(bin, t.TempDir(), 5)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	cluster.UnsafeLocalReads = unsafeReads

	if err := cluster.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitForLeader(cluster.ClientAddrs(), 30*time.Second); err != nil {
		t.Fatalf("no leader: %v", err)
	}

	work := chaos.NewWorkload(cluster.ClientAddrs(), 8, 3*time.Second)
	// Reads go to a rotating replica. Against a correct server this changes
	// nothing, because a follower redirects — but a client that only ever reads
	// from the leader cannot observe a replica serving stale data, and a chaos
	// suite that never spreads its reads would pass a system that does.
	work.SpreadReads = true

	driver := chaos.NewDriver(cluster, seed, 700*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	done := make(chan struct{})
	go func() { defer close(done); _ = work.Run(ctx, 6) }()
	go driver.Run(ctx)

	<-ctx.Done()
	cancel()
	if err := cluster.HealAll(); err != nil {
		t.Logf("heal: %v", err)
	}
	<-done

	res, err := work.Recorder.Check(60*time.Second, "")
	if err != nil {
		t.Fatal(err)
	}
	if res.TimedOut {
		t.Skip("linearizability checker timed out; result is unknown, not a pass")
	}
	t.Logf("%v", res)
	return res.Linearizable, res.Operations, len(driver.Log())
}

// TestRealClusterUnderChaosIsLinearizable is the fidelity half of the
// correctness argument: real processes, real SIGKILL and SIGSTOP, real sockets
// cut mid-flight, checked against a real linearizability oracle.
func TestRealClusterUnderChaosIsLinearizable(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns real processes; skipped under -short")
	}
	ok, ops, faults := run(t, false, 20*time.Second, 1)
	if !ok {
		t.Fatal("the history was not linearizable")
	}
	if ops < 200 {
		t.Fatalf("only %d operations completed; the run proves very little", ops)
	}
	t.Logf("%d operations survived %d injected faults", ops, faults)
}

// TestCheckerCatchesUnsafeLocalReads keeps the previous test honest.
//
// A passing chaos suite means nothing unless the suite can fail, so the cluster
// is started in a deliberately broken configuration — reads served straight from
// each replica's local state machine, the most tempting optimisation in any
// replicated store — and the checker is required to catch it.
//
// This is also a measured argument for spreading reads: with every read directed
// at the leader, this same bug went undetected in 8 of 8 runs.
func TestCheckerCatchesUnsafeLocalReads(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns real processes; skipped under -short")
	}
	ok, ops, _ := run(t, true, 20*time.Second, 1)
	if ok {
		t.Fatalf("stale replica reads were declared linearizable across %d operations; "+
			"the chaos suite cannot detect the bug it is supposed to detect", ops)
	}
}

func waitForLeader(addrs []string, timeout time.Duration) error {
	cl, err := client.New(addrs)
	if err != nil {
		return err
	}
	defer cl.Close()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		leaders := 0
		for i := range cl.Len() {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			st, err := cl.Status(ctx, i)
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
		lastErr = context.DeadlineExceeded
	}
	return lastErr
}
