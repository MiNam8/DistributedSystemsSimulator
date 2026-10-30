package chaos

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Driver applies random faults to a running cluster.
type Driver struct {
	Cluster  *Cluster
	Interval time.Duration

	rng *rand.Rand

	mu  sync.Mutex
	log []string
}

// NewDriver returns a fault driver seeded for reproducibility.
//
// The seed makes the *sequence of decisions* reproducible, not the run: real
// processes, real sockets and a real scheduler mean two runs of one seed will
// differ. That is the honest division of labour in this project — the simulator
// gives exact replay, this gives fidelity, and pretending either does both would
// be worse than having neither.
func NewDriver(c *Cluster, seed uint64, interval time.Duration) *Driver {
	return &Driver{
		Cluster:  c,
		Interval: interval,
		rng:      rand.New(rand.NewPCG(seed, 0x9E3779B97F4A7C15)),
	}
}

// Log returns the faults applied, in order.
func (d *Driver) Log() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.log...)
}

func (d *Driver) note(format string, args ...any) {
	d.mu.Lock()
	d.log = append(d.log, fmt.Sprintf("%s  "+format,
		append([]any{time.Now().Format("15:04:05.000")}, args...)...))
	d.mu.Unlock()
}

// Run applies one fault per interval until ctx is cancelled.
func (d *Driver) Run(ctx context.Context) {
	ticker := time.NewTicker(d.Interval)
	defer ticker.Stop()

	// Every so often the cluster is fully repaired, so it can actually make
	// progress between storms. Without this, sustained chaos on a small cluster
	// means the workload spends the whole run timing out, and a run in which
	// nothing commits proves very little.
	sinceHeal := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if sinceHeal >= 6 {
				d.Cluster.HealNetwork()
				d.note("heal network")
				sinceHeal = 0
				continue
			}
			sinceHeal++
			d.act()
		}
	}
}

func (d *Driver) act() {
	nodes := d.Cluster.Nodes
	n := len(nodes)
	pick := func() int { return nodes[d.rng.IntN(n)].ID }

	switch d.rng.IntN(8) {
	case 0: // kill -9, but keep a quorum alive so the run makes progress
		down := 0
		for _, nd := range nodes {
			if !d.Cluster.Running(nd.ID) {
				down++
			}
		}
		if down >= (n-1)/2 {
			d.restartSomething()
			return
		}
		id := pick()
		if d.Cluster.Running(id) {
			_ = d.Cluster.Kill(id)
			d.note("kill -9 node %d", id)
		}

	case 1:
		d.restartSomething()

	case 2: // SIGSTOP: a long GC pause or a descheduled VM
		id := pick()
		if d.Cluster.Running(id) && !d.Cluster.Paused(id) {
			_ = d.Cluster.Pause(id)
			d.note("SIGSTOP node %d", id)
		}

	case 3:
		for _, nd := range nodes {
			if d.Cluster.Paused(nd.ID) {
				_ = d.Cluster.Resume(nd.ID)
				d.note("SIGCONT node %d", nd.ID)
				return
			}
		}

	case 4: // split the cluster at a random point
		cut := 1 + d.rng.IntN(n-1)
		var a, b []int
		for i, nd := range nodes {
			if i < cut {
				a = append(a, nd.ID)
			} else {
				b = append(b, nd.ID)
			}
		}
		d.Cluster.Partition(a, b)
		d.note("partition %v | %v", a, b)

	case 5: // one-way cut: the asymmetry symmetric testing never produces
		from, to := pick(), pick()
		if from != to {
			d.Cluster.Cut(from, to)
			d.note("one-way cut %d -> %d", from, to)
		}

	case 6: // isolate a single node entirely
		id := pick()
		d.Cluster.Isolate(id)
		d.note("isolate node %d", id)

	default: // slow a link to the point where it looks nearly dead
		from, to := pick(), pick()
		if from != to {
			lat := time.Duration(50+d.rng.IntN(400)) * time.Millisecond
			d.Cluster.SetLatency(from, to, lat)
			d.note("latency %d -> %d = %v", from, to, lat)
		}
	}
}

func (d *Driver) restartSomething() {
	for _, nd := range d.Cluster.Nodes {
		if !d.Cluster.Running(nd.ID) {
			if err := d.Cluster.Restart(nd.ID); err == nil {
				d.note("restart node %d", nd.ID)
			}
			return
		}
	}
}
