package chaos

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Node is one supervised kvserver process.
type Node struct {
	ID int
	// PeerAddr is where this node listens. ClientAddr is the same port; clients
	// connect directly rather than through a proxy, so a partition between nodes
	// does not also cut the client off and turn every fault into a timeout.
	PeerAddr string
	DataDir  string
	LogPath  string

	mu      sync.Mutex
	cmd     *exec.Cmd
	paused  bool
	running bool
}

type link struct{ from, to int }

// Cluster supervises a set of kvserver processes and the proxies between them.
type Cluster struct {
	Binary string
	Dir    string
	Nodes  []*Node

	// UnsafeLocalReads starts the nodes with reads served locally, which is a
	// known-broken configuration used to verify the checker can actually fail.
	UnsafeLocalReads bool

	// ExtraArgs are appended to every node's command line, so a benchmark can
	// vary batching and pipelining without a separate supervisor.
	ExtraArgs []string

	proxies map[link]*Proxy
	mu      sync.Mutex
}

// NewCluster prepares n nodes and the n*(n-1) directed links between them.
// Nothing is started until [Cluster.Start].
func NewCluster(binary, dir string, n int) (*Cluster, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	c := &Cluster{Binary: binary, Dir: dir, proxies: make(map[link]*Proxy)}

	ports, err := freePorts(n)
	if err != nil {
		return nil, err
	}
	for i := range n {
		id := i + 1
		c.Nodes = append(c.Nodes, &Node{
			ID:       id,
			PeerAddr: fmt.Sprintf("127.0.0.1:%d", ports[i]),
			DataDir:  filepath.Join(dir, fmt.Sprintf("node%d", id)),
			LogPath:  filepath.Join(dir, fmt.Sprintf("node%d.log", id)),
		})
	}
	// One proxy per ordered pair, so links are directed and A→B can be cut
	// while B→A still carries traffic.
	for _, from := range c.Nodes {
		for _, to := range c.Nodes {
			if from.ID == to.ID {
				continue
			}
			p, err := NewProxy(fmt.Sprintf("%d->%d", from.ID, to.ID), to.PeerAddr)
			if err != nil {
				c.Close()
				return nil, err
			}
			c.proxies[link{from.ID, to.ID}] = p
		}
	}
	return c, nil
}

// freePorts reserves n ports by binding and immediately releasing them.
//
// Inherently racy — another process can claim one in the gap — but the
// alternative is letting each node bind :0, and then no node could be told its
// peers' addresses before they all started.
func freePorts(n int) ([]int, error) {
	var ports []int
	var held []net.Listener
	defer func() {
		for _, l := range held {
			l.Close()
		}
	}()
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		held = append(held, l)
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}

// peerSpec builds the -peers argument for one node: its own real address, and
// every other node reached through that node's outgoing proxy.
func (c *Cluster) peerSpec(self *Node) string {
	var parts []string
	for _, n := range c.Nodes {
		if n.ID == self.ID {
			parts = append(parts, fmt.Sprintf("%d=%s", n.ID, n.PeerAddr))
			continue
		}
		parts = append(parts, fmt.Sprintf("%d=%s", n.ID, c.proxies[link{self.ID, n.ID}].Addr()))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// ClientAddrs returns the addresses clients should dial.
func (c *Cluster) ClientAddrs() []string {
	var out []string
	for _, n := range c.Nodes {
		out = append(out, n.PeerAddr)
	}
	return out
}

// Start launches every node.
func (c *Cluster) Start() error {
	for _, n := range c.Nodes {
		if err := c.startNode(n); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) startNode(n *Node) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.running {
		return nil
	}
	logFile, err := os.OpenFile(n.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	args := []string{
		fmt.Sprintf("-id=%d", n.ID),
		"-peers=" + c.peerSpec(n),
		"-data=" + n.DataDir,
		"-election-timeout=400ms",
		"-heartbeat=80ms",
	}
	if c.UnsafeLocalReads {
		args = append(args, "-unsafe-local-reads")
	}
	args = append(args, c.ExtraArgs...)
	cmd := exec.Command(c.Binary, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// Its own process group, so a kill reaches the server and nothing else.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return err
	}
	n.cmd, n.running, n.paused = cmd, true, false

	go func() {
		_ = cmd.Wait()
		logFile.Close()
	}()
	return nil
}

// Kill terminates a node with SIGKILL — no cleanup, no graceful shutdown, no
// chance to flush anything. Whatever survives is what was genuinely durable.
func (c *Cluster) Kill(id int) error {
	n := c.node(id)
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running {
		return nil
	}
	// SIGCONT first: a SIGSTOPped process cannot be reaped, and killing without
	// resuming leaves a zombie that never exits.
	_ = n.cmd.Process.Signal(syscall.SIGCONT)
	err := n.cmd.Process.Kill()
	n.running, n.paused = false, false
	return err
}

// Restart brings a killed node back from its durable state alone.
func (c *Cluster) Restart(id int) error { return c.startNode(c.node(id)) }

// Pause freezes a node with SIGSTOP, as a long garbage collection pause or a
// descheduled VM does. The process stays alive and its TCP buffers keep filling,
// so on resume it faces a burst of stale messages — which is precisely the
// adversary a stalled node meets in production.
func (c *Cluster) Pause(id int) error {
	n := c.node(id)
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running || n.paused {
		return nil
	}
	n.paused = true
	return n.cmd.Process.Signal(syscall.SIGSTOP)
}

// Resume unfreezes a paused node.
func (c *Cluster) Resume(id int) error {
	n := c.node(id)
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.running || !n.paused {
		return nil
	}
	n.paused = false
	return n.cmd.Process.Signal(syscall.SIGCONT)
}

// Running reports whether a node's process is alive.
func (c *Cluster) Running(id int) bool {
	n := c.node(id)
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.running
}

// Paused reports whether a node is frozen.
func (c *Cluster) Paused(id int) bool {
	n := c.node(id)
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.paused
}

// Cut severs the directed link from → to.
func (c *Cluster) Cut(from, to int) {
	if p, ok := c.proxies[link{from, to}]; ok {
		p.Cut()
	}
}

// Isolate cuts a node off in both directions.
func (c *Cluster) Isolate(id int) {
	for _, n := range c.Nodes {
		if n.ID != id {
			c.Cut(id, n.ID)
			c.Cut(n.ID, id)
		}
	}
}

// Partition splits the cluster into two groups that cannot reach each other.
func (c *Cluster) Partition(a, b []int) {
	for _, x := range a {
		for _, y := range b {
			c.Cut(x, y)
			c.Cut(y, x)
		}
	}
}

// SetAllLatency adds a one-way delay to every inter-node link, so a benchmark
// can measure a cluster spread across a network rather than one on loopback.
func (c *Cluster) SetAllLatency(d time.Duration) {
	for _, p := range c.proxies {
		p.SetLatency(d)
	}
}

// SetLatency adds a one-way delay to a directed link.
func (c *Cluster) SetLatency(from, to int, d time.Duration) {
	if p, ok := c.proxies[link{from, to}]; ok {
		p.SetLatency(d)
	}
}

// HealNetwork restores every link.
func (c *Cluster) HealNetwork() {
	for _, p := range c.proxies {
		p.Heal()
		p.SetLatency(0)
	}
}

// HealAll restores the network, resumes paused nodes and restarts dead ones.
// It is the precondition for asserting anything about liveness.
func (c *Cluster) HealAll() error {
	c.HealNetwork()
	for _, n := range c.Nodes {
		if err := c.Resume(n.ID); err != nil {
			return err
		}
		if !c.Running(n.ID) {
			if err := c.Restart(n.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close stops every node and proxy.
func (c *Cluster) Close() {
	for _, n := range c.Nodes {
		_ = c.Kill(n.ID)
	}
	for _, p := range c.proxies {
		_ = p.Close()
	}
}

func (c *Cluster) node(id int) *Node {
	for _, n := range c.Nodes {
		if n.ID == id {
			return n
		}
	}
	panic(fmt.Sprintf("chaos: no node %d", id))
}
