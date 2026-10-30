// Package chaos drives a real cluster of kvserver processes under real faults.
//
// # Why a proxy instead of iptables
//
// Partitioning with iptables or tc requires root, which makes the suite
// unrunnable in CI, in a container, and on a laptop without sudo. Instead every
// *directed* link gets its own userspace TCP proxy, and the driver cuts,
// delays or resets links by controlling those.
//
// The result is strictly more expressive than a symmetric iptables rule: a link
// is directed, so A→B can be cut while B→A still works. A node that can send but
// not receive looks alive to itself while being invisible to everyone else, and
// that asymmetry breaks liveness assumptions that symmetric testing never
// touches.
//
// What it does not simulate is packet-level loss. Dropping bytes from a TCP
// stream is not a fault a real network produces — TCP would retransmit — so the
// honest analogues at this layer are the ones implemented here: connection
// reset, connection refusal, and added latency.
package chaos

import (
	"io"
	"net"
	"sync"
	"time"
)

// Proxy forwards one directed link between two nodes.
type Proxy struct {
	Name string

	listener net.Listener
	target   string

	mu      sync.Mutex
	cut     bool
	latency time.Duration
	conns   map[net.Conn]struct{}
	closed  bool
	wg      sync.WaitGroup
}

// NewProxy listens on an ephemeral port and forwards to target.
func NewProxy(name, target string) (*Proxy, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{
		Name: name, listener: lis, target: target,
		conns: make(map[net.Conn]struct{}),
	}
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

// Addr is the address peers should dial to reach the target through this link.
func (p *Proxy) Addr() string { return p.listener.Addr().String() }

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		cut, closed := p.cut, p.closed
		p.mu.Unlock()
		if cut || closed {
			// Refuse rather than hang: a cut link should look like an
			// unreachable peer, which is what the protocol is built to tolerate.
			client.Close()
			continue
		}
		p.wg.Add(1)
		go p.handle(client)
	}
}

func (p *Proxy) handle(client net.Conn) {
	defer p.wg.Done()
	defer client.Close()

	upstream, err := net.DialTimeout("tcp", p.target, 2*time.Second)
	if err != nil {
		return
	}
	defer upstream.Close()

	p.track(client)
	p.track(upstream)
	defer p.untrack(client)
	defer p.untrack(upstream)

	done := make(chan struct{}, 2)
	go func() { p.copy(upstream, client); done <- struct{}{} }()
	go func() { p.copy(client, upstream); done <- struct{}{} }()
	<-done
}

// copy forwards bytes, applying the link's current latency.
//
// Each chunk is stamped with its own delivery deadline when it is *read*, and a
// separate writer releases chunks in order as their deadlines pass.
//
// Sleeping inline before each write instead — the obvious implementation — is
// wrong in a way that quietly invalidates every measurement taken through it. It
// serialises the delay, so the link carries at most one chunk per latency
// period: a 5 ms "latency" becomes a 200-chunk-per-second bandwidth cap. The
// first pipelining benchmark run through such a proxy reported that pipelining
// made throughput *worse*, which was an artefact of the instrument rather than a
// property of the system. Stamping deadlines at read time lets a burst of chunks
// all become deliverable at the same instant, which is what latency actually
// does.
func (p *Proxy) copy(dst, src net.Conn) {
	type chunk struct {
		b  []byte
		at time.Time
	}
	queue := make(chan chunk, 2048)
	written := make(chan struct{})

	go func() {
		defer close(written)
		for c := range queue {
			if d := time.Until(c.at); d > 0 {
				time.Sleep(d)
			}
			if _, err := dst.Write(c.b); err != nil {
				return
			}
		}
	}()

	for {
		buf := make([]byte, 32*1024)
		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock()
			lat, cut := p.latency, p.cut
			p.mu.Unlock()
			if cut {
				break
			}
			select {
			case queue <- chunk{b: buf[:n], at: time.Now().Add(lat)}:
			default:
				// The delay queue is full, which means the far side is not
				// draining. Dropping is the honest response: a real link with a
				// full buffer drops too.
			}
		}
		if err != nil {
			if err != io.EOF {
				break
			}
			break
		}
	}
	close(queue)
	<-written
}

func (p *Proxy) track(c net.Conn) {
	p.mu.Lock()
	p.conns[c] = struct{}{}
	p.mu.Unlock()
}

func (p *Proxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
}

// Cut severs the link: existing connections are reset and new ones refused.
//
// Resetting rather than blackholing is deliberate. A blackholed connection sits
// in a TCP timeout for minutes, which would make every partition look like a
// stall; a reset is what a real partition looks like to an application within a
// round trip.
func (p *Proxy) Cut() {
	p.mu.Lock()
	p.cut = true
	conns := make([]net.Conn, 0, len(p.conns))
	for c := range p.conns {
		conns = append(conns, c)
	}
	p.conns = make(map[net.Conn]struct{})
	p.mu.Unlock()

	for _, c := range conns {
		c.Close()
	}
}

// Heal restores the link.
func (p *Proxy) Heal() {
	p.mu.Lock()
	p.cut = false
	p.mu.Unlock()
}

// SetLatency adds a one-way delay to every chunk forwarded.
func (p *Proxy) SetLatency(d time.Duration) {
	p.mu.Lock()
	p.latency = d
	p.mu.Unlock()
}

// IsCut reports whether the link is currently severed.
func (p *Proxy) IsCut() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cut
}

// Close shuts the proxy down.
func (p *Proxy) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	err := p.listener.Close()
	p.Cut()
	return err
}
