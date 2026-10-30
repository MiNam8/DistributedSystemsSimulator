// Package metrics is a small Prometheus exposition-format exporter.
//
// It is written here rather than pulled in because the exposition format is a
// few lines of text and the alternative is a large dependency tree for something
// this project uses in exactly one place. The judgement is the same one applied
// to the linearizability checker in reverse: Porcupine is imported because a
// checker you wrote and only trust because you wrote it is worth little, whereas
// a text encoder is verifiable by reading it.
//
// Everything here is safe for concurrent use — metrics are read by an HTTP
// handler while the node's own goroutine writes them.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Counter only ever increases.
type Counter struct {
	name, help string
	v          atomic.Uint64
}

// Add increases the counter by n.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Inc increases the counter by one.
func (c *Counter) Inc() { c.v.Add(1) }

// Value returns the current count.
func (c *Counter) Value() uint64 { return c.v.Load() }

// Set mirrors a monotonic counter maintained elsewhere. It is the right tool
// only when the source is itself monotonic — a counter that can go down breaks
// every rate() query built on it.
func (c *Counter) Set(v uint64) { c.v.Store(v) }

// Gauge can move in either direction.
type Gauge struct {
	name, help string
	v          atomic.Int64
}

// Set replaces the gauge's value.
func (g *Gauge) Set(v int64) { g.v.Store(v) }

// Value returns the current value.
func (g *Gauge) Value() int64 { return g.v.Load() }

// Histogram counts observations into fixed buckets.
//
// Buckets are cumulative in the exposition format ("le" means less-or-equal),
// which is what lets Prometheus interpolate quantiles across instances. Storing
// them non-cumulatively and summing at render time keeps the hot path a single
// atomic increment.
type Histogram struct {
	name, help string
	bounds     []float64
	counts     []atomic.Uint64
	sum        atomic.Uint64 // fixed-point microseconds, so the hot path stays lock-free
	count      atomic.Uint64
}

// Observe records a value, in seconds.
func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.bounds, v)
	if i >= len(h.counts) {
		i = len(h.counts) - 1
	}
	h.counts[i].Add(1)
	h.sum.Add(uint64(v * 1e6))
	h.count.Add(1)
}

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// MeanSeconds returns the arithmetic mean of the observations.
func (h *Histogram) MeanSeconds() float64 {
	n := h.count.Load()
	if n == 0 {
		return 0
	}
	return float64(h.sum.Load()) / 1e6 / float64(n)
}

// Registry holds a set of metrics and renders them.
type Registry struct {
	mu         sync.Mutex
	counters   []*Counter
	gauges     []*Gauge
	histograms []*Histogram
	labels     string
}

// New returns a registry whose metrics all carry the given labels.
func New(labels map[string]string) *Registry {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "%s=%q", k, labels[k])
	}
	return &Registry{labels: b.String()}
}

// Counter registers and returns a counter.
func (r *Registry) Counter(name, help string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := &Counter{name: name, help: help}
	r.counters = append(r.counters, c)
	return c
}

// Gauge registers and returns a gauge.
func (r *Registry) Gauge(name, help string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	g := &Gauge{name: name, help: help}
	r.gauges = append(r.gauges, g)
	return g
}

// Histogram registers and returns a histogram with the given upper bounds, in
// seconds.
func (r *Registry) Histogram(name, help string, bounds []float64) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	h := &Histogram{
		name: name, help: help, bounds: bounds,
		counts: make([]atomic.Uint64, len(bounds)+1), // one extra for +Inf
	}
	r.histograms = append(r.histograms, h)
	return h
}

// LatencyBuckets spans 50µs to 2s, which covers an fsync on anything from NVMe
// to a struggling network filesystem.
func LatencyBuckets() []float64 {
	return []float64{
		0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005,
		0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2,
	}
}

// WriteTo renders every metric in the Prometheus text exposition format.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var b strings.Builder
	label := func(extra string) string {
		switch {
		case r.labels == "" && extra == "":
			return ""
		case extra == "":
			return "{" + r.labels + "}"
		case r.labels == "":
			return "{" + extra + "}"
		default:
			return "{" + r.labels + "," + extra + "}"
		}
	}

	for _, c := range r.counters {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n%s%s %d\n",
			c.name, c.help, c.name, c.name, label(""), c.v.Load())
	}
	for _, g := range r.gauges {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s%s %d\n",
			g.name, g.help, g.name, g.name, label(""), g.v.Load())
	}
	for _, h := range r.histograms {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s histogram\n", h.name, h.help, h.name)
		var cumulative uint64
		for i, bound := range h.bounds {
			cumulative += h.counts[i].Load()
			fmt.Fprintf(&b, "%s_bucket%s %d\n", h.name,
				label("le=\""+strconv.FormatFloat(bound, 'g', -1, 64)+"\""), cumulative)
		}
		cumulative += h.counts[len(h.bounds)].Load()
		fmt.Fprintf(&b, "%s_bucket%s %d\n", h.name, label("le=\"+Inf\""), cumulative)
		fmt.Fprintf(&b, "%s_sum%s %g\n", h.name, label(""), float64(h.sum.Load())/1e6)
		fmt.Fprintf(&b, "%s_count%s %d\n", h.name, label(""), h.count.Load())
	}

	n, err := io.WriteString(w, b.String())
	return int64(n), err
}
