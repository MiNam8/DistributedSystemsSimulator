package metrics_test

import (
	"strings"
	"testing"

	"github.com/MiNam8/kvstore/metrics"
)

func render(t *testing.T, r *metrics.Registry) string {
	t.Helper()
	var b strings.Builder
	if _, err := r.WriteTo(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestCounterAndGauge(t *testing.T) {
	r := metrics.New(map[string]string{"node": "7"})
	c := r.Counter("things_total", "Things.")
	g := r.Gauge("level", "A level.")

	c.Add(3)
	c.Inc()
	g.Set(-2)

	out := render(t, r)
	for _, want := range []string{
		"# TYPE things_total counter",
		`things_total{node="7"} 4`,
		"# TYPE level gauge",
		`level{node="7"} -2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

// TestHistogramBucketsAreCumulative guards the one part of the exposition format
// that is easy to get wrong.
//
// Prometheus buckets are cumulative — "le" means less-or-equal — and a histogram
// rendered with per-bucket counts instead produces quantiles that are silently,
// confidently wrong rather than obviously broken.
func TestHistogramBucketsAreCumulative(t *testing.T) {
	r := metrics.New(nil)
	h := r.Histogram("latency_seconds", "Latency.", []float64{0.001, 0.01, 0.1})

	h.Observe(0.0005) // <= 0.001
	h.Observe(0.005)  // <= 0.01
	h.Observe(0.005)  // <= 0.01
	h.Observe(5)      // +Inf only

	out := render(t, r)
	for _, want := range []string{
		`latency_seconds_bucket{le="0.001"} 1`,
		`latency_seconds_bucket{le="0.01"} 3`,
		`latency_seconds_bucket{le="0.1"} 3`,
		`latency_seconds_bucket{le="+Inf"} 4`,
		"latency_seconds_count 4",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	if h.Count() != 4 {
		t.Errorf("Count = %d, want 4", h.Count())
	}
	if mean := h.MeanSeconds(); mean < 1.2 || mean > 1.3 {
		t.Errorf("MeanSeconds = %v, want ~1.25", mean)
	}
}

func TestLabelsCombineWithBucketLabel(t *testing.T) {
	r := metrics.New(map[string]string{"node": "1", "dc": "eu"})
	r.Histogram("h_seconds", "H.", []float64{1}).Observe(0.5)

	out := render(t, r)
	// Registry labels sort, and the bucket label is appended after them.
	if !strings.Contains(out, `h_seconds_bucket{dc="eu",node="1",le="1"} 1`) {
		t.Errorf("labels not combined correctly:\n%s", out)
	}
}

func TestEmptyHistogramRendersZero(t *testing.T) {
	r := metrics.New(nil)
	h := r.Histogram("empty_seconds", "Empty.", []float64{1})
	if h.MeanSeconds() != 0 {
		t.Error("mean of no observations should be 0, not a division by zero")
	}
	if !strings.Contains(render(t, r), "empty_seconds_count 0") {
		t.Error("an unobserved histogram should still render")
	}
}
