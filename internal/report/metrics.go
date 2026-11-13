// Package report exposes what the verifier found: Prometheus metrics for
// alerting, and human-readable gap reports for reading.
package report

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/Vishv07/lossledger/internal/verify"
)

// Metrics holds the normative metric set from PRD section 7.8.
//
// The names are part of the contract: dashboards and alert rules are written
// against them, so changing one requires owner approval the same way changing
// the anomaly taxonomy does.
type Metrics struct {
	Emitted *prometheus.CounterVec
	Arrived *prometheus.CounterVec
	Missing *prometheus.CounterVec
	Dup     *prometheus.CounterVec
	Order   *prometheus.CounterVec
	Late    *prometheus.CounterVec
	Latency *prometheus.HistogramVec

	// FalseMissing is the integrity metric. It counts sequences this verifier
	// declared missing that later arrived, which means we accused a pipeline
	// of losing data it went on to deliver.
	//
	// It is exported from day one and must be zero in every published run. A
	// nonzero value is a P0 against the verifier, ahead of all feature work
	// (PRD section 0.6). It is deliberately impossible to publish a result
	// without also publishing this number.
	FalseMissing *prometheus.CounterVec

	Invalid *prometheus.CounterVec
	Queries *prometheus.CounterVec
}

// NewMetrics registers the metric set on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Emitted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lossledger_emitted_total",
			Help: "Canary events written by the emitter.",
		}, []string{"src", "shape"}),

		Arrived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lossledger_arrived_total",
			Help: "Canary events observed at a sink.",
		}, []string{"src", "sink"}),

		Missing: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lossledger_missing_total",
			Help: "Canary events finalised as missing after the settle window and a mandatory re-check.",
		}, []string{"src", "sink"}),

		Dup: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lossledger_dup_total",
			Help: "Duplicate deliveries, classified as expected replay or as a bug.",
		}, []string{"src", "sink", "class"}),

		Order: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lossledger_order_total",
			Help: "Events whose arrival order violated sequence order, or whose multi-line body arrived partially.",
		}, []string{"src", "sink"}),

		Late: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lossledger_late_total",
			Help: "Events that arrived after the settle window but before max_wait.",
		}, []string{"src", "sink"}),

		Latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "lossledger_delivery_latency_seconds",
			Help: "Delay between emission and first observation at a sink.",
			// Buckets span sub-second to several minutes, because the
			// interesting range runs from an in-memory pipeline to one that
			// has just come back from an outage.
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
		}, []string{"sink"}),

		FalseMissing: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lossledger_false_missing_total",
			Help: "Events declared missing that later arrived. Must be zero; any other value is a bug in this verifier, not in the pipeline.",
		}, []string{"sink"}),

		Invalid: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lossledger_invalid_lines_total",
			Help: "Lines carrying a canary token that failed signature verification.",
		}, []string{"sink"}),

		Queries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lossledger_verifier_queries_total",
			Help: "Queries the verifier issued, by kind, so the cost of auditing is visible.",
		}, []string{"sink", "kind"}),
	}

	if reg != nil {
		reg.MustRegister(
			m.Emitted, m.Arrived, m.Missing, m.Dup, m.Order,
			m.Late, m.Latency, m.FalseMissing, m.Invalid, m.Queries,
		)
	}
	return m
}

// Observe writes a verdict into the metric set.
func (m *Metrics) Observe(v verify.Verdict) {
	for _, f := range v.Findings {
		switch f.Anomaly {
		case verify.Missing:
			m.Missing.WithLabelValues(f.Src, f.Sink).Add(float64(f.Count))
		case verify.DupBug:
			m.Dup.WithLabelValues(f.Src, f.Sink, "bug").Add(float64(f.Count))
		case verify.DupReplay:
			m.Dup.WithLabelValues(f.Src, f.Sink, "replay").Add(float64(f.Count))
		case verify.Order:
			m.Order.WithLabelValues(f.Src, f.Sink).Add(float64(f.Count))
		case verify.Late:
			m.Late.WithLabelValues(f.Src, f.Sink).Add(float64(f.Count))
		}
	}
}

// ObserveQueries records per-sink query counts.
func (m *Metrics) ObserveQueries(counts map[string]map[string]uint64) {
	for sink, byKind := range counts {
		for kind, n := range byKind {
			m.Queries.WithLabelValues(sink, kind).Add(float64(n))
		}
	}
}
