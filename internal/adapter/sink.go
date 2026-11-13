// Package adapter holds the Sink interface and its implementations.
//
// A sink adapter is the only thing that knows how to ask a particular store
// what it holds. Adapters are read-only by construction: the interface exposes
// no write method, so a verifier cannot modify the system it audits even by
// mistake (PRD section 8.5).
package adapter

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Errors adapters share.
var (
	ErrNotFound    = errors.New("adapter: no such sink")
	ErrUnavailable = errors.New("adapter: sink unavailable")
)

// Entry is one delivered canary record as the sink holds it.
type Entry struct {
	Seq uint64
	// IngestTS is when the store says it received the record. Stores disagree
	// about what this means (index time, insert time, flush time), which is
	// why latency is a secondary metric and never a headline claim.
	IngestTS time.Time
	// DupCount is how many rows the sink holds for this sequence. One is
	// normal; more than one is a duplicate to be classified.
	DupCount int
	// Partial marks a multi-line event whose continuation lines did not all
	// arrive. The parent sequence counts as delivered, flagged.
	Partial bool
}

// SeqIter streams entries from a bisected range.
type SeqIter interface {
	Next() bool
	Entry() Entry
	Err() error
	Close() error
}

// Sink is the plugin surface. An adapter is expected to be roughly 150 to 250
// lines; anything much larger usually means store-specific logic has leaked in
// that belongs in the docs instead.
type Sink interface {
	// Name identifies the sink in metrics, reports and labels.
	Name() string

	// CountRange returns how many distinct sequences in [from, to] the sink
	// holds for (src, epoch), and the total row count over the same range.
	// distinct is never greater than total; the difference is duplicate rows.
	//
	// This is the cheap path. The verifier calls it over wide ranges and only
	// descends when the answer disagrees with what was emitted.
	CountRange(ctx context.Context, src, epoch string, from, to uint64) (distinct, total uint64, err error)

	// FetchRange streams per-sequence detail. Called only on bisected ranges
	// that already disagree, never for bulk verification.
	FetchRange(ctx context.Context, src, epoch string, from, to uint64) (SeqIter, error)

	// SettleHint is the sink's own consistency floor: index refresh interval,
	// async insert flush, ingester flush. It seeds the adaptive settle window
	// so we do not accuse a store of loss while it is still writing.
	SettleHint() time.Duration

	// Doctor checks connectivity, authentication, and that the canary fields
	// are actually queryable. A sink that answers queries but cannot see the
	// token fields would report total loss, so this check is not optional.
	Doctor(ctx context.Context) error

	// Close releases connections.
	Close() error
}

// PartialReporter is an optional capability a sink may implement.
//
// A multi-line event whose continuation lines were partly lost is invisible to
// CountRange: the parent sequence arrived exactly once, so the cheap path sees
// a clean range and never looks closer. Detecting it needs a separate question,
// which not every store can answer cheaply.
//
// Adapters that can answer it implement this and get partial detection for
// free. Adapters that cannot simply do not implement it, and their reports say
// so rather than quietly claiming no partials were found.
type PartialReporter interface {
	// PartialSeqs returns the sequences in [from, to] whose multi-line event
	// arrived incompletely.
	PartialSeqs(ctx context.Context, src, epoch string, from, to uint64) ([]uint64, error)
}

// Range is an inclusive sequence interval.
type Range struct {
	From uint64
	To   uint64
}

func (r Range) String() string {
	if r.From == r.To {
		return fmt.Sprintf("%d", r.From)
	}
	return fmt.Sprintf("%d-%d", r.From, r.To)
}

// Width is the number of sequences the range covers.
func (r Range) Width() uint64 {
	if r.To < r.From {
		return 0
	}
	return r.To - r.From + 1
}

// Contains reports whether seq falls inside the range.
func (r Range) Contains(seq uint64) bool {
	return seq >= r.From && seq <= r.To
}

// Split halves the range for bisection. A range of width one cannot split and
// reports ok = false.
func (r Range) Split() (left, right Range, ok bool) {
	if r.Width() < 2 {
		return Range{}, Range{}, false
	}
	mid := r.From + (r.To-r.From)/2
	return Range{r.From, mid}, Range{mid + 1, r.To}, true
}

// Registry maps sink names to constructed adapters.
type Registry struct {
	sinks map[string]Sink
	order []string
}

// NewRegistry indexes sinks by name.
func NewRegistry(sinks ...Sink) (*Registry, error) {
	r := &Registry{sinks: make(map[string]Sink, len(sinks))}
	for _, s := range sinks {
		if s == nil {
			return nil, fmt.Errorf("adapter: nil sink in registry")
		}
		name := s.Name()
		if _, dup := r.sinks[name]; dup {
			return nil, fmt.Errorf("adapter: duplicate sink name %q", name)
		}
		r.sinks[name] = s
		r.order = append(r.order, name)
	}
	return r, nil
}

// Get returns a sink by name.
func (r *Registry) Get(name string) (Sink, error) {
	s, ok := r.sinks[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	return s, nil
}

// All returns every sink in registration order.
func (r *Registry) All() []Sink {
	out := make([]Sink, 0, len(r.order))
	for _, name := range r.order {
		out = append(out, r.sinks[name])
	}
	return out
}

// Names lists sink names in registration order.
func (r *Registry) Names() []string {
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

// Doctor runs every sink's check and reports all failures together, so an
// operator fixes one round of problems rather than discovering them serially.
func (r *Registry) Doctor(ctx context.Context) error {
	var failures []error
	for _, name := range r.order {
		if err := r.sinks[name].Doctor(ctx); err != nil {
			failures = append(failures, fmt.Errorf("sink %s: %w", name, err))
		}
	}
	return errors.Join(failures...)
}

// Close closes every sink, returning all errors together.
func (r *Registry) Close() error {
	var failures []error
	for _, name := range r.order {
		if err := r.sinks[name].Close(); err != nil {
			failures = append(failures, fmt.Errorf("sink %s: %w", name, err))
		}
	}
	return errors.Join(failures...)
}
