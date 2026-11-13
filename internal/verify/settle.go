package verify

import (
	"sort"
	"sync"
	"time"
)

// Settle bounds. A floor stops a sink that reports an implausibly small
// consistency delay from making us trigger-happy; a ceiling stops an outage
// from deferring every judgement forever.
const (
	MinSettle      = 5 * time.Second
	DefaultMaxWait = 10 * time.Minute

	// LatencyMultiple is how many times the observed p99 delivery latency the
	// settle window allows. Three is deliberately generous: the cost of
	// waiting is a slower verdict, and the cost of not waiting is a false
	// accusation, which is the one outcome this project cannot survive.
	LatencyMultiple = 3
)

// SettleConfig tunes the window for one sink.
type SettleConfig struct {
	// Hint is the sink's own reported consistency floor.
	Hint time.Duration

	// MaxWait caps the window and is also the point past which a sequence
	// stops being considered in flight.
	MaxWait time.Duration

	// RecheckDelay is how long after a sequence becomes provisionally missing
	// the mandatory re-query happens. Zero means half the settle window.
	RecheckDelay time.Duration
}

func (c *SettleConfig) applyDefaults() {
	if c.MaxWait <= 0 {
		c.MaxWait = DefaultMaxWait
	}
}

// LatencyTracker keeps a bounded sample of delivery latencies so the settle
// window can adapt to how the pipeline is actually behaving rather than to how
// it behaved when someone wrote the config file.
type LatencyTracker struct {
	mu      sync.Mutex
	samples []time.Duration
	cap     int
	sorted  bool
}

// NewLatencyTracker returns a tracker holding at most capacity samples.
func NewLatencyTracker(capacity int) *LatencyTracker {
	if capacity <= 0 {
		capacity = 1024
	}
	return &LatencyTracker{cap: capacity, samples: make([]time.Duration, 0, capacity)}
}

// Observe records one delivery latency. Negative values are discarded: they
// mean the sink's clock disagrees with ours, and a negative latency would drag
// the window below what the pipeline needs.
func (t *LatencyTracker) Observe(d time.Duration) {
	if d < 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.samples) < t.cap {
		t.samples = append(t.samples, d)
	} else {
		// Replace the oldest sample. A ring keeps the window responsive to a
		// pipeline that just got slower.
		copy(t.samples, t.samples[1:])
		t.samples[len(t.samples)-1] = d
	}
	t.sorted = false
}

// Quantile returns the latency at q, where q is between 0 and 1.
func (t *LatencyTracker) Quantile(q float64) time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.samples) == 0 {
		return 0
	}
	if !t.sorted {
		sort.Slice(t.samples, func(i, j int) bool { return t.samples[i] < t.samples[j] })
		t.sorted = true
	}
	if q <= 0 {
		return t.samples[0]
	}
	if q >= 1 {
		return t.samples[len(t.samples)-1]
	}
	idx := int(q * float64(len(t.samples)-1))
	return t.samples[idx]
}

// P99 is the quantile the settle window is built on.
func (t *LatencyTracker) P99() time.Duration { return t.Quantile(0.99) }

// Count returns how many samples are held.
func (t *LatencyTracker) Count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.samples)
}

// Window computes the adaptive settle duration for one sink.
type Window struct {
	cfg     SettleConfig
	latency *LatencyTracker
}

// NewWindow builds a settle window.
func NewWindow(cfg SettleConfig) *Window {
	cfg.applyDefaults()
	return &Window{cfg: cfg, latency: NewLatencyTracker(4096)}
}

// Observe feeds a delivery latency into the adaptive calculation.
func (w *Window) Observe(d time.Duration) { w.latency.Observe(d) }

// Duration returns the current settle window:
//
//	settle = clamp(max(hint, p99 * 3), MinSettle, MaxWait)
//
// Both inputs push the window outward only. Nothing here can shrink the window
// below the floor, which is what makes the monotonicity property in PRD
// section 10.3 hold.
func (w *Window) Duration() time.Duration {
	settle := w.cfg.Hint
	if adaptive := w.latency.P99() * LatencyMultiple; adaptive > settle {
		settle = adaptive
	}
	if settle < MinSettle {
		settle = MinSettle
	}
	if settle > w.cfg.MaxWait {
		settle = w.cfg.MaxWait
	}
	return settle
}

// RecheckDelay returns how long to wait before the mandatory re-query.
func (w *Window) RecheckDelay() time.Duration {
	if w.cfg.RecheckDelay > 0 {
		return w.cfg.RecheckDelay
	}
	return w.Duration() / 2
}

// MaxWait returns the configured ceiling.
func (w *Window) MaxWait() time.Duration { return w.cfg.MaxWait }

// State is where one sequence sits in the judgement pipeline.
type State uint8

const (
	// InFlight means too young to judge. Most sequences are here most of the
	// time, and a sequence here can never be reported missing.
	InFlight State = iota

	// Provisional means settled and absent, awaiting the mandatory re-query.
	Provisional

	// Rechecked means the re-query ran and the sequence was still absent. Only
	// from here may it be declared missing.
	Rechecked

	// Arrived means the sink holds it.
	Arrived
)

func (s State) String() string {
	switch s {
	case InFlight:
		return "in_flight"
	case Provisional:
		return "provisional"
	case Rechecked:
		return "rechecked"
	case Arrived:
		return "arrived"
	}
	return "unknown"
}

// Scheduler tracks per-sequence judgement state for one (src, epoch, sink).
//
// It owns every time-dependent decision, which is what leaves the classifier a
// pure function of the observation the scheduler produces.
type Scheduler struct {
	window *Window

	mu sync.Mutex
	// emittedAt is when each sequence was emitted, the clock the settle window
	// is measured from.
	emittedAt map[uint64]time.Time
	// provisionalAt is when a sequence first became provisionally missing.
	provisionalAt map[uint64]time.Time
	arrived       *Ledger
	settled       *Ledger
	rechecked     *Ledger
	declared      *Ledger

	falseMissing uint64
	lateArrivals []uint64
}

// NewScheduler returns a scheduler using window.
func NewScheduler(window *Window) *Scheduler {
	return &Scheduler{
		window:        window,
		emittedAt:     map[uint64]time.Time{},
		provisionalAt: map[uint64]time.Time{},
		arrived:       NewLedger(),
		settled:       NewLedger(),
		rechecked:     NewLedger(),
		declared:      NewLedger(),
	}
}

// NoteEmitted records when a sequence was written.
func (s *Scheduler) NoteEmitted(seq uint64, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.emittedAt[seq]; !exists {
		s.emittedAt[seq] = at
	}
}

// NoteArrived records a delivery.
//
// If the sequence had already been declared missing, this is an integrity
// failure: we accused the pipeline of losing something it went on to deliver.
// The counter incremented here is the one that must be zero in every published
// run (PRD section 0.6), and the sequence is reclassified as late.
func (s *Scheduler) NoteArrived(seq uint64, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.arrived.Contains(seq) {
		return
	}
	s.arrived.Add(seq)
	delete(s.provisionalAt, seq)
	s.rechecked.Remove(seq)

	if s.declared.Contains(seq) {
		s.falseMissing++
		s.lateArrivals = append(s.lateArrivals, seq)
		s.declared.Remove(seq)
	}

	if emitted, ok := s.emittedAt[seq]; ok && !at.IsZero() {
		if latency := at.Sub(emitted); latency >= 0 {
			s.window.Observe(latency)
		}
	}
}

// Advance moves every tracked sequence to the state the clock justifies, and
// returns the sequences whose mandatory re-query is now due.
//
// Nothing here declares anything missing. Advance only establishes which
// sequences are old enough to be judged; the classifier decides the verdict.
func (s *Scheduler) Advance(now time.Time) (needRecheck []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	settle := s.window.Duration()
	recheckAfter := s.window.RecheckDelay()

	for seq, emitted := range s.emittedAt {
		if s.arrived.Contains(seq) {
			continue
		}
		age := now.Sub(emitted)
		if age < settle {
			continue // still in flight
		}

		s.settled.Add(seq)
		first, seen := s.provisionalAt[seq]
		if !seen {
			s.provisionalAt[seq] = now
			continue
		}
		if !s.rechecked.Contains(seq) && now.Sub(first) >= recheckAfter {
			needRecheck = append(needRecheck, seq)
		}
	}

	sort.Slice(needRecheck, func(i, j int) bool { return needRecheck[i] < needRecheck[j] })
	return needRecheck
}

// ConfirmRecheck records that the mandatory re-query ran and the sequence was
// still absent, which is the last gate before it may be called missing.
func (s *Scheduler) ConfirmRecheck(seqs []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seq := range seqs {
		if !s.arrived.Contains(seq) {
			s.rechecked.Add(seq)
		}
	}
}

// Declare records that these sequences were reported missing, so that a later
// arrival can be recognised as the integrity failure it is.
func (s *Scheduler) Declare(seqs []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seq := range seqs {
		if !s.arrived.Contains(seq) {
			s.declared.Add(seq)
		}
	}
}

// StateOf reports where a sequence currently sits.
func (s *Scheduler) StateOf(seq uint64) State {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.arrived.Contains(seq):
		return Arrived
	case s.rechecked.Contains(seq):
		return Rechecked
	case s.settled.Contains(seq):
		return Provisional
	default:
		return InFlight
	}
}

// Snapshot returns the ledgers the classifier needs, copied so the classifier
// observes a stable view while the scheduler keeps running.
func (s *Scheduler) Snapshot() (arrived, settled, rechecked *Ledger, falseMissing uint64, late []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.arrived.Clone(), s.settled.Clone(), s.rechecked.Clone(),
		s.falseMissing, append([]uint64(nil), s.lateArrivals...)
}

// FalseMissing returns the integrity counter.
func (s *Scheduler) FalseMissing() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.falseMissing
}

// InFlightCount returns how many tracked sequences are still too young to judge.
func (s *Scheduler) InFlightCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n uint64
	for seq := range s.emittedAt {
		if !s.arrived.Contains(seq) && !s.settled.Contains(seq) {
			n++
		}
	}
	return n
}

// Window exposes the settle window for metrics and reports. Every report states
// the window it used, so a reader can judge whether a verdict was fair.
func (s *Scheduler) Window() *Window { return s.window }
