package verify

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Vishv07/lossledger/internal/adapter"
)

// DefaultDetailThreshold is the range width at or below which the verifier
// stops bisecting and fetches per-sequence detail.
//
// The cheap path is what makes the verifier affordable: one CountRange call can
// clear a million sequences. Detail is only ever fetched for ranges that
// already disagree, so the expensive query count scales with the amount of loss
// rather than with the volume of traffic (PRD section 8.2).
const DefaultDetailThreshold = 512

// VerifierConfig configures one verifier.
type VerifierConfig struct {
	// Interval is how often a sweep runs in production mode.
	Interval time.Duration

	// DetailThreshold is where bisection stops and detail begins.
	DetailThreshold uint64

	// MaxWait caps the settle window.
	MaxWait time.Duration

	// RecheckDelay overrides the default of half the settle window.
	RecheckDelay time.Duration

	// Classify tunes the anomaly classifier.
	Classify Config

	// Now overrides the clock. Tests set it; production leaves it nil.
	Now func() time.Time
}

func (c *VerifierConfig) applyDefaults() {
	if c.Interval <= 0 {
		c.Interval = time.Minute
	}
	if c.DetailThreshold == 0 {
		c.DetailThreshold = DefaultDetailThreshold
	}
	if c.MaxWait <= 0 {
		c.MaxWait = DefaultMaxWait
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.Classify.DupFactorBound == 0 && c.Classify.ReplayWindow == 0 && c.Classify.OrderTolerance == 0 {
		c.Classify = DefaultConfig()
	}
}

// streamKey identifies one accounting unit: a source's epoch as seen by one
// sink. Loss at one sink and not another is what makes attribution possible,
// so the three parts are never collapsed (PRD section 7.7).
type streamKey struct {
	src   string
	epoch string
	sink  string
}

// stream is the verifier's state for one accounting unit.
type stream struct {
	key       streamKey
	scheduler *Scheduler

	mu         sync.Mutex
	duplicates map[uint64]int
	ingestTS   map[uint64]time.Time
	partial    map[uint64]bool

	// expected is the highest sequence legitimately expected, and truncated
	// records whether the epoch ended without a final line.
	expected  uint64
	truncated bool
	queries   map[string]uint64 // query kind to count, for overhead accounting
}

func newStream(key streamKey, window *Window) *stream {
	return &stream{
		key:        key,
		scheduler:  NewScheduler(window),
		duplicates: map[uint64]int{},
		ingestTS:   map[uint64]time.Time{},
		partial:    map[uint64]bool{},
		queries:    map[string]uint64{},
	}
}

// Verifier reconciles what the emitters wrote against what the sinks hold.
//
// It is a query client and nothing more. It never writes to a sink, never
// reads collector internals, and never consumes a collector's own counters as
// verdict input. A verdict is only ever built from what a store will admit to
// holding (PRD section 0.7).
type Verifier struct {
	cfg   VerifierConfig
	sinks *adapter.Registry

	mu      sync.Mutex
	streams map[streamKey]*stream

	invalidLines uint64
}

// NewVerifier builds a verifier over a sink registry.
func NewVerifier(cfg VerifierConfig, sinks *adapter.Registry) (*Verifier, error) {
	cfg.applyDefaults()
	if sinks == nil || len(sinks.All()) == 0 {
		return nil, fmt.Errorf("verify: no sinks registered")
	}
	return &Verifier{cfg: cfg, sinks: sinks, streams: map[streamKey]*stream{}}, nil
}

// streamFor returns the state for one accounting unit, creating it on first use.
func (v *Verifier) streamFor(key streamKey) *stream {
	v.mu.Lock()
	defer v.mu.Unlock()

	if s, ok := v.streams[key]; ok {
		return s
	}
	var hint time.Duration
	if sink, err := v.sinks.Get(key.sink); err == nil {
		hint = sink.SettleHint()
	}
	s := newStream(key, NewWindow(SettleConfig{
		Hint:         hint,
		MaxWait:      v.cfg.MaxWait,
		RecheckDelay: v.cfg.RecheckDelay,
	}))
	v.streams[key] = s
	return s
}

// NoteEmitted tells the verifier a sequence was written, which starts its
// settle clock at every sink.
//
// In production the verifier learns this from the emitter's own ledger; in the
// self-validation gate the test states it directly.
func (v *Verifier) NoteEmitted(src, epoch string, seq uint64, at time.Time) {
	for _, name := range v.sinks.Names() {
		s := v.streamFor(streamKey{src, epoch, name})
		s.scheduler.NoteEmitted(seq, at)
		s.mu.Lock()
		if seq > s.expected {
			s.expected = seq
		}
		s.mu.Unlock()
	}
}

// CloseEpoch records the epoch's final sequence.
//
// clean = true means the emitter wrote a final line, so every sequence up to
// final is legitimately expected and its absence is loss. clean = false means
// the emitter crashed, and nothing past the highest observed sequence may be
// called missing (PRD section 7.4).
func (v *Verifier) CloseEpoch(src, epoch string, finalSeq uint64, clean bool) {
	for _, name := range v.sinks.Names() {
		s := v.streamFor(streamKey{src, epoch, name})
		s.mu.Lock()
		if clean {
			s.expected = finalSeq
			s.truncated = false
		} else {
			s.truncated = true
			if finalSeq < s.expected {
				// A truncated epoch's expectation can only ever shrink to what
				// was actually observed. Growing it would invent gaps out of
				// an ordinary crash.
				s.expected = finalSeq
			}
		}
		s.mu.Unlock()
	}
}

// NoteInvalidLine records a line that carried a token but failed verification.
func (v *Verifier) NoteInvalidLine(n uint64) {
	v.mu.Lock()
	v.invalidLines += n
	v.mu.Unlock()
}

// Sweep runs one reconciliation pass over every sink and stream.
func (v *Verifier) Sweep(ctx context.Context) error {
	now := v.cfg.Now()

	v.mu.Lock()
	keys := make([]streamKey, 0, len(v.streams))
	for k := range v.streams {
		keys = append(keys, k)
	}
	v.mu.Unlock()

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].src != keys[j].src {
			return keys[i].src < keys[j].src
		}
		if keys[i].epoch != keys[j].epoch {
			return keys[i].epoch < keys[j].epoch
		}
		return keys[i].sink < keys[j].sink
	})

	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		sink, err := v.sinks.Get(key.sink)
		if err != nil {
			return err
		}
		s := v.streamFor(key)

		s.mu.Lock()
		expected := s.expected
		s.mu.Unlock()
		if expected == 0 {
			continue
		}

		if err := v.reconcile(ctx, sink, s, adapter.Range{From: 1, To: expected}); err != nil {
			return fmt.Errorf("verify: reconciling %s/%s at %s: %w", key.src, key.epoch, key.sink, err)
		}

		// Partial multi-line events never show up in a count, so a sink that
		// can answer the question separately is asked it directly.
		if reporter, ok := sink.(adapter.PartialReporter); ok {
			partials, err := reporter.PartialSeqs(ctx, key.src, key.epoch, 1, expected)
			if err != nil {
				return fmt.Errorf("verify: querying partials for %s/%s at %s: %w", key.src, key.epoch, key.sink, err)
			}
			if len(partials) > 0 {
				s.mu.Lock()
				for _, seq := range partials {
					s.partial[seq] = true
				}
				s.mu.Unlock()
				s.countQuery("partial")
			}
		}

		// Sequences whose mandatory re-query is due get one more look before
		// anything is declared missing.
		if due := s.scheduler.Advance(now); len(due) > 0 {
			if err := v.recheck(ctx, sink, s, due); err != nil {
				return err
			}
		}
	}
	return nil
}

// reconcile walks a range, using the cheap path where it can and bisecting
// where it cannot.
func (v *Verifier) reconcile(ctx context.Context, sink adapter.Sink, s *stream, r adapter.Range) error {
	if r.Width() == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	distinct, total, err := sink.CountRange(ctx, s.key.src, s.key.epoch, r.From, r.To)
	if err != nil {
		return err
	}
	s.countQuery("count")

	// The cheap path: everything present exactly once, nothing to look at.
	if distinct == r.Width() && total == distinct {
		s.markArrivedRange(r)
		return nil
	}

	// Narrow enough to look at directly.
	if r.Width() <= v.cfg.DetailThreshold {
		return v.fetchDetail(ctx, sink, s, r)
	}

	left, right, ok := r.Split()
	if !ok {
		return v.fetchDetail(ctx, sink, s, r)
	}
	if err := v.reconcile(ctx, sink, s, left); err != nil {
		return err
	}
	return v.reconcile(ctx, sink, s, right)
}

// fetchDetail records per-sequence facts for a range that disagreed.
func (v *Verifier) fetchDetail(ctx context.Context, sink adapter.Sink, s *stream, r adapter.Range) error {
	iter, err := sink.FetchRange(ctx, s.key.src, s.key.epoch, r.From, r.To)
	if err != nil {
		return err
	}
	defer iter.Close()
	s.countQuery("fetch")

	for iter.Next() {
		e := iter.Entry()
		s.scheduler.NoteArrived(e.Seq, e.IngestTS)

		s.mu.Lock()
		if e.DupCount > 1 {
			s.duplicates[e.Seq] = e.DupCount
		}
		if !e.IngestTS.IsZero() {
			s.ingestTS[e.Seq] = e.IngestTS
		}
		if e.Partial {
			s.partial[e.Seq] = true
		}
		s.mu.Unlock()
	}
	return iter.Err()
}

// recheck re-queries sequences whose mandatory second look is due. Only after
// this confirms absence may a sequence be called missing.
func (v *Verifier) recheck(ctx context.Context, sink adapter.Sink, s *stream, seqs []uint64) error {
	stillAbsent := make([]uint64, 0, len(seqs))

	for _, r := range CompressRanges(seqs) {
		iter, err := sink.FetchRange(ctx, s.key.src, s.key.epoch, r.From, r.To)
		if err != nil {
			return err
		}
		s.countQuery("fetch")

		found := map[uint64]bool{}
		for iter.Next() {
			e := iter.Entry()
			found[e.Seq] = true
			s.scheduler.NoteArrived(e.Seq, e.IngestTS)
		}
		if err := iter.Err(); err != nil {
			iter.Close()
			return err
		}
		iter.Close()

		for seq := r.From; seq <= r.To; seq++ {
			if !found[seq] {
				stillAbsent = append(stillAbsent, seq)
			}
		}
	}

	s.scheduler.ConfirmRecheck(stillAbsent)
	return nil
}

// markArrivedRange records a whole range as delivered via the cheap path.
func (s *stream) markArrivedRange(r adapter.Range) {
	for seq := r.From; seq <= r.To; seq++ {
		s.scheduler.NoteArrived(seq, time.Time{})
	}
}

func (s *stream) countQuery(kind string) {
	s.mu.Lock()
	s.queries[kind]++
	s.mu.Unlock()
}

// observation builds the classifier's input for one stream.
func (s *stream) observation(cfg Config) (Observation, uint64, uint64) {
	arrived, settled, rechecked, falseMissing, late := s.scheduler.Snapshot()

	s.mu.Lock()
	defer s.mu.Unlock()

	dups := make(map[uint64]int, len(s.duplicates))
	for k, val := range s.duplicates {
		dups[k] = val
	}
	ingest := make(map[uint64]time.Time, len(s.ingestTS))
	for k, val := range s.ingestTS {
		ingest[k] = val
	}
	partial := make([]uint64, 0, len(s.partial))
	for seq := range s.partial {
		partial = append(partial, seq)
	}
	sort.Slice(partial, func(i, j int) bool { return partial[i] < partial[j] })

	return Observation{
		Src:          s.key.src,
		Epoch:        s.key.epoch,
		Sink:         s.key.sink,
		Expected:     s.expected,
		Truncated:    s.truncated,
		Arrived:      arrived,
		Settled:      settled,
		Rechecked:    rechecked,
		Duplicates:   dups,
		IngestTimes:  ingest,
		Partial:      partial,
		LateArrivals: late,
	}, falseMissing, s.expected
}

// Verdict classifies every stream and returns the overall result.
func (v *Verifier) Verdict() Verdict {
	v.mu.Lock()
	streams := make([]*stream, 0, len(v.streams))
	for _, s := range v.streams {
		streams = append(streams, s)
	}
	invalid := v.invalidLines
	v.mu.Unlock()

	sort.Slice(streams, func(i, j int) bool {
		a, b := streams[i].key, streams[j].key
		if a.src != b.src {
			return a.src < b.src
		}
		if a.epoch != b.epoch {
			return a.epoch < b.epoch
		}
		return a.sink < b.sink
	})

	out := Verdict{Worst: Clean, InvalidLines: invalid}

	for _, s := range streams {
		obs, falseMissing, expected := s.observation(v.cfg.Classify)
		findings := Classify(obs, v.cfg.Classify)

		// A missing verdict is also a declaration: if any of these sequences
		// later arrive, that is an integrity failure and must be counted.
		for _, f := range findings {
			if f.Anomaly != Missing {
				continue
			}
			var seqs []uint64
			for _, r := range f.Ranges {
				for seq := r.From; seq <= r.To; seq++ {
					seqs = append(seqs, seq)
				}
			}
			s.scheduler.Declare(seqs)
			out.MissingN += f.Count
		}

		out.Findings = append(out.Findings, findings...)
		out.Emitted += expected
		out.Arrived += obs.Arrived.Count()
		out.InFlight += s.scheduler.InFlightCount()
		out.FalseMissing += falseMissing
	}

	out.Worst = Worst(out.Findings)
	return out
}

// Run sweeps on the configured interval until ctx is cancelled.
func (v *Verifier) Run(ctx context.Context) error {
	ticker := time.NewTicker(v.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := v.Sweep(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

// QueryCounts returns per-sink query totals by kind, which feeds the overhead
// metric so an operator can see what auditing costs them.
func (v *Verifier) QueryCounts() map[string]map[string]uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()

	out := map[string]map[string]uint64{}
	for key, s := range v.streams {
		s.mu.Lock()
		for kind, n := range s.queries {
			if out[key.sink] == nil {
				out[key.sink] = map[string]uint64{}
			}
			out[key.sink][kind] += n
		}
		s.mu.Unlock()
	}
	return out
}
