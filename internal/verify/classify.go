// Package verify holds the ledger, the settle scheduler and the anomaly
// classifier: everything that turns "what we emitted" and "what the sink holds"
// into a verdict.
//
// The classifier is the component whose correctness the whole project rests on.
// A false accusation against a collector is worse than missing a real loss,
// because the first destroys the trust that makes the second worth reporting.
package verify

import (
	"fmt"
	"sort"
	"time"

	"github.com/Vishv07/lossledger/internal/adapter"
)

// Anomaly is one class of delivery defect.
type Anomaly string

const (
	// Missing means a sequence was absent after the settle window and after a
	// mandatory re-check. It is the only class that accuses the pipeline of
	// losing data.
	Missing Anomaly = "MISSING"

	// DupBug is duplication outside any replay window, or beyond the
	// configured duplication factor.
	DupBug Anomaly = "DUP_BUG"

	// Order means arrival order violated sequence order beyond tolerance, or a
	// multi-line event was only partly delivered.
	Order Anomaly = "ORDER"

	// Late means the sequence arrived after the settle window but before
	// max_wait. Sequences reclassified from a premature Missing land here.
	Late Anomaly = "LATE"

	// Clean means every sequence is accounted for exactly once.
	Clean Anomaly = "CLEAN"

	// DupReplay is an expected at-least-once duplicate following a collector
	// restart. It is informational and never a failure.
	DupReplay Anomaly = "DUP_REPLAY"
)

// Severity orders the taxonomy. Exit codes mirror these values, so the shell
// sees the same ordering the report does (PRD section 7.6).
func (a Anomaly) Severity() int {
	switch a {
	case Missing:
		return 40
	case DupBug:
		return 30
	case Order:
		return 20
	case Late:
		return 10
	default: // Clean, DupReplay
		return 0
	}
}

// ExitCode is the process exit status for a verdict of this severity. Harness
// errors use 2, which is deliberately below every anomaly value so a crash can
// never be mistaken for a finding.
const ExitHarnessError = 2

// Finding is one anomaly over one range of sequences at one sink.
type Finding struct {
	Anomaly Anomaly
	Src     string
	Epoch   string
	Sink    string
	Ranges  []adapter.Range
	Count   uint64

	// Detail carries class-specific context: the duplication factor for
	// DUP_BUG, the observed delay for LATE.
	Detail string

	// Heuristic marks a classification made without ground truth about
	// restarts. Production mode sets it; the gauntlet, which knows exactly
	// when it killed a collector, does not. A heuristic DUP_BUG is never
	// published as a finding (PRD section 7.4).
	Heuristic bool
}

func (f Finding) String() string {
	s := fmt.Sprintf("%s %s/%s at %s: %d", f.Anomaly, f.Src, f.Epoch, f.Sink, f.Count)
	if len(f.Ranges) > 0 {
		s += " [" + FormatRanges(f.Ranges) + "]"
	}
	if f.Detail != "" {
		s += " (" + f.Detail + ")"
	}
	if f.Heuristic {
		s += " [heuristic]"
	}
	return s
}

// Verdict is the overall result of a verification pass.
type Verdict struct {
	Worst    Anomaly
	Findings []Finding

	Emitted  uint64
	Arrived  uint64
	MissingN uint64
	InFlight uint64

	// FalseMissing counts sequences that were declared Missing and later
	// arrived. It must be zero in every published run. If it is ever nonzero
	// that is a P0 against the verifier, ahead of all feature work
	// (PRD section 0.6).
	FalseMissing uint64

	InvalidLines uint64
}

// ExitCode returns the process exit status for this verdict.
func (v Verdict) ExitCode() int { return v.Worst.Severity() }

// Conservation checks the invariant from PRD section 10.3 property 5: every
// emitted sequence is either arrived, missing, or still in flight. A violation
// means the ledger has lost track of a sequence, which is a harness bug.
func (v Verdict) Conservation() error {
	accounted := v.Arrived + v.MissingN + v.InFlight
	if accounted != v.Emitted {
		return fmt.Errorf("verify: conservation violated: emitted %d but arrived %d + missing %d + in flight %d = %d",
			v.Emitted, v.Arrived, v.MissingN, v.InFlight, accounted)
	}
	return nil
}

// Config tunes the classifier.
type Config struct {
	// DupFactorBound is the duplication multiple past which duplicates are
	// classified DUP_BUG regardless of restart proximity.
	DupFactorBound int

	// ReplayWindow is how long after an observed restart duplicates are still
	// considered expected replay.
	ReplayWindow time.Duration

	// OrderTolerance is how far arrival order may deviate from sequence order
	// before it counts as an ORDER anomaly.
	OrderTolerance int

	// Heuristic marks every classification as made without restart ground
	// truth. Production run mode sets this; the gauntlet does not.
	Heuristic bool
}

// DefaultConfig returns the normative defaults.
func DefaultConfig() Config {
	return Config{
		DupFactorBound: 3,
		ReplayWindow:   2 * time.Minute,
		OrderTolerance: 64,
	}
}

// Observation is what the classifier is given about one (src, epoch, sink).
//
// It is a plain value with no clock and no I/O, which is what makes the
// classifier a pure function: the same observation always yields the same
// findings (PRD section 10.3 property 1).
type Observation struct {
	Src   string
	Epoch string
	Sink  string

	// Expected is the highest sequence legitimately expected. For an epoch
	// closed cleanly this is the final line's value. For a truncated epoch it
	// is the highest sequence observed anywhere, never more.
	Expected uint64

	// Truncated marks an epoch that ended without a final line, meaning the
	// emitter crashed. Nothing past Expected may be called missing.
	Truncated bool

	// Arrived lists sequences the sink holds.
	Arrived *Ledger

	// Settled is the set of sequences old enough to judge. A sequence outside
	// this set is in flight, never missing.
	Settled *Ledger

	// Rechecked is the set of settled-and-absent sequences that have already
	// been re-queried once. Only these may become Missing.
	Rechecked *Ledger

	// Duplicates maps sequence to how many rows the sink holds for it.
	Duplicates map[uint64]int

	// RestartWindows are time ranges in which a collector restart is known or
	// suspected, used to separate replay duplicates from bugs.
	RestartWindows []TimeRange

	// IngestTimes maps sequence to the sink's reported ingest time, used for
	// ordering checks and latency.
	IngestTimes map[uint64]time.Time

	// Partial lists sequences whose multi-line event arrived incompletely.
	Partial []uint64

	// LateArrivals are sequences that arrived after being declared missing.
	// Their presence is what drives the integrity metric.
	LateArrivals []uint64
}

// TimeRange is an inclusive time interval.
type TimeRange struct {
	From time.Time
	To   time.Time
}

// Contains reports whether t falls inside the range.
func (r TimeRange) Contains(t time.Time) bool {
	return !t.Before(r.From) && !t.After(r.To)
}

// Classify turns one observation into findings.
//
// It performs no I/O and reads no clock. Everything time-dependent was decided
// before this call, by the settle scheduler, which is what lets the properties
// in PRD section 10.3 be tested exhaustively.
func Classify(obs Observation, cfg Config) []Finding {
	var findings []Finding

	if missing := classifyMissing(obs); len(missing) > 0 {
		findings = append(findings, Finding{
			Anomaly: Missing,
			Src:     obs.Src, Epoch: obs.Epoch, Sink: obs.Sink,
			Ranges: CompressRanges(missing),
			Count:  uint64(len(missing)),
		})
	}

	bugs, replays := classifyDuplicates(obs, cfg)
	if len(bugs) > 0 {
		findings = append(findings, Finding{
			Anomaly: DupBug,
			Src:     obs.Src, Epoch: obs.Epoch, Sink: obs.Sink,
			Ranges:    CompressRanges(bugs),
			Count:     uint64(len(bugs)),
			Detail:    fmt.Sprintf("duplication factor above %d or outside any replay window", cfg.DupFactorBound),
			Heuristic: cfg.Heuristic,
		})
	}
	if len(replays) > 0 {
		findings = append(findings, Finding{
			Anomaly: DupReplay,
			Src:     obs.Src, Epoch: obs.Epoch, Sink: obs.Sink,
			Ranges:    CompressRanges(replays),
			Count:     uint64(len(replays)),
			Detail:    "expected at-least-once replay after a restart",
			Heuristic: cfg.Heuristic,
		})
	}

	if out := classifyOrder(obs, cfg); len(out) > 0 {
		findings = append(findings, Finding{
			Anomaly: Order,
			Src:     obs.Src, Epoch: obs.Epoch, Sink: obs.Sink,
			Ranges: CompressRanges(out),
			Count:  uint64(len(out)),
			Detail: "arrival order deviates from sequence order, or a multi-line event arrived partially",
		})
	}

	if len(obs.LateArrivals) > 0 {
		late := append([]uint64(nil), obs.LateArrivals...)
		sort.Slice(late, func(i, j int) bool { return late[i] < late[j] })
		findings = append(findings, Finding{
			Anomaly: Late,
			Src:     obs.Src, Epoch: obs.Epoch, Sink: obs.Sink,
			Ranges: CompressRanges(late),
			Count:  uint64(len(late)),
			Detail: "arrived after the settle window",
		})
	}

	sort.SliceStable(findings, func(i, j int) bool {
		return findings[i].Anomaly.Severity() > findings[j].Anomaly.Severity()
	})
	return findings
}

// classifyMissing returns the sequences that may legitimately be called lost.
//
// A sequence qualifies only when all four hold: it is at or below the expected
// high-water mark, it is absent from the sink, it is old enough that the settle
// window has passed, and it has survived one mandatory re-query. Each condition
// removes a way the harness could accuse a pipeline of a loss it did not cause.
func classifyMissing(obs Observation) []uint64 {
	if obs.Expected == 0 {
		return nil
	}

	var missing []uint64
	for seq := uint64(1); seq <= obs.Expected; seq++ {
		if obs.Arrived != nil && obs.Arrived.Contains(seq) {
			continue
		}
		// Not yet settled means still in flight, which is not a loss.
		if obs.Settled == nil || !obs.Settled.Contains(seq) {
			continue
		}
		// An accusation must be earned twice (PRD principle 2).
		if obs.Rechecked == nil || !obs.Rechecked.Contains(seq) {
			continue
		}
		missing = append(missing, seq)
	}
	return missing
}

// classifyDuplicates separates expected replay from genuine duplication bugs.
func classifyDuplicates(obs Observation, cfg Config) (bugs, replays []uint64) {
	if len(obs.Duplicates) == 0 {
		return nil, nil
	}

	seqs := make([]uint64, 0, len(obs.Duplicates))
	for seq := range obs.Duplicates {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	for _, seq := range seqs {
		count := obs.Duplicates[seq]
		if count <= 1 {
			continue
		}
		// Past the factor bound it is a bug regardless of restart proximity: no
		// amount of legitimate replay duplicates a record four times.
		if cfg.DupFactorBound > 0 && count > cfg.DupFactorBound {
			bugs = append(bugs, seq)
			continue
		}
		if inAnyWindow(obs.IngestTimes[seq], obs.RestartWindows) {
			replays = append(replays, seq)
			continue
		}
		bugs = append(bugs, seq)
	}
	return bugs, replays
}

func inAnyWindow(t time.Time, windows []TimeRange) bool {
	if t.IsZero() {
		// Without an ingest time we cannot place the duplicate relative to a
		// restart. We class it as replay, because the alternative is accusing
		// a pipeline on the strength of a missing timestamp.
		return true
	}
	for _, w := range windows {
		if w.Contains(t) {
			return true
		}
	}
	return false
}

// classifyOrder finds sequences that arrived out of order beyond tolerance, and
// multi-line events that arrived incompletely.
func classifyOrder(obs Observation, cfg Config) []uint64 {
	seen := map[uint64]bool{}
	var out []uint64

	for _, seq := range obs.Partial {
		if !seen[seq] {
			seen[seq] = true
			out = append(out, seq)
		}
	}

	if len(obs.IngestTimes) > 1 {
		type arrival struct {
			seq uint64
			at  time.Time
		}
		arrivals := make([]arrival, 0, len(obs.IngestTimes))
		for seq, at := range obs.IngestTimes {
			if !at.IsZero() {
				arrivals = append(arrivals, arrival{seq, at})
			}
		}
		// Order by arrival, then look for sequences that landed far from where
		// their number says they should have.
		sort.Slice(arrivals, func(i, j int) bool {
			if arrivals[i].at.Equal(arrivals[j].at) {
				return arrivals[i].seq < arrivals[j].seq
			}
			return arrivals[i].at.Before(arrivals[j].at)
		})

		byPosition := make(map[uint64]int, len(arrivals))
		for pos, a := range arrivals {
			byPosition[a.seq] = pos
		}

		bySeq := append([]arrival(nil), arrivals...)
		sort.Slice(bySeq, func(i, j int) bool { return bySeq[i].seq < bySeq[j].seq })

		for expectedPos, a := range bySeq {
			actualPos := byPosition[a.seq]
			drift := actualPos - expectedPos
			if drift < 0 {
				drift = -drift
			}
			if cfg.OrderTolerance > 0 && drift > cfg.OrderTolerance && !seen[a.seq] {
				seen[a.seq] = true
				out = append(out, a.seq)
			}
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Worst returns the highest-severity anomaly among findings, treating an empty
// set as Clean. DUP_REPLAY never raises the verdict.
func Worst(findings []Finding) Anomaly {
	worst := Clean
	for _, f := range findings {
		if f.Anomaly == DupReplay {
			continue
		}
		if f.Anomaly.Severity() > worst.Severity() {
			worst = f.Anomaly
		}
	}
	return worst
}
