package verify

import (
	"context"
	"testing"
	"time"

	"github.com/Vishv07/lossledger/internal/adapter"
)

// holdingSink is a store that holds exactly the sequences it was given.
type holdingSink struct {
	name string
	held map[uint64]bool
}

func (p *holdingSink) Name() string                 { return p.name }
func (p *holdingSink) SettleHint() time.Duration    { return time.Millisecond }
func (p *holdingSink) Doctor(context.Context) error { return nil }
func (p *holdingSink) Close() error                 { return nil }

func (p *holdingSink) CountRange(ctx context.Context, _, _ string, from, to uint64) (uint64, uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	var n uint64
	for s := from; s <= to; s++ {
		if p.held[s] {
			n++
		}
	}
	return n, n, nil
}

func (p *holdingSink) FetchRange(ctx context.Context, _, _ string, from, to uint64) (adapter.SeqIter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []adapter.Entry
	for s := from; s <= to; s++ {
		if p.held[s] {
			out = append(out, adapter.Entry{Seq: s, DupCount: 1})
		}
	}
	return &sliceIter{entries: out}, nil
}

type sliceIter struct {
	entries []adapter.Entry
	i       int
}

func (s *sliceIter) Next() bool           { s.i++; return s.i <= len(s.entries) }
func (s *sliceIter) Entry() adapter.Entry { return s.entries[s.i-1] }
func (s *sliceIter) Err() error           { return nil }
func (s *sliceIter) Close() error         { return nil }

func fullSet(n uint64) map[uint64]bool {
	m := make(map[uint64]bool, n)
	for s := uint64(1); s <= n; s++ {
		m[s] = true
	}
	return m
}

const testEpoch = "01JCV00000000000000000AA"

// TestEmittedCountsEventsNotDeliveries is the regression test for a real bug.
//
// Emission belongs to a source and an epoch. Counting it once per sink made a
// run with two sinks report twice as many events as were ever written, so the
// report told the operator a number that was simply false.
func TestEmittedCountsEventsNotDeliveries(t *testing.T) {
	const emitted = 1000
	held := fullSet(emitted)

	for _, sinkCount := range []int{1, 2, 3} {
		sinks := make([]adapter.Sink, 0, sinkCount)
		for i := 0; i < sinkCount; i++ {
			sinks = append(sinks, &holdingSink{name: string(rune('a' + i)), held: held})
		}
		reg, err := adapter.NewRegistry(sinks...)
		if err != nil {
			t.Fatal(err)
		}

		now := time.Unix(1_700_000_000, 0)
		v, err := NewVerifier(VerifierConfig{Now: func() time.Time { return now }}, reg)
		if err != nil {
			t.Fatal(err)
		}
		for s := uint64(1); s <= emitted; s++ {
			v.NoteEmitted("node-a", testEpoch, s, now)
		}
		v.CloseEpoch("node-a", testEpoch, emitted, true)
		if err := v.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}

		got := v.Verdict()
		if got.Emitted != emitted {
			t.Errorf("with %d sinks, Emitted = %d, want %d", sinkCount, got.Emitted, emitted)
		}
		// Deliveries are counted per sink, which is the correct unit for them.
		if want := uint64(emitted * sinkCount); got.Arrived != want {
			t.Errorf("with %d sinks, Arrived = %d, want %d", sinkCount, got.Arrived, want)
		}
		if len(got.PerSink) != sinkCount {
			t.Errorf("PerSink has %d entries, want %d", len(got.PerSink), sinkCount)
		}
		if err := got.Conservation(); err != nil {
			t.Errorf("with %d sinks: %v", sinkCount, err)
		}
	}
}

// TestConservationIsCheckedPerSink is the regression test for why the bug
// above survived.
//
// The invariant was checked over the totals, so an error that appeared on both
// sides of the equation cancelled out and the check reported healthy. PRD
// section 10.3 specifies it per (source, epoch, sink) for exactly this reason.
func TestConservationIsCheckedPerSink(t *testing.T) {
	// Sink a is short by 10 and sink b is over by 10. Across the totals the
	// two errors cancel exactly, which is what the old global check compared,
	// so it reported healthy books while both sinks had lost track.
	v := Verdict{
		Emitted: 100,
		PerSink: []SinkTally{
			{Sink: "a", Expected: 100, Arrived: 90},
			{Sink: "b", Expected: 100, Arrived: 110},
		},
	}

	var expected, accounted uint64
	for _, t := range v.PerSink {
		expected += t.Expected
		accounted += t.Arrived + t.Missing + t.InFlight
	}
	if expected != accounted {
		t.Fatalf("fixture does not demonstrate the point: totals %d and %d differ", expected, accounted)
	}

	err := v.Conservation()
	if err == nil {
		t.Fatal("conservation passed books that balance only because two errors cancelled")
	}
	for _, sink := range []string{"sink a", "sink b"} {
		if !contains(err.Error(), sink) {
			t.Errorf("the error does not name %s: %v", sink, err)
		}
	}
}

func TestConservationRefusesToPassUnverifiableBooks(t *testing.T) {
	// No per-sink accounting and a nonzero emission count means the tally was
	// never built. Returning nil there would report healthy for a run nothing
	// had actually checked.
	v := Verdict{Emitted: 500}
	if err := v.Conservation(); err == nil {
		t.Error("conservation passed a verdict carrying no per-sink accounting")
	}

	// A run that emitted nothing legitimately has nothing to account for.
	if err := (Verdict{}).Conservation(); err != nil {
		t.Errorf("conservation failed on an empty run: %v", err)
	}
}

func TestConservationPassesWhenEverySinkBalances(t *testing.T) {
	v := Verdict{
		Emitted: 100,
		PerSink: []SinkTally{
			{Sink: "a", Expected: 100, Arrived: 95, Missing: 3, InFlight: 2},
			{Sink: "b", Expected: 100, Arrived: 100},
		},
	}
	if err := v.Conservation(); err != nil {
		t.Errorf("conservation failed on balanced books: %v", err)
	}
}

// TestLossAtOneSinkOnly checks the attribution the PRD promises in section 7.7:
// sequences missing at one sink only point at that sink's path, not the source.
func TestLossAtOneSinkOnly(t *testing.T) {
	const emitted = 500

	complete := fullSet(emitted)
	lossy := fullSet(emitted)
	for s := uint64(200); s <= 249; s++ {
		delete(lossy, s)
	}

	reg, err := adapter.NewRegistry(
		&holdingSink{name: "good", held: complete},
		&holdingSink{name: "lossy", held: lossy},
	)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Unix(1_700_000_000, 0)
	clock := now
	v, err := NewVerifier(VerifierConfig{
		Now:     func() time.Time { return clock },
		MaxWait: time.Minute,
	}, reg)
	if err != nil {
		t.Fatal(err)
	}

	for s := uint64(1); s <= emitted; s++ {
		v.NoteEmitted("node-a", testEpoch, s, now)
	}
	v.CloseEpoch("node-a", testEpoch, emitted, true)

	// Three sweeps with the clock advancing: settle, mandatory re-check, verdict.
	for i := 0; i < 3; i++ {
		if err := v.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		clock = clock.Add(2 * time.Minute)
	}

	got := v.Verdict()
	if got.Worst != Missing {
		t.Fatalf("verdict = %s, want MISSING", got.Worst)
	}
	if got.Emitted != emitted {
		t.Errorf("Emitted = %d, want %d", got.Emitted, emitted)
	}

	bySink := map[string]SinkTally{}
	for _, tl := range got.PerSink {
		bySink[tl.Sink] = tl
	}
	if n := bySink["good"].Missing; n != 0 {
		t.Errorf("the healthy sink was blamed for %d missing sequences", n)
	}
	if n := bySink["lossy"].Missing; n != 50 {
		t.Errorf("the lossy sink is missing %d sequences, want 50", n)
	}
	if got.FalseMissing != 0 {
		t.Errorf("false missing = %d, must be zero", got.FalseMissing)
	}
	if err := got.Conservation(); err != nil {
		t.Errorf("conservation: %v", err)
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0
}

func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}

// TestMissingNeedsTheClockToAdvance pins the requirement behind a real bug.
//
// The verify command swept twice in the same millisecond. Every sequence was
// settled, but the mandatory re-check delay never elapsed, so nothing was ever
// confirmed absent and the command reported CLEAN with 54 sequences deleted
// from the store. A false negative on a tool whose job is finding loss.
//
// The property: sweeping without letting time pass can never produce MISSING.
// Any caller reconciling historical data has to drive the clock forward.
func TestMissingNeedsTheClockToAdvance(t *testing.T) {
	const emitted = 200

	lossy := fullSet(emitted)
	for s := uint64(50); s <= 79; s++ {
		delete(lossy, s)
	}

	run := func(advanceBetweenSweeps bool) Verdict {
		reg, err := adapter.NewRegistry(&holdingSink{name: "sink", held: lossy})
		if err != nil {
			t.Fatal(err)
		}
		clock := time.Unix(1_700_000_000, 0)
		v, err := NewVerifier(VerifierConfig{
			Now: func() time.Time { return clock }, MaxWait: time.Minute,
		}, reg)
		if err != nil {
			t.Fatal(err)
		}

		emittedAt := clock.Add(-10 * time.Minute)
		for s := uint64(1); s <= emitted; s++ {
			v.NoteEmitted("node-a", testEpoch, s, emittedAt)
		}
		v.CloseEpoch("node-a", testEpoch, emitted, true)

		for i := 0; i < 3; i++ {
			if err := v.Sweep(context.Background()); err != nil {
				t.Fatal(err)
			}
			if advanceBetweenSweeps {
				clock = clock.Add(2 * time.Minute)
			}
		}
		return v.Verdict()
	}

	frozen := run(false)
	if frozen.MissingN != 0 {
		t.Errorf("with a frozen clock, missing = %d; the mandatory re-check cannot have completed", frozen.MissingN)
	}
	// The books refuse to balance, which is the signal a caller must not
	// ignore. This is what the command now treats as fatal rather than
	// reporting a clean verdict alongside it.
	if err := frozen.Conservation(); err == nil {
		t.Error("conservation passed while 30 sequences were unaccounted for; " +
			"without this signal a frozen-clock run looks healthy")
	}

	advanced := run(true)
	if advanced.MissingN != 30 {
		t.Errorf("with the clock advancing, missing = %d, want 30", advanced.MissingN)
	}
	if advanced.Worst != Missing {
		t.Errorf("verdict = %s, want MISSING", advanced.Worst)
	}
	if err := advanced.Conservation(); err != nil {
		t.Errorf("conservation: %v", err)
	}
}
