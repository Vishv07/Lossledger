package verify

import (
	"math/rand"
	"testing"
	"time"

	"github.com/Vishv07/lossledger/internal/adapter"
)

// obsBuilder assembles an Observation for a test without repeating the
// boilerplate of four ledgers.
type obsBuilder struct {
	expected  uint64
	arrived   []uint64
	settled   []uint64
	rechecked []uint64
	dups      map[uint64]int
	ingest    map[uint64]time.Time
	partial   []uint64
	late      []uint64
	restarts  []TimeRange
	truncated bool
}

func (b obsBuilder) build() Observation {
	return Observation{
		Src: "node-a", Epoch: "01JCV0000000000000000000", Sink: "test",
		Expected:       b.expected,
		Truncated:      b.truncated,
		Arrived:        LedgerOf(b.arrived...),
		Settled:        LedgerOf(b.settled...),
		Rechecked:      LedgerOf(b.rechecked...),
		Duplicates:     b.dups,
		IngestTimes:    b.ingest,
		Partial:        b.partial,
		LateArrivals:   b.late,
		RestartWindows: b.restarts,
	}
}

func seqRange(from, to uint64) []uint64 {
	var out []uint64
	for s := from; s <= to; s++ {
		out = append(out, s)
	}
	return out
}

func findingFor(findings []Finding, a Anomaly) (Finding, bool) {
	for _, f := range findings {
		if f.Anomaly == a {
			return f, true
		}
	}
	return Finding{}, false
}

func TestClassifyCleanWhenAllArrived(t *testing.T) {
	obs := obsBuilder{expected: 1000, arrived: seqRange(1, 1000)}.build()
	findings := Classify(obs, DefaultConfig())
	if len(findings) != 0 {
		t.Fatalf("a complete delivery produced findings: %v", findings)
	}
	if got := Worst(findings); got != Clean {
		t.Errorf("Worst = %v, want CLEAN", got)
	}
}

// TestMissingRequiresAllFourConditions is the core safety property. A sequence
// may only be called missing when every gate has been passed; each subtest
// removes one gate and expects no accusation.
func TestMissingRequiresAllFourConditions(t *testing.T) {
	full := obsBuilder{
		expected:  100,
		arrived:   append(seqRange(1, 49), seqRange(51, 100)...),
		settled:   []uint64{50},
		rechecked: []uint64{50},
	}

	t.Run("all conditions met", func(t *testing.T) {
		f, ok := findingFor(Classify(full.build(), DefaultConfig()), Missing)
		if !ok {
			t.Fatal("a genuine loss was not reported")
		}
		if f.Count != 1 || len(f.Ranges) != 1 || f.Ranges[0] != (adapter.Range{From: 50, To: 50}) {
			t.Errorf("missing finding = %v, want exactly sequence 50", f)
		}
	})

	t.Run("not settled means in flight", func(t *testing.T) {
		b := full
		b.settled = nil
		if _, ok := findingFor(Classify(b.build(), DefaultConfig()), Missing); ok {
			t.Error("an unsettled sequence was reported missing")
		}
	})

	t.Run("not rechecked means unconfirmed", func(t *testing.T) {
		b := full
		b.rechecked = nil
		if _, ok := findingFor(Classify(b.build(), DefaultConfig()), Missing); ok {
			t.Error("a sequence was reported missing without the mandatory re-check")
		}
	})

	t.Run("above the expected high-water mark", func(t *testing.T) {
		b := full
		b.expected = 49
		if _, ok := findingFor(Classify(b.build(), DefaultConfig()), Missing); ok {
			t.Error("a sequence above the expected mark was reported missing")
		}
	})

	t.Run("arrived after all", func(t *testing.T) {
		b := full
		b.arrived = seqRange(1, 100)
		if _, ok := findingFor(Classify(b.build(), DefaultConfig()), Missing); ok {
			t.Error("a delivered sequence was reported missing")
		}
	})
}

// Property 1 from PRD section 10.3: classification is deterministic.
func TestPropertyPurity(t *testing.T) {
	rnd := rand.New(rand.NewSource(99))
	for trial := 0; trial < 50; trial++ {
		b := randomObs(rnd)
		obs := b.build()
		first := Classify(obs, DefaultConfig())
		second := Classify(obs, DefaultConfig())

		if len(first) != len(second) {
			t.Fatalf("trial %d: finding counts differ: %d vs %d", trial, len(first), len(second))
		}
		for i := range first {
			if first[i].Anomaly != second[i].Anomaly || first[i].Count != second[i].Count {
				t.Fatalf("trial %d: findings differ at %d: %v vs %v", trial, i, first[i], second[i])
			}
			if !rangesEq(first[i].Ranges, second[i].Ranges) {
				t.Fatalf("trial %d: ranges differ at %d", trial, i)
			}
		}
	}
}

// Property 2: lengthening the settle window never creates new MISSING.
//
// A longer window can only move sequences out of the settled set, and only
// settled sequences can be accused. Shrinking the settled set must therefore
// shrink the accusation.
func TestPropertyMonotoneSettle(t *testing.T) {
	rnd := rand.New(rand.NewSource(7))
	for trial := 0; trial < 50; trial++ {
		b := randomObs(rnd)

		wide := b
		// A longer settle window: fewer sequences old enough to judge.
		if len(b.settled) > 0 {
			wide.settled = b.settled[:len(b.settled)/2]
		}

		before := countMissing(Classify(b.build(), DefaultConfig()))
		after := countMissing(Classify(wide.build(), DefaultConfig()))

		if after > before {
			t.Fatalf("trial %d: lengthening the settle window created missing sequences: %d then %d",
				trial, before, after)
		}
	}
}

// Property 3: a truncated epoch never contributes MISSING past its last
// observed sequence.
func TestPropertyEpochSafety(t *testing.T) {
	// The emitter crashed after 500. Nothing above that was ever written, so
	// nothing above that can have been lost.
	obs := obsBuilder{
		expected:  500,
		truncated: true,
		arrived:   seqRange(1, 500),
		settled:   seqRange(501, 1000),
		rechecked: seqRange(501, 1000),
	}.build()

	if f, ok := findingFor(Classify(obs, DefaultConfig()), Missing); ok {
		t.Errorf("a truncated epoch produced phantom missing sequences: %v", f)
	}
}

// Property 4: widening a replay window never turns DUP_REPLAY into DUP_BUG.
func TestPropertyReplayWindowWidening(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	dups := map[uint64]int{10: 2, 20: 2, 30: 2}
	ingest := map[uint64]time.Time{
		10: base.Add(1 * time.Minute),
		20: base.Add(5 * time.Minute),
		30: base.Add(9 * time.Minute),
	}

	narrow := obsBuilder{
		expected: 100, arrived: seqRange(1, 100), dups: dups, ingest: ingest,
		restarts: []TimeRange{{From: base, To: base.Add(2 * time.Minute)}},
	}.build()
	wide := obsBuilder{
		expected: 100, arrived: seqRange(1, 100), dups: dups, ingest: ingest,
		restarts: []TimeRange{{From: base, To: base.Add(10 * time.Minute)}},
	}.build()

	narrowBugs := countOf(Classify(narrow, DefaultConfig()), DupBug)
	wideBugs := countOf(Classify(wide, DefaultConfig()), DupBug)

	if wideBugs > narrowBugs {
		t.Errorf("widening the replay window created duplicate bugs: %d then %d", narrowBugs, wideBugs)
	}
	if wideBugs != 0 {
		t.Errorf("a window covering every duplicate still reported %d bugs", wideBugs)
	}
}

func TestDuplicateFactorBoundOverridesReplayWindow(t *testing.T) {
	// No amount of legitimate replay copies a record five times, so the factor
	// bound outranks restart proximity.
	base := time.Unix(1_700_000_000, 0)
	obs := obsBuilder{
		expected: 100, arrived: seqRange(1, 100),
		dups:     map[uint64]int{42: 5},
		ingest:   map[uint64]time.Time{42: base},
		restarts: []TimeRange{{From: base.Add(-time.Hour), To: base.Add(time.Hour)}},
	}.build()

	f, ok := findingFor(Classify(obs, DefaultConfig()), DupBug)
	if !ok {
		t.Fatal("a duplication factor of 5 inside a replay window was not reported as a bug")
	}
	if f.Count != 1 {
		t.Errorf("DUP_BUG count = %d, want 1", f.Count)
	}
}

func TestDuplicatesInsideReplayWindowAreInformational(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	obs := obsBuilder{
		expected: 100, arrived: seqRange(1, 100),
		dups:     map[uint64]int{42: 2},
		ingest:   map[uint64]time.Time{42: base},
		restarts: []TimeRange{{From: base.Add(-time.Minute), To: base.Add(time.Minute)}},
	}.build()

	findings := Classify(obs, DefaultConfig())
	if _, ok := findingFor(findings, DupBug); ok {
		t.Error("an at-least-once replay duplicate was reported as a bug")
	}
	if _, ok := findingFor(findings, DupReplay); !ok {
		t.Error("the replay duplicate was not reported at all")
	}
	if got := Worst(findings); got != Clean {
		t.Errorf("Worst = %v, want CLEAN: a replay duplicate must never fail a run", got)
	}
}

func TestPartialMultilineBecomesOrder(t *testing.T) {
	obs := obsBuilder{
		expected: 100, arrived: seqRange(1, 100),
		partial: []uint64{7, 8, 9},
	}.build()

	f, ok := findingFor(Classify(obs, DefaultConfig()), Order)
	if !ok {
		t.Fatal("a partially delivered multi-line event produced no ORDER finding")
	}
	if f.Count != 3 {
		t.Errorf("ORDER count = %d, want 3", f.Count)
	}
}

func TestLateArrivalsReported(t *testing.T) {
	obs := obsBuilder{
		expected: 100, arrived: seqRange(1, 100),
		late: []uint64{55, 56},
	}.build()

	f, ok := findingFor(Classify(obs, DefaultConfig()), Late)
	if !ok {
		t.Fatal("late arrivals were not reported")
	}
	if f.Count != 2 {
		t.Errorf("LATE count = %d, want 2", f.Count)
	}
	if got := Worst(Classify(obs, DefaultConfig())); got != Late {
		t.Errorf("Worst = %v, want LATE", got)
	}
}

func TestSeverityOrdering(t *testing.T) {
	// Exit codes mirror severity, and the shell must see the same ordering the
	// report does.
	want := []struct {
		a    Anomaly
		code int
	}{
		{Missing, 40}, {DupBug, 30}, {Order, 20}, {Late, 10}, {Clean, 0}, {DupReplay, 0},
	}
	for _, tc := range want {
		if got := tc.a.Severity(); got != tc.code {
			t.Errorf("%s severity = %d, want %d", tc.a, got, tc.code)
		}
	}
	if Missing.Severity() <= DupBug.Severity() || DupBug.Severity() <= Order.Severity() ||
		Order.Severity() <= Late.Severity() || Late.Severity() <= Clean.Severity() {
		t.Error("severity ordering is not strictly descending")
	}
	if ExitHarnessError >= Late.Severity() {
		t.Error("the harness error code must sit below every anomaly so a crash cannot read as a finding")
	}
}

func TestWorstIgnoresReplay(t *testing.T) {
	findings := []Finding{{Anomaly: DupReplay, Count: 100}}
	if got := Worst(findings); got != Clean {
		t.Errorf("Worst = %v, want CLEAN: replay duplicates never fail a run", got)
	}
}

func TestConservation(t *testing.T) {
	cases := []struct {
		name    string
		v       Verdict
		wantErr bool
	}{
		{"balanced", Verdict{Emitted: 100, Arrived: 90, MissingN: 5, InFlight: 5}, false},
		{"all arrived", Verdict{Emitted: 100, Arrived: 100}, false},
		{"short", Verdict{Emitted: 100, Arrived: 90, MissingN: 5, InFlight: 1}, true},
		{"over", Verdict{Emitted: 100, Arrived: 90, MissingN: 50, InFlight: 5}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.v.Conservation(); (err != nil) != tc.wantErr {
				t.Errorf("Conservation() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestMissingRangesAreCompressed(t *testing.T) {
	// A report quotes ranges because "2041 to 2118" is actionable and a column
	// of 78 integers is not.
	absent := append(seqRange(10, 20), seqRange(50, 52)...)
	arrived := []uint64{}
	for s := uint64(1); s <= 100; s++ {
		missing := false
		for _, a := range absent {
			if a == s {
				missing = true
				break
			}
		}
		if !missing {
			arrived = append(arrived, s)
		}
	}

	obs := obsBuilder{expected: 100, arrived: arrived, settled: absent, rechecked: absent}.build()
	f, ok := findingFor(Classify(obs, DefaultConfig()), Missing)
	if !ok {
		t.Fatal("no missing finding")
	}
	want := []adapter.Range{{From: 10, To: 20}, {From: 50, To: 52}}
	if !rangesEq(f.Ranges, want) {
		t.Errorf("ranges = %v, want %v", f.Ranges, want)
	}
	if f.Count != 14 {
		t.Errorf("count = %d, want 14", f.Count)
	}
}

// randomObs builds a varied observation for the property tests.
func randomObs(rnd *rand.Rand) obsBuilder {
	expected := uint64(rnd.Intn(500) + 50)
	var arrived, settled, rechecked []uint64

	for s := uint64(1); s <= expected; s++ {
		if rnd.Float64() < 0.9 {
			arrived = append(arrived, s)
			continue
		}
		if rnd.Float64() < 0.8 {
			settled = append(settled, s)
			if rnd.Float64() < 0.8 {
				rechecked = append(rechecked, s)
			}
		}
	}

	dups := map[uint64]int{}
	for i := 0; i < rnd.Intn(5); i++ {
		if len(arrived) > 0 {
			dups[arrived[rnd.Intn(len(arrived))]] = rnd.Intn(5) + 2
		}
	}

	return obsBuilder{expected: expected, arrived: arrived, settled: settled, rechecked: rechecked, dups: dups}
}

func countMissing(findings []Finding) uint64 { return countOf(findings, Missing) }

func countOf(findings []Finding, a Anomaly) uint64 {
	var n uint64
	for _, f := range findings {
		if f.Anomaly == a {
			n += f.Count
		}
	}
	return n
}

func rangesEq(a, b []adapter.Range) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
