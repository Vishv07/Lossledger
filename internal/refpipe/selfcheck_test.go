package refpipe

import (
	"context"
	"testing"

	"github.com/Vishv07/lossledger/internal/adapter"
)

var gateKey = []byte("lossledger-selfcheck-key")

// TestSelfCheckGate is the M1 acceptance criterion. If this fails, the harness
// is not trustworthy and nothing it reports about a real collector means
// anything.
func TestSelfCheckGate(t *testing.T) {
	res, err := Run(context.Background(), Options{Seqs: 4000, Seed: 7, Key: gateKey})
	if err != nil {
		t.Fatalf("self-check could not run: %v", err)
	}
	if !res.Passed() {
		t.Fatalf("self-validation gate failed:\n%s", res)
	}
	t.Logf("\n%s", res)
}

// TestSelfCheckAcrossSeeds runs the gate at several seeds, so a pass is not an
// accident of where one sabotage happened to land.
func TestSelfCheckAcrossSeeds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping multi-seed gate in short mode")
	}
	for _, seed := range []int64{1, 2, 3, 11, 101} {
		seed := seed
		t.Run("seed", func(t *testing.T) {
			res, err := Run(context.Background(), Options{Seqs: 2000, Seed: seed, Key: gateKey})
			if err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
			if !res.Passed() {
				t.Fatalf("seed %d failed the gate:\n%s", seed, res)
			}
		})
	}
}

// TestSabotageExpectationIsDerived guards the property that makes the gate
// meaningful: the expected verdict comes from the sabotage itself, so the two
// cannot drift apart.
func TestSabotageExpectationIsDerived(t *testing.T) {
	sab := Bad(10_000, 42)
	want := sab.Expect()

	if want.MissingCount == 0 {
		t.Error("sabotage injected no drops, so the gate would prove nothing")
	}
	if want.DupBugCount == 0 {
		t.Error("sabotage injected no duplicates")
	}
	if want.OrderCount == 0 {
		t.Error("sabotage injected no partial multi-line events")
	}

	var dropped uint64
	for _, r := range sab.Drop {
		dropped += r.Width()
	}
	if dropped != want.MissingCount {
		t.Errorf("expected missing count %d does not match the injected drop width %d",
			want.MissingCount, dropped)
	}

	if sab.DuplicateFactor <= 3 {
		t.Errorf("duplicate factor %d is within the default bound, so duplicates would classify as replay rather than as a bug",
			sab.DuplicateFactor)
	}
}

func TestSabotageIsDeterministic(t *testing.T) {
	first, second := Bad(8000, 5), Bad(8000, 5)
	if !rangesEqual(first.Drop, second.Drop) {
		t.Error("same seed produced different drop ranges")
	}
	if !rangesEqual(first.Duplicate, second.Duplicate) {
		t.Error("same seed produced different duplicate ranges")
	}

	other := Bad(8000, 6)
	if rangesEqual(first.Drop, other.Drop) {
		t.Error("different seeds produced identical drop ranges")
	}
}

func TestSabotageZonesDoNotOverlap(t *testing.T) {
	// Overlapping sabotages would make the expected verdict ambiguous: a
	// sequence that is both dropped and duplicated cannot be both.
	for _, seed := range []int64{1, 7, 13, 99, 1234} {
		sab := Bad(12_000, seed)
		seen := map[uint64]string{}
		check := func(ranges []adapter.Range, label string) {
			for _, r := range ranges {
				for seq := r.From; seq <= r.To; seq++ {
					if prior, dup := seen[seq]; dup {
						t.Errorf("seed %d: sequence %d is both %s and %s", seed, seq, prior, label)
					}
					seen[seq] = label
				}
			}
		}
		check(sab.Drop, "dropped")
		check(sab.Duplicate, "duplicated")
		check(sab.TruncateContinuations, "truncated")
	}
}

func TestGoodSabotageIsEmpty(t *testing.T) {
	if !Good().IsEmpty() {
		t.Error("the faithful collector carries a sabotage")
	}
	if Bad(1000, 1).IsEmpty() {
		t.Error("the sabotaged collector carries no sabotage")
	}
}
