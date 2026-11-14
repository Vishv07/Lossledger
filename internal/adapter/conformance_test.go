package adapter

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Vishv07/lossledger/internal/wire"
)

// TestFileSinkConformance runs the shared specification against the reference
// adapter. The reference adapter is the control: if the suite cannot pass
// against a store we read directly off disk, the suite itself is wrong.
func TestFileSinkConformance(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewFileSink("conformance", dir, testKey)
	if err != nil {
		t.Fatal(err)
	}

	Conformance{
		Sink: sink,
		Seed: func(ctx context.Context, src, epoch string, seqs []uint64, dups map[uint64]int) error {
			return seedDir(dir, src, epoch, seqs, dups)
		},
	}.Run(t)
}

// seedDir writes canary lines into a directory, one file per epoch so the
// suite's cases do not share state.
func seedDir(dir, src, epoch string, seqs []uint64, dups map[uint64]int) error {
	f, err := os.Create(filepath.Join(dir, "seed-"+epoch+".log"))
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	for _, seq := range seqs {
		line := wire.Token{
			Kind: wire.KindCanary, Src: src, Epoch: epoch,
			Seq: seq, TS: int64(seq), Shape: "plain",
		}.Format(testKey)

		copies := 1
		if n, ok := dups[seq]; ok {
			copies = n
		}
		for i := 0; i < copies; i++ {
			if _, err := fmt.Fprintln(w, line); err != nil {
				return err
			}
		}
	}
	return nil
}

// recorder captures what the suite reports, so the suite's own failure
// behaviour can be tested. A conformance suite that passes everything is
// worthless, and this is how we know it does not.
type recorder struct {
	errors []string
	fatal  []string
}

func (r *recorder) Helper()                   {}
func (r *recorder) Logf(f string, a ...any)   {}
func (r *recorder) Errorf(f string, a ...any) { r.errors = append(r.errors, fmt.Sprintf(f, a...)) }
func (r *recorder) Fatalf(f string, a ...any) { r.fatal = append(r.fatal, fmt.Sprintf(f, a...)) }
func (r *recorder) failed() bool              { return len(r.errors) > 0 || len(r.fatal) > 0 }

// brokenSink wraps a working adapter and corrupts one behaviour, so we can
// prove the suite notices.
type brokenSink struct {
	Sink
	collapseDuplicates bool
	exclusiveUpperEnd  bool
	errorOnUnknownSrc  bool
	negativeSettle     bool
}

func (b brokenSink) SettleHint() time.Duration {
	if b.negativeSettle {
		return -time.Second
	}
	return b.Sink.SettleHint()
}

func (b brokenSink) CountRange(ctx context.Context, src, epoch string, from, to uint64) (uint64, uint64, error) {
	if b.errorOnUnknownSrc && src == "no-such-source" {
		return 0, 0, fmt.Errorf("unknown source")
	}
	if b.exclusiveUpperEnd && to > from {
		to--
	}
	d, total, err := b.Sink.CountRange(ctx, src, epoch, from, to)
	if b.collapseDuplicates {
		total = d
	}
	return d, total, err
}

func TestConformanceCatchesBrokenAdapters(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(s Sink) Sink
	}{
		{
			// Collapsing duplicates into distinct hides every DUP_BUG.
			name:   "duplicates collapsed into distinct",
			mutate: func(s Sink) Sink { return brokenSink{Sink: s, collapseDuplicates: true} },
		},
		{
			// An exclusive upper bound loses one sequence per range, which
			// bisection then multiplies into phantom gaps.
			name:   "upper bound treated as exclusive",
			mutate: func(s Sink) Sink { return brokenSink{Sink: s, exclusiveUpperEnd: true} },
		},
		{
			// A source the store has not seen is zero rows, not an error.
			name:   "unknown source returns an error",
			mutate: func(s Sink) Sink { return brokenSink{Sink: s, errorOnUnknownSrc: true} },
		},
		{
			// A negative settle hint would drag the window below what the
			// store actually needs.
			name:   "negative settle hint",
			mutate: func(s Sink) Sink { return brokenSink{Sink: s, negativeSettle: true} },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			base, err := NewFileSink("broken", dir, testKey)
			if err != nil {
				t.Fatal(err)
			}

			rec := &recorder{}
			Conformance{
				Sink: tc.mutate(base),
				Seed: func(ctx context.Context, src, epoch string, seqs []uint64, dups map[uint64]int) error {
					return seedDir(dir, src, epoch, seqs, dups)
				},
			}.Run(rec)

			if !rec.failed() {
				t.Errorf("the suite passed an adapter that %s; it is not testing what it claims", tc.name)
			}
		})
	}
}

func TestConformanceRejectsMissingWiring(t *testing.T) {
	rec := &recorder{}
	Conformance{}.Run(rec)
	if len(rec.fatal) == 0 {
		t.Error("the suite ran with no sink under test")
	}

	sink, _ := NewFileSink("x", t.TempDir(), testKey)
	rec = &recorder{}
	Conformance{Sink: sink}.Run(rec)
	if len(rec.fatal) == 0 {
		t.Error("the suite ran with no seed function")
	}
}

func TestSortedKeys(t *testing.T) {
	got := SortedKeys(map[uint64]int{9: 1, 2: 1, 40: 1, 1: 1})
	want := []uint64{1, 2, 9, 40}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SortedKeys = %v, want %v", got, want)
		}
	}
}
