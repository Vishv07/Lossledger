package adapter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Vishv07/lossledger/internal/wire"
)

var testKey = []byte("lossledger-adapter-test-key")

// writeCanary writes count canary lines into dir, skipping any sequence in
// skip and duplicating any sequence in dup.
func writeCanary(t *testing.T, dir string, count uint64, skip map[uint64]bool, dup map[uint64]int) string {
	t.Helper()

	epoch := wire.MustULID(time.Unix(1_700_000_000, 0)).String()
	f, err := os.Create(filepath.Join(dir, "collected.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	for seq := uint64(1); seq <= count; seq++ {
		if skip[seq] {
			continue
		}
		line := wire.Token{
			Kind: wire.KindCanary, Src: "node-a", Epoch: epoch,
			Seq: seq, TS: int64(seq), Shape: "plain",
		}.Format(testKey)

		copies := 1
		if n, ok := dup[seq]; ok {
			copies = n
		}
		for i := 0; i < copies; i++ {
			fmt.Fprintln(f, line)
		}
	}
	return epoch
}

func TestFileSinkCountRange(t *testing.T) {
	dir := t.TempDir()
	epoch := writeCanary(t, dir, 1000, map[uint64]bool{500: true, 501: true}, map[uint64]int{10: 3})

	sink, err := NewFileSink("test", dir, testKey)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	cases := []struct {
		name                string
		from, to            uint64
		wantDist, wantTotal uint64
	}{
		{"whole epoch", 1, 1000, 998, 1000},
		{"clean window", 100, 200, 101, 101},
		{"window with the gap", 495, 505, 9, 9},
		{"window with the duplicate", 5, 15, 11, 13},
		{"beyond the data", 2000, 3000, 0, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			distinct, total, err := sink.CountRange(ctx, "node-a", epoch, tc.from, tc.to)
			if err != nil {
				t.Fatal(err)
			}
			if distinct != tc.wantDist {
				t.Errorf("distinct = %d, want %d", distinct, tc.wantDist)
			}
			if total != tc.wantTotal {
				t.Errorf("total = %d, want %d", total, tc.wantTotal)
			}
			if distinct > total {
				t.Error("distinct exceeded total, which is impossible")
			}
		})
	}
}

func TestFileSinkRejectsForeignKey(t *testing.T) {
	// A line that looks like a canary but is not signed by us must never enter
	// the ledger, or anyone could pollute a run by writing plausible lines.
	dir := t.TempDir()
	epoch := writeCanary(t, dir, 100, nil, nil)

	sink, err := NewFileSink("test", dir, []byte("a-different-key"))
	if err != nil {
		t.Fatal(err)
	}
	distinct, _, err := sink.CountRange(context.Background(), "node-a", epoch, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if distinct != 0 {
		t.Errorf("distinct = %d, want 0: lines signed by another deployment were counted", distinct)
	}
	if n := sink.InvalidLines(); n != 100 {
		t.Errorf("InvalidLines = %d, want 100", n)
	}
}

func TestFileSinkFetchRangeIsOrdered(t *testing.T) {
	dir := t.TempDir()
	epoch := writeCanary(t, dir, 200, map[uint64]bool{50: true}, map[uint64]int{60: 2})

	sink, _ := NewFileSink("test", dir, testKey)
	iter, err := sink.FetchRange(context.Background(), "node-a", epoch, 40, 70)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	var last uint64
	var seen int
	for iter.Next() {
		e := iter.Entry()
		if e.Seq <= last {
			t.Fatalf("entries are not ascending: %d after %d", e.Seq, last)
		}
		if e.Seq == 50 {
			t.Error("a sequence that was never written came back from FetchRange")
		}
		if e.Seq == 60 && e.DupCount != 2 {
			t.Errorf("DupCount for 60 = %d, want 2", e.DupCount)
		}
		last = e.Seq
		seen++
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != 30 { // 40 to 70 inclusive is 31, minus the skipped 50
		t.Errorf("saw %d entries, want 30", seen)
	}
}

func TestFileSinkEpochState(t *testing.T) {
	t.Run("clean shutdown", func(t *testing.T) {
		dir := t.TempDir()
		epoch := writeCanary(t, dir, 100, nil, nil)

		f, _ := os.OpenFile(filepath.Join(dir, "collected.log"), os.O_APPEND|os.O_WRONLY, 0o644)
		fmt.Fprintln(f, wire.Token{
			Kind: wire.KindFinal, Src: "node-a", Epoch: epoch, TS: 1, Final: 100,
		}.Format(testKey))
		f.Close()

		sink, _ := NewFileSink("test", dir, testKey)
		state := sink.EpochState("node-a", epoch)
		if !state.HasFinal {
			t.Error("a final line was written but not detected")
		}
		if state.FinalSeq != 100 {
			t.Errorf("FinalSeq = %d, want 100", state.FinalSeq)
		}
	})

	t.Run("crash", func(t *testing.T) {
		dir := t.TempDir()
		epoch := writeCanary(t, dir, 100, nil, nil)

		sink, _ := NewFileSink("test", dir, testKey)
		state := sink.EpochState("node-a", epoch)
		if state.HasFinal {
			t.Error("a crashed epoch was reported as cleanly closed")
		}
		if state.MaxSeq != 100 {
			t.Errorf("MaxSeq = %d, want 100", state.MaxSeq)
		}
	})
}

func TestFileSinkDoctor(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		sink, _ := NewFileSink("test", t.TempDir(), testKey)
		if err := sink.Doctor(context.Background()); err != nil {
			t.Errorf("Doctor failed on a readable directory: %v", err)
		}
	})

	t.Run("missing directory", func(t *testing.T) {
		// Failing loudly matters here: the alternative is silently reporting
		// that every single sequence was lost.
		sink, _ := NewFileSink("test", filepath.Join(t.TempDir(), "nope"), testKey)
		if err := sink.Doctor(context.Background()); err == nil {
			t.Error("Doctor passed on a directory that does not exist")
		}
	})

	t.Run("not a directory", func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "a-file")
		os.WriteFile(file, []byte("x"), 0o644)
		sink, _ := NewFileSink("test", file, testKey)
		if err := sink.Doctor(context.Background()); err == nil {
			t.Error("Doctor passed on a path that is not a directory")
		}
	})
}

func TestFileSinkNeedsKey(t *testing.T) {
	if _, err := NewFileSink("test", t.TempDir(), nil); err != wire.ErrNoKey {
		t.Errorf("error = %v, want ErrNoKey: an unsigned ledger can be polluted by anyone", err)
	}
}

func TestRangeSplit(t *testing.T) {
	cases := []struct {
		name                string
		in                  Range
		wantOK              bool
		wantLeft, wantRight Range
	}{
		{"even", Range{1, 100}, true, Range{1, 50}, Range{51, 100}},
		{"odd", Range{1, 101}, true, Range{1, 51}, Range{52, 101}},
		{"pair", Range{7, 8}, true, Range{7, 7}, Range{8, 8}},
		{"single", Range{5, 5}, false, Range{}, Range{}},
		{"inverted", Range{9, 5}, false, Range{}, Range{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			left, right, ok := tc.in.Split()
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if left != tc.wantLeft || right != tc.wantRight {
				t.Errorf("split = %v and %v, want %v and %v", left, right, tc.wantLeft, tc.wantRight)
			}
			if left.Width()+right.Width() != tc.in.Width() {
				t.Error("splitting a range lost or gained sequences")
			}
		})
	}
}

func TestRegistryRejectsDuplicateNames(t *testing.T) {
	a, _ := NewFileSink("same", t.TempDir(), testKey)
	b, _ := NewFileSink("same", t.TempDir(), testKey)
	if _, err := NewRegistry(a, b); err == nil {
		t.Error("the registry accepted two sinks with the same name")
	}
	if _, err := NewRegistry(nil); err == nil {
		t.Error("the registry accepted a nil sink")
	}
}

func TestFileSinkImplementsPartialReporter(t *testing.T) {
	// Partial multi-line delivery is invisible to CountRange, so this optional
	// capability is the only way the reference sink can report it.
	var sink any
	sink, _ = NewFileSink("test", t.TempDir(), testKey)
	if _, ok := sink.(PartialReporter); !ok {
		t.Error("FileSink no longer implements PartialReporter, so partial delivery would go undetected")
	}
}
