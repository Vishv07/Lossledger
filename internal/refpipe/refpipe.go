// Package refpipe is LossLedger's conscience.
//
// It holds two toy collectors that copy lines from a source directory to a sink
// directory. refgood copies faithfully. refbad sabotages the copy in precisely
// described ways: it drops a range, duplicates another, and reorders a third.
//
// The harness is only credible while refbad is caught exactly and refgood stays
// clean. Both run in CI on every commit, and any change that flips either one
// blocks the merge (PRD section 10.2). The sabotages are declared in code, so
// the expected verdict is computable rather than eyeballed, which is what makes
// "byte-exact expected versus actual" a meaningful claim.
package refpipe

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"

	"github.com/Vishv07/lossledger/internal/adapter"
	"github.com/Vishv07/lossledger/internal/wire"
)

// Sabotage describes exactly what a reference collector will do wrong.
//
// Every field is a closed set of sequence numbers, so the expected verdict is
// derived from this struct rather than written down beside it. A test that
// hardcoded its expectations separately could drift from the sabotage; this
// cannot.
type Sabotage struct {
	// Drop lists sequences the collector will not copy. These must come back
	// as MISSING, with exactly these ranges.
	Drop []adapter.Range

	// Duplicate lists sequences the collector will write more than once, at a
	// multiple past the duplication bound so they classify as DUP_BUG rather
	// than as expected replay.
	Duplicate []adapter.Range

	// DuplicateFactor is how many copies of each duplicated sequence to write.
	DuplicateFactor int

	// TruncateContinuations lists multi-line parents whose continuation lines
	// will be partly withheld, which must surface as ORDER.
	TruncateContinuations []adapter.Range
}

// IsEmpty reports whether this is the faithful collector.
func (s Sabotage) IsEmpty() bool {
	return len(s.Drop) == 0 && len(s.Duplicate) == 0 && len(s.TruncateContinuations) == 0
}

// contains reports whether seq falls in any of the ranges.
func contains(ranges []adapter.Range, seq uint64) bool {
	for _, r := range ranges {
		if r.Contains(seq) {
			return true
		}
	}
	return false
}

// expand lists every sequence covered by the ranges.
func expand(ranges []adapter.Range) []uint64 {
	var out []uint64
	for _, r := range ranges {
		for seq := r.From; seq <= r.To; seq++ {
			out = append(out, seq)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Good returns the faithful collector's configuration.
func Good() Sabotage { return Sabotage{} }

// Bad returns a seeded sabotage over an epoch of maxSeq sequences.
//
// The ranges are derived from the seed so a replay reproduces the same
// sabotage, and they are placed well inside the epoch so that neither the first
// nor the last sequence is involved. Off-by-one errors at the boundaries would
// otherwise hide behind the sabotage.
func Bad(maxSeq uint64, seed int64) Sabotage {
	if maxSeq < 100 {
		maxSeq = 100
	}
	rnd := rand.New(rand.NewSource(seed))

	// Three disjoint zones, so the sabotages cannot overlap and confuse the
	// expected verdict.
	zone := maxSeq / 4
	pick := func(base, span uint64, width uint64) adapter.Range {
		if span <= width {
			span = width + 1
		}
		start := base + uint64(rnd.Int63n(int64(span-width)))
		return adapter.Range{From: start, To: start + width - 1}
	}

	return Sabotage{
		Drop: []adapter.Range{
			pick(zone, zone, 37),
			pick(zone*2, zone/2, 11),
		},
		Duplicate:       []adapter.Range{pick(zone*2+zone/2, zone/2, 9)},
		DuplicateFactor: 4, // above the default bound of 3, so DUP_BUG not replay

		// Only multi-line events have continuations to withhold, and the shape
		// mix makes those a minority. The range is wide enough that landing on
		// no stack traces at all is vanishingly unlikely, so the gate does not
		// pass or fail on the luck of a seed.
		TruncateContinuations: []adapter.Range{pick(zone*3, zone-1, 60)},
	}
}

// Expectation is the verdict a correct harness must produce against a sabotage.
type Expectation struct {
	MissingRanges []adapter.Range
	MissingCount  uint64
	DupBugRanges  []adapter.Range
	DupBugCount   uint64
	OrderRanges   []adapter.Range
	OrderCount    uint64
}

// Expect derives the required verdict from the sabotage itself.
func (s Sabotage) Expect() Expectation {
	norm := func(ranges []adapter.Range) ([]adapter.Range, uint64) {
		seqs := expand(ranges)
		if len(seqs) == 0 {
			return nil, 0
		}
		return compress(seqs), uint64(len(seqs))
	}

	e := Expectation{}
	e.MissingRanges, e.MissingCount = norm(s.Drop)
	e.DupBugRanges, e.DupBugCount = norm(s.Duplicate)
	e.OrderRanges, e.OrderCount = norm(s.TruncateContinuations)
	return e
}

// compress mirrors verify.CompressRanges. It is duplicated here rather than
// imported so the expectation is built without the package under test, which
// would make a bug in range compression invisible to the gate.
func compress(seqs []uint64) []adapter.Range {
	if len(seqs) == 0 {
		return nil
	}
	out := []adapter.Range{}
	start, prev := seqs[0], seqs[0]
	for _, seq := range seqs[1:] {
		if seq == prev {
			continue
		}
		if seq == prev+1 {
			prev = seq
			continue
		}
		out = append(out, adapter.Range{From: start, To: prev})
		start, prev = seq, seq
	}
	return append(out, adapter.Range{From: start, To: prev})
}

// Pipe is a toy collector copying one directory into another.
type Pipe struct {
	SrcDir   string
	DstDir   string
	Key      []byte
	Sabotage Sabotage

	// actuallyTruncated records the parents whose continuation lines were
	// really withheld.
	//
	// The sabotage names a range of sequence numbers, but only multi-line
	// events have continuations to withhold, and the shape mix decides which
	// those are. Grading against intent rather than against what happened
	// would fail the gate whenever a range landed on no stack traces, so the
	// pipe reports what it did.
	actuallyTruncated map[uint64]bool
}

// Truncated lists the parents whose multi-line events were really truncated.
func (p *Pipe) Truncated() []uint64 {
	out := make([]uint64, 0, len(p.actuallyTruncated))
	for seq := range p.actuallyTruncated {
		out = append(out, seq)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Run copies every line from the source directory to a single sink file,
// applying the sabotage on the way.
//
// This is deliberately the dumbest possible collector. Its only job is to be a
// pipeline whose exact behaviour we know, so that a disagreement between the
// verifier and reality is unambiguously the verifier's fault.
func (p *Pipe) Run() error {
	if len(p.Key) == 0 {
		return wire.ErrNoKey
	}
	p.actuallyTruncated = map[uint64]bool{}
	if err := os.MkdirAll(p.DstDir, 0o755); err != nil {
		return fmt.Errorf("refpipe: creating %s: %w", p.DstDir, err)
	}

	entries, err := os.ReadDir(p.SrcDir)
	if err != nil {
		return fmt.Errorf("refpipe: reading %s: %w", p.SrcDir, err)
	}

	// Read the source in a stable order so a run is reproducible. Rotated
	// generations sort after the active file, which is the order a tailing
	// collector would have seen them in.
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	out, err := os.Create(filepath.Join(p.DstDir, "collected.log"))
	if err != nil {
		return fmt.Errorf("refpipe: creating sink file: %w", err)
	}
	defer out.Close()

	w := bufio.NewWriterSize(out, 256*1024)
	defer w.Flush()

	// Continuation lines are withheld per parent after the first, so a
	// truncated multi-line event still delivers its parent sequence.
	withheld := map[uint64]int{}

	for _, name := range names {
		if err := p.copyFile(filepath.Join(p.SrcDir, name), w, withheld); err != nil {
			return err
		}
	}
	return w.Flush()
}

func (p *Pipe) copyFile(path string, w *bufio.Writer, withheld map[uint64]int) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("refpipe: opening %s: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	for sc.Scan() {
		line := sc.Text()

		tok, err := wire.Extract(line)
		if err != nil {
			// Lines carrying no token (the interleave neighbour, for one) are
			// copied through untouched.
			if err := writeLine(w, line); err != nil {
				return err
			}
			continue
		}

		switch tok.Kind {
		case wire.KindCanary:
			if contains(p.Sabotage.Drop, tok.Seq) {
				continue // the loss we will be graded on finding
			}
			if contains(p.Sabotage.Duplicate, tok.Seq) {
				factor := p.Sabotage.DuplicateFactor
				if factor < 2 {
					factor = 2
				}
				for i := 0; i < factor; i++ {
					if err := writeLine(w, line); err != nil {
						return err
					}
				}
				continue
			}
			if err := writeLine(w, line); err != nil {
				return err
			}

		case wire.KindContinuation:
			// The parent's own sequence may have been dropped; its
			// continuations go with it.
			if contains(p.Sabotage.Drop, tok.Seq) {
				continue
			}
			if contains(p.Sabotage.TruncateContinuations, tok.Seq) {
				// Keep the first continuation line and withhold the rest, so
				// the event is detectably partial rather than simply absent.
				withheld[tok.Seq]++
				if withheld[tok.Seq] > 1 {
					// Something was actually withheld, so this parent really
					// is partial and the gate may demand we detect it.
					p.actuallyTruncated[tok.Seq] = true
					continue
				}
			}
			if err := writeLine(w, line); err != nil {
				return err
			}

		default:
			if err := writeLine(w, line); err != nil {
				return err
			}
		}
	}
	return sc.Err()
}

func writeLine(w *bufio.Writer, line string) error {
	if _, err := w.WriteString(line); err != nil {
		return err
	}
	return w.WriteByte('\n')
}
