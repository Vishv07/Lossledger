package refpipe

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Vishv07/lossledger/internal/adapter"
	"github.com/Vishv07/lossledger/internal/emit"
	"github.com/Vishv07/lossledger/internal/verify"
)

// Options configures a self-check run.
type Options struct {
	// Seqs is how many sequences each reference epoch emits.
	Seqs int

	// Seed drives the emitter and the sabotage placement.
	Seed int64

	// Key signs the canary tokens.
	Key []byte

	// WorkDir holds the scratch directories. Empty means a temporary
	// directory removed when the run finishes.
	WorkDir string

	// Shapes is the emitter's shape mix.
	Shapes map[string]float64
}

func (o *Options) applyDefaults() {
	if o.Seqs <= 0 {
		o.Seqs = 5000
	}
	if o.Seed == 0 {
		o.Seed = 1
	}
	if len(o.Shapes) == 0 {
		o.Shapes = map[string]float64{"plain": 0.6, "json": 0.2, "stacktrace": 0.2}
	}
}

// Check is one assertion the gate makes.
type Check struct {
	Name   string
	Passed bool
	Want   string
	Got    string
}

func (c Check) String() string {
	status := "PASS"
	if !c.Passed {
		status = "FAIL"
	}
	s := fmt.Sprintf("  [%s] %s", status, c.Name)
	if !c.Passed {
		s += fmt.Sprintf("\n         want: %s\n          got: %s", c.Want, c.Got)
	}
	return s
}

// Result is the outcome of the gate.
type Result struct {
	Checks []Check
}

// Passed reports whether every check held.
func (r Result) Passed() bool {
	for _, c := range r.Checks {
		if !c.Passed {
			return false
		}
	}
	return true
}

// Failures returns only the checks that failed.
func (r Result) Failures() []Check {
	var out []Check
	for _, c := range r.Checks {
		if !c.Passed {
			out = append(out, c)
		}
	}
	return out
}

func (r Result) String() string {
	var b strings.Builder
	for _, c := range r.Checks {
		b.WriteString(c.String())
		b.WriteByte('\n')
	}
	passed := 0
	for _, c := range r.Checks {
		if c.Passed {
			passed++
		}
	}
	fmt.Fprintf(&b, "\n%d of %d checks passed", passed, len(r.Checks))
	return b.String()
}

func (r *Result) add(name string, passed bool, want, got string) {
	r.Checks = append(r.Checks, Check{Name: name, Passed: passed, Want: want, Got: got})
}

// clock is a controllable time source, so the gate exercises settle windows
// without waiting out real minutes.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// Run executes the self-validation gate.
//
// It emits a known epoch, runs it through both reference collectors, and checks
// that the verifier returns clean for the faithful one and the exact injected
// anomalies for the sabotaged one.
func Run(ctx context.Context, opts Options) (Result, error) {
	opts.applyDefaults()
	var result Result

	work := opts.WorkDir
	if work == "" {
		tmp, err := os.MkdirTemp("", "lossledger-selfcheck-")
		if err != nil {
			return result, fmt.Errorf("refpipe: creating work directory: %w", err)
		}
		defer os.RemoveAll(tmp)
		work = tmp
	}

	srcDir := filepath.Join(work, "source")
	if err := os.MkdirAll(srcDir, 0o755); err != nil {
		return result, err
	}

	// Emit a known epoch, closed cleanly so every sequence below the final is
	// legitimately expected.
	epoch, emitted, err := emitEpoch(srcDir, opts, true)
	if err != nil {
		return result, err
	}
	result.add("emitter wrote the requested sequences",
		emitted == uint64(opts.Seqs),
		fmt.Sprintf("%d", opts.Seqs), fmt.Sprintf("%d", emitted))

	// The faithful collector must come back clean.
	goodDir := filepath.Join(work, "refgood")
	good := &Pipe{SrcDir: srcDir, DstDir: goodDir, Key: opts.Key, Sabotage: Good()}
	if err := good.Run(); err != nil {
		return result, fmt.Errorf("refpipe: running refgood: %w", err)
	}
	goodVerdict, err := verifyDir(ctx, goodDir, "refgood", opts.Key, "node-ref", epoch, emitted)
	if err != nil {
		return result, err
	}

	result.add("refgood verdict is CLEAN",
		goodVerdict.Worst == verify.Clean,
		string(verify.Clean), string(goodVerdict.Worst))
	result.add("refgood reports no missing sequences",
		goodVerdict.MissingN == 0,
		"0", fmt.Sprintf("%d", goodVerdict.MissingN))
	result.add("refgood accounts for every emitted sequence",
		goodVerdict.Arrived == emitted,
		fmt.Sprintf("%d arrived", emitted), fmt.Sprintf("%d arrived", goodVerdict.Arrived))
	result.add("refgood integrity metric is zero",
		goodVerdict.FalseMissing == 0,
		"0", fmt.Sprintf("%d", goodVerdict.FalseMissing))
	if err := goodVerdict.Conservation(); err != nil {
		result.add("refgood conservation holds", false, "emitted = arrived + missing + in flight", err.Error())
	} else {
		result.add("refgood conservation holds", true, "", "")
	}

	// The sabotaged collector must be caught exactly.
	sab := Bad(emitted, opts.Seed)
	want := sab.Expect()

	badDir := filepath.Join(work, "refbad")
	bad := &Pipe{SrcDir: srcDir, DstDir: badDir, Key: opts.Key, Sabotage: sab}
	if err := bad.Run(); err != nil {
		return result, fmt.Errorf("refpipe: running refbad: %w", err)
	}
	badVerdict, err := verifyDir(ctx, badDir, "refbad", opts.Key, "node-ref", epoch, emitted)
	if err != nil {
		return result, err
	}

	result.add("refbad verdict is MISSING",
		badVerdict.Worst == verify.Missing,
		string(verify.Missing), string(badVerdict.Worst))

	gotMissing := findingRanges(badVerdict, verify.Missing)
	result.add("refbad missing ranges match the injected drops exactly",
		rangesEqual(gotMissing, want.MissingRanges),
		verify.FormatRanges(want.MissingRanges), verify.FormatRanges(gotMissing))
	result.add("refbad missing count matches",
		badVerdict.MissingN == want.MissingCount,
		fmt.Sprintf("%d", want.MissingCount), fmt.Sprintf("%d", badVerdict.MissingN))

	gotDup := findingRanges(badVerdict, verify.DupBug)
	result.add("refbad duplicate-bug ranges match the injected duplicates exactly",
		rangesEqual(gotDup, want.DupBugRanges),
		verify.FormatRanges(want.DupBugRanges), verify.FormatRanges(gotDup))

	gotOrder := findingRanges(badVerdict, verify.Order)
	truncated := compress(bad.Truncated())
	result.add("refbad order anomalies cover the truncated multi-line events",
		rangesCover(gotOrder, truncated),
		"covers "+verify.FormatRanges(truncated), verify.FormatRanges(gotOrder))
	result.add("refbad actually truncated at least one multi-line event",
		len(truncated) > 0,
		"at least one", fmt.Sprintf("%d", len(truncated)))

	result.add("refbad integrity metric is zero",
		badVerdict.FalseMissing == 0,
		"0", fmt.Sprintf("%d", badVerdict.FalseMissing))

	// A crashed emitter leaves a truncated epoch. Nothing past the last
	// sequence it managed to write may be called missing.
	truncDir := filepath.Join(work, "truncated-source")
	if err := os.MkdirAll(truncDir, 0o755); err != nil {
		return result, err
	}
	truncEpoch, truncEmitted, err := emitEpoch(truncDir, Options{
		Seqs: opts.Seqs / 2, Seed: opts.Seed + 1, Key: opts.Key, Shapes: opts.Shapes,
	}, false)
	if err != nil {
		return result, err
	}
	truncSink := filepath.Join(work, "truncated-sink")
	if err := (&Pipe{SrcDir: truncDir, DstDir: truncSink, Key: opts.Key}).Run(); err != nil {
		return result, err
	}
	truncVerdict, err := verifyDir(ctx, truncSink, "trunc", opts.Key, "node-ref", truncEpoch, truncEmitted)
	if err != nil {
		return result, err
	}
	result.add("a crashed emitter produces no phantom missing sequences",
		truncVerdict.MissingN == 0 && truncVerdict.Worst == verify.Clean,
		"CLEAN with 0 missing",
		fmt.Sprintf("%s with %d missing", truncVerdict.Worst, truncVerdict.MissingN))

	return result, nil
}

// emitEpoch writes one epoch into dir and returns its identifier and length.
func emitEpoch(dir string, opts Options, clean bool) (string, uint64, error) {
	opts.applyDefaults()

	root, err := emit.NewRoot(dir)
	if err != nil {
		return "", 0, err
	}
	// Rotate often, so the reference run exercises the rotation path rather
	// than writing one tidy file nothing realistic would produce.
	writer, err := emit.NewFileWriter(emit.FileConfig{
		Root: root, Name: "canary.log", MaxLines: 500, Keep: 200,
	})
	if err != nil {
		return "", 0, err
	}

	set, err := emit.NewSet(emit.Builtins())
	if err != nil {
		return "", 0, err
	}
	em, err := emit.New(emit.Config{
		Src:           "node-ref",
		Key:           opts.Key,
		Seed:          opts.Seed,
		Deterministic: true,
		Rate:          100,
		AckRate:       true,
		Shapes:        opts.Shapes,
	}, writer, set)
	if err != nil {
		return "", 0, err
	}

	if err := em.EmitN(opts.Seqs); err != nil {
		return "", 0, err
	}
	if clean {
		err = em.Close()
	} else {
		err = em.Abort()
	}
	if err != nil {
		return "", 0, err
	}
	return em.Epoch(), em.Emitted(), nil
}

// verifyDir runs a full verification against a directory sink, driving a
// synthetic clock far enough forward that settle windows and the mandatory
// re-check both elapse.
func verifyDir(ctx context.Context, dir, sinkName string, key []byte, src, epoch string, emitted uint64) (verify.Verdict, error) {
	sink, err := adapter.NewFileSink(sinkName, dir, key)
	if err != nil {
		return verify.Verdict{}, err
	}
	registry, err := adapter.NewRegistry(sink)
	if err != nil {
		return verify.Verdict{}, err
	}

	clk := &clock{now: time.Unix(1_700_000_000, 0).UTC()}
	v, err := verify.NewVerifier(verify.VerifierConfig{
		Interval: time.Second,
		MaxWait:  time.Minute,
		Now:      clk.Now,
	}, registry)
	if err != nil {
		return verify.Verdict{}, err
	}

	emittedAt := clk.Now()
	for seq := uint64(1); seq <= emitted; seq++ {
		v.NoteEmitted(src, epoch, seq, emittedAt)
	}

	// The epoch state the sink can see tells us whether the emitter shut down
	// cleanly, which decides what may be called missing.
	state := sink.EpochState(src, epoch)
	if state.HasFinal {
		v.CloseEpoch(src, epoch, state.FinalSeq, true)
	} else {
		v.CloseEpoch(src, epoch, state.MaxSeq, false)
	}

	// Three sweeps with the clock advancing between them: the first reconciles
	// and marks absences provisional, the second runs the mandatory re-check,
	// the third settles the verdict.
	for i := 0; i < 3; i++ {
		if err := v.Sweep(ctx); err != nil {
			return verify.Verdict{}, err
		}
		clk.advance(2 * time.Minute)
	}
	v.NoteInvalidLine(sink.InvalidLines())
	return v.Verdict(), nil
}

// findingRanges collects the ranges reported for one anomaly class.
func findingRanges(v verify.Verdict, a verify.Anomaly) []adapter.Range {
	var out []adapter.Range
	for _, f := range v.Findings {
		if f.Anomaly == a {
			out = append(out, f.Ranges...)
		}
	}
	return out
}

// rangesEqual compares two range lists exactly.
func rangesEqual(got, want []adapter.Range) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// rangesCover reports whether every sequence in want appears somewhere in got.
//
// Order anomalies are checked by coverage rather than equality: a partially
// delivered multi-line event is one way a sequence becomes an ORDER finding,
// and arrival-order drift is another, so the harness may legitimately report
// more than the sabotage injected. It may never report less.
func rangesCover(got, want []adapter.Range) bool {
	if len(want) == 0 {
		return true
	}
	covered := map[uint64]bool{}
	for _, r := range got {
		for seq := r.From; seq <= r.To; seq++ {
			covered[seq] = true
		}
	}
	for _, r := range want {
		for seq := r.From; seq <= r.To; seq++ {
			if !covered[seq] {
				return false
			}
		}
	}
	return true
}
