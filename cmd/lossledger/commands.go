package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/Vishv07/lossledger/internal/adapter"
	"github.com/Vishv07/lossledger/internal/emit"
	"github.com/Vishv07/lossledger/internal/refpipe"
	"github.com/Vishv07/lossledger/internal/report"
	"github.com/Vishv07/lossledger/internal/verify"
	"github.com/Vishv07/lossledger/internal/wire"
)

// shapeMix parses a "plain=0.5,stacktrace=0.5" flag value.
func shapeMix(s string) (map[string]float64, error) {
	out := map[string]float64{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, weight, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("shape mix entry %q is not name=weight", part)
		}
		var w float64
		if _, err := fmt.Sscanf(strings.TrimSpace(weight), "%f", &w); err != nil {
			return nil, fmt.Errorf("shape mix entry %q has an unreadable weight: %w", part, err)
		}
		out[strings.TrimSpace(name)] = w
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("shape mix is empty")
	}
	return out, nil
}

// requireKey loads the signing key, with an error that says how to set it.
func requireKey() ([]byte, error) {
	key, err := wire.KeyFromEnv()
	if err != nil {
		return nil, fmt.Errorf("no signing key: set %s to a value shared by the emitter and the verifier.\n"+
			"  Without it the ledger cannot tell our canary lines from any other line that looks like one", wire.KeyEnvVar)
	}
	return key, nil
}

// runDoctor checks the environment and every configured sink.
func runDoctor(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	sinkDir := fs.String("filesink", "", "directory to check as a reference sink")
	if err := fs.Parse(args); err != nil {
		return verify.ExitHarnessError, err
	}

	var problems []string
	ok := func(label, detail string) { fmt.Printf("  ok    %-22s %s\n", label, detail) }
	bad := func(label, detail string) {
		fmt.Printf("  FAIL  %-22s %s\n", label, detail)
		problems = append(problems, label)
	}

	fmt.Println("lossledger doctor")
	fmt.Println()

	ok("go runtime", runtime.Version())
	if runtime.GOOS != "linux" {
		// Not fatal: the Go code is portable and the testbed runs Linux
		// containers. But fault injection and journald assume Linux, so a
		// result from elsewhere carries a caveat and should say so.
		fmt.Printf("  warn  %-22s running on %s; v1 targets Linux, so journald and disk-pressure injection are unavailable\n",
			"platform", runtime.GOOS)
	} else {
		ok("platform", runtime.GOOS)
	}

	if _, err := wire.KeyFromEnv(); err != nil {
		bad("signing key", fmt.Sprintf("%s is not set", wire.KeyEnvVar))
	} else {
		ok("signing key", wire.KeyEnvVar+" is set")
	}

	if *sinkDir != "" {
		key, err := requireKey()
		if err != nil {
			bad("filesink", err.Error())
		} else if sink, err := adapter.NewFileSink("filesink", *sinkDir, key); err != nil {
			bad("filesink", err.Error())
		} else if err := sink.Doctor(ctx); err != nil {
			bad("filesink", err.Error())
		} else {
			ok("filesink", *sinkDir)
		}
	}

	fmt.Println()
	if len(problems) > 0 {
		fmt.Printf("%d check(s) failed: %s\n", len(problems), strings.Join(problems, ", "))
		return verify.ExitHarnessError, nil
	}
	fmt.Println("all checks passed")
	return 0, nil
}

// runEmit writes canary lines until interrupted.
func runEmit(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("emit", flag.ExitOnError)
	dir := fs.String("dir", "", "directory to write canary files into (required)")
	name := fs.String("name", "canary.log", "active file name")
	src := fs.String("src", "", "source identifier, unique per emitting unit (defaults to the hostname)")
	rate := fs.Float64("rate", 10, "lines per second")
	shapes := fs.String("shapes", "plain=0.6,json=0.2,stacktrace=0.2", "shape mix as name=weight pairs")
	maxLines := fs.Int64("rotate-lines", 10000, "rotate the active file after this many lines, 0 to disable")
	keep := fs.Int("keep", 5, "rotated generations to retain")
	count := fs.Int("count", 0, "emit exactly this many events then stop, 0 to run until interrupted")
	seed := fs.Int64("seed", 0, "seed for shape selection")
	ack := fs.Bool("i-know", false, "acknowledge a rate above the safe cap")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return fmt.Errorf("emit needs -dir")
	}

	key, err := requireKey()
	if err != nil {
		return err
	}
	if *src == "" {
		host, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("no -src given and the hostname is unreadable: %w", err)
		}
		*src = host
	}

	mix, err := shapeMix(*shapes)
	if err != nil {
		return err
	}
	root, err := emit.NewRoot(*dir)
	if err != nil {
		return err
	}
	writer, err := emit.NewFileWriter(emit.FileConfig{
		Root: root, Name: *name, MaxLines: *maxLines, Keep: *keep,
	})
	if err != nil {
		return err
	}
	set, err := emit.NewSet(emit.Builtins())
	if err != nil {
		return err
	}
	em, err := emit.New(emit.Config{
		Src: *src, Rate: *rate, Shapes: mix, Seed: *seed, Key: key, AckRate: *ack,
	}, writer, set)
	if err != nil {
		return err
	}

	fmt.Printf("emitting as src=%s epoch=%s at %.1f lines/s into %s\n", em.Src(), em.Epoch(), *rate, root)

	if *count > 0 {
		if err := em.EmitN(*count); err != nil {
			return err
		}
	} else if err := em.Run(ctx); err != nil {
		return err
	}

	// Close writes the final line, which is what tells the verifier this was a
	// clean shutdown rather than a crash.
	if err := em.Close(); err != nil {
		return err
	}
	stats := em.Stats()
	fmt.Printf("wrote %d events, epoch closed at seq %d\n", stats.Emitted, stats.FinalSeq)
	return nil
}

// verifyFlags are shared by verify and run.
type verifyFlags struct {
	dir      string
	src      string
	epoch    string
	maxWait  time.Duration
	interval time.Duration
	asJSON   bool
}

func (f *verifyFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.dir, "filesink", "", "directory to query as a reference sink (required)")
	fs.StringVar(&f.src, "src", "", "source identifier to verify, empty for every source found")
	fs.StringVar(&f.epoch, "epoch", "", "epoch to verify, empty for every epoch found")
	fs.DurationVar(&f.maxWait, "max-wait", verify.DefaultMaxWait, "ceiling on the settle window")
	fs.DurationVar(&f.interval, "interval", time.Minute, "sweep interval")
	fs.BoolVar(&f.asJSON, "json", false, "emit the report as JSON")
}

// runVerify reconciles a directory sink against the sequences it should hold.
func runVerify(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	var vf verifyFlags
	vf.register(fs)
	if err := fs.Parse(args); err != nil {
		return verify.ExitHarnessError, err
	}
	if vf.dir == "" {
		return verify.ExitHarnessError, fmt.Errorf("verify needs -filesink")
	}

	key, err := requireKey()
	if err != nil {
		return verify.ExitHarnessError, err
	}
	sink, err := adapter.NewFileSink("filesink", vf.dir, key)
	if err != nil {
		return verify.ExitHarnessError, err
	}
	if err := sink.Doctor(ctx); err != nil {
		return verify.ExitHarnessError, err
	}
	registry, err := adapter.NewRegistry(sink)
	if err != nil {
		return verify.ExitHarnessError, err
	}

	v, err := verify.NewVerifier(verify.VerifierConfig{
		Interval: vf.interval,
		MaxWait:  vf.maxWait,
	}, registry)
	if err != nil {
		return verify.ExitHarnessError, err
	}

	// The sink tells us which streams it holds and where each epoch ended.
	streams := sink.Streams()
	if len(streams) == 0 {
		return verify.ExitHarnessError, fmt.Errorf("no canary streams found in %s", vf.dir)
	}

	now := time.Now()
	for _, s := range streams {
		src, epoch := s[0], s[1]
		if vf.src != "" && src != vf.src {
			continue
		}
		if vf.epoch != "" && epoch != vf.epoch {
			continue
		}
		state := sink.EpochState(src, epoch)
		upTo := state.MaxSeq
		if state.HasFinal && state.FinalSeq > upTo {
			upTo = state.FinalSeq
		}
		// Emission times are not recoverable from the sink alone, so the
		// settle clock starts from a point already past the window. This is
		// the standalone-verify path; the run command knows the real times.
		emittedAt := now.Add(-2 * vf.maxWait)
		for seq := uint64(1); seq <= upTo; seq++ {
			v.NoteEmitted(src, epoch, seq, emittedAt)
		}
		v.CloseEpoch(src, epoch, upTo, state.HasFinal)
	}

	for i := 0; i < 2; i++ {
		if err := v.Sweep(ctx); err != nil {
			return verify.ExitHarnessError, err
		}
	}
	v.NoteInvalidLine(sink.InvalidLines())

	verdict := v.Verdict()
	return emitReport(verdict, registry.Names(), vf.asJSON)
}

// runRun emits and verifies together against a directory sink.
func runRun(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	srcDir := fs.String("dir", "", "directory the emitter writes into (required)")
	sinkDir := fs.String("filesink", "", "directory to query as a sink, defaults to -dir")
	count := fs.Int("count", 1000, "events to emit")
	shapes := fs.String("shapes", "plain=0.6,json=0.2,stacktrace=0.2", "shape mix as name=weight pairs")
	asJSON := fs.Bool("json", false, "emit the report as JSON")
	if err := fs.Parse(args); err != nil {
		return verify.ExitHarnessError, err
	}
	if *srcDir == "" {
		return verify.ExitHarnessError, fmt.Errorf("run needs -dir")
	}
	if *sinkDir == "" {
		*sinkDir = *srcDir
	}

	key, err := requireKey()
	if err != nil {
		return verify.ExitHarnessError, err
	}
	mix, err := shapeMix(*shapes)
	if err != nil {
		return verify.ExitHarnessError, err
	}

	root, err := emit.NewRoot(*srcDir)
	if err != nil {
		return verify.ExitHarnessError, err
	}
	writer, err := emit.NewFileWriter(emit.FileConfig{Root: root, Name: "canary.log", MaxLines: 2000, Keep: 50})
	if err != nil {
		return verify.ExitHarnessError, err
	}
	set, err := emit.NewSet(emit.Builtins())
	if err != nil {
		return verify.ExitHarnessError, err
	}
	em, err := emit.New(emit.Config{Src: "local", Key: key, Shapes: mix, AckRate: true, Rate: 1000}, writer, set)
	if err != nil {
		return verify.ExitHarnessError, err
	}

	emittedAt := time.Now()
	if err := em.EmitN(*count); err != nil {
		return verify.ExitHarnessError, err
	}
	if err := em.Close(); err != nil {
		return verify.ExitHarnessError, err
	}

	sink, err := adapter.NewFileSink("filesink", *sinkDir, key)
	if err != nil {
		return verify.ExitHarnessError, err
	}
	registry, err := adapter.NewRegistry(sink)
	if err != nil {
		return verify.ExitHarnessError, err
	}

	// A synthetic clock lets the settle window and the mandatory re-check
	// elapse without the command sitting idle for ten minutes.
	elapsed := emittedAt.Add(-2 * verify.DefaultMaxWait)
	v, err := verify.NewVerifier(verify.VerifierConfig{MaxWait: verify.DefaultMaxWait}, registry)
	if err != nil {
		return verify.ExitHarnessError, err
	}
	for seq := uint64(1); seq <= em.Emitted(); seq++ {
		v.NoteEmitted(em.Src(), em.Epoch(), seq, elapsed)
	}
	v.CloseEpoch(em.Src(), em.Epoch(), em.Stats().FinalSeq, true)

	for i := 0; i < 2; i++ {
		if err := v.Sweep(ctx); err != nil {
			return verify.ExitHarnessError, err
		}
	}
	v.NoteInvalidLine(sink.InvalidLines())

	return emitReport(v.Verdict(), registry.Names(), *asJSON)
}

// runSelfcheck runs the reference pipelines and grades this build.
func runSelfcheck(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("selfcheck", flag.ExitOnError)
	seqs := fs.Int("seqs", 5000, "sequences per reference epoch")
	seed := fs.Int64("seed", 1, "seed for the emitter and the sabotage")
	keep := fs.String("keep", "", "keep the scratch directories here for inspection")
	if err := fs.Parse(args); err != nil {
		return verify.ExitHarnessError, err
	}

	// The gate needs a key but not a shared one: nothing leaves this process.
	key, err := wire.KeyFromEnv()
	if err != nil {
		key = []byte("lossledger-selfcheck-local-key")
	}

	fmt.Println("lossledger selfcheck")
	fmt.Println("running the reference pipelines against this build")
	fmt.Println()

	res, err := refpipe.Run(ctx, refpipe.Options{
		Seqs: *seqs, Seed: *seed, Key: key, WorkDir: *keep,
	})
	if err != nil {
		return verify.ExitHarnessError, err
	}

	fmt.Println(res)
	if !res.Passed() {
		fmt.Println("\nThe self-validation gate failed. This build cannot be trusted to judge a real")
		fmt.Println("pipeline: it does not correctly judge one whose behaviour we already know.")
		return verify.ExitHarnessError, nil
	}
	fmt.Println("\nGate passed. refgood is clean and refbad is caught exactly.")
	return 0, nil
}

// emitReport renders a verdict and returns its exit code.
func emitReport(v verify.Verdict, sinks []string, asJSON bool) (int, error) {
	r := report.GapReport{
		Verdict:     v,
		Sinks:       sinks,
		Limitations: report.DefaultLimitations(sinks),
	}
	var err error
	if asJSON {
		err = r.WriteJSON(os.Stdout)
	} else {
		err = r.WriteText(os.Stdout)
	}
	if err != nil {
		return verify.ExitHarnessError, err
	}

	// A verifier that accused the pipeline wrongly has failed at its one job,
	// so that outranks whatever it thinks it found.
	if v.FalseMissing > 0 {
		return verify.ExitHarnessError, nil
	}
	return v.ExitCode(), nil
}
