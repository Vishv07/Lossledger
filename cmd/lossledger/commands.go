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

// sinkFlags are the store endpoints a command can be pointed at.
//
// A command that can only reach the reference adapter cannot audit anything
// real, so these are shared rather than redeclared per command.
type sinkFlags struct {
	fileDir  string
	chAddr   string
	chDB     string
	chTable  string
	chUser   string
	chPass   string
	chSettle time.Duration
}

func (f *sinkFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.fileDir, "filesink", "", "directory to query as a reference sink")
	fs.StringVar(&f.chAddr, "clickhouse", "", "ClickHouse HTTP endpoint, for example http://localhost:8123")
	fs.StringVar(&f.chDB, "clickhouse-db", "", "ClickHouse database")
	fs.StringVar(&f.chTable, "clickhouse-table", "lossledger_canary", "ClickHouse canary table")
	fs.StringVar(&f.chUser, "clickhouse-user", "", "ClickHouse user")
	fs.StringVar(&f.chPass, "clickhouse-password", "", "ClickHouse password")
	fs.DurationVar(&f.chSettle, "clickhouse-settle", 5*time.Second, "ClickHouse consistency floor")
}

// build assembles the registry. It returns the file sink separately because
// the standalone verify path needs it to discover which streams exist.
func (f *sinkFlags) build(key []byte) (*adapter.Registry, *adapter.FileSink, error) {
	var sinks []adapter.Sink
	var fileSink *adapter.FileSink

	if f.fileDir != "" {
		fs, err := adapter.NewFileSink("filesink", f.fileDir, key)
		if err != nil {
			return nil, nil, err
		}
		fileSink = fs
		sinks = append(sinks, fs)
	}
	if f.chAddr != "" {
		ch, err := adapter.NewClickHouse(adapter.ClickHouseConfig{
			Addr: f.chAddr, Database: f.chDB, Table: f.chTable,
			User: f.chUser, Password: f.chPass, Settle: f.chSettle,
		})
		if err != nil {
			return nil, nil, err
		}
		sinks = append(sinks, ch)
	}
	if len(sinks) == 0 {
		return nil, nil, fmt.Errorf("no sink given: pass -filesink, -clickhouse, or both")
	}

	reg, err := adapter.NewRegistry(sinks...)
	return reg, fileSink, err
}

// runDoctor checks the environment and every configured sink.
func runDoctor(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	var sf sinkFlags
	sf.register(fs)
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

	if sf.fileDir != "" || sf.chAddr != "" {
		key, err := requireKey()
		if err != nil {
			bad("sinks", err.Error())
		} else if reg, _, err := sf.build(key); err != nil {
			bad("sinks", err.Error())
		} else {
			for _, sink := range reg.All() {
				if err := sink.Doctor(ctx); err != nil {
					bad("sink "+sink.Name(), err.Error())
				} else {
					ok("sink "+sink.Name(), fmt.Sprintf("settle floor %s", sink.SettleHint()))
				}
			}
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

func runVerify(ctx context.Context, args []string) (int, error) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	var sf sinkFlags
	sf.register(fs)
	src := fs.String("src", "", "source identifier to verify")
	epoch := fs.String("epoch", "", "epoch to verify")
	maxWait := fs.Duration("max-wait", verify.DefaultMaxWait, "ceiling on the settle window")
	statePath := fs.String("state", "", "file to persist verifier state in, so a restart does not fabricate gaps")
	asJSON := fs.Bool("json", false, "emit the report as JSON")
	if err := fs.Parse(args); err != nil {
		return verify.ExitHarnessError, err
	}

	key, err := requireKey()
	if err != nil {
		return verify.ExitHarnessError, err
	}
	registry, fileSink, err := sf.build(key)
	if err != nil {
		return verify.ExitHarnessError, err
	}
	defer registry.Close()

	if err := registry.Doctor(ctx); err != nil {
		return verify.ExitHarnessError, err
	}

	v, err := verify.NewVerifier(verify.VerifierConfig{MaxWait: *maxWait}, registry)
	if err != nil {
		return verify.ExitHarnessError, err
	}

	// Resume from the last run if a state file was given. Without this a
	// restart re-derives from nothing and anything past its settle window
	// looks absent, which is loss the harness caused itself.
	var store verify.Store
	if *statePath != "" {
		store, err = verify.NewFileStore(*statePath)
		if err != nil {
			return verify.ExitHarnessError, err
		}
		if err := v.LoadState(store); err != nil && err != verify.ErrNoState {
			fmt.Fprintf(os.Stderr, "lossledger: could not resume state, re-deriving: %v\n", err)
		}
	}

	// Work out which streams to verify. The reference adapter can enumerate
	// them; a real store needs to be told, because asking it to list every
	// source it has ever seen is not a cheap query.
	type stream struct{ src, epoch string }
	var streams []stream

	switch {
	case *src != "" && *epoch != "":
		streams = append(streams, stream{*src, *epoch})
	case fileSink != nil:
		for _, s := range fileSink.Streams() {
			streams = append(streams, stream{s[0], s[1]})
		}
	default:
		return verify.ExitHarnessError, fmt.Errorf(
			"give -src and -epoch, or point -filesink at the emitter's directory so the streams can be discovered")
	}
	if len(streams) == 0 {
		return verify.ExitHarnessError, fmt.Errorf("no canary streams found")
	}

	// The settle window and the mandatory re-check are measured against a
	// clock. Sweeping twice in the same millisecond means the re-check delay
	// never elapses, nothing is ever confirmed absent, and the command can
	// never report MISSING however much is gone. So the clock is driven
	// forward explicitly between sweeps.
	//
	// This is sound here because the emission times are already synthetic:
	// a store cannot tell us when a line was written, so verify treats every
	// sequence as emitted long enough ago to judge.
	clock := time.Now().Add(-4 * *maxWait)
	v.SetClock(func() time.Time { return clock })

	now := clock
	for _, st := range streams {
		upTo, hasFinal := epochExtent(ctx, registry, fileSink, st.src, st.epoch)
		if upTo == 0 {
			continue
		}
		// Emission times are not recoverable from a store, so the settle clock
		// starts from a point already past the window. The run command, which
		// did the emitting, knows the real times.
		emittedAt := now.Add(-2 * *maxWait)
		for seq := uint64(1); seq <= upTo; seq++ {
			v.NoteEmitted(st.src, st.epoch, seq, emittedAt)
		}
		v.CloseEpoch(st.src, st.epoch, upTo, hasFinal)
	}

	// Three passes: reconcile and mark absences provisional, run the
	// mandatory re-check, then settle the verdict.
	for i := 0; i < 3; i++ {
		if err := v.Sweep(ctx); err != nil {
			return verify.ExitHarnessError, err
		}
		clock = clock.Add(2 * *maxWait)
	}
	if fileSink != nil {
		v.NoteInvalidLine(fileSink.InvalidLines())
	}

	verdict := v.Verdict()

	if store != nil {
		if err := v.SaveState(store); err != nil {
			fmt.Fprintf(os.Stderr, "lossledger: could not save state: %v\n", err)
		}
	}
	return emitReport(verdict, registry.Names(), *asJSON)
}

// epochExtent asks the sinks how far an epoch ran and whether it closed
// cleanly. A clean close means every sequence below the final is legitimately
// expected; a crash means nothing past the highest observed may be called
// missing.
func epochExtent(ctx context.Context, reg *adapter.Registry, fileSink *adapter.FileSink, src, epoch string) (uint64, bool) {
	var upTo uint64
	var hasFinal bool

	if fileSink != nil {
		st := fileSink.EpochState(src, epoch)
		hasFinal = st.HasFinal
		upTo = st.MaxSeq
		if st.HasFinal && st.FinalSeq > upTo {
			upTo = st.FinalSeq
		}
	}
	for _, sink := range reg.All() {
		ch, ok := sink.(*adapter.ClickHouse)
		if !ok {
			continue
		}
		st, err := ch.EpochState(ctx, src, epoch)
		if err != nil {
			continue
		}
		if st.HasFinal {
			hasFinal = true
		}
		if st.MaxSeq > upTo {
			upTo = st.MaxSeq
		}
		if st.HasFinal && st.FinalSeq > upTo {
			upTo = st.FinalSeq
		}
	}
	return upTo, hasFinal
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
	// Books that do not balance cannot support any verdict, clean or
	// otherwise. Reporting CLEAN here would be the worst outcome available:
	// the ledger has lost track of sequences and the exit code would tell a
	// pipeline it passed.
	if err := v.Conservation(); err != nil {
		fmt.Fprintf(os.Stderr, "lossledger: the verdict is not trustworthy: %v\n", err)
		return verify.ExitHarnessError, nil
	}
	return v.ExitCode(), nil
}
