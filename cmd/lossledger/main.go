// Command lossledger audits log pipelines end to end.
//
// It writes sequence-numbered, signed canary lines through a real pipeline and
// then asks each store which sequence numbers it actually holds. The difference
// is the report.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Vishv07/lossledger/internal/verify"
)

// version is overridden at build time with the release tag.
var version = "dev"

const usage = `lossledger audits whether a log pipeline delivers what was written.

usage: lossledger <command> [flags]

commands:
  doctor      check the environment and every configured sink
  emit        write canary lines through a pipeline
  verify      reconcile what was written against what the sinks hold
  run         emit and verify together, for a single node or a demo
  selfcheck   run the reference pipelines and grade this build against them
  version     print the version

Run "lossledger <command> -h" for the flags of one command.

exit codes mirror anomaly severity, so a shell sees the same ordering a report does:
  0   clean
  10  late
  20  order
  30  duplicate bug
  40  missing
  2   harness error, which is deliberately below every anomaly so a crash
      can never be mistaken for a finding
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(verify.ExitHarnessError)
	}

	// A signal cancels the context rather than killing the process, so an
	// emitter gets the chance to write its final line. Without that line the
	// verifier would read the shutdown as a crash and truncate the epoch.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	args := os.Args[2:]
	var err error
	code := 0

	switch os.Args[1] {
	case "doctor":
		code, err = runDoctor(ctx, args)
	case "emit":
		err = runEmit(ctx, args)
	case "verify":
		code, err = runVerify(ctx, args)
	case "run":
		code, err = runRun(ctx, args)
	case "selfcheck":
		code, err = runSelfcheck(ctx, args)
	case "version":
		fmt.Printf("lossledger %s\n", version)
	case "-h", "--help", "help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "lossledger: unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(verify.ExitHarnessError)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "lossledger: %v\n", err)
		os.Exit(verify.ExitHarnessError)
	}
	os.Exit(code)
}
