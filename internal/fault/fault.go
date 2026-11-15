// Package fault holds the injectors the gauntlet uses to provoke the loss
// classes that per-component counters cannot see.
//
// Every injector obeys two rules. It anchors on an emission count rather than
// on wall clock wherever it can, so a replayed run fires at the same point in
// the stream. And it touches only things the harness owns: the emitter's own
// files, and compose projects the gauntlet created. Nothing here reaches into
// the pipeline under test (PRD section 8.4).
package fault

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Kind names an injector.
type Kind string

const (
	// RotateStorm rotates the emitter's own log file at pathological speed,
	// optionally leaving files alive for less time than a collector's
	// file-discovery interval. This is the class behind the Vector cooldown
	// default and the short-lived-pod problem.
	RotateStorm Kind = "rotatestorm"

	// SinkOutage pauses or disconnects a store for a fixed period, which
	// exercises buffer and overflow behaviour in the collector.
	SinkOutage Kind = "sinkoutage"

	// Sigkill kills the collector at a scenario-defined emission count, which
	// produces checkpoint replay duplicates on restart.
	Sigkill Kind = "sigkill"

	// DiskPressure fills the volume the emitter writes into.
	DiskPressure Kind = "diskpressure"
)

// Errors an injector can return.
var (
	ErrNotArmed   = fmt.Errorf("fault: injector is not armed")
	ErrAlreadyRun = fmt.Errorf("fault: injector has already fired")
)

// Target is what an injector acts on. The emitter supplies the rotation hook;
// the testbed supplies the container controls. An injector that needs a
// control its target does not provide fails at arm time rather than silently
// doing nothing mid-run.
type Target interface {
	// Rotate forces the emitter's active file to rotate.
	Rotate() error
}

// Config describes one injection.
type Config struct {
	Kind Kind

	// AtCount is the emission count at which the injector fires. Anchoring on
	// the stream rather than on the clock is what makes a replay land in the
	// same place.
	AtCount uint64

	// Jitter spreads the firing point by up to this many emissions, drawn from
	// the run's seed so it stays reproducible.
	Jitter uint64

	// Seed drives the jitter.
	Seed int64

	// Params carries injector-specific settings.
	Params Params
}

// Params are the per-injector knobs. Only the fields the chosen Kind reads
// are consulted.
type Params struct {
	// Rotations is how many times RotateStorm rotates.
	Rotations int

	// MinFileLifetime is how long each rotated file is allowed to live. Set it
	// below a collector's discovery interval to reproduce the class where a
	// file is gone before anything tailed it.
	MinFileLifetime time.Duration

	// Duration is how long SinkOutage holds a store down.
	Duration time.Duration
}

// Result records what an injector actually did, so a report can state the
// provocation alongside the loss rather than asserting a cause.
type Result struct {
	Kind Kind

	// FiredAt is the emission count the injector actually fired at, jitter
	// included. Reports quote this, not the configured AtCount.
	FiredAt uint64

	StartedAt  time.Time
	FinishedAt time.Time

	// Rotations is how many rotations RotateStorm performed.
	Rotations int

	// Detail is a human-readable summary for the report.
	Detail string
}

func (r Result) String() string {
	return fmt.Sprintf("%s fired at emission %d, ran %s: %s",
		r.Kind, r.FiredAt, r.FinishedAt.Sub(r.StartedAt).Round(time.Millisecond), r.Detail)
}

// Injector is the interface the scenario runner drives.
type Injector interface {
	// Kind names the injector.
	Kind() Kind

	// FiresAt returns the emission count this injector will fire at, jitter
	// already resolved. The runner polls the emitter's count against it.
	FiresAt() uint64

	// Fire performs the injection. It is called once.
	Fire(ctx context.Context, target Target) (Result, error)

	// Fired reports whether Fire has already run.
	Fired() bool
}

// New builds an injector from a config.
func New(cfg Config) (Injector, error) {
	firesAt := cfg.AtCount
	if cfg.Jitter > 0 {
		// Seeded, so two runs of the same scenario fire at the same emission.
		rnd := rand.New(rand.NewSource(cfg.Seed))
		firesAt += uint64(rnd.Int63n(int64(cfg.Jitter) + 1))
	}

	switch cfg.Kind {
	case RotateStorm:
		if cfg.Params.Rotations <= 0 {
			return nil, fmt.Errorf("fault: rotatestorm needs a positive rotation count")
		}
		// Built in place rather than copied from a local: base carries a mutex,
		// and copying it would duplicate the lock rather than share it.
		r := &rotateStorm{}
		r.kind, r.firesAt, r.params = cfg.Kind, firesAt, cfg.Params
		return r, nil
	case SinkOutage, Sigkill, DiskPressure:
		// These need container controls the testbed supplies in M3. Failing
		// here is deliberate: an injector that silently does nothing would
		// produce a clean run that looks like a passing result.
		return nil, fmt.Errorf("fault: injector %q is not implemented yet; "+
			"it needs the compose testbed controls", cfg.Kind)
	default:
		return nil, fmt.Errorf("fault: unknown injector %q", cfg.Kind)
	}
}

// base holds what every injector shares.
type base struct {
	kind    Kind
	firesAt uint64
	params  Params

	mu    sync.Mutex
	fired bool
}

func (b *base) Kind() Kind      { return b.kind }
func (b *base) FiresAt() uint64 { return b.firesAt }

func (b *base) Fired() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.fired
}

// claim marks the injector as fired, returning false if it already had been.
func (b *base) claim() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fired {
		return false
	}
	b.fired = true
	return true
}

// rotateStorm rotates the emitter's file as fast as the scenario asks.
//
// The point is not the rotation itself but the window it opens. A collector
// that is mid-read when the file moves, or whose discovery interval is longer
// than the file's lifetime, loses the lines in between and has no counter for
// them.
type rotateStorm struct{ base }

func (r *rotateStorm) Fire(ctx context.Context, target Target) (Result, error) {
	if target == nil {
		return Result{}, ErrNotArmed
	}
	if !r.claim() {
		return Result{}, ErrAlreadyRun
	}

	res := Result{Kind: RotateStorm, FiredAt: r.firesAt, StartedAt: time.Now()}

	for i := 0; i < r.params.Rotations; i++ {
		if err := ctx.Err(); err != nil {
			res.FinishedAt = time.Now()
			res.Rotations = i
			return res, err
		}
		if err := target.Rotate(); err != nil {
			res.FinishedAt = time.Now()
			res.Rotations = i
			return res, fmt.Errorf("fault: rotation %d of %d: %w", i+1, r.params.Rotations, err)
		}
		res.Rotations++

		if r.params.MinFileLifetime > 0 {
			select {
			case <-ctx.Done():
				res.FinishedAt = time.Now()
				return res, ctx.Err()
			case <-time.After(r.params.MinFileLifetime):
			}
		}
	}

	res.FinishedAt = time.Now()
	res.Detail = fmt.Sprintf("%d rotations, each file alive for %s",
		res.Rotations, r.params.MinFileLifetime)
	return res, nil
}
