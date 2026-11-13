package emit

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Vishv07/lossledger/internal/wire"
)

// Config describes one emitting unit.
type Config struct {
	// Src identifies the emitting unit: a node, pod or container name. It must
	// be unique across the deployment, because the ledger is keyed on it.
	Src string

	// Rate is lines per second. Capped at SafeRateCap unless AckRate is set.
	Rate float64

	// Shapes maps profile name to relative weight.
	Shapes map[string]float64

	// Seed drives every random choice the emitter makes. Two runs with the
	// same seed and the same deterministic clock emit byte-identical output,
	// which is what makes a gauntlet replay meaningful.
	Seed int64

	// Key signs every token.
	Key []byte

	// AckRate records that the operator deliberately exceeded the rate cap.
	AckRate bool

	// Deterministic replaces the wall clock with a synthetic one that advances
	// by exactly one emission interval per line. Gauntlet runs set this so a
	// replay reproduces timestamps as well as content; production runs do not,
	// because a canary should carry the real time it was written.
	Deterministic bool

	// StartTime seeds the synthetic clock. Ignored unless Deterministic.
	StartTime time.Time

	// Now overrides the wall clock. Tests use it; production leaves it nil.
	Now func() time.Time
}

func (c *Config) applyDefaults() {
	if c.Rate == 0 {
		c.Rate = 10
	}
	if len(c.Shapes) == 0 {
		c.Shapes = map[string]float64{"plain": 1}
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.StartTime.IsZero() {
		c.StartTime = time.Unix(1_700_000_000, 0).UTC()
	}
}

func (c Config) validate() error {
	if c.Src == "" {
		return fmt.Errorf("emit: config needs a source identifier")
	}
	if len(c.Key) == 0 {
		return wire.ErrNoKey
	}
	return CheckRate(c.Rate, c.AckRate)
}

// Emitter writes canary lines for one source through one writer.
//
// It is the only component that assigns sequence numbers, and it assigns them
// strictly monotonically within an epoch. A gap in the ledger therefore always
// means the pipeline lost something, never that the emitter skipped a number.
type Emitter struct {
	cfg    Config
	writer Writer
	mix    *Mix
	rnd    *rand.Rand

	epoch    wire.ULID
	epochStr string

	mu       sync.Mutex
	seq      uint64
	closed   bool
	finalSeq uint64

	emitted atomic.Uint64
	byShape sync.Map // profile name to *atomic.Uint64
}

// New builds an emitter. The epoch is minted here, so constructing an emitter
// is what starts an epoch, and a restart necessarily produces a new one.
func New(cfg Config, w Writer, set *Set) (*Emitter, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if w == nil {
		return nil, fmt.Errorf("emit: nil writer")
	}

	mix, err := NewMix(set, cfg.Shapes)
	if err != nil {
		return nil, err
	}

	rnd := rand.New(rand.NewSource(cfg.Seed))

	// The epoch ULID draws from the same seeded source in deterministic mode,
	// so a replay reproduces the epoch identifier too.
	var epoch wire.ULID
	if cfg.Deterministic {
		epoch, err = wire.NewULID(cfg.StartTime, rnd)
	} else {
		epoch, err = wire.NewULID(cfg.Now(), nil)
	}
	if err != nil {
		return nil, fmt.Errorf("emit: minting epoch: %w", err)
	}

	return &Emitter{
		cfg:      cfg,
		writer:   w,
		mix:      mix,
		rnd:      rnd,
		epoch:    epoch,
		epochStr: epoch.String(),
	}, nil
}

// Epoch returns this emitter's epoch identifier.
func (e *Emitter) Epoch() string { return e.epochStr }

// Src returns the source identifier.
func (e *Emitter) Src() string { return e.cfg.Src }

// Emitted reports how many sequence numbers have been written.
func (e *Emitter) Emitted() uint64 { return e.emitted.Load() }

// interval is the gap between emissions at the configured rate.
func (e *Emitter) interval() time.Duration {
	return time.Duration(float64(time.Second) / e.cfg.Rate)
}

// timestampFor returns the emit time for a sequence number.
func (e *Emitter) timestampFor(seq uint64) time.Time {
	if e.cfg.Deterministic {
		return e.cfg.StartTime.Add(time.Duration(seq-1) * e.interval())
	}
	return e.cfg.Now()
}

// EmitOne writes exactly one logical event and returns its sequence number.
//
// A multi-line shape still consumes exactly one sequence number: the ledger
// counts events, not physical lines, so a stack trace that arrives whole and a
// plain line that arrives whole are both one accounted unit.
func (e *Emitter) EmitOne() (uint64, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return 0, fmt.Errorf("emit: emitter is closed")
	}
	e.seq++
	seq := e.seq
	profile := e.mix.Pick(e.rnd)
	// Rendering happens under the lock because it draws from the seeded
	// source, and a shared source visited in a racing order would destroy
	// reproducibility.
	lines := profile.Render(wire.Token{
		Kind:  wire.KindCanary,
		Src:   e.cfg.Src,
		Epoch: e.epochStr,
		Seq:   seq,
		TS:    e.timestampFor(seq).UnixNano(),
	}, e.cfg.Key, e.rnd)
	e.mu.Unlock()

	if err := e.writer.WriteLines(lines); err != nil {
		return 0, fmt.Errorf("emit: writing seq %d: %w", seq, err)
	}

	e.emitted.Add(1)
	counter, _ := e.byShape.LoadOrStore(profile.Name, new(atomic.Uint64))
	counter.(*atomic.Uint64).Add(1)
	return seq, nil
}

// EmitN writes n events as fast as the writer accepts them, ignoring pacing.
// Tests and the self-validation gate use it; production uses Run.
func (e *Emitter) EmitN(n int) error {
	for i := 0; i < n; i++ {
		if _, err := e.EmitOne(); err != nil {
			return err
		}
	}
	return nil
}

// Run emits at the configured rate until ctx is cancelled.
//
// It returns nil on cancellation: a cancelled emitter has done its job, and
// surfacing context.Canceled would make every clean shutdown look like a
// failure in the exit code.
func (e *Emitter) Run(ctx context.Context) error {
	ticker := time.NewTicker(e.interval())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := e.EmitOne(); err != nil {
				return err
			}
		}
	}
}

// Close writes the final line for this epoch and closes the writer.
//
// The final line is what lets the verifier distinguish a clean shutdown from a
// crash. With it, every sequence below the stated final is legitimately
// expected and its absence is loss. Without it, the epoch is truncated and the
// verifier must not count anything past the highest sequence it observed, or it
// would invent gaps out of an ordinary restart (PRD section 7.4).
func (e *Emitter) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.finalSeq = e.seq
	final := e.seq
	e.mu.Unlock()

	token := wire.Token{
		Kind:  wire.KindFinal,
		Src:   e.cfg.Src,
		Epoch: e.epochStr,
		TS:    e.timestampFor(final + 1).UnixNano(),
		Final: final,
	}
	if err := e.writer.WriteLines([]string{token.Format(e.cfg.Key)}); err != nil {
		e.writer.Close()
		return fmt.Errorf("emit: writing final line: %w", err)
	}
	return e.writer.Close()
}

// Abort closes the writer without writing a final line, imitating a crash.
// The self-validation gate uses it to prove that a truncated epoch never
// produces phantom missing sequences.
func (e *Emitter) Abort() error {
	e.mu.Lock()
	e.closed = true
	e.finalSeq = e.seq
	e.mu.Unlock()
	return e.writer.Close()
}

// Stats summarises what an emitter produced.
type Stats struct {
	Src      string
	Epoch    string
	Emitted  uint64
	FinalSeq uint64
	ByShape  map[string]uint64
}

// Stats returns a snapshot for reports and metrics.
func (e *Emitter) Stats() Stats {
	e.mu.Lock()
	finalSeq := e.finalSeq
	e.mu.Unlock()

	byShape := make(map[string]uint64)
	e.byShape.Range(func(k, v any) bool {
		byShape[k.(string)] = v.(*atomic.Uint64).Load()
		return true
	})

	return Stats{
		Src:      e.cfg.Src,
		Epoch:    e.epochStr,
		Emitted:  e.emitted.Load(),
		FinalSeq: finalSeq,
		ByShape:  byShape,
	}
}
