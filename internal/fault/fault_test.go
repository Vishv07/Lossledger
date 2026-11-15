package fault

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// countingTarget records rotations and can be made to fail.
type countingTarget struct {
	mu        sync.Mutex
	rotations int
	failAfter int // 0 means never fail
}

func (c *countingTarget) Rotate() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rotations++
	if c.failAfter > 0 && c.rotations > c.failAfter {
		return errors.New("disk full")
	}
	return nil
}

func (c *countingTarget) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rotations
}

func TestRotateStormRotatesTheRequestedNumberOfTimes(t *testing.T) {
	inj, err := New(Config{Kind: RotateStorm, AtCount: 2000, Params: Params{Rotations: 12}})
	if err != nil {
		t.Fatal(err)
	}

	target := &countingTarget{}
	res, err := inj.Fire(context.Background(), target)
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if target.count() != 12 {
		t.Errorf("rotations performed = %d, want 12", target.count())
	}
	if res.Rotations != 12 {
		t.Errorf("Result.Rotations = %d, want 12", res.Rotations)
	}
	if res.FiredAt != 2000 {
		t.Errorf("FiredAt = %d, want 2000", res.FiredAt)
	}
	if res.FinishedAt.Before(res.StartedAt) {
		t.Error("Result finished before it started")
	}
}

// TestFiringPointIsSeeded is what makes a replay meaningful: the same scenario
// and seed must provoke the pipeline at the same point in the stream.
func TestFiringPointIsSeeded(t *testing.T) {
	cfg := Config{Kind: RotateStorm, AtCount: 5000, Jitter: 500, Seed: 7, Params: Params{Rotations: 1}}

	first, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if first.FiresAt() != second.FiresAt() {
		t.Errorf("same seed fired at %d and %d", first.FiresAt(), second.FiresAt())
	}

	cfg.Seed = 8
	third, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if third.FiresAt() == first.FiresAt() {
		t.Log("different seeds coincided, which is possible but should be rare")
	}

	// Jitter must stay inside its declared bound, or a scenario's described
	// firing point stops matching what the report says happened.
	for _, seed := range []int64{1, 2, 3, 99, 1234, 55555} {
		cfg.Seed = seed
		inj, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if got := inj.FiresAt(); got < 5000 || got > 5500 {
			t.Errorf("seed %d fired at %d, outside [5000, 5500]", seed, got)
		}
	}
}

func TestRotateStormFiresOnlyOnce(t *testing.T) {
	inj, _ := New(Config{Kind: RotateStorm, AtCount: 10, Params: Params{Rotations: 3}})
	target := &countingTarget{}

	if _, err := inj.Fire(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if !inj.Fired() {
		t.Error("Fired() is false after firing")
	}

	// A runner that polls an emission count could call Fire twice on adjacent
	// polls. Doubling the provocation would make the report wrong about what
	// the pipeline was subjected to.
	if _, err := inj.Fire(context.Background(), target); !errors.Is(err, ErrAlreadyRun) {
		t.Errorf("second Fire returned %v, want ErrAlreadyRun", err)
	}
	if target.count() != 3 {
		t.Errorf("rotations = %d after a double fire, want 3", target.count())
	}
}

func TestRotateStormReportsPartialWorkOnFailure(t *testing.T) {
	// If the storm dies halfway, the report has to say how many rotations
	// actually happened. Claiming the configured number would describe a
	// provocation the pipeline never received.
	inj, _ := New(Config{Kind: RotateStorm, AtCount: 1, Params: Params{Rotations: 10}})
	target := &countingTarget{failAfter: 4}

	res, err := inj.Fire(context.Background(), target)
	if err == nil {
		t.Fatal("Fire succeeded against a target that fails after four rotations")
	}
	if res.Rotations != 4 {
		t.Errorf("Result.Rotations = %d, want 4", res.Rotations)
	}
}

func TestRotateStormRespectsContext(t *testing.T) {
	inj, _ := New(Config{
		Kind: RotateStorm, AtCount: 1,
		Params: Params{Rotations: 1000, MinFileLifetime: 50 * time.Millisecond},
	})
	target := &countingTarget{}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	start := time.Now()
	res, err := inj.Fire(ctx, target)
	if err == nil {
		t.Error("Fire ignored a cancelled context")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Fire took %v to notice cancellation", elapsed)
	}
	if res.Rotations == 0 {
		t.Error("Result reports no rotations despite having run")
	}
	if res.Rotations >= 1000 {
		t.Error("Fire completed every rotation despite cancellation")
	}
}

func TestRotateStormHonoursFileLifetime(t *testing.T) {
	// The lifetime is the whole point: a file that lives longer than the
	// collector's discovery interval gets tailed, and nothing is lost.
	const lifetime = 30 * time.Millisecond
	inj, _ := New(Config{
		Kind: RotateStorm, AtCount: 1,
		Params: Params{Rotations: 4, MinFileLifetime: lifetime},
	})

	start := time.Now()
	if _, err := inj.Fire(context.Background(), &countingTarget{}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 4*lifetime {
		t.Errorf("four rotations at %v lifetime took only %v", lifetime, elapsed)
	}
}

func TestNewRejectsBadConfigs(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"unknown kind", Config{Kind: "nonsense", AtCount: 1}},
		{"rotatestorm with no rotations", Config{Kind: RotateStorm, AtCount: 1}},
		{"rotatestorm with negative rotations", Config{Kind: RotateStorm, AtCount: 1, Params: Params{Rotations: -1}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg); err == nil {
				t.Error("New accepted an invalid config")
			}
		})
	}
}

// TestUnimplementedInjectorsFailLoudly guards the worst failure mode here: an
// injector that silently does nothing produces a clean run, and a clean run
// reads as the pipeline passing.
func TestUnimplementedInjectorsFailLoudly(t *testing.T) {
	for _, k := range []Kind{SinkOutage, Sigkill, DiskPressure} {
		if _, err := New(Config{Kind: k, AtCount: 1}); err == nil {
			t.Errorf("injector %q constructed successfully but is not implemented; "+
				"a scenario using it would report a clean run having provoked nothing", k)
		}
	}
}

func TestFireWithoutTarget(t *testing.T) {
	inj, _ := New(Config{Kind: RotateStorm, AtCount: 1, Params: Params{Rotations: 1}})
	if _, err := inj.Fire(context.Background(), nil); !errors.Is(err, ErrNotArmed) {
		t.Errorf("Fire with no target returned %v, want ErrNotArmed", err)
	}
}
