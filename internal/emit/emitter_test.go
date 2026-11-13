package emit

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Vishv07/lossledger/internal/wire"
)

var testKey = []byte("lossledger-emit-test-key")

func testSet(t *testing.T) *Set {
	t.Helper()
	set, err := NewSet(Builtins())
	if err != nil {
		t.Fatalf("NewSet: %v", err)
	}
	return set
}

// capture is a Writer that keeps every line in memory.
type capture struct {
	lines  []string
	groups int
	closed bool
}

func (c *capture) WriteLines(lines []string) error {
	c.lines = append(c.lines, lines...)
	c.groups++
	return nil
}
func (c *capture) Rotate() error    { return nil }
func (c *capture) Sync() error      { return nil }
func (c *capture) Close() error     { c.closed = true; return nil }
func (c *capture) Describe() string { return "capture" }

func TestEmitterSequencesFromOneWithoutGaps(t *testing.T) {
	cap := &capture{}
	e, err := New(Config{Src: "node-a", Key: testKey, Deterministic: true, Seed: 1}, cap, testSet(t))
	if err != nil {
		t.Fatal(err)
	}
	const n = 500
	if err := e.EmitN(n); err != nil {
		t.Fatal(err)
	}

	var seen []uint64
	for _, line := range cap.lines {
		tok, err := wire.Extract(line)
		if err != nil || tok.Kind != wire.KindCanary {
			continue
		}
		seen = append(seen, tok.Seq)
	}

	if len(seen) != n {
		t.Fatalf("emitted %d canary tokens, want %d", len(seen), n)
	}
	for i, got := range seen {
		if want := uint64(i + 1); got != want {
			t.Fatalf("sequence %d = %d, want %d (emitter must never skip)", i, got, want)
		}
	}
	if e.Emitted() != n {
		t.Errorf("Emitted() = %d, want %d", e.Emitted(), n)
	}
}

// TestEmitterDeterminism is an M1 acceptance criterion: same seed, identical
// output, byte for byte.
func TestEmitterDeterminism(t *testing.T) {
	run := func() []string {
		cap := &capture{}
		e, err := New(Config{
			Src:           "node-a",
			Key:           testKey,
			Seed:          42,
			Deterministic: true,
			Rate:          50,
			Shapes:        map[string]float64{"plain": 0.5, "json": 0.2, "stacktrace": 0.3},
		}, cap, testSet(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := e.EmitN(300); err != nil {
			t.Fatal(err)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		return cap.lines
	}

	first, second := run(), run()
	if len(first) != len(second) {
		t.Fatalf("line counts differ between seeded runs: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("seeded runs diverged at line %d:\n first: %s\nsecond: %s", i, first[i], second[i])
		}
	}
}

func TestEmitterDifferentSeedsDiverge(t *testing.T) {
	run := func(seed int64) []string {
		cap := &capture{}
		e, _ := New(Config{
			Src: "node-a", Key: testKey, Seed: seed, Deterministic: true,
			Shapes: map[string]float64{"plain": 0.5, "stacktrace": 0.5},
		}, cap, testSet(t))
		e.EmitN(200)
		return cap.lines
	}
	if len(run(1)) == len(run(2)) {
		t.Log("line counts coincided, comparing content")
	}
	a, b := run(1), run(2)
	same := len(a) == len(b)
	if same {
		for i := range a {
			if a[i] != b[i] {
				same = false
				break
			}
		}
	}
	if same {
		t.Error("two different seeds produced identical output")
	}
}

func TestEmitterRestartMintsNewEpoch(t *testing.T) {
	// Restart honesty (PRD section 7.4): a new process is a new epoch, so a
	// restart can never look like a gap in the previous one.
	set := testSet(t)
	first, err := New(Config{Src: "node-a", Key: testKey}, &capture{}, set)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(Config{Src: "node-a", Key: testKey}, &capture{}, set)
	if err != nil {
		t.Fatal(err)
	}
	if first.Epoch() == second.Epoch() {
		t.Error("two emitter instances shared an epoch identifier")
	}
	if !wire.ValidULID(first.Epoch()) {
		t.Errorf("epoch %q is not a valid ULID", first.Epoch())
	}
}

func TestEmitterCloseWritesFinalLine(t *testing.T) {
	cap := &capture{}
	e, err := New(Config{Src: "node-a", Key: testKey, Deterministic: true}, cap, testSet(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.EmitN(17); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	last := cap.lines[len(cap.lines)-1]
	tok, err := wire.Extract(last)
	if err != nil {
		t.Fatalf("final line did not parse: %v (%q)", err, last)
	}
	if tok.Kind != wire.KindFinal {
		t.Fatalf("last line kind = %v, want final", tok.Kind)
	}
	if tok.Final != 17 {
		t.Errorf("final sequence = %d, want 17", tok.Final)
	}
	if !tok.Verify(testKey) {
		t.Error("final line failed signature verification")
	}
	if !cap.closed {
		t.Error("Close did not close the underlying writer")
	}
}

func TestEmitterAbortWritesNoFinalLine(t *testing.T) {
	// A crash leaves the epoch truncated. The verifier must then refuse to
	// count anything past the highest sequence it saw.
	cap := &capture{}
	e, _ := New(Config{Src: "node-a", Key: testKey, Deterministic: true}, cap, testSet(t))
	e.EmitN(9)
	if err := e.Abort(); err != nil {
		t.Fatal(err)
	}
	for _, line := range cap.lines {
		if tok, err := wire.Extract(line); err == nil && tok.Kind == wire.KindFinal {
			t.Fatal("Abort wrote a final line; a crash must not look like a clean shutdown")
		}
	}
}

func TestEmitterRejectsBadConfig(t *testing.T) {
	set := testSet(t)
	cases := []struct {
		name string
		cfg  Config
	}{
		{"no source", Config{Key: testKey}},
		{"no key", Config{Src: "a"}},
		{"rate over cap", Config{Src: "a", Key: testKey, Rate: 10_000}},
		{"unknown shape", Config{Src: "a", Key: testKey, Shapes: map[string]float64{"nope": 1}}},
		{"negative weight", Config{Src: "a", Key: testKey, Shapes: map[string]float64{"plain": -1}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg, &capture{}, set); err == nil {
				t.Error("New accepted an invalid config")
			}
		})
	}
}

func TestEmitterEveryTokenVerifies(t *testing.T) {
	cap := &capture{}
	e, _ := New(Config{
		Src: "node-a", Key: testKey, Deterministic: true, Seed: 7,
		Shapes: map[string]float64{"plain": 1, "json": 1, "stacktrace": 1, "oversize": 1, "interleave": 1},
	}, cap, testSet(t))
	if err := e.EmitN(400); err != nil {
		t.Fatal(err)
	}
	e.Close()

	var canary, continuation int
	for _, line := range cap.lines {
		tok, err := wire.Extract(line)
		if err != nil {
			continue // interleave neighbour lines carry no token by design
		}
		if !tok.Verify(testKey) {
			t.Fatalf("token failed verification: %q", line)
		}
		switch tok.Kind {
		case wire.KindCanary:
			canary++
		case wire.KindContinuation:
			continuation++
		}
	}
	if canary != 400 {
		t.Errorf("canary tokens = %d, want 400", canary)
	}
	if continuation == 0 {
		t.Error("no continuation markers were written despite the stacktrace shape being in the mix")
	}
}

func TestEmitterRunRespectsContext(t *testing.T) {
	cap := &capture{}
	e, _ := New(Config{Src: "node-a", Key: testKey, Rate: 100, AckRate: false}, cap, testSet(t))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()

	start := time.Now()
	if err := e.Run(ctx); err != nil {
		t.Fatalf("Run returned an error on clean cancellation: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Run took %v to notice cancellation", elapsed)
	}
	if e.Emitted() == 0 {
		t.Error("Run emitted nothing")
	}
}

// TestEmitterThroughFileWriter exercises the real on-disk path including
// rotation, then reads every generation back and checks the ledger is whole.
func TestEmitterThroughFileWriter(t *testing.T) {
	dir := t.TempDir()
	root, err := NewRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewFileWriter(FileConfig{Root: root, Name: "canary.log", MaxLines: 50, Keep: 20})
	if err != nil {
		t.Fatal(err)
	}

	e, err := New(Config{Src: "node-a", Key: testKey, Deterministic: true, Seed: 3}, w, testSet(t))
	if err != nil {
		t.Fatal(err)
	}
	const n = 600
	if err := e.EmitN(n); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	if w.Rotations() == 0 {
		t.Fatal("expected the writer to have rotated")
	}

	seen := map[uint64]bool{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		f, err := os.Open(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
		for sc.Scan() {
			if tok, err := wire.Extract(sc.Text()); err == nil && tok.Kind == wire.KindCanary {
				seen[tok.Seq] = true
			}
		}
		f.Close()
	}

	for seq := uint64(1); seq <= n; seq++ {
		if !seen[seq] {
			t.Fatalf("sequence %d is missing from disk; the emitter lost its own line", seq)
		}
	}
}

func TestFileWriterRotationKeepsGenerationBound(t *testing.T) {
	dir := t.TempDir()
	root, _ := NewRoot(dir)
	w, err := NewFileWriter(FileConfig{Root: root, Name: "canary.log", Keep: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 10; i++ {
		if err := w.WriteLines([]string{"line"}); err != nil {
			t.Fatal(err)
		}
		if err := w.Rotate(); err != nil {
			t.Fatal(err)
		}
	}

	entries, _ := os.ReadDir(dir)
	if len(entries) > 4 { // active file plus Keep generations
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("rotation kept %d files (%s), want at most 4", len(entries), strings.Join(names, ", "))
	}
}

func TestStdoutWriter(t *testing.T) {
	var buf bytes.Buffer
	w := NewStdoutWriter(&buf)
	if err := w.WriteLines([]string{"first", "second"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "first\nsecond\n" {
		t.Errorf("stdout writer wrote %q", got)
	}
}
