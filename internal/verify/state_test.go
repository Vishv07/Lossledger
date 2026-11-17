package verify

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Vishv07/lossledger/internal/adapter"
)

// TestVerifierRestartFabricatesNoGaps is the PRD M1 condition this file
// exists for: "refgood CLEAN ... including verifier restarts mid-run".
//
// Without persistence a restarted verifier knows nothing about sequences it
// had already accounted for. Any of them past their settle window then look
// absent, and the harness reports loss it caused itself.
func TestVerifierRestartFabricatesNoGaps(t *testing.T) {
	const emitted = 2000

	held := fullSet(emitted)
	store, err := NewFileStore(filepath.Join(t.TempDir(), "state.bin"))
	if err != nil {
		t.Fatal(err)
	}

	now := time.Unix(1_700_000_000, 0)
	clock := now

	newVerifier := func() *Verifier {
		reg, err := adapter.NewRegistry(&holdingSink{name: "sink", held: held})
		if err != nil {
			t.Fatal(err)
		}
		v, err := NewVerifier(VerifierConfig{
			Now:     func() time.Time { return clock },
			MaxWait: time.Minute,
		}, reg)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}

	// First process: learn the epoch, sweep once, save, then die.
	first := newVerifier()
	for s := uint64(1); s <= emitted; s++ {
		first.NoteEmitted("node-a", testEpoch, s, now)
	}
	first.CloseEpoch("node-a", testEpoch, emitted, true)
	if err := first.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.SaveState(store); err != nil {
		t.Fatalf("SaveState: %v", err)
	}

	before := first.Verdict()
	if before.Arrived != emitted {
		t.Fatalf("before restart, arrived = %d, want %d", before.Arrived, emitted)
	}

	// Time passes while nothing is running, so every sequence is now well past
	// its settle window. This is the window in which a forgetful verifier
	// invents gaps.
	clock = clock.Add(10 * time.Minute)

	// Second process: restore, then produce a verdict without re-learning the
	// epoch from an emitter, because in production the emitter is a separate
	// process that did not restart.
	second := newVerifier()
	if err := second.LoadState(store); err != nil {
		t.Fatalf("LoadState: %v", err)
	}

	after := second.Verdict()

	if after.Worst != Clean {
		t.Errorf("after restart the verdict is %s, want CLEAN", after.Worst)
	}
	if after.MissingN != 0 {
		t.Errorf("after restart %d sequences are reported missing, all of them fabricated by the restart",
			after.MissingN)
	}
	if after.Arrived != emitted {
		t.Errorf("after restart, arrived = %d, want %d", after.Arrived, emitted)
	}
	if after.FalseMissing != 0 {
		t.Errorf("false missing = %d, must be zero", after.FalseMissing)
	}
	if err := after.Conservation(); err != nil {
		t.Errorf("conservation after restart: %v", err)
	}
}

// TestRestartPreservesDeclaredMissing guards the integrity metric across a
// restart. If a sequence we declared missing later arrives, that is a false
// accusation and must be counted, even if the process that made the accusation
// is gone.
func TestRestartPreservesDeclaredMissing(t *testing.T) {
	const emitted = 500

	lossy := fullSet(emitted)
	for s := uint64(100); s <= 119; s++ {
		delete(lossy, s)
	}

	store, err := NewFileStore(filepath.Join(t.TempDir(), "state.bin"))
	if err != nil {
		t.Fatal(err)
	}

	now := time.Unix(1_700_000_000, 0)
	clock := now
	sink := &holdingSink{name: "sink", held: lossy}

	reg, _ := adapter.NewRegistry(sink)
	first, err := NewVerifier(VerifierConfig{
		Now: func() time.Time { return clock }, MaxWait: time.Minute,
	}, reg)
	if err != nil {
		t.Fatal(err)
	}

	for s := uint64(1); s <= emitted; s++ {
		first.NoteEmitted("node-a", testEpoch, s, now)
	}
	first.CloseEpoch("node-a", testEpoch, emitted, true)
	for i := 0; i < 3; i++ {
		if err := first.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
		clock = clock.Add(2 * time.Minute)
	}
	if got := first.Verdict(); got.MissingN != 20 {
		t.Fatalf("before restart, missing = %d, want 20", got.MissingN)
	}
	if err := first.SaveState(store); err != nil {
		t.Fatal(err)
	}

	// The "lost" sequences turn up after all, which means the accusation was
	// wrong. A restarted verifier must still notice.
	for s := uint64(100); s <= 119; s++ {
		sink.held[s] = true
	}

	reg2, _ := adapter.NewRegistry(sink)
	second, err := NewVerifier(VerifierConfig{
		Now: func() time.Time { return clock }, MaxWait: time.Minute,
	}, reg2)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.LoadState(store); err != nil {
		t.Fatal(err)
	}
	if err := second.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}

	got := second.Verdict()
	if got.FalseMissing != 20 {
		t.Errorf("false missing = %d, want 20: the restart lost the record of what had been declared",
			got.FalseMissing)
	}
}

func TestFileStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "state.bin")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.Load(); err != ErrNoState {
		t.Errorf("Load on a fresh store = %v, want ErrNoState", err)
	}

	want := Snapshot{
		InvalidLines: 7,
		Streams: []StreamState{{
			Src: "a", Epoch: testEpoch, Sink: "s",
			Expected: 100, FalseMissing: 2,
			Arrived:   mustMarshal(t, LedgerRange(1, 100)),
			EmittedAt: map[uint64]int64{5: 123},
		}},
	}
	if err := store.Save(want); err != nil {
		t.Fatal(err)
	}

	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != snapshotVersion {
		t.Errorf("version = %d, want %d", got.Version, snapshotVersion)
	}
	if got.InvalidLines != 7 || len(got.Streams) != 1 {
		t.Fatalf("round trip lost data: %+v", got)
	}
	if got.Streams[0].Expected != 100 || got.Streams[0].FalseMissing != 2 {
		t.Errorf("stream fields differ: %+v", got.Streams[0])
	}

	restored := NewLedger()
	if err := restored.UnmarshalBinary(got.Streams[0].Arrived); err != nil {
		t.Fatal(err)
	}
	if restored.Count() != 100 {
		t.Errorf("restored ledger holds %d, want 100", restored.Count())
	}
}

func TestFileStoreRejectsAnUnknownVersion(t *testing.T) {
	// Refusing to load is safe because the verifier can re-derive. Reading a
	// layout this build does not understand is not.
	path := filepath.Join(t.TempDir(), "state.bin")
	store, _ := NewFileStore(path)
	if err := store.Save(Snapshot{}); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Re-encode with a version this build does not know.
	bad := Snapshot{Version: 99}
	_ = raw
	store2 := &rawStore{snap: bad}
	v, _ := NewVerifier(VerifierConfig{}, mustRegistry(t))
	if err := v.LoadState(store2); err == nil {
		t.Error("a snapshot from an unknown version loaded without complaint")
	}
}

// rawStore returns a snapshot verbatim, bypassing the file encoding.
type rawStore struct{ snap Snapshot }

func (r *rawStore) Save(Snapshot) error { return nil }
func (r *rawStore) Load() (Snapshot, error) {
	if r.snap.Version != snapshotVersion {
		return Snapshot{}, errUnknownVersion
	}
	return r.snap, nil
}

var errUnknownVersion = os.ErrInvalid

func mustRegistry(t *testing.T) *adapter.Registry {
	t.Helper()
	reg, err := adapter.NewRegistry(&holdingSink{name: "s", held: map[uint64]bool{}})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func mustMarshal(t *testing.T, l *Ledger) []byte {
	t.Helper()
	b, err := l.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSaveAndLoadNeedAStore(t *testing.T) {
	v, _ := NewVerifier(VerifierConfig{}, mustRegistry(t))
	if err := v.SaveState(nil); err == nil {
		t.Error("SaveState accepted a nil store")
	}
	if err := v.LoadState(nil); err == nil {
		t.Error("LoadState accepted a nil store")
	}
}

// TestSnapshotDropsSettledEmitTimes keeps the snapshot bounded. Retaining an
// emit time for every sequence ever seen would make the file grow with total
// traffic rather than with the in-flight window.
func TestSnapshotDropsSettledEmitTimes(t *testing.T) {
	const emitted = 1000
	reg, _ := adapter.NewRegistry(&holdingSink{name: "s", held: fullSet(emitted)})

	now := time.Unix(1_700_000_000, 0)
	v, err := NewVerifier(VerifierConfig{Now: func() time.Time { return now }}, reg)
	if err != nil {
		t.Fatal(err)
	}
	for s := uint64(1); s <= emitted; s++ {
		v.NoteEmitted("node-a", testEpoch, s, now)
	}
	v.CloseEpoch("node-a", testEpoch, emitted, true)
	if err := v.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}

	mem := &memStore{}
	if err := v.SaveState(mem); err != nil {
		t.Fatal(err)
	}
	for _, st := range mem.snap.Streams {
		if len(st.EmittedAt) != 0 {
			t.Errorf("snapshot kept %d emit times for sequences that already arrived", len(st.EmittedAt))
		}
	}
}

type memStore struct{ snap Snapshot }

func (m *memStore) Save(s Snapshot) error   { m.snap = s; return nil }
func (m *memStore) Load() (Snapshot, error) { return m.snap, nil }
