package verify

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Why this file exists.
//
// A verifier that forgets everything on restart re-derives its ledger from
// scratch. For the window between restart and the next full sweep it knows
// nothing about sequences it had already accounted for, and any of them that
// are past their settle window look absent. That is a false accusation caused
// by the harness restarting, which is the one failure this project cannot
// have (PRD section 7.5).
//
// So the ledger is persisted, and on restart the verifier resumes from what it
// knew rather than from nothing.

// StreamState is one accounting unit's durable state.
//
// Bitmaps travel as their roaring serialisation rather than as sequence lists,
// which is what keeps a day of 173 million sequences to a size worth writing.
type StreamState struct {
	Src       string
	Epoch     string
	Sink      string
	Expected  uint64
	Truncated bool

	Arrived   []byte
	Settled   []byte
	Rechecked []byte
	Declared  []byte

	// EmittedAt carries only sequences still awaiting judgement. A sequence
	// that has arrived or been declared no longer needs its emit time, and
	// keeping every one of them would make the snapshot grow without bound.
	EmittedAt map[uint64]int64

	// ProvisionalAt preserves where each absent sequence is in the settle
	// pipeline. Without it a restart would restart the mandatory re-check
	// clock, which is harmless but delays every verdict.
	ProvisionalAt map[uint64]int64

	FalseMissing uint64
	LateArrivals []uint64
	Duplicates   map[uint64]int
	Partial      []uint64
}

// Snapshot is everything a verifier needs to resume.
type Snapshot struct {
	Version      int
	SavedAt      time.Time
	InvalidLines uint64
	Streams      []StreamState
}

// snapshotVersion guards against reading a layout this build does not
// understand. A refusal to load is safe: the verifier re-derives. Silently
// misreading an old layout is not.
const snapshotVersion = 1

// Store persists and restores verifier state.
type Store interface {
	Save(Snapshot) error
	Load() (Snapshot, error)
}

// ErrNoState reports that nothing has been saved yet, which is the normal
// state of a first run and never an error worth stopping for.
var ErrNoState = fmt.Errorf("verify: no saved state")

// FileStore keeps the snapshot in a single file.
//
// One file written atomically rather than an embedded key-value store: the
// access pattern is a whole-state write on an interval and a whole-state read
// at startup, which is the one shape where a database earns nothing and costs
// a dependency.
type FileStore struct{ path string }

// NewFileStore returns a store backed by path.
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, fmt.Errorf("verify: state store needs a path")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("verify: creating state directory: %w", err)
		}
	}
	return &FileStore{path: path}, nil
}

// Save writes the snapshot atomically.
//
// Temp file then rename, so a crash mid-write leaves the previous snapshot
// intact. A truncated snapshot would be worse than none: it could restore a
// partial ledger and report the missing half as lost.
func (f *FileStore) Save(s Snapshot) error {
	s.Version = snapshotVersion
	s.SavedAt = time.Now().UTC()

	tmp, err := os.CreateTemp(filepath.Dir(f.path), ".lossledger-state-*")
	if err != nil {
		return fmt.Errorf("verify: creating temp state file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if err := gob.NewEncoder(tmp).Encode(s); err != nil {
		tmp.Close()
		return fmt.Errorf("verify: encoding state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("verify: syncing state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("verify: closing state: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return fmt.Errorf("verify: installing state file: %w", err)
	}
	return nil
}

// Load reads the snapshot. A missing file is ErrNoState, not a failure.
func (f *FileStore) Load() (Snapshot, error) {
	file, err := os.Open(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			return Snapshot{}, ErrNoState
		}
		return Snapshot{}, fmt.Errorf("verify: opening state: %w", err)
	}
	defer file.Close()

	var s Snapshot
	if err := gob.NewDecoder(file).Decode(&s); err != nil {
		return Snapshot{}, fmt.Errorf("verify: decoding state: %w", err)
	}
	if s.Version != snapshotVersion {
		return Snapshot{}, fmt.Errorf("verify: state file is version %d, this build reads version %d",
			s.Version, snapshotVersion)
	}
	return s, nil
}

// exportState captures the scheduler's durable state, dropping emit times for
// sequences that no longer need judging.
func (s *Scheduler) exportState() (arrived, settled, rechecked, declared []byte,
	emittedAt, provisionalAt map[uint64]int64, falseMissing uint64, late []uint64, err error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if arrived, err = s.arrived.MarshalBinary(); err != nil {
		return
	}
	if settled, err = s.settled.MarshalBinary(); err != nil {
		return
	}
	if rechecked, err = s.rechecked.MarshalBinary(); err != nil {
		return
	}
	if declared, err = s.declared.MarshalBinary(); err != nil {
		return
	}

	emittedAt = make(map[uint64]int64)
	for seq, at := range s.emittedAt {
		// An arrived sequence is settled business. Keeping its emit time would
		// grow the snapshot by one entry per sequence ever seen.
		if s.arrived.Contains(seq) {
			continue
		}
		emittedAt[seq] = at.UnixNano()
	}
	provisionalAt = make(map[uint64]int64, len(s.provisionalAt))
	for seq, at := range s.provisionalAt {
		provisionalAt[seq] = at.UnixNano()
	}

	falseMissing = s.falseMissing
	late = append([]uint64(nil), s.lateArrivals...)
	return
}

// importState restores a scheduler from a snapshot.
func (s *Scheduler) importState(st StreamState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, p := range []struct {
		data   []byte
		ledger *Ledger
	}{
		{st.Arrived, s.arrived},
		{st.Settled, s.settled},
		{st.Rechecked, s.rechecked},
		{st.Declared, s.declared},
	} {
		if len(p.data) == 0 {
			continue
		}
		if err := p.ledger.UnmarshalBinary(p.data); err != nil {
			return fmt.Errorf("verify: restoring ledger: %w", err)
		}
	}

	for seq, ns := range st.EmittedAt {
		s.emittedAt[seq] = time.Unix(0, ns)
	}
	for seq, ns := range st.ProvisionalAt {
		s.provisionalAt[seq] = time.Unix(0, ns)
	}
	s.falseMissing = st.FalseMissing
	s.lateArrivals = append([]uint64(nil), st.LateArrivals...)
	return nil
}

// SaveState writes the verifier's state to the store.
func (v *Verifier) SaveState(store Store) error {
	if store == nil {
		return fmt.Errorf("verify: no state store")
	}

	v.mu.Lock()
	streams := make([]*stream, 0, len(v.streams))
	for _, s := range v.streams {
		streams = append(streams, s)
	}
	invalid := v.invalidLines
	v.mu.Unlock()

	snap := Snapshot{InvalidLines: invalid}

	for _, s := range streams {
		arrived, settled, rechecked, declared, emittedAt, provisionalAt, falseMissing, late, err := s.scheduler.exportState()
		if err != nil {
			return err
		}

		s.mu.Lock()
		dups := make(map[uint64]int, len(s.duplicates))
		for k, n := range s.duplicates {
			dups[k] = n
		}
		partial := make([]uint64, 0, len(s.partial))
		for seq := range s.partial {
			partial = append(partial, seq)
		}
		st := StreamState{
			Src: s.key.src, Epoch: s.key.epoch, Sink: s.key.sink,
			Expected: s.expected, Truncated: s.truncated,
		}
		s.mu.Unlock()

		sort.Slice(partial, func(i, j int) bool { return partial[i] < partial[j] })

		st.Arrived, st.Settled, st.Rechecked, st.Declared = arrived, settled, rechecked, declared
		st.EmittedAt, st.ProvisionalAt = emittedAt, provisionalAt
		st.FalseMissing, st.LateArrivals = falseMissing, late
		st.Duplicates, st.Partial = dups, partial

		snap.Streams = append(snap.Streams, st)
	}

	sort.Slice(snap.Streams, func(i, j int) bool {
		a, b := snap.Streams[i], snap.Streams[j]
		if a.Src != b.Src {
			return a.Src < b.Src
		}
		if a.Epoch != b.Epoch {
			return a.Epoch < b.Epoch
		}
		return a.Sink < b.Sink
	})

	return store.Save(snap)
}

// LoadState restores the verifier from the store.
//
// A missing snapshot is not an error: a first run has nothing to resume from.
// A snapshot this build cannot read is also not fatal, because re-deriving is
// always available; it is reported so the operator knows a sweep will be
// needed before verdicts are trustworthy again.
func (v *Verifier) LoadState(store Store) error {
	if store == nil {
		return fmt.Errorf("verify: no state store")
	}

	snap, err := store.Load()
	if err != nil {
		return err
	}

	for _, st := range snap.Streams {
		key := streamKey{src: st.Src, epoch: st.Epoch, sink: st.Sink}
		s := v.streamFor(key)

		if err := s.scheduler.importState(st); err != nil {
			return err
		}

		s.mu.Lock()
		s.expected = st.Expected
		s.truncated = st.Truncated
		for k, n := range st.Duplicates {
			s.duplicates[k] = n
		}
		for _, seq := range st.Partial {
			s.partial[seq] = true
		}
		s.mu.Unlock()
	}

	v.mu.Lock()
	v.invalidLines = snap.InvalidLines
	v.mu.Unlock()
	return nil
}

// StateSummary describes what a snapshot holds, for doctor and for logs.
func (s Snapshot) StateSummary() string {
	var arrived uint64
	for _, st := range s.Streams {
		arrived += uint64(len(st.EmittedAt))
	}
	return fmt.Sprintf("snapshot v%d saved %s: %d streams, %d sequences still awaiting judgement",
		s.Version, s.SavedAt.Format(time.RFC3339), len(s.Streams), arrived)
}
