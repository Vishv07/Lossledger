package adapter

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/Vishv07/lossledger/internal/wire"
)

// maxLineBytes bounds the scanner so an oversize profile (64 KB by default)
// reads whole, with headroom for a collector that wrapped it in JSON.
const maxLineBytes = 4 * 1024 * 1024

// FileSink reads a directory of files as if it were a log store.
//
// It exists so the self-validation gate (PRD section 10.2) runs with no
// external dependencies at all. If the harness cannot be trusted against a
// store whose contents we can read directly off disk, it cannot be trusted
// against Elasticsearch either, and we would rather learn that in a unit test
// than in a published finding.
type FileSink struct {
	name string
	dir  string
	key  []byte

	// settle is reported as this sink's consistency floor. A directory is
	// immediately consistent once a line is flushed, so the floor is small but
	// not zero: the writer may be mid-flush when we scan.
	settle time.Duration

	mu    sync.Mutex
	cache *scan
}

// scan is one snapshot of the directory, keyed by source and epoch.
type scan struct {
	takenAt time.Time
	fprint  string
	streams map[string]*stream // key is src + "|" + epoch
	invalid uint64
}

type stream struct {
	entries map[uint64]*Entry
	// contSeen counts continuation markers observed per parent sequence, which
	// is how a partially delivered multi-line event is detected.
	contSeen map[uint64]int
	// contWant is the continuation total each parent declared. Fewer arrivals
	// than this means continuation lines were lost.
	contWant map[uint64]int
	finalSeq uint64
	hasFinal bool
	maxSeq   uint64
}

// NewFileSink builds a reference sink over dir. The key must match the
// emitter's: lines failing verification are counted invalid, never ingested.
func NewFileSink(name, dir string, key []byte) (*FileSink, error) {
	if name == "" {
		name = "filesink"
	}
	if len(key) == 0 {
		return nil, wire.ErrNoKey
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("adapter: resolving %s: %w", dir, err)
	}
	return &FileSink{name: name, dir: abs, key: key, settle: 250 * time.Millisecond}, nil
}

func (f *FileSink) Name() string { return f.name }

func (f *FileSink) SettleHint() time.Duration { return f.settle }

// SetSettleHint overrides the reported consistency floor. Tests use it to make
// settle behaviour observable without waiting.
func (f *FileSink) SetSettleHint(d time.Duration) { f.settle = d }

func (f *FileSink) Close() error { return nil }

// Doctor checks the directory is readable. A missing directory is a
// configuration error worth failing loudly, because the alternative is
// reporting that every single sequence was lost.
func (f *FileSink) Doctor(ctx context.Context) error {
	info, err := os.Stat(f.dir)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrUnavailable, f.dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: %s is not a directory", ErrUnavailable, f.dir)
	}
	if _, err := os.ReadDir(f.dir); err != nil {
		return fmt.Errorf("%w: reading %s: %v", ErrUnavailable, f.dir, err)
	}
	return nil
}

// fingerprint summarises the directory's files, their sizes and modification
// times, so a scan can be reused while nothing has changed.
func (f *FileSink) fingerprint() (string, error) {
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return "", err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		names = append(names, fmt.Sprintf("%s:%d:%d", e.Name(), info.Size(), info.ModTime().UnixNano()))
	}
	sort.Strings(names)
	return fmt.Sprint(names), nil
}

// snapshot returns a current scan, reusing the cached one when the directory is
// unchanged.
func (f *FileSink) snapshot() (*scan, error) {
	fprint, err := f.fingerprint()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if f.cache != nil && f.cache.fprint == fprint {
		return f.cache, nil
	}

	s := &scan{takenAt: time.Now(), fprint: fprint, streams: map[string]*stream{}}

	entries, err := os.ReadDir(f.dir)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := f.scanFile(filepath.Join(f.dir, e.Name()), s); err != nil {
			return nil, err
		}
	}

	f.cache = s
	return s, nil
}

func (f *FileSink) scanFile(path string, s *scan) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// A rotation can remove a file between listing and opening. That
			// is the pipeline behaving normally, not an error.
			return nil
		}
		return fmt.Errorf("%w: opening %s: %v", ErrUnavailable, path, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil
	}
	ingest := info.ModTime()

	sc := bufio.NewScanner(file)
	sc.Buffer(make([]byte, 0, 64*1024), maxLineBytes)

	for sc.Scan() {
		tok, err := wire.Extract(sc.Text())
		if err != nil {
			continue // not one of ours
		}
		if !tok.Verify(f.key) {
			// A line that looks like a canary but is not signed by us never
			// enters the ledger. Counting it would let anyone pollute a run.
			s.invalid++
			continue
		}

		key := tok.Src + "|" + tok.Epoch
		st := s.streams[key]
		if st == nil {
			st = &stream{
				entries:  map[uint64]*Entry{},
				contSeen: map[uint64]int{},
				contWant: map[uint64]int{},
			}
			s.streams[key] = st
		}

		switch tok.Kind {
		case wire.KindCanary:
			if existing, ok := st.entries[tok.Seq]; ok {
				existing.DupCount++
				continue
			}
			st.entries[tok.Seq] = &Entry{Seq: tok.Seq, IngestTS: ingest, DupCount: 1}
			if tok.Seq > st.maxSeq {
				st.maxSeq = tok.Seq
			}
		case wire.KindContinuation:
			st.contSeen[tok.Seq]++
			// The expected total travels on the line itself. Falling back to
			// the highest index seen would make a truncated event look whole,
			// since the withheld lines are exactly the ones we cannot see.
			want := tok.ContTotal
			if want == 0 {
				want = tok.ContIndex
			}
			if want > st.contWant[tok.Seq] {
				st.contWant[tok.Seq] = want
			}
		case wire.KindFinal:
			st.hasFinal = true
			if tok.Final > st.finalSeq {
				st.finalSeq = tok.Final
			}
		}
	}

	if err := sc.Err(); err != nil {
		return fmt.Errorf("%w: scanning %s: %v", ErrUnavailable, path, err)
	}
	return nil
}

// markPartials flags events whose continuation lines did not all arrive.
func (st *stream) markPartials() {
	for seq, want := range st.contWant {
		if e, ok := st.entries[seq]; ok && st.contSeen[seq] < want {
			e.Partial = true
		}
	}
}

// CountRange implements the cheap path.
func (f *FileSink) CountRange(ctx context.Context, src, epoch string, from, to uint64) (uint64, uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	s, err := f.snapshot()
	if err != nil {
		return 0, 0, err
	}
	st := s.streams[src+"|"+epoch]
	if st == nil {
		return 0, 0, nil
	}

	var distinct, total uint64
	// Iterating the range rather than the map keeps cost proportional to the
	// window under test, which is what the bisection strategy assumes.
	if to-from < uint64(len(st.entries)) {
		for seq := from; seq <= to; seq++ {
			if e, ok := st.entries[seq]; ok {
				distinct++
				total += uint64(e.DupCount)
			}
		}
	} else {
		for seq, e := range st.entries {
			if seq >= from && seq <= to {
				distinct++
				total += uint64(e.DupCount)
			}
		}
	}
	return distinct, total, nil
}

// fileIter walks a materialised slice of entries.
type fileIter struct {
	entries []Entry
	idx     int
}

func (it *fileIter) Next() bool {
	if it.idx >= len(it.entries) {
		return false
	}
	it.idx++
	return true
}
func (it *fileIter) Entry() Entry { return it.entries[it.idx-1] }
func (it *fileIter) Err() error   { return nil }
func (it *fileIter) Close() error { return nil }

// FetchRange returns per-sequence detail, ordered by sequence.
func (f *FileSink) FetchRange(ctx context.Context, src, epoch string, from, to uint64) (SeqIter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	s, err := f.snapshot()
	if err != nil {
		return nil, err
	}
	st := s.streams[src+"|"+epoch]
	if st == nil {
		return &fileIter{}, nil
	}
	st.markPartials()

	out := make([]Entry, 0, to-from+1)
	for seq := from; seq <= to; seq++ {
		if e, ok := st.entries[seq]; ok {
			out = append(out, *e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return &fileIter{entries: out}, nil
}

// PartialSeqs implements PartialReporter.
//
// The reference adapter can answer this directly because it holds every
// continuation marker it scanned. A real store answers the same question with a
// grouped count of LLC rows per parent sequence.
func (f *FileSink) PartialSeqs(ctx context.Context, src, epoch string, from, to uint64) ([]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	s, err := f.snapshot()
	if err != nil {
		return nil, err
	}
	st := s.streams[src+"|"+epoch]
	if st == nil {
		return nil, nil
	}
	st.markPartials()

	var out []uint64
	for seq, entry := range st.entries {
		if entry.Partial && seq >= from && seq <= to {
			out = append(out, seq)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// EpochState is what the verifier needs to know about an epoch's termination.
type EpochState struct {
	HasFinal bool
	FinalSeq uint64
	MaxSeq   uint64
}

// EpochState reports whether the epoch was closed cleanly and where it ended.
//
// This is not part of the Sink interface because real stores learn it the same
// way they learn everything else: by querying for the final line. The reference
// adapter exposes it directly so the verifier's epoch logic can be tested
// without a store.
func (f *FileSink) EpochState(src, epoch string) EpochState {
	f.mu.Lock()
	defer f.mu.Unlock()

	s, err := f.snapshot()
	if err != nil {
		return EpochState{}
	}
	st := s.streams[src+"|"+epoch]
	if st == nil {
		return EpochState{}
	}
	return EpochState{HasFinal: st.hasFinal, FinalSeq: st.finalSeq, MaxSeq: st.maxSeq}
}

// InvalidLines reports how many signature failures this sink has seen, which
// feeds lossledger_invalid_lines_total.
func (f *FileSink) InvalidLines() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.snapshot()
	if err != nil {
		return 0
	}
	return s.invalid
}

// Streams lists the (src, epoch) pairs this sink currently holds.
func (f *FileSink) Streams() [][2]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.snapshot()
	if err != nil {
		return nil
	}
	out := make([][2]string, 0, len(s.streams))
	for key := range s.streams {
		for i := 0; i < len(key); i++ {
			if key[i] == '|' {
				out = append(out, [2]string{key[:i], key[i+1:]})
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i][0] != out[j][0] {
			return out[i][0] < out[j][0]
		}
		return out[i][1] < out[j][1]
	})
	return out
}
