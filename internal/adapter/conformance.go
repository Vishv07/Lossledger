package adapter

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// Conformance is one specification every sink adapter must satisfy.
//
// A verdict is only as trustworthy as the adapter that produced it. An adapter
// that miscounts a range, or that reports a settle hint shorter than its store
// actually needs, turns this project into a machine for libelling collectors.
// So the behaviour is pinned here once and every adapter is run against it,
// rather than each adapter being spot-checked by whoever wrote it.
//
// The suite is deliberately usable from an ordinary test:
//
//	func TestClickHouseConformance(t *testing.T) {
//	    Conformance{Sink: sink, Seed: seedFn}.Run(t)
//	}
type Conformance struct {
	// Sink is the adapter under test.
	Sink Sink

	// Seed writes the given sequences into the store so the suite can ask for
	// them back. It must be synchronous: by the time it returns, the store has
	// accepted the rows. Duplicates maps a sequence to how many copies to
	// write, and omitted sequences are written exactly once.
	//
	// The suite never writes to a store itself, because the Sink interface
	// deliberately has no write method.
	Seed func(ctx context.Context, src, epoch string, seqs []uint64, duplicates map[uint64]int) error

	// Src and Epoch namespace the suite's data. Leave empty for defaults.
	Src   string
	Epoch string
}

// T is the subset of testing.TB the suite uses, so the suite itself can be
// tested without a real test runner.
type T interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
}

const (
	defaultSrc = "conformance-src"

	// epochBase is 22 characters of a ULID. Each case appends four more to get
	// its own 26-character epoch. The suffix has to stay inside Crockford
	// base32 or the verifier rejects every line as malformed, which would make
	// a working adapter look like it was losing everything.
	epochBase = "01JCV00000000000000000"
)

// crockford is the ULID alphabet, with the letters that look like digits removed.
const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// caseEpoch returns a distinct, valid epoch for one case, so the cases cannot
// contaminate each other through a shared stream.
func caseEpoch(base string, n int) string {
	if len(base) >= 26 {
		base = base[:22]
	}
	for len(base) < 22 {
		base += "0"
	}
	suffix := make([]byte, 4)
	for i := 3; i >= 0; i-- {
		suffix[i] = crockford[n%32]
		n /= 32
	}
	return base + string(suffix)
}

// Run executes every case. It is the single definition of what an adapter must
// do, so a change here is a change to the contract and needs the same scrutiny
// as a change to the anomaly taxonomy.
func (c Conformance) Run(t T) {
	t.Helper()

	if c.Sink == nil {
		t.Fatalf("conformance: no sink under test")
		return
	}
	if c.Seed == nil {
		t.Fatalf("conformance: no seed function; the suite cannot write to a store itself")
		return
	}
	src, epoch := c.Src, c.Epoch
	if src == "" {
		src = defaultSrc
	}
	if epoch == "" {
		epoch = epochBase
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	c.checkDoctor(ctx, t)
	c.checkSettleHint(t)
	c.checkEmptyStore(ctx, t, src, epoch)
	c.checkExactCount(ctx, t, src, epoch)
	c.checkGapsAreVisible(ctx, t, src, epoch)
	c.checkDuplicatesSeparateFromDistinct(ctx, t, src, epoch)
	c.checkFetchIsOrderedAndComplete(ctx, t, src, epoch)
	c.checkRangeBoundariesAreInclusive(ctx, t, src, epoch)
	c.checkUnknownStreamIsEmptyNotError(ctx, t, epoch)
	c.checkContextCancellation(t, src, epoch)
}

// checkDoctor: an adapter that cannot see its own canary fields would report
// total loss, so Doctor has to verify queryability rather than just reachability.
func (c Conformance) checkDoctor(ctx context.Context, t T) {
	t.Helper()
	if err := c.Sink.Doctor(ctx); err != nil {
		t.Errorf("Doctor failed against a store the suite expects to be healthy: %v", err)
	}
	if c.Sink.Name() == "" {
		t.Errorf("Name is empty; metrics and reports label every finding with it")
	}
}

// checkSettleHint: a hint of zero claims the store is immediately consistent.
// Almost none are, and understating it is how a harness accuses a store of
// losing a row it had not finished writing.
func (c Conformance) checkSettleHint(t T) {
	t.Helper()
	if h := c.Sink.SettleHint(); h < 0 {
		t.Errorf("SettleHint is negative (%v)", h)
	}
}

func (c Conformance) checkEmptyStore(ctx context.Context, t T, src, epoch string) {
	t.Helper()
	// A range nobody has written to holds nothing. Reporting an error here
	// would make a fresh epoch look like a broken sink.
	// A valid but unused epoch. Using a malformed one would pass this check for
	// the wrong reason: the verifier would reject the lines rather than the
	// store reporting them absent.
	distinct, total, err := c.Sink.CountRange(ctx, src, caseEpoch(epoch, 31), 1, 1000)
	if err != nil {
		t.Errorf("CountRange on an unseeded epoch returned an error: %v", err)
		return
	}
	if distinct != 0 || total != 0 {
		t.Errorf("CountRange on an unseeded epoch = (%d, %d), want (0, 0)", distinct, total)
	}
}

func (c Conformance) checkExactCount(ctx context.Context, t T, src, epoch string) {
	t.Helper()
	ep := caseEpoch(epoch, 1)
	seqs := seqRange(1, 500)
	if err := c.Seed(ctx, src, ep, seqs, nil); err != nil {
		t.Fatalf("seeding: %v", err)
		return
	}

	distinct, total, err := c.Sink.CountRange(ctx, src, ep, 1, 500)
	if err != nil {
		t.Errorf("CountRange: %v", err)
		return
	}
	// This is the cheap path the whole cost model rests on: a complete range
	// must count exactly, with no duplicate inflation, or the verifier bisects
	// a range that was never broken.
	if distinct != 500 {
		t.Errorf("distinct over a complete range = %d, want 500", distinct)
	}
	if total != 500 {
		t.Errorf("total over a complete range = %d, want 500", total)
	}
}

func (c Conformance) checkGapsAreVisible(ctx context.Context, t T, src, epoch string) {
	t.Helper()
	ep := caseEpoch(epoch, 2)
	// Three gaps: a single, a short run, and one at the top of the range.
	var seqs []uint64
	missing := map[uint64]bool{7: true, 40: true, 41: true, 42: true, 200: true}
	for s := uint64(1); s <= 200; s++ {
		if !missing[s] {
			seqs = append(seqs, s)
		}
	}
	if err := c.Seed(ctx, src, ep, seqs, nil); err != nil {
		t.Fatalf("seeding: %v", err)
		return
	}

	distinct, _, err := c.Sink.CountRange(ctx, src, ep, 1, 200)
	if err != nil {
		t.Errorf("CountRange: %v", err)
		return
	}
	if want := uint64(len(seqs)); distinct != want {
		t.Errorf("distinct over a gapped range = %d, want %d; the adapter is not seeing the gaps", distinct, want)
	}
}

func (c Conformance) checkDuplicatesSeparateFromDistinct(ctx context.Context, t T, src, epoch string) {
	t.Helper()
	ep := caseEpoch(epoch, 3)
	seqs := seqRange(1, 100)
	dups := map[uint64]int{10: 3, 20: 2}

	if err := c.Seed(ctx, src, ep, seqs, dups); err != nil {
		t.Fatalf("seeding: %v", err)
		return
	}

	distinct, total, err := c.Sink.CountRange(ctx, src, ep, 1, 100)
	if err != nil {
		t.Errorf("CountRange: %v", err)
		return
	}
	// Collapsing duplicates into distinct would hide a DUP_BUG; inflating
	// distinct by them would invent loss elsewhere. Both numbers are required
	// and they must disagree by exactly the duplicate rows.
	if distinct != 100 {
		t.Errorf("distinct with duplicates present = %d, want 100", distinct)
	}
	if want := uint64(100 + 2 + 1); total != want {
		t.Errorf("total with duplicates present = %d, want %d", total, want)
	}
	if distinct > total {
		t.Errorf("distinct (%d) exceeds total (%d), which is impossible", distinct, total)
	}
}

func (c Conformance) checkFetchIsOrderedAndComplete(ctx context.Context, t T, src, epoch string) {
	t.Helper()
	ep := caseEpoch(epoch, 4)
	var seqs []uint64
	for s := uint64(1); s <= 60; s++ {
		if s != 33 {
			seqs = append(seqs, s)
		}
	}
	if err := c.Seed(ctx, src, ep, seqs, map[uint64]int{12: 2}); err != nil {
		t.Fatalf("seeding: %v", err)
		return
	}

	iter, err := c.Sink.FetchRange(ctx, src, ep, 20, 50)
	if err != nil {
		t.Errorf("FetchRange: %v", err)
		return
	}
	defer iter.Close()

	var got []uint64
	var last uint64
	for iter.Next() {
		e := iter.Entry()
		if last != 0 && e.Seq <= last {
			t.Errorf("FetchRange is not ascending: %d after %d", e.Seq, last)
		}
		if e.Seq < 20 || e.Seq > 50 {
			t.Errorf("FetchRange returned %d, outside the requested range 20 to 50", e.Seq)
		}
		if e.DupCount < 1 {
			t.Errorf("entry %d has DupCount %d; a returned entry exists at least once", e.Seq, e.DupCount)
		}
		last = e.Seq
		got = append(got, e.Seq)
	}
	if err := iter.Err(); err != nil {
		t.Errorf("FetchRange iteration: %v", err)
	}

	if want := 30; len(got) != want { // 20 to 50 inclusive is 31, minus the absent 33
		t.Errorf("FetchRange returned %d entries, want %d", len(got), want)
	}
	for _, s := range got {
		if s == 33 {
			t.Errorf("FetchRange returned sequence 33, which was never written")
		}
	}
}

func (c Conformance) checkRangeBoundariesAreInclusive(ctx context.Context, t T, src, epoch string) {
	t.Helper()
	ep := caseEpoch(epoch, 5)
	if err := c.Seed(ctx, src, ep, seqRange(1, 10), nil); err != nil {
		t.Fatalf("seeding: %v", err)
		return
	}

	// Off-by-one at a range boundary is the error that bisection multiplies:
	// a lost endpoint reappears as a phantom gap at every level of recursion.
	cases := []struct {
		name     string
		from, to uint64
		want     uint64
	}{
		{"single sequence", 5, 5, 1},
		{"lower edge", 1, 1, 1},
		{"upper edge", 10, 10, 1},
		{"whole range", 1, 10, 10},
		{"past the end", 11, 20, 0},
		{"straddling the end", 8, 12, 3},
	}
	for _, tc := range cases {
		distinct, _, err := c.Sink.CountRange(ctx, src, ep, tc.from, tc.to)
		if err != nil {
			t.Errorf("CountRange(%s): %v", tc.name, err)
			continue
		}
		if distinct != tc.want {
			t.Errorf("CountRange(%s) over [%d, %d] = %d, want %d (ranges are inclusive at both ends)",
				tc.name, tc.from, tc.to, distinct, tc.want)
		}
	}
}

func (c Conformance) checkUnknownStreamIsEmptyNotError(ctx context.Context, t T, epoch string) {
	t.Helper()
	// A source the store has never heard of is a legitimate state during a
	// rollout. It is zero rows, not a failure.
	if _, _, err := c.Sink.CountRange(ctx, "no-such-source", epoch, 1, 10); err != nil {
		t.Errorf("CountRange for an unknown source returned an error rather than zero: %v", err)
	}
	iter, err := c.Sink.FetchRange(ctx, "no-such-source", epoch, 1, 10)
	if err != nil {
		t.Errorf("FetchRange for an unknown source returned an error rather than an empty iterator: %v", err)
		return
	}
	defer iter.Close()
	if iter.Next() {
		t.Errorf("FetchRange for an unknown source returned an entry")
	}
}

func (c Conformance) checkContextCancellation(t T, src, epoch string) {
	t.Helper()
	// A verifier shutting down mid-sweep must not hang on a slow store.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := c.Sink.CountRange(ctx, src, epoch, 1, 10); err == nil {
		t.Errorf("CountRange ignored a cancelled context")
	}
	if _, err := c.Sink.FetchRange(ctx, src, epoch, 1, 10); err == nil {
		t.Errorf("FetchRange ignored a cancelled context")
	}
}

// seqRange lists every sequence in [from, to].
func seqRange(from, to uint64) []uint64 {
	if to < from {
		return nil
	}
	out := make([]uint64, 0, to-from+1)
	for s := from; s <= to; s++ {
		out = append(out, s)
	}
	return out
}

// SortedKeys returns a map's keys in ascending order, so adapters that need a
// stable iteration order for their own queries have one.
func SortedKeys(m map[uint64]int) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Describe summarises what the suite checks, for the adapter guide.
func (c Conformance) Describe() string {
	return fmt.Sprintf("conformance suite against sink %q: doctor, settle hint, empty store, "+
		"exact count, gap visibility, duplicate separation, fetch ordering, inclusive bounds, "+
		"unknown stream, context cancellation", c.Sink.Name())
}
