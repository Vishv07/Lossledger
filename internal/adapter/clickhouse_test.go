package adapter

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeCH is an in-memory stand-in for ClickHouse's HTTP interface.
//
// It is not a ClickHouse emulator. It implements exactly the handful of
// statements this adapter issues, with the semantics the real server has:
// inclusive range bounds, uniqExact counting distinct sequences while count()
// counts rows, and a HAVING clause over grouped continuation rows.
//
// What this proves: the adapter builds correct SQL, binds its parameters,
// parses the response format, and satisfies the shared conformance suite.
//
// What it does not prove: that a real ClickHouse server answers these
// statements the same way. That needs the compose testbed and is not claimed
// anywhere in this package.
type fakeCH struct {
	mu   sync.Mutex
	rows []chRow

	// failWith makes the next query return this status, so error handling is
	// exercised rather than assumed.
	failWith int
	// lastParams records the bound parameters of the most recent query, which
	// is how the tests check that nothing is interpolated into the SQL.
	lastParams map[string]string
	queries    []string
}

type chRow struct {
	src, ep   string
	seq       uint64
	kind      string
	contTotal int
	ingestMS  int64
}

func newFakeCH(t *testing.T) (*fakeCH, *ClickHouse) {
	t.Helper()
	f := &fakeCH{}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)

	sink, err := NewClickHouse(ClickHouseConfig{Addr: srv.URL, Settle: time.Second})
	if err != nil {
		t.Fatalf("NewClickHouse: %v", err)
	}
	return f, sink
}

// insert adds rows the way the pipeline under test would have.
func (f *fakeCH) insert(src, ep string, seqs []uint64, dups map[uint64]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, seq := range seqs {
		n := 1
		if d, ok := dups[seq]; ok {
			n = d
		}
		for i := 0; i < n; i++ {
			f.rows = append(f.rows, chRow{
				src: src, ep: ep, seq: seq, kind: "canary",
				ingestMS: 1_700_000_000_000 + int64(seq),
			})
		}
	}
}

func (f *fakeCH) insertContinuations(src, ep string, seq uint64, arrived, declared int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := 0; i < arrived; i++ {
		f.rows = append(f.rows, chRow{src: src, ep: ep, seq: seq, kind: "continuation", contTotal: declared})
	}
}

func (f *fakeCH) insertFinal(src, ep string, finalSeq uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, chRow{src: src, ep: ep, kind: "final", contTotal: int(finalSeq)})
}

func (f *fakeCH) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	sql := q.Get("query")

	params := map[string]string{}
	for k, v := range q {
		if strings.HasPrefix(k, "param_") && len(v) > 0 {
			params[strings.TrimPrefix(k, "param_")] = v[0]
		}
	}

	f.mu.Lock()
	f.lastParams = params
	f.queries = append(f.queries, sql)
	fail := f.failWith
	f.failWith = 0
	rows := append([]chRow(nil), f.rows...)
	f.mu.Unlock()

	if fail != 0 {
		w.WriteHeader(fail)
		fmt.Fprintln(w, "Code: 60. DB::Exception: synthetic failure")
		return
	}

	src, ep := params["src"], params["ep"]
	from, _ := strconv.ParseUint(params["from"], 10, 64)
	to, _ := strconv.ParseUint(params["to"], 10, 64)

	inRange := func(r chRow) bool {
		// Inclusive at both ends, as ClickHouse's BETWEEN and >= / <= are.
		return r.src == src && r.ep == ep && r.seq >= from && r.seq <= to
	}

	switch {
	case strings.Contains(sql, "WHERE 1 = 0"):
		// Doctor's column check returns no rows but must not error.
		w.WriteHeader(http.StatusOK)

	case strings.TrimSpace(sql) == "SELECT 1":
		fmt.Fprintln(w, "1")

	case strings.Contains(sql, "countIf(kind = 'final')"):
		var finals, finalSeq, maxSeq uint64
		for _, r := range rows {
			if r.src != src || r.ep != ep {
				continue
			}
			if r.kind == "final" {
				finals++
				if uint64(r.contTotal) > finalSeq {
					finalSeq = uint64(r.contTotal)
				}
			}
			if r.kind == "canary" && r.seq > maxSeq {
				maxSeq = r.seq
			}
		}
		fmt.Fprintf(w, "%d\t%d\t%d\n", finals, finalSeq, maxSeq)

	case strings.Contains(sql, "HAVING count() < max(cont_total)"):
		arrived := map[uint64]int{}
		declared := map[uint64]int{}
		for _, r := range rows {
			if r.kind != "continuation" || !inRange(r) {
				continue
			}
			arrived[r.seq]++
			if r.contTotal > declared[r.seq] {
				declared[r.seq] = r.contTotal
			}
		}
		var out []uint64
		for seq, got := range arrived {
			if got < declared[seq] {
				out = append(out, seq)
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		for _, seq := range out {
			fmt.Fprintf(w, "%d\n", seq)
		}

	case strings.Contains(sql, "uniqExact(seq), count()"):
		seen := map[uint64]bool{}
		var total uint64
		for _, r := range rows {
			if r.kind != "canary" || !inRange(r) {
				continue
			}
			seen[r.seq] = true
			total++
		}
		fmt.Fprintf(w, "%d\t%d\n", len(seen), total)

	case strings.Contains(sql, "toUnixTimestamp64Milli(max(ingest_ts))"):
		grouped := map[uint64]*struct {
			count  int
			latest int64
		}{}
		for _, r := range rows {
			if r.kind != "canary" || !inRange(r) {
				continue
			}
			g := grouped[r.seq]
			if g == nil {
				g = &struct {
					count  int
					latest int64
				}{}
				grouped[r.seq] = g
			}
			g.count++
			if r.ingestMS > g.latest {
				g.latest = r.ingestMS
			}
		}
		keys := make([]uint64, 0, len(grouped))
		for k := range grouped {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		for _, k := range keys {
			fmt.Fprintf(w, "%d\t%d\t%d\n", k, grouped[k].latest, grouped[k].count)
		}

	default:
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Code: 62. DB::Exception: unrecognised statement: %s", sql)
	}
}

// TestClickHouseConformance runs the shared specification. This is the test
// that matters: it is the same suite the reference adapter passes, so the two
// adapters are held to one standard rather than to whatever their authors
// happened to check.
func TestClickHouseConformance(t *testing.T) {
	fake, sink := newFakeCH(t)

	Conformance{
		Sink: sink,
		Seed: func(ctx context.Context, src, epoch string, seqs []uint64, dups map[uint64]int) error {
			fake.insert(src, epoch, seqs, dups)
			return nil
		},
	}.Run(t)
}

func TestClickHouseCountSeparatesDistinctFromTotal(t *testing.T) {
	fake, sink := newFakeCH(t)
	fake.insert("node-a", "ep1", seqRange(1, 50), map[uint64]int{7: 3, 9: 2})

	distinct, total, err := sink.CountRange(context.Background(), "node-a", "ep1", 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	if distinct != 50 {
		t.Errorf("distinct = %d, want 50", distinct)
	}
	// Collapsing these would hide a duplicate bug; inflating distinct by them
	// would invent loss elsewhere.
	if total != 53 {
		t.Errorf("total = %d, want 53", total)
	}
}

// TestClickHouseBindsParametersRatherThanInterpolating is a security property.
// Source and epoch identifiers come from data the pipeline carried, so a value
// that reached the SQL text could rewrite the query.
func TestClickHouseBindsParameters(t *testing.T) {
	fake, sink := newFakeCH(t)

	hostile := "a' OR 1=1 --"
	if _, _, err := sink.CountRange(context.Background(), hostile, "ep1", 1, 10); err != nil {
		t.Fatal(err)
	}

	fake.mu.Lock()
	sql := fake.queries[len(fake.queries)-1]
	params := fake.lastParams
	fake.mu.Unlock()

	if strings.Contains(sql, hostile) {
		t.Errorf("the source identifier reached the SQL text:\n%s", sql)
	}
	if params["src"] != hostile {
		t.Errorf("bound src = %q, want the raw value %q", params["src"], hostile)
	}
	if !strings.Contains(sql, "{src:String}") {
		t.Errorf("query does not use a server-side placeholder:\n%s", sql)
	}
}

func TestClickHouseRejectsBadIdentifiers(t *testing.T) {
	// Table and database names cannot be bound as parameters, so they are the
	// one place an injection could land. They are validated instead.
	cases := []string{
		"canary; DROP TABLE x",
		"canary'",
		"canary table",
		"1canary",
		"canary-table",
		"",
	}
	for _, table := range cases {
		t.Run(table, func(t *testing.T) {
			_, err := NewClickHouse(ClickHouseConfig{Addr: "http://localhost:8123", Table: table})
			if table == "" {
				// Empty falls back to the default, which is legitimate.
				if err != nil {
					t.Errorf("empty table name should default, got %v", err)
				}
				return
			}
			if err == nil {
				t.Errorf("accepted table name %q", table)
			}
		})
	}
}

func TestClickHouseDoctor(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		_, sink := newFakeCH(t)
		if err := sink.Doctor(context.Background()); err != nil {
			t.Errorf("Doctor failed against a healthy server: %v", err)
		}
	})

	t.Run("server returns an error", func(t *testing.T) {
		fake, sink := newFakeCH(t)
		fake.mu.Lock()
		fake.failWith = http.StatusInternalServerError
		fake.mu.Unlock()

		if err := sink.Doctor(context.Background()); err == nil {
			t.Error("Doctor passed against a server returning 500")
		}
	})

	t.Run("unreachable", func(t *testing.T) {
		sink, err := NewClickHouse(ClickHouseConfig{
			Addr: "http://127.0.0.1:1", Timeout: 2 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.Doctor(context.Background()); err == nil {
			t.Error("Doctor passed against an unreachable server")
		}
	})
}

func TestClickHousePartialDetection(t *testing.T) {
	fake, sink := newFakeCH(t)
	fake.insert("node-a", "ep1", seqRange(1, 10), nil)
	// 12 continuation rows declared, only 3 arrived.
	fake.insertContinuations("node-a", "ep1", 4, 3, 12)
	// A whole one, which must not be flagged.
	fake.insertContinuations("node-a", "ep1", 5, 8, 8)

	got, err := sink.PartialSeqs(context.Background(), "node-a", "ep1", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 4 {
		t.Errorf("PartialSeqs = %v, want [4]", got)
	}

	// The same fact must reach the verifier through FetchRange, which is where
	// it is actually consumed.
	iter, err := sink.FetchRange(context.Background(), "node-a", "ep1", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	for iter.Next() {
		e := iter.Entry()
		if e.Seq == 4 && !e.Partial {
			t.Error("sequence 4 arrived with a truncated body but was not flagged partial")
		}
		if e.Seq == 5 && e.Partial {
			t.Error("sequence 5 arrived whole but was flagged partial")
		}
	}
}

func TestClickHouseEpochState(t *testing.T) {
	fake, sink := newFakeCH(t)
	fake.insert("node-a", "crash", seqRange(1, 40), nil)
	fake.insert("node-a", "clean", seqRange(1, 25), nil)
	fake.insertFinal("node-a", "clean", 25)

	t.Run("clean shutdown", func(t *testing.T) {
		st, err := sink.EpochState(context.Background(), "node-a", "clean")
		if err != nil {
			t.Fatal(err)
		}
		if !st.HasFinal || st.FinalSeq != 25 {
			t.Errorf("EpochState = %+v, want a final line at 25", st)
		}
	})

	t.Run("crash", func(t *testing.T) {
		st, err := sink.EpochState(context.Background(), "node-a", "crash")
		if err != nil {
			t.Fatal(err)
		}
		// Without a final line nothing past the highest observed sequence may
		// be called missing, so MaxSeq is what the verifier needs.
		if st.HasFinal {
			t.Error("a crashed epoch was reported as cleanly closed")
		}
		if st.MaxSeq != 40 {
			t.Errorf("MaxSeq = %d, want 40", st.MaxSeq)
		}
	})
}

func TestClickHouseServerErrorSurfaces(t *testing.T) {
	fake, sink := newFakeCH(t)
	fake.mu.Lock()
	fake.failWith = http.StatusServiceUnavailable
	fake.mu.Unlock()

	// A store that is down must read as unavailable, never as zero rows. Zero
	// rows would be reported as total loss against a healthy pipeline.
	if _, _, err := sink.CountRange(context.Background(), "a", "ep", 1, 10); err == nil {
		t.Fatal("CountRange returned success when the server answered 503")
	}
}

func TestClickHouseContextCancellation(t *testing.T) {
	_, sink := newFakeCH(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := sink.CountRange(ctx, "a", "ep", 1, 10); err == nil {
		t.Error("CountRange ignored a cancelled context")
	}
	if _, err := sink.FetchRange(ctx, "a", "ep", 1, 10); err == nil {
		t.Error("FetchRange ignored a cancelled context")
	}
}

func TestClickHouseInvertedRangeIsEmpty(t *testing.T) {
	_, sink := newFakeCH(t)
	d, total, err := sink.CountRange(context.Background(), "a", "ep", 100, 1)
	if err != nil || d != 0 || total != 0 {
		t.Errorf("inverted range = (%d, %d, %v), want (0, 0, nil)", d, total, err)
	}
}

func TestClickHouseSchemaNamesEveryColumnTheAdapterQueries(t *testing.T) {
	_, sink := newFakeCH(t)
	schema := sink.Schema()
	for _, col := range []string{"src", "ep", "seq", "ts", "shape", "kind", "cont_index", "cont_total", "ingest_ts"} {
		if !strings.Contains(schema, col) {
			t.Errorf("the shipped schema omits column %q, which Doctor requires", col)
		}
	}
}

func TestClickHouseConfigValidation(t *testing.T) {
	if _, err := NewClickHouse(ClickHouseConfig{}); err == nil {
		t.Error("accepted a config with no address")
	}
	sink, err := NewClickHouse(ClickHouseConfig{Addr: "http://localhost:8123"})
	if err != nil {
		t.Fatal(err)
	}
	if sink.Name() != "clickhouse" {
		t.Errorf("default name = %q", sink.Name())
	}
	if sink.SettleHint() != defaultCHSettle {
		t.Errorf("default settle = %v, want %v", sink.SettleHint(), defaultCHSettle)
	}
}
