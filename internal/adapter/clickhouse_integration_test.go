package adapter

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// These tests run against a real ClickHouse server, which is the only thing
// that can settle whether the adapter's SQL means what the unit tests assume.
//
// The unit tests use a double that implements the same statements with the
// same semantics. That proves the adapter builds correct SQL and parses the
// response format. It cannot prove ClickHouse agrees, because the double was
// written from the same understanding as the adapter and would share any
// misunderstanding.
//
// Skipped unless LOSSLEDGER_CH_ADDR is set, so the ordinary suite and CI need
// no Docker:
//
//	docker compose -f testbed/vector-clickhouse.compose.yaml up -d
//	LOSSLEDGER_CH_ADDR=http://127.0.0.1:18123 go test ./internal/adapter/ -run Integration
const chAddrEnv = "LOSSLEDGER_CH_ADDR"

func liveClickHouse(t *testing.T) *ClickHouse {
	t.Helper()
	addr := os.Getenv(chAddrEnv)
	if addr == "" {
		t.Skipf("set %s to run against a real server", chAddrEnv)
	}

	sink, err := NewClickHouse(ClickHouseConfig{
		Addr:     addr,
		Table:    "lossledger_canary",
		User:     envOr("LOSSLEDGER_CH_USER", "lossledger"),
		Password: envOr("LOSSLEDGER_CH_PASSWORD", "lossledger"),
		Settle:   2 * time.Second,
		Timeout:  30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Doctor(context.Background()); err != nil {
		t.Fatalf("the server at %s is not usable: %v", addr, err)
	}
	return sink
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// chInsert writes rows directly, standing in for the pipeline. The suite never
// writes through the adapter, which has no write method by design.
func chInsert(t *testing.T, addr string, rows []string) {
	t.Helper()
	if len(rows) == 0 {
		return
	}
	body := "INSERT INTO lossledger_canary (src, ep, seq, ts, shape, kind, cont_index, cont_total) VALUES " +
		strings.Join(rows, ",")

	req, err := http.NewRequest(http.MethodPost, addr+"/", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(envOr("LOSSLEDGER_CH_USER", "lossledger"), envOr("LOSSLEDGER_CH_PASSWORD", "lossledger"))

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf := make([]byte, 2048)
		n, _ := resp.Body.Read(buf)
		t.Fatalf("insert returned %d: %s", resp.StatusCode, buf[:n])
	}
}

// TestClickHouseIntegrationConformance runs the shared specification against a
// real server. This is the test that upgrades the adapter from "passes against
// a double" to "verified".
func TestClickHouseIntegrationConformance(t *testing.T) {
	sink := liveClickHouse(t)
	addr := os.Getenv(chAddrEnv)

	Conformance{
		Sink: sink,
		// Each case gets its own epoch, so the suite does not need a clean
		// database and will not disturb data a run already produced.
		Src: fmt.Sprintf("conformance-%d", time.Now().UnixNano()),
		Seed: func(ctx context.Context, src, epoch string, seqs []uint64, dups map[uint64]int) error {
			var rows []string
			for _, seq := range seqs {
				n := 1
				if d, ok := dups[seq]; ok {
					n = d
				}
				for i := 0; i < n; i++ {
					rows = append(rows, fmt.Sprintf("('%s','%s',%d,now64(9),'plain','canary',0,0)", src, epoch, seq))
				}
			}
			chInsert(t, addr, rows)
			// ClickHouse acknowledges the insert before the part is necessarily
			// visible to every replica. A single node is immediately
			// consistent, but a brief wait keeps the suite honest if this is
			// ever pointed at a cluster.
			time.Sleep(300 * time.Millisecond)
			return nil
		},
	}.Run(t)
}

// TestClickHouseIntegrationAgainstPipelineData verifies the adapter against
// rows a real Vector produced, rather than rows the test wrote. The difference
// matters: Vector decides the column types and the timestamp encoding, and a
// mismatch there would be invisible to a test that inserts its own rows.
func TestClickHouseIntegrationAgainstPipelineData(t *testing.T) {
	sink := liveClickHouse(t)
	ctx := context.Background()

	src := os.Getenv("LOSSLEDGER_CH_SRC")
	epoch := os.Getenv("LOSSLEDGER_CH_EPOCH")
	if src == "" || epoch == "" {
		t.Skip("set LOSSLEDGER_CH_SRC and LOSSLEDGER_CH_EPOCH to a run the pipeline produced")
	}

	state, err := sink.EpochState(ctx, src, epoch)
	if err != nil {
		t.Fatalf("EpochState: %v", err)
	}
	t.Logf("epoch state: hasFinal=%v finalSeq=%d maxSeq=%d", state.HasFinal, state.FinalSeq, state.MaxSeq)
	if state.MaxSeq == 0 {
		t.Fatal("the store holds no canary rows for this source and epoch")
	}

	upTo := state.MaxSeq
	if state.HasFinal && state.FinalSeq > upTo {
		upTo = state.FinalSeq
	}

	distinct, total, err := sink.CountRange(ctx, src, epoch, 1, upTo)
	if err != nil {
		t.Fatalf("CountRange: %v", err)
	}
	t.Logf("counted %d distinct and %d total over 1 to %d", distinct, total, upTo)

	if distinct > total {
		t.Errorf("distinct %d exceeds total %d, which is impossible", distinct, total)
	}

	// Bisection assumes a range split in two counts to the same total as the
	// whole. If ClickHouse disagreed with the adapter about inclusive bounds,
	// this is where it would show.
	mid := upTo / 2
	lo, _, err := sink.CountRange(ctx, src, epoch, 1, mid)
	if err != nil {
		t.Fatal(err)
	}
	hi, _, err := sink.CountRange(ctx, src, epoch, mid+1, upTo)
	if err != nil {
		t.Fatal(err)
	}
	if lo+hi != distinct {
		t.Errorf("halves count %d + %d = %d but the whole counts %d: the bounds are not inclusive the way bisection assumes",
			lo, hi, lo+hi, distinct)
	}

	// Detail has to agree with the count over the same window.
	iter, err := sink.FetchRange(ctx, src, epoch, 1, mid)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()

	var fetched uint64
	var last uint64
	var withIngest int
	for iter.Next() {
		e := iter.Entry()
		if last != 0 && e.Seq <= last {
			t.Fatalf("FetchRange is not ascending: %d after %d", e.Seq, last)
		}
		last = e.Seq
		fetched++
		if !e.IngestTS.IsZero() {
			withIngest++
		}
	}
	if err := iter.Err(); err != nil {
		t.Fatal(err)
	}
	if fetched != lo {
		t.Errorf("FetchRange returned %d entries but CountRange said %d over the same window", fetched, lo)
	}
	// Ingest timestamps drive the latency histogram and the ordering check. If
	// the column default or the encoding were wrong they would all be zero.
	if withIngest == 0 && fetched > 0 {
		t.Error("no entry carried an ingest timestamp, so latency and ordering would be blind")
	}
}
