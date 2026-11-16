package adapter

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ClickHouse queries a ClickHouse table holding canary rows.
//
// It speaks the HTTP interface rather than the native protocol, which keeps the
// project free of a database driver dependency and makes the adapter testable
// against a server that really answers the queries. Every value is passed as a
// server-side query parameter, so nothing this adapter sends is built by string
// concatenation.
//
// The verifier is read-only by construction: the Sink interface has no write
// method, and nothing here issues anything but SELECT.
type ClickHouse struct {
	name  string
	addr  string // base URL, for example http://localhost:8123
	table string
	db    string

	user     string
	password string

	settle time.Duration
	client *http.Client
}

// ClickHouseConfig configures the adapter.
type ClickHouseConfig struct {
	// Name labels this sink in metrics and reports. Defaults to "clickhouse".
	Name string

	// Addr is the HTTP endpoint, for example http://localhost:8123.
	Addr string

	// Database and Table locate the canary rows. Table defaults to
	// lossledger_canary.
	Database string
	Table    string

	User     string
	Password string

	// Settle is the consistency floor reported to the verifier.
	//
	// ClickHouse async inserts flush on a timer (async_insert_busy_timeout_ms,
	// one second by default), and a distributed table adds more. Reporting a
	// floor shorter than the server actually needs is how a harness accuses a
	// store of losing a row it had not finished writing, so the default here is
	// deliberately generous.
	Settle time.Duration

	// Timeout bounds a single query.
	Timeout time.Duration
}

const (
	defaultCHTable   = "lossledger_canary"
	defaultCHSettle  = 5 * time.Second
	defaultCHTimeout = 30 * time.Second
)

// NewClickHouse builds the adapter. It performs no I/O; call Doctor for that.
func NewClickHouse(cfg ClickHouseConfig) (*ClickHouse, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("adapter: clickhouse needs an address")
	}
	if _, err := url.Parse(cfg.Addr); err != nil {
		return nil, fmt.Errorf("adapter: clickhouse address %q: %w", cfg.Addr, err)
	}
	if cfg.Name == "" {
		cfg.Name = "clickhouse"
	}
	if cfg.Table == "" {
		cfg.Table = defaultCHTable
	}
	if cfg.Settle <= 0 {
		cfg.Settle = defaultCHSettle
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultCHTimeout
	}
	// A table name reaches the query as an identifier rather than a parameter,
	// so it is validated here instead of being trusted.
	if err := validIdentifier(cfg.Table); err != nil {
		return nil, fmt.Errorf("adapter: clickhouse table: %w", err)
	}
	if cfg.Database != "" {
		if err := validIdentifier(cfg.Database); err != nil {
			return nil, fmt.Errorf("adapter: clickhouse database: %w", err)
		}
	}

	return &ClickHouse{
		name:     cfg.Name,
		addr:     strings.TrimSuffix(cfg.Addr, "/"),
		table:    cfg.Table,
		db:       cfg.Database,
		user:     cfg.User,
		password: cfg.Password,
		settle:   cfg.Settle,
		client:   &http.Client{Timeout: cfg.Timeout},
	}, nil
}

// validIdentifier rejects anything that is not a plain SQL identifier.
// ClickHouse has no placeholder for table names, so this is the guard.
func validIdentifier(s string) error {
	if s == "" {
		return fmt.Errorf("empty")
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := c == '_' ||
			(c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9')
		if !ok {
			return fmt.Errorf("%q contains %q, which is not allowed in an identifier", s, string(c))
		}
	}
	if s[0] >= '0' && s[0] <= '9' {
		return fmt.Errorf("%q starts with a digit", s)
	}
	return nil
}

func (c *ClickHouse) Name() string              { return c.name }
func (c *ClickHouse) SettleHint() time.Duration { return c.settle }
func (c *ClickHouse) Close() error              { c.client.CloseIdleConnections(); return nil }

// qualified returns the table reference used in queries.
func (c *ClickHouse) qualified() string {
	if c.db == "" {
		return c.table
	}
	return c.db + "." + c.table
}

// query runs sql with the given parameters and returns the response body as
// tab separated rows.
//
// Parameters travel as param_<name> and are referenced in the SQL as
// {name:Type}, which is ClickHouse's server-side binding. No value is ever
// interpolated into the statement.
func (c *ClickHouse) query(ctx context.Context, sql string, params map[string]string) ([][]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	q := url.Values{}
	q.Set("query", sql)
	q.Set("default_format", "TabSeparated")
	for k, v := range params {
		q.Set("param_"+k, v)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr+"/?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("%w: building request: %v", ErrUnavailable, err)
	}
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		// A cancelled context surfaces as the context error rather than as an
		// availability problem, so a verifier shutting down mid-sweep does not
		// look like a store outage.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer resp.Body.Close()

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)

	var rows [][]string
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		rows = append(rows, strings.Split(line, "\t"))
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading response: %v", ErrUnavailable, err)
	}

	if resp.StatusCode != http.StatusOK {
		detail := ""
		if len(rows) > 0 {
			detail = strings.Join(rows[0], " ")
		}
		return nil, fmt.Errorf("%w: clickhouse returned %d: %s", ErrUnavailable, resp.StatusCode, detail)
	}
	return rows, nil
}

// rangeParams are shared by every range query.
func rangeParams(src, epoch string, from, to uint64) map[string]string {
	return map[string]string{
		"src":  src,
		"ep":   epoch,
		"from": strconv.FormatUint(from, 10),
		"to":   strconv.FormatUint(to, 10),
	}
}

// CountRange implements the cheap path.
//
// uniqExact rather than uniq: the approximate variant would make a complete
// range occasionally look short, and the verifier would bisect a range that was
// never broken, or worse, report a gap that does not exist.
func (c *ClickHouse) CountRange(ctx context.Context, src, epoch string, from, to uint64) (uint64, uint64, error) {
	if to < from {
		return 0, 0, nil
	}

	sql := fmt.Sprintf(`SELECT uniqExact(seq), count()
FROM %s
WHERE src = {src:String}
  AND ep = {ep:String}
  AND kind = 'canary'
  AND seq >= {from:UInt64}
  AND seq <= {to:UInt64}`, c.qualified())

	rows, err := c.query(ctx, sql, rangeParams(src, epoch, from, to))
	if err != nil {
		return 0, 0, err
	}
	if len(rows) == 0 || len(rows[0]) < 2 {
		// An aggregate always returns a row. No row means an empty body, which
		// is a store that holds nothing for this stream, not a failure.
		return 0, 0, nil
	}

	distinct, err := strconv.ParseUint(rows[0][0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: unreadable distinct count %q", ErrUnavailable, rows[0][0])
	}
	total, err := strconv.ParseUint(rows[0][1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: unreadable total count %q", ErrUnavailable, rows[0][1])
	}
	if distinct > total {
		return 0, 0, fmt.Errorf("%w: distinct %d exceeds total %d, which is impossible",
			ErrUnavailable, distinct, total)
	}
	return distinct, total, nil
}

// FetchRange streams per-sequence detail for a range that already disagreed.
func (c *ClickHouse) FetchRange(ctx context.Context, src, epoch string, from, to uint64) (SeqIter, error) {
	if to < from {
		return &fileIter{}, nil
	}

	// toUnixTimestamp64Milli keeps the ingest time machine readable without
	// depending on the server's timezone or date formatting settings.
	sql := fmt.Sprintf(`SELECT seq, toUnixTimestamp64Milli(max(ingest_ts)), count()
FROM %s
WHERE src = {src:String}
  AND ep = {ep:String}
  AND kind = 'canary'
  AND seq >= {from:UInt64}
  AND seq <= {to:UInt64}
GROUP BY seq
ORDER BY seq`, c.qualified())

	rows, err := c.query(ctx, sql, rangeParams(src, epoch, from, to))
	if err != nil {
		return nil, err
	}

	partial, err := c.partialSet(ctx, src, epoch, from, to)
	if err != nil {
		return nil, err
	}

	entries := make([]Entry, 0, len(rows))
	for _, r := range rows {
		if len(r) < 3 {
			continue
		}
		seq, err := strconv.ParseUint(r[0], 10, 64)
		if err != nil {
			continue
		}
		var ingest time.Time
		if ms, err := strconv.ParseInt(r[1], 10, 64); err == nil && ms > 0 {
			ingest = time.UnixMilli(ms).UTC()
		}
		dup, err := strconv.Atoi(r[2])
		if err != nil || dup < 1 {
			dup = 1
		}
		entries = append(entries, Entry{
			Seq:      seq,
			IngestTS: ingest,
			DupCount: dup,
			Partial:  partial[seq],
		})
	}
	return &fileIter{entries: entries}, nil
}

// partialSet returns the sequences whose multi-line body arrived incompletely.
func (c *ClickHouse) partialSet(ctx context.Context, src, epoch string, from, to uint64) (map[uint64]bool, error) {
	// A continuation row declares how many siblings it expects. Fewer arrivals
	// than the declared total means continuation lines were lost. Without the
	// declared total we could not tell a truncated event from a whole one,
	// because the withheld rows are exactly the ones we cannot see.
	sql := fmt.Sprintf(`SELECT seq
FROM %s
WHERE src = {src:String}
  AND ep = {ep:String}
  AND kind = 'continuation'
  AND seq >= {from:UInt64}
  AND seq <= {to:UInt64}
GROUP BY seq
HAVING count() < max(cont_total)`, c.qualified())

	rows, err := c.query(ctx, sql, rangeParams(src, epoch, from, to))
	if err != nil {
		return nil, err
	}

	out := make(map[uint64]bool, len(rows))
	for _, r := range rows {
		if len(r) == 0 {
			continue
		}
		if seq, err := strconv.ParseUint(r[0], 10, 64); err == nil {
			out[seq] = true
		}
	}
	return out, nil
}

// PartialSeqs implements PartialReporter.
func (c *ClickHouse) PartialSeqs(ctx context.Context, src, epoch string, from, to uint64) ([]uint64, error) {
	set, err := c.partialSet(ctx, src, epoch, from, to)
	if err != nil {
		return nil, err
	}
	out := make([]uint64, 0, len(set))
	for seq := range set {
		out = append(out, seq)
	}
	return sortedU64(out), nil
}

// EpochState reports whether the epoch was closed cleanly and where it ended.
func (c *ClickHouse) EpochState(ctx context.Context, src, epoch string) (EpochState, error) {
	sql := fmt.Sprintf(`SELECT
  countIf(kind = 'final'),
  maxOrDefault(if(kind = 'final', cont_total, 0)),
  maxOrDefault(if(kind = 'canary', seq, 0))
FROM %s
WHERE src = {src:String} AND ep = {ep:String}`, c.qualified())

	rows, err := c.query(ctx, sql, map[string]string{"src": src, "ep": epoch})
	if err != nil {
		return EpochState{}, err
	}
	if len(rows) == 0 || len(rows[0]) < 3 {
		return EpochState{}, nil
	}

	finals, _ := strconv.ParseUint(rows[0][0], 10, 64)
	finalSeq, _ := strconv.ParseUint(rows[0][1], 10, 64)
	maxSeq, _ := strconv.ParseUint(rows[0][2], 10, 64)

	return EpochState{HasFinal: finals > 0, FinalSeq: finalSeq, MaxSeq: maxSeq}, nil
}

// Doctor checks connectivity and that the canary columns are queryable.
//
// Reachability alone is not enough. A server that answers queries but cannot
// see the canary columns would report every sequence as absent, which is a
// total-loss verdict against a pipeline that is working perfectly.
func (c *ClickHouse) Doctor(ctx context.Context) error {
	if _, err := c.query(ctx, "SELECT 1", nil); err != nil {
		return fmt.Errorf("clickhouse at %s is not answering: %w", c.addr, err)
	}

	// Name every column the adapter depends on, so a missing one is reported
	// here rather than as phantom data loss during a run.
	sql := fmt.Sprintf(`SELECT src, ep, seq, ts, shape, ingest_ts, kind, cont_total
FROM %s
WHERE 1 = 0`, c.qualified())

	if _, err := c.query(ctx, sql, nil); err != nil {
		return fmt.Errorf("the canary table %s is not queryable, so this sink would report "+
			"every sequence as missing: %w", c.qualified(), err)
	}
	return nil
}

// sortedU64 returns the values in ascending order.
func sortedU64(in []uint64) []uint64 {
	for i := 1; i < len(in); i++ {
		for j := i; j > 0 && in[j] < in[j-1]; j-- {
			in[j], in[j-1] = in[j-1], in[j]
		}
	}
	return in
}

// Schema is the table this adapter expects. It ships here rather than only in
// the documentation so that the adapter and the schema cannot drift apart.
func (c *ClickHouse) Schema() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  src        LowCardinality(String),
  ep         String,
  seq        UInt64,
  ts         DateTime64(9),
  shape      LowCardinality(String),
  kind       LowCardinality(String),
  cont_index UInt32 DEFAULT 0,
  cont_total UInt32 DEFAULT 0,
  ingest_ts  DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
ORDER BY (src, ep, seq);`, c.qualified())
}
