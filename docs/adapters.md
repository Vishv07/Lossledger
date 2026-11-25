# Writing a sink adapter

An adapter is the only part of LossLedger that knows how to ask a particular
store what it holds. Everything else works in sequence numbers.

A verdict is only as trustworthy as the adapter that produced it. An adapter
that miscounts a range turns this project into a machine for libelling
collectors, so the behaviour is pinned once in a shared suite and every adapter
is run against it.

Aim for 150 to 250 lines. Much more usually means store-specific knowledge has
leaked in that belongs in this document instead.

---

## The interface

```go
type Sink interface {
    Name() string
    CountRange(ctx, src, epoch string, from, to uint64) (distinct, total uint64, err error)
    FetchRange(ctx, src, epoch string, from, to uint64) (SeqIter, error)
    SettleHint() time.Duration
    Doctor(ctx) error
    Close() error
}
```

There is no write method, and there will not be one. The verifier is read-only
against your stores by construction rather than by discipline.

### What each method has to get right

**CountRange** is the cheap path. The verifier calls it over wide ranges and
only looks closer when the answer disagrees, so the entire cost model rests on
it being cheap.

- `distinct` counts distinct sequences. `total` counts rows. They differ by
  exactly the duplicate rows.
- Collapsing them hides every duplicate bug. Inflating `distinct` by duplicates
  invents loss somewhere else.
- **Use an exact aggregate.** An approximate distinct count makes a complete
  range occasionally look short, and the verifier then reports a gap that does
  not exist.
- Bounds are **inclusive at both ends**. An exclusive upper bound loses one
  sequence per range, and bisection multiplies that into phantom gaps at every
  level of recursion.

**FetchRange** is called only on ranges that already disagreed. Return entries
in ascending sequence order, each with a duplicate count of at least one.

**SettleHint** is your store's own consistency floor: an index refresh
interval, an async insert flush, an ingester flush. **Be honest and generous.**
Reporting a shorter delay than the store really needs is how the harness
accuses a store of losing a row it had not finished writing.

**Doctor** must verify the canary fields are *queryable*, not merely that the
connection works. A store that answers queries but cannot see the token fields
reports every sequence as absent, which is a total-loss verdict against a
pipeline that is working perfectly.

### An optional capability

```go
type PartialReporter interface {
    PartialSeqs(ctx, src, epoch string, from, to uint64) ([]uint64, error)
}
```

A multi-line event whose continuation lines were partly lost is invisible to
`CountRange`: the parent sequence arrived exactly once, so the cheap path sees
a clean range and never looks closer.

Implement this if your store can answer the question cheaply. If it cannot, do
not implement it, and reports will say partial detection was unavailable rather
than quietly claiming none were found.

---

## The conformance suite

```go
func TestMyAdapterConformance(t *testing.T) {
    adapter.Conformance{
        Sink: sink,
        Seed: func(ctx context.Context, src, epoch string, seqs []uint64, dups map[uint64]int) error {
            // Write these rows into the store, synchronously.
            return nil
        },
    }.Run(t)
}
```

It checks ten things: doctor, settle hint, an empty store, exact counting, gap
visibility, duplicates kept separate from distinct, fetch ordering, inclusive
bounds, an unknown stream returning zero rather than an error, and context
cancellation.

The suite tests itself. `conformance_test.go` runs deliberately broken adapters
through it and fails if they pass, so a suite that has stopped checking anything
is caught rather than trusted.

---

## ClickHouse

Shipped as `adapter.NewClickHouse`. It speaks the **HTTP interface** rather
than the native protocol, which keeps the project free of a database driver
dependency.

### Table

```sql
CREATE TABLE IF NOT EXISTS lossledger_canary (
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
ORDER BY (src, ep, seq);
```

`kind` is `canary`, `continuation` or `final`. `ingest_ts` defaults to the
server clock, which is what the latency histogram and the ordering check read.

The adapter also exposes `Schema()`, returning this statement with your
configured database and table name filled in, so the shipped schema and the
queries cannot drift apart.

### Queries it issues

| Method | Statement |
|---|---|
| CountRange | `SELECT uniqExact(seq), count() ... WHERE kind = 'canary' AND seq >= ? AND seq <= ?` |
| FetchRange | `SELECT seq, toUnixTimestamp64Milli(max(ingest_ts)), count() ... GROUP BY seq ORDER BY seq` |
| PartialSeqs | `... WHERE kind = 'continuation' GROUP BY seq HAVING count() < max(cont_total)` |
| EpochState | `SELECT countIf(kind = 'final'), ...` |
| Doctor | `SELECT 1`, then every canary column with `WHERE 1 = 0` |

`uniqExact` is deliberate. `uniq` is approximate above a threshold and would
make a complete range occasionally look short.

### Parameters, not interpolation

Values travel as `param_<name>` and are referenced as `{name:String}`, which is
ClickHouse's server-side binding. Source and epoch identifiers come from data
the pipeline carried, so a value that reached the SQL text could rewrite the
query.

Table and database names cannot be bound, so they are validated as plain
identifiers at construction and rejected otherwise.

### Configuration

```go
sink, err := adapter.NewClickHouse(adapter.ClickHouseConfig{
    Addr:     "http://localhost:8123",
    Database: "observability",
    Table:    "lossledger_canary",
    Settle:   5 * time.Second,
})
```

`Settle` defaults to 5 seconds. ClickHouse async inserts flush on a timer
(`async_insert_busy_timeout_ms`, one second by default) and a distributed table
adds more, so raise it rather than lower it if you are unsure.

### What has been verified

**Against a real ClickHouse server**, through the compose testbed:

- The shared conformance suite passes in full.
- An end-to-end run was measured: the emitter wrote 2,000 events into a
  directory, Vector tailed it and wrote rows, and the adapter counted exactly
  2,000 distinct sequences from 1 to 2,000 with the final line intact.
- Rows were then deleted from the store and the tool reported
  `MISSING ranges 640-689, 1200-1203`, which is exactly what was removed.
- A fresh clean run afterwards reported CLEAN with no false positives.

The bisection assumption is checked directly against the server: a range split
in half must count to the same total as the whole, which is where a
disagreement about inclusive bounds would surface.

Run it yourself:

```sh
export CANARY_DIR=/tmp/ll-canary && mkdir -p $CANARY_DIR
set -a && . testbed/versions.lock && set +a
docker compose -f testbed/vector-clickhouse.compose.yaml up -d

LOSSLEDGER_CH_ADDR=http://127.0.0.1:18123 go test ./internal/adapter/ -run Integration -v
```

The integration tests skip unless `LOSSLEDGER_CH_ADDR` is set, so the ordinary
suite and CI need no Docker.

### What is still not verified

A single node. Behaviour against a cluster, a distributed table, or a replica
lagging behind its leader is untested, and that is where `SettleHint` matters
most. Raise it rather than lower it if you are unsure.
