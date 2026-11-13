# Methodology

How LossLedger reaches a verdict, and what that verdict does and does not mean.

## The accounting model

Every canary event gets a sequence number, strictly monotonic within an epoch,
starting at 1. The emitter is the only component that assigns them and it never
skips. A gap in the ledger therefore always means the pipeline lost something,
never that the emitter missed a number.

An epoch is a ULID minted when the emitter starts. A restart mints a new one,
so a restart can never look like a gap in the previous epoch.

## Why absence is not loss

A log pipeline is asynchronous. At any instant some lines are legitimately in
flight: buffered in the agent, in a broker, or written but not yet visible to a
query because the store has not refreshed. Calling those lines lost would be a
false accusation, and a false accusation is worse than a missed detection,
because it destroys the credibility that makes detection worth reporting.

So a sequence is only `MISSING` when four things hold:

1. It is at or below the epoch's expected high-water mark.
2. The sink does not hold it.
3. The settle window has elapsed since it was emitted.
4. A mandatory re-query has run and it is still absent.

## The settle window

```
settle = clamp(max(sink_hint, observed_p99_latency * 3), 5s, max_wait)
```

`sink_hint` is the store's own consistency floor: Elasticsearch's refresh
interval, ClickHouse's async insert flush, Loki's ingester flush. The adaptive
term lets the window grow when the pipeline is genuinely slower than usual, so
a backlog does not turn into a page.

Both inputs push the window outward only. Nothing can shrink it below the
floor, which is what makes this property hold:

> Lengthening the settle window never creates new `MISSING`.

That is checked as a property test, not merely asserted.

The window is printed in every report, so a reader can judge whether a verdict
was fair instead of having to trust that it was.

## The integrity metric

`lossledger_false_missing_total` counts sequences this verifier declared
missing that later arrived. It is the shame counter.

It is exported from day one, printed on every report whether it is zero or not,
and must be zero in every published run. A nonzero value means the harness
accused a pipeline of losing data it went on to deliver, which is a bug in
LossLedger, not in your pipeline. It is treated as higher priority than any
feature work.

It is deliberately impossible to publish a result from this tool without also
publishing this number.

## Why not use the collector's own counters

Because they structurally cannot see the losses that matter. A component counts
the drops it notices; the expensive failures are the ones nothing notices. A
verifier that consumed collector self-reports as verdict input would inherit
exactly the blind spot it exists to cover.

Collector logs and metrics may be collected for a report. They are never used
to compute a verdict.

## Duplicates

At-least-once pipelines duplicate after a restart. That is correct behaviour,
so `DUP_REPLAY` is informational and never fails a run.

A duplicate is only a bug when it falls outside any restart window, or when the
duplication factor exceeds the bound (three by default), because no amount of
legitimate replay copies a record four times.

In gauntlet mode the harness knows exactly when it killed a collector, so the
classification is exact. In production mode it uses a time-window heuristic and
labels the result `heuristic`. **A heuristic `DUP_BUG` is never published as a
finding.**

## Attribution, and its limits

With an emitter and sinks only, two conclusions are available:

- The same sequences are missing at every sink, which points at the source side.
- Sequences are missing at one sink only, which points at that sink's path.

That is the whole of it. Per-hop attribution across a broker needs a tap at the
broker, which is v2. Every report carries this limit as a footnote.

## Bisection, and what verification costs

Verifying 173 million sequences per day by listing them would cost more than
the pipeline being audited. Instead:

1. Ask `CountRange` for a wide range. If the distinct count equals the width
   and nothing is duplicated, the whole range is verified in one query.
2. Otherwise split and recurse.
3. Fetch per-sequence detail only at leaves below the detail threshold.

Expensive queries scale with the amount of loss, not with the volume of
traffic. A clean pipeline costs about one query per source per sink per
interval.

## What a clean result means

That this pipeline delivered these canary lines, in these shapes, under these
conditions, within this settle window.

It is not a proof that your pipeline never loses anything. It is evidence about
a specific population of lines over a specific interval. Every report carries a
Limitations section naming what was not tested, and that section is not
optional.
