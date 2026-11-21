# The testbed

A composed pipeline for measuring what a collector actually delivers. Vector
tails files into ClickHouse, and nothing in it is configured to help the
harness: the canary is just another log line on the way through.

## Running it

```sh
export CANARY_DIR=/tmp/ll-canary && mkdir -p $CANARY_DIR
set -a && . testbed/versions.lock && set +a
docker compose -f testbed/vector-clickhouse.compose.yaml up -d
```

ClickHouse listens on `localhost:18123` with user and password `lossledger`.
Change the port with `CLICKHOUSE_HTTP_PORT` if that clashes.

Emit through it, then verify:

```sh
export LOSSLEDGER_HMAC_KEY=testbed-key
lossledger emit   -dir $CANARY_DIR -src node-a -count 2000 -rate 500 -i-know
lossledger verify -clickhouse http://127.0.0.1:18123 \
  -clickhouse-user lossledger -clickhouse-password lossledger \
  -src node-a -epoch <the epoch emit printed>
```

Tear down with `docker compose -f testbed/vector-clickhouse.compose.yaml down -v`.
The `-v` matters: without it Vector's checkpoint survives and the next run
resumes mid-file rather than starting clean.

## Why it is shaped this way

**Versions are pinned in `versions.lock`.** A finding names the version it was
measured on. A cell whose software changed underneath it is not reproducible,
and a result that cannot be reproduced is not a finding.

**Vector runs close to its defaults.** The point of a matrix cell is to measure
what a collector does as people actually run it. `glob_minimum_cooldown_ms` is
left at 60000 deliberately: that default is the subject of the first intended
finding, because a file living for less time than the cooldown can be gone
before Vector ever looks at it.

Anything tuned away from a default is named in the scenario and reported beside
the result, never applied quietly.

**The ClickHouse healthcheck queries the canary table**, not just the server. A
store that is reachable but has no table would report every sequence as
missing, which is a total-loss verdict against a pipeline that is working.

**The Vector sink blocks rather than drops when its buffer fills.** A collector
that silently dropped on overflow would make the testbed the source of loss the
harness then attributes to the pipeline, which is the confusion this exists to
avoid.

**Compose projects are prefixed `ll-gauntlet-`** so a sweeper can find what a
crashed run left behind and never touch a project it did not create.

## What this has established

An end-to-end run of 2,000 events arrived complete: 2,000 distinct sequences,
1 through 2,000, final line intact. Deleting rows from the store afterwards
produced `MISSING ranges 640-689, 1200-1203`, exactly the sequences removed,
and a fresh run then reported CLEAN with no false positives.

Both directions matter. A tool that cannot find loss is useless, and one that
invents it is worse.
