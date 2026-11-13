# LossLedger: Product Requirements Document (PRD)

| Field | Value |
|---|---|
| Status | Approved for build, **gated by M0 validation spike** |
| Version | 1.0 (2026-11-11) |
| Owner | Vishv (project owner / reviewer) |
| Implementer | Autonomous coding agent (Claude Code) |
| Target | v1 public release in ~3 weeks of part-time work after M0 passes |
| License | Apache-2.0 |
| Repo name | `lossledger` |
| Sibling project | SyncGauntlet (same owner, same fault-injection discipline; LossLedger is sequenced AFTER SyncGauntlet v1 unless the owner says otherwise) |

---

## 0. Instructions for the implementing agent: READ FIRST

You are building this project from scratch on the owner's local Linux machine. This document is the single source of truth; you do not have access to the research conversations that produced it. Everything you need: motivation, evidence, scope, specs, verification: is in here.

**Operating rules:**

1. **Milestones are sequential and gated.** Do not start Milestone N+1 until every acceptance criterion of Milestone N passes. Each milestone ends with a status note in `docs/progress/` (what was built, deviations from this PRD and why, what's next).
2. **M0 is a kill-gate, not a formality.** This project's demand is real but narrower than it looks (§2.4). M0 exists to let reality veto the project cheaply. If M0's criteria say "stop and report," you stop and report to the owner: you do not proceed on momentum.
3. **Decisions you may make alone:** internal package layout, private naming, choice of Go libraries (prefer stdlib; justify each dependency in `docs/decisions/`), test organization, CLI flag spelling.
4. **Decisions you must surface to the owner first:** changing the anomaly taxonomy (§7.6) or metric names (§7.8), breaking the config/wire format after M2, adding a sink adapter not listed in §7.5, publishing anything publicly, filing issues on upstream projects, anything requiring root (§8.4 forbids it).
5. **Never weaken a verdict to make a run look clean.** If the verifier reports MISSING and you suspect a harness bug, prove it with the reference pipeline (§10.2), fix the harness, and record it in `docs/decisions/`. The product is trust; a single false accusation against a collector or store destroys it (§13, risk 1).
6. **The integrity metric is sacred.** `lossledger_false_missing_total` (entries declared missing that later arrived) must be exported from day one and must be zero in every published run. If it is ever nonzero, that is a P0 against the verifier, above all feature work.
7. **The harness never touches the pipeline under test.** The emitter is only another log producer; the verifier is only another query client. No patching collectors, no privileged hooks into stores, no reading collector internals to compute verdicts (collector logs/metrics may be *collected* for reports, never used as verdict input).
8. **Safety rails (§8.5) are implemented in M1, before any fault injection exists.**
9. Write idiomatic Go. `gofmt`, `go vet`, `golangci-lint` clean at every commit. Table-driven tests. No panics across package boundaries. Conventional commits (`feat:`, `fix:`, `test:`, `docs:`): the history is part of the portfolio.
10. When this PRD is ambiguous, prefer the interpretation that maximizes **verdict trustworthiness** first, **determinism/reproducibility** second, **convenience** last. Record the choice in `docs/decisions/`.

**Environment assumptions (verify in `lossledger doctor`, fail with actionable errors):** Linux kernel ≥ 5.10, Go ≥ 1.22, Docker + docker compose v2 (for the sink/collector testbed), ~15 GB free disk, no root at any milestone. Collector binaries/images (Vector, Fluent Bit, OTel Collector, Alloy, Filebeat) and store images (Elasticsearch or OpenSearch, ClickHouse, Loki) are pulled as Docker images in M2+; record exact tags in `testbed/versions.lock`.

---

## 1. One-page overview

**LossLedger is a black-box, end-to-end delivery auditor for log pipelines.** A tiny *emitter* writes sequence-numbered, HMAC-signed canary log lines through the user's REAL pipeline: the same files, stdout streams, and journald sockets their applications use, in realistic shapes (multi-line stack traces, oversized lines, interleaved timestamps). A *verifier* then asks each sink: Elasticsearch/OpenSearch, ClickHouse, Loki (v1 set): "which sequence numbers do you actually hold?", keeps per-source bitmaps, and reports **exact gap ranges, duplicates (classified replay vs bug), ordering anomalies, and delivery latency** as Prometheus metrics and human gap reports.

The one-sentence pitch: **double-entry bookkeeping for logs: sent is one side of the ledger, arrived is the other, and the books must balance.**

The one-insight pitch: per-component drop counters (`fluentbit_output_dropped_records_total`, Vector's `component_discarded_events_total`, `otelcol_exporter_send_failed_*`) can only count the drops a component *notices*. The losses that hurt: rotation races, file-discovery delays, timestamp-ordering drops, ingest-pipeline rejections: are precisely the ones nobody counts. Only source→sink accounting sees them.

**Two modes, one engine:**
- `lossledger run`: always-on production auditing (DaemonSet/sidecar emitter + central verifier).
- `lossledger gauntlet`: CI mode: spin a composed pipeline, inject faults (rotation storm, sink outage, collector SIGKILL, disk pressure), fail the build if loss exceeds budget.

**The launch artifact** is not the repo; it is the first published **cross-collector loss matrix**: {Vector, Fluent Bit, OTel Collector, Alloy, Filebeat} × {4 fault scenarios} × {3 stores}, default configs, every cell reproducible from a committed script. No such public table exists (verified Nov 2026).

**What v1 is not:** not a log shipper, not a metrics/traces auditor (logs only), not a benchmark of throughput (latency is recorded as a secondary metric only), not a Kubernetes operator (plain binaries + compose in v1; Helm is post-v1), not an agent/LLM anything.

---

## 2. Motivation: why we felt this need, with evidence and with honest caveats

### 2.1 The problem class

A log pipeline is a chain: application → file/stdout/journald → node agent (tail, parse, buffer) → optional broker (Kafka) → store ingest (parse, index). Every hop can lose data, and each hop's self-reporting covers only the failure modes its authors anticipated. Four structurally invisible loss classes recur across vendors and years:

1. **Discovery races:** a short-lived pod writes and exits before the agent's file-discovery interval fires; the file is gone before anyone tails it. Nothing increments.
2. **Rotation races:** a file rotates while the agent is mid-read; lines in the gap window vanish; under load, records get split or dropped with no counter.
3. **Semantic drops:** the source intentionally skips data it considers invalid: e.g., lines whose timestamp is earlier than the previous line (stdout/stderr interleaving triggers this constantly): without treating it as an error.
4. **Ingest rejections:** the store's pipeline rejects a document (mapping conflict, malformed JSON, oversized line) after the agent has already counted it as delivered.

### 2.2 The documented evidence (carry this into README and launch post)

- **grafana/alloy #7076 (Sep 2026):** user-run canary measured **199 of 5,035 entries lost over 14 hours (4%)** because `loki.source.kubernetes` silently drops out-of-order-timestamp lines. kubectl showed 31 entries in 15 minutes; Loki had 23. Zero errors reported anywhere.
- **VictoriaMetrics collector benchmark (Mar 2026):** Vector's default `glob_minimum_cooldown_ms=60000` means a new pod's logs can be **gone before the agent ever looks**; two major collectors split records during rotation even at low CPU.
- **fluent/fluent-bit #2509 (2020) and #2955 (2021):** GKE users with pods that silently stopped shipping for hours; "I didn't find any errors in fluent bit as well as elasticsearch." In/out counters matched while logs vanished.
- **Elastic Discuss "Losing logs in ELK":** Filebeat→Logstash→ES losing logs "often and sometimes in a massive way" while Logstash in/filtered/out stats were "almost the same": the counters lied.
- **UK Ministry of Justice Cloud Platform incident (2024-07-25):** the log store stopped receiving; a human noticed via Slack ~40 minutes later; the runbook's closing line: "These logs have not been recovered."
- **The internal-tool tell (strongest signal):** AWS runs **release-blocking log-loss tests** for its Fluent Bit distribution; Grafana built **loki-canary** (Loki-only, with a documented sent/received race, grafana/loki #7142); VictoriaMetrics built **log-verifier** (verifies against a *fake* sink only: its README states delivery metrics are unavailable against a real backend); OpenTelemetry has an open contrib issue (#20552) requesting no-data-loss E2E tests. Four organizations independently funded the same internal harness. None of them ships a generic, multi-collector, real-store tool.

### 2.3 Why the gap persisted (the moat statement)

Each vendor's canary serves its own backend, because that is all each vendor needs; a neutral cross-vendor auditor belongs to nobody's roadmap. Per-component counters are the industry's default answer and they structurally cannot see unnoticed loss. And the work is unglamorous correctness plumbing: settle windows, epoch bookkeeping, bisection queries: the category of problem that (as five research passes confirmed) does not get swarmed the way product gaps do. The nearest mover is VictoriaMetrics (their verifier is one generalization away), which sets the clock but does not close the gap today.

### 2.4 Honest demand caveats (these go in the README's Limitations too)

Most teams tolerate 1–2% log loss without noticing, because most log lines are never read. The acute audience is: (a) teams where logs are legally/contractually load-bearing (audit trails, security events, regulated industries), (b) platform/observability teams at vendors and large orgs: the ones who already build this internally, (c) engineers mid-incident who just discovered the missing line, which is everyone eventually but only *after* the incident. This is a top-10–20%-of-logging-operations tool plus a homelab tail: thousands of deployments, not hundreds of thousands: sufficient for an open-source reputation project (the Jepsen model: readers outnumber runners ~100:1 and the citations are the point), insufficient for mass-market dreams. **M0 exists because of this caveat.**

### 2.5 Why this owner

The owner runs ELK and ClickHouse pipelines professionally (with Kafka in the path), so ring-1 pain is verifiable on infrastructure they already operate; and the sibling project SyncGauntlet exercises the identical discipline (fault injection, determinism, self-validation gates, honest-boundary design). LossLedger is the same thesis: *instruments that prove infrastructure tells the truth*: applied to a second domain, sharing a fault-injection library.

---

## 3. Goals and non-goals

### 3.1 Goals (v1)

- G1. **Exact accounting:** for every (source, epoch) the verifier can state precisely which sequence numbers arrived at each sink, which are duplicated (and whether replay- or bug-class), which are out of order, and which are UNACCOUNTED after the settle window: with gap ranges, not just totals.
- G2. **Zero false accusations:** `lossledger_false_missing_total` is exported, tested, and zero across all published runs (§7.7, §10).
- G3. **Three sink adapters** behind a stable interface (~150–250 lines each): Elasticsearch/OpenSearch, ClickHouse, Loki.
- G4. **Realistic emission:** ≥ 5 shape profiles (plain, JSON, multi-line stack trace, oversize 64 KB, stderr/stdout interleaved timestamps) because loss is content-dependent; a tidy-one-liner canary proves nothing (§7.2).
- G5. **Restart honesty:** epoch system + duplicate taxonomy such that emitter restarts and collector checkpoint replays are classified correctly, never reported as loss or as bug-duplicates (§7.4).
- G6. **Gauntlet CI mode:** composed testbed + 4 fault injectors (rotation storm, sink outage, collector SIGKILL, disk pressure) + loss-budget gate with severity-ordered exit codes (§7.9, §7.10).
- G7. **The loss matrix, published:** ≥ {3 collectors} × {4 faults} × {2 stores} in v1's report #1 (full 5×4×3 is the stretch), every non-clean cell reproducible from a committed script, upstream maintainers notified before publication (§12).
- G8. **Contributor-ready at launch:** adapter guide, shape-profile guide, ≥ 5 labeled good-first-issues, CONTRIBUTING, CoC, CI running the self-validation gate on every commit.

### 3.2 Non-goals (v1)

- NG1. Metrics and traces auditing (logs only; the design must not preclude it, but no code).
- NG2. Kafka-hop attribution ("lost between Kafka and ClickHouse"): v2; v1 attributes loss to source-side vs sink-side only where the evidence allows (§7.7).
- NG3. Kubernetes operator/Helm chart (post-v1; v1 ships static binaries + compose testbed + systemd unit example).
- NG4. VictoriaLogs and Filebeat/Alloy adapters are stretch, not gate (matrix minimum in G7).
- NG5. Throughput benchmarking as a headline; no "fastest collector" claims ever.
- NG6. Windows/macOS. Linux only.
- NG7. Any remediation/replay of lost logs: LossLedger proves loss; it does not repair it.
- NG8. No LLM/agent features of any kind.

---

## 4. Users and use cases

| # | User | Job to be done | Usage shape |
|---|---|---|---|
| U1 | SRE / platform / observability team | "Prove our pipeline delivers; page me with exact gaps when it doesn't." | Emitter DaemonSet/sidecar + central verifier, always-on; alerts on `lossledger_missing_total` deltas |
| U2 | Platform team gating config changes | "This Vector config/version bump must not regress delivery." | `lossledger gauntlet --budget data_loss=0` in CI on every collector-config PR |
| U3 | Evaluator choosing a collector/store | "Which one can I trust, measured, not vendor-benchmarked?" | Reads the published loss matrix; optionally re-runs it against their own config in an afternoon |
| U4 | Collector/store maintainers | "Give me a reproducible case, not a vibe." | Receives findings (config + shape profile + fault script + seq ranges); adopts gauntlet for regression testing |
| U5 | Compliance/audit owner | "Evidence of log completeness for the auditor." | Monthly `lossledger report` with per-sink accounting and the integrity metric |

Primary v1 ordering: U2 and U4 (the harness is most credible in controlled CI first), then U1, with U3 served by the published matrix. U5 is documentation-only in v1.

---

## 5. Product principles

1. **Honest boundary:** nothing injected into or patched onto the pipeline under test. Every reported gap is one a real application's logs would have suffered.
2. **An accusation must be earned twice:** settle-window re-query before declaring MISSING, and any finding published externally must reproduce 10/10 from its committed scenario. The integrity metric (G2) is published alongside every result.
3. **Severity ordering is the interface:** MISSING > DUP_BUG > ORDER > LATE > CLEAN drives exit codes, CI budgets, report sorting, and alert levels. (DUP_REPLAY is informational, never a failure.)
4. **Scenarios and shapes are data:** a contributor adds coverage by writing YAML/profile files, never by touching the engine.
5. **Honest about limits:** every report carries a Limitations section naming what was NOT tested (stores, collectors, fault classes, Kubernetes-native sources) and §2.4's demand caveats stay in the README.
6. **Methodology first, findings second, zero gloating:** reports praise what passed as loudly as what failed; maintainers see findings before the public does.

---

## 6. System architecture

```
                 ┌────────────────────────────────────────────────────────────┐
                 │ USER'S PIPELINE (untouched)                                │
 emitter ×N ──▶  │  file / stdout / journald → agent (Vector|FluentBit|OTel|  │ ──▶ sinks
 (shape          │  Alloy|Filebeat) [→ Kafka, optional, v2 attribution]      │     ES/OS · ClickHouse · Loki
  profiles,      └────────────────────────────────────────────────────────────┘        │
  seq+epoch+hmac)                                                                      │ adapter queries
                                                                                       ▼
                                   ┌──────────────────────────────────────────────────────┐
                                   │ VERIFIER (one process)                               │
                                   │  sink adapters → range counts → bisection on         │
                                   │  mismatch → roaring bitmap per (source,epoch,sink)   │
                                   │  settle-window scheduler → anomaly classifier        │
                                   │  state in embedded store (Pebble/Bolt): restartable │
                                   └───────────────┬──────────────────────────────────────┘
                                                   ▼
                              /metrics (Prometheus) · gap report (text/JSON) · runs.db (SQLite, gauntlet)
```

**Language:** Go. Single static binaries for emitter and verifier (one repo, two `cmd/`s), first-class process control for the gauntlet, and the target ecosystem (Loki, Vector-adjacent tooling, ClickHouse clients) is Go-friendly. Do not substitute without owner approval.

**Suggested layout (agent may refine inside packages):**

```
cmd/lossledger/          CLI: run | verify | gauntlet | report | selfcheck | doctor
internal/emit/           line format, shape profiles, writers (file/stdout/journald), pacing
internal/wire/           canary line schema, HMAC, parse/validate (shared emitter↔verifier)
internal/verify/         bitmaps, settle scheduler, anomaly classifier, integrity accounting
internal/adapter/        Sink interface + es, clickhouse, loki, filesink (reference)
internal/fault/          injectors: rotatestorm, sinkoutage, sigkill, diskpressure  ← shared DNA with SyncGauntlet
internal/testbed/        compose orchestration, versions.lock, scenario runner
internal/report/         prometheus exporter, gap reports, matrix report generator (md+html)
scenarios/               gauntlet scenario YAMLs (data)
profiles/                shape profile definitions (data)
docs/                    methodology.md, adapters.md, profiles.md, decisions/, progress/, findings/
```

---

## 7. Functional specification

### 7.1 Canary line format (normative: `internal/wire`)

Every canary line embeds one token, greppable and parse-stable through any sane pipeline:

```
LL1|src=<source_id>|ep=<epoch_ulid>|seq=<uint64>|ts=<unix_nanos>|shape=<profile>|hmac=<12-hex>
```

- `LL1` is the format version sentinel. Parsers must ignore unknown trailing fields (forward compat).
- `src` identifies the emitting unit (node/pod), `ep` is a ULID minted at emitter start (restart ⇒ new epoch), `seq` is strictly monotonic within an epoch starting at 1, `ts` is emit time.
- `hmac` = first 12 hex chars of HMAC-SHA256(key, "src|ep|seq|ts") with a per-deployment key; the verifier rejects lines failing HMAC (prevents accidental or deliberate fakes polluting the ledger) and counts them in `lossledger_invalid_lines_total`.
- Shape profiles may wrap the token (JSON field, mid-stack-trace line); the token itself is never split across physical lines, and for multi-line shapes the token lives on the FIRST physical line; subsequent continuation lines carry `LLC|src|ep|seq|n=<k>` markers so partial-delivery of a multi-line event is detectable as `ORDER`/`PARTIAL` evidence (partial counts as the parent seq arrived, flagged `partial=true`).

### 7.2 Emitter (`internal/emit`)

- Writers: (a) file writer with size/time-based self-rotation (configurable, to exercise rotation paths), (b) stdout writer (for container runtimes), (c) journald writer via socket. v1 gate: (a) and (b); journald is stretch.
- Pacing: fixed rate (default 10 lines/s per source) with optional seeded burst mode (gauntlet scenarios set this).
- Shape profiles (data files in `profiles/`, normative set): `plain`, `json`, `stacktrace` (multi-line, 8–40 continuation lines), `oversize` (configurable, default 64 KB), `interleave` (two streams writing alternately with deliberately non-monotonic timestamps: this is the alloy-#7076 trigger). Each profile declares how the token is embedded.
- Determinism: in gauntlet mode all randomness (burst timing, shape mix, sizes) derives from a single seed; two runs with the same seed emit byte-identical sequences. In production `run` mode, pacing may use wall clock but seq/epoch semantics are identical.
- Resource bound: emitter must stay under ~15 MB RSS and be safe to run as a DaemonSet; no cgo.

### 7.3 Sink adapters (`internal/adapter`): the plugin surface

```go
type Sink interface {
    Name() string
    // CountRange returns how many DISTINCT seqs in [from,to] for (src,epoch) the sink holds,
    // plus the sink's own total row count for that range (distinct<=total; total-distinct = dup rows).
    CountRange(ctx context.Context, src, epoch string, from, to uint64) (distinct, total uint64, err error)
    // FetchRange streams (seq, ingestTS, dupCount) detail for [from,to]; called only on bisected
    // mismatching ranges, never for bulk verification.
    FetchRange(ctx context.Context, src, epoch string, from, to uint64) (iter SeqIter, err error)
    // SettleHint returns the sink's consistency delay floor (e.g., ES refresh, CH async flush,
    // Loki ingester flush) used to seed the adaptive settle window.
    SettleHint() time.Duration
    // Doctor validates connectivity, auth, and the presence/queryability of the canary fields.
    Doctor(ctx context.Context) error
}
```

- **Elasticsearch/OpenSearch:** query via PIT + `search_after` on the token fields (ingest guidance in docs: index the token as keyword/longs via a provided ingest pipeline or rely on the shipped `json` profile); `CountRange` via `value_count`+`cardinality` aggs on `ll.seq` (cardinality is approximate above threshold: adapter must switch to composite-agg exact counting below 40k-range width and document the bound).
- **ClickHouse:** `CountRange` = `SELECT uniqExact(seq), count() ... WHERE src=? AND ep=? AND seq BETWEEN ? AND ?`; detail via ordered range SELECT. Guidance for a canary table or materialized-view extraction ships in docs; adapter also supports querying a raw-string table with `extract()` at reduced speed (documented).
- **Loki:** LogQL range queries filtered on the token; honor Loki's query limits by chunking time ranges; parity target is loki-canary minus its sent/received race (our settle scheduler is pull-based, so the race class does not exist).
- **filesink (reference adapter):** reads a directory of files as if it were a store. Exists so the self-validation gate (§10.2) runs with zero external dependencies.

### 7.4 Epochs, restarts, and duplicate taxonomy (normative)

- Emitter restart ⇒ new epoch; old epoch is finalized at its last observed seq (the verifier learns the final seq from a `LLF|...|final=<seq>` flush line the emitter writes on clean shutdown; on crash, the final seq is inferred as max observed with `epoch_truncated=true`, and seqs beyond max observed are NOT counted as missing).
- `DUP_REPLAY`: duplicate deliveries of seqs within the window [checkpoint_horizon] after a collector restart/crash event in the same time region: informational, expected with at-least-once pipelines.
- `DUP_BUG`: duplicates outside any restart window, or duplication factor > configured bound (default 3×): a finding-class anomaly.
- The gauntlet knows when it injected a SIGKILL, so classification in CI is exact; in production `run` mode the verifier uses a time-window heuristic around observed delivery discontinuities and labels the classification `heuristic=true`. Never report heuristic DUP_BUG as a finding.

### 7.5 Verifier core (`internal/verify`)

- State: roaring bitmap per (src, epoch, sink) + per-seq first-seen ingest timestamp ring for latency percentiles; persisted to the embedded store so verifier restarts never fabricate gaps (on restart, re-sync by re-querying CountRange for the live window).
- Scheduler: every `interval` (default 60 s in run mode; continuous in gauntlet), for each (src,epoch,sink): query `CountRange` for the unverified window; if distinct == expected, mark the whole range verified (cheap path); else bisect: split range, recurse, `FetchRange` only on leaf ranges below `detail_threshold` (default 512). This is the scale answer (§ hard problem 3).
- Settle window: `settle = max(SettleHint, observed_p99_latency × 3)` adaptive per sink, bounded by `[5s, max_wait]` (`max_wait` default 10 min run-mode, scenario-set in gauntlet). A seq older than `settle` and absent is PROVISIONAL_MISSING; one mandatory re-query after `recheck_delay` (default settle/2) before it becomes MISSING. A seq that arrives after being declared MISSING increments `lossledger_false_missing_total` AND is reclassified LATE: the metric is the shame counter (G2).
- Out-of-order: ingest order deviating from seq order beyond a tolerance window ⇒ ORDER anomaly (informational in v1, severity below DUP_BUG).

### 7.6 Anomaly taxonomy (normative: changes require owner approval)

| Anomaly | Definition | Severity |
|---|---|---|
| `MISSING` | seq absent from sink after settle + mandatory re-check; exact ranges reported | 40 |
| `DUP_BUG` | duplication outside replay windows or beyond dup-factor bound | 30 |
| `ORDER` | arrival order violates seq order beyond tolerance; or multi-line event partially delivered (`partial`) | 20 |
| `LATE` | arrived after settle but before max_wait (includes reclassified false-missing) | 10 |
| `CLEAN` | all seqs accounted exactly once (DUP_REPLAY permitted, reported separately) | 0 |

Exit codes mirror severity: 0 CLEAN, 10 LATE, 20 ORDER, 30 DUP_BUG, 40 MISSING, 2 harness error (harness errors must never masquerade as anomalies). `gauntlet --budget` accepts per-anomaly budgets (e.g., `--budget missing=0,dup_bug=0,late=50`).

### 7.7 Attribution (v1 scope, honest)

With emitter and sinks only, the verifier can distinguish: (a) lost-at-all-sinks for the same seqs ⇒ source-side (file/agent) loss; (b) lost-at-one-sink only ⇒ that sink's path. It reports attribution at exactly this granularity and no finer. Per-hop attribution (Kafka tap) is NG2/v2; the report template carries an "attribution limits" footnote.

### 7.8 Metrics (normative names)

```
lossledger_emitted_total{src,shape}
lossledger_arrived_total{src,sink}
lossledger_missing_total{src,sink}            # finalized MISSING
lossledger_dup_total{src,sink,class}          # class = replay|bug
lossledger_order_total{src,sink}
lossledger_late_total{src,sink}
lossledger_delivery_latency_seconds{sink}     # histogram
lossledger_false_missing_total{sink}          # THE integrity metric, must stay 0
lossledger_invalid_lines_total{sink}          # HMAC failures
lossledger_verifier_queries_total{sink,kind}  # kind = count|fetch  (overhead accounting)
```

### 7.9 Fault injectors (`internal/fault`, gauntlet-only, rootless)

| Injector | Mechanism | Reproduces |
|---|---|---|
| `rotatestorm` | emitter's own file writer rotates at pathological speed/size; optionally creates N short-lived files below the agent's discovery interval | rotation races; short-lived pod loss (the Vector cooldown class) |
| `sinkoutage` | pause/stop the sink container (docker pause) or firewall it via the compose network for T seconds | store outage; buffer/overflow behavior |
| `sigkill` | SIGKILL the collector container/process group at a scenario-defined emission count ± seeded jitter | collector crash; checkpoint replay duplicates |
| `diskpressure` | emitter writes into a small tmpfs/loopback the compose testbed provisions; fill it to threshold during emission | disk-full at source |

Determinism: injectors anchor on emission counts (not wall clock) wherever possible; jitter is seeded; each gauntlet run records (scenario, seed, versions.lock) so `lossledger gauntlet --replay <run-id>` re-executes exactly.

### 7.10 Gauntlet scenarios (YAML, data): normative example

```yaml
# scenarios/rotation_storm_vector.yaml
name: rotation_storm_vector
pipeline: testbed/vector-es-ch.compose.yaml     # collector+stores under test
emit:
  rate: 50/s
  shapes: {plain: 0.5, stacktrace: 0.3, interleave: 0.2}
  duration: 120s
fault:
  type: rotatestorm
  at_count: 2000
  params: {rotations: 30, min_file_lifetime_ms: 400}
verify:
  max_wait: 90s
budget: {missing: 0, dup_bug: 0}
runs: {seeds: [1..10]}
```

### 7.11 CLI (normative surface)

```
lossledger doctor                       # env + sink connectivity + canary-field queryability
lossledger emit    --config emit.yaml   # standalone emitter (prod mode)
lossledger verify  --config verify.yaml # standalone verifier (prod mode)
lossledger run     --config ll.yaml     # emit+verify supervised together (single-node/demo)
lossledger gauntlet --scenario s.yaml [--seed N] [--replay RUN] [--budget ...]
lossledger report  --db runs.db --out report/      # matrix report md+html
lossledger selfcheck                    # §10.2 gate against the reference pipeline
```

---

## 8. Non-functional requirements

### 8.1 Trust & reproducibility standard
Published findings reproduce 10/10 from (scenario, seed, versions.lock). `false_missing_total` = 0 across all published runs; any nonzero occurrence anywhere is a release-blocking bug. Verifier restart during a run must not change final accounting (tested).

### 8.2 Performance
Emitter ≤ 15 MB RSS at 100 lines/s. Verifier handles 200 sources × 10 lines/s (≈ 173 M seqs/day) with the cheap-path CountRange strategy at < 1 query/source/sink/interval amortized and < 500 MB RSS; bitmaps for a day fit comfortably (roaring). A full gauntlet scenario (120 s emission + settle) completes ≤ 6 min; the v1 matrix (≤ 60 scenario-seed cells) completes overnight on one 8-core machine with `--jobs 4`.

### 8.3 Observability of the harness itself
Structured slog JSON per run; `--keep` preserves the testbed and all store volumes for autopsy; failed teardown is loud.

### 8.4 Privileges
No root anywhere. Fault injection uses docker-compose controls (pause/stop/network disconnect) and emitter-owned filesystem behavior only: never host iptables/tc. `doctor` verifies docker access and refuses degraded silently.

### 8.5 Safety rails (built in M1)
- Emitter refuses to write outside its configured directories (symlink-resolved path assertion; violation aborts).
- Verifier is read-only against sinks by construction (adapters expose no write methods); gauntlet creates/destroys ONLY compose projects it owns, prefixed `ll-gauntlet-`, and `doctor --sweep` removes orphans.
- Production mode defaults: canary rate capped (≤ 100 lines/s/source unless `--i-know`), index/table names namespaced `lossledger-*` guidance, and the README warns about canary volume in billed/SaaS stores.
- The HMAC key never appears in metrics, reports, or logs.

---

## 9. Milestones and acceptance criteria

### M0: Validation spike (2–4 days): KILL-GATE
Build the smallest honest probe: emitter (plain profile, file+stdout) + verifier with the **filesink** reference adapter + ONE real adapter (pick ClickHouse or Elasticsearch, whichever the owner's environment reaches fastest) + a crude compose testbed with ONE collector (Vector). No faults beyond a manual rotation script. Run it for ≥ 48 h continuous against the compose pipeline AND, if the owner provides access, against a real/staging pipeline of theirs.
**Accept (proceed):** the probe's accounting is self-consistent (false_missing=0, invalid_lines=0) AND at least one of: (i) it detected ≥ 1 genuine delivery anomaly (any severity) in realistic operation, or (ii) the owner, reviewing the 48 h ledger, states the always-green proof itself is worth having (the U5/compliance read), or (iii) a deliberately mis-configured collector (rotation below discovery interval) is caught exactly.
**Stop-and-report:** if (iii) fails: the probe cannot catch a KNOWN loss: the core concept is broken; report to owner. If (iii) passes but (i)/(ii) are both negative, report "works, but demand unproven on our own infra" and await owner decision. M0's ledger output and the owner's verdict go in `docs/progress/M0.md`.

### M1: Trustworthy core + reference gate (days 1–5 post-M0)
Wire format + HMAC; emitter with profiles {plain, json, stacktrace}; verifier with bitmaps, settle scheduler, re-check, anomaly classifier, Prometheus exporter, embedded-state persistence; filesink adapter; safety rails (§8.5); **refpipe**: a scripted reference pipeline (a toy Go "collector" copying files to filesink) with two builds: `refgood` (correct) and `refbad` (drops a seeded range during rotation, duplicates on restart, reorders a window): mirroring SyncGauntlet's conscience.
**Accept (THE GATE):** selfcheck shows refbad caught with exactly the injected MISSING ranges, DUP_BUG counts, and ORDER windows (byte-exact expected-vs-actual in CI); refgood CLEAN over ≥ 100k seqs including emitter restarts (epochs) and verifier restarts mid-run; false_missing_total = 0 throughout; same seed ⇒ identical emitter output (diff-empty).

### M2: Real adapters (days 6–9)
ES/OpenSearch + ClickHouse adapters (CountRange cheap path, bisection, FetchRange, Doctor, SettleHint); ingest guidance docs; `interleave` + `oversize` profiles.
**Accept:** selfcheck matrix passes against real ES and CH in compose; cardinality-approximation bound handled and tested; adapter conformance test suite (one spec, all adapters) green; a seeded 1 M-seq backfill verifies in < 60 s per sink via cheap path.

### M3: Collectors + faults (days 10–14)
Testbed compose for Vector, Fluent Bit, OTel Collector (file source → ES + CH); the four injectors (§7.9); scenario runner + runs.db; `--replay`.
**Accept:** every injector produces its intended anomaly class against refbad-style expectations on at least one collector; 10/10 replay on every scenario marked reproducible; zero exit-2 harness errors across a 30-cell smoke matrix; Vector short-lived-file scenario demonstrably flips CLEAN↔MISSING when `glob_minimum_cooldown_ms` is tuned (the storyboard finding).
**Timebox rule:** any collector whose compose integration exceeds 1.5 days is deferred to stretch with a note: the matrix minimum (G7) governs.

### M4: Loki + the matrix (days 15–18)
Loki adapter; full matrix run (G7 minimum, 5×4×3 stretch); triage: reproduce, minimize, draft findings in `docs/findings/` (NOT filed: owner approval).
**Accept:** matrix complete with per-cell numbers, latency percentiles, dup classes; every non-clean cell has a one-command repro; findings drafts ready; false_missing_total = 0 across the whole matrix.

### M5: Report + launch package (days 19–21)
`report` generator (md + self-contained html); methodology.md (settle windows, taxonomy, attribution limits, §2.4 caveats verbatim); adapters.md + profiles.md guides; CONTRIBUTING/CoC/SECURITY; ≥ 5 good-first-issues; launch post draft; disclosure clocks per §12 started for findings.
**Accept:** a stranger following adapters.md reaches a compiling adapter skeleton; report #1 renders from runs.db; owner has everything needed to publish with one approval; repo stays private until owner confirms employer OSS clearance.

---

## 10. Verification strategy (how we know the auditor isn't lying)

### 10.1 Layers
Unit (wire, bitmap ops, settle math) → property-based (classifier invariants, §10.3) → adapter conformance suite (one spec run against filesink, ES, CH, Loki) → selfcheck gate (§10.2) in CI on every commit → the matrix itself as continuous dogfood → soak (M0's 48 h + a weekly scheduled 24 h refgood soak in CI).

### 10.2 Self-validation gate (the conscience)
`refgood`/`refbad` (§M1) live in-tree forever. refbad's sabotages are documented in `internal/refpipe/README.md`, each mapped to the exact anomaly it must trigger with exact expected ranges (seeded ⇒ computable). The harness is only credible while refbad is caught precisely and refgood stays CLEAN; any change flipping either blocks merge. This mirrors SyncGauntlet §10.2 deliberately: shared discipline, shared credibility story.

### 10.3 Classifier properties (minimum set)
(1) Purity: classify(bitmaps, events, clock) is deterministic: same inputs, same anomalies. (2) Monotone settle: lengthening settle windows never creates new MISSING (can only reclassify LATE→CLEAN path). (3) Epoch safety: truncated epochs never contribute MISSING beyond max observed seq. (4) Replay windows: widening a replay window never converts DUP_REPLAY→DUP_BUG. (5) Conservation: emitted = arrived_distinct + missing + in_flight per (src,epoch,sink) at every report instant.

### 10.4 Flake discipline
A tool that audits pipelines cannot tolerate its own flakes: any CI flake is P1; scenario cells with verdicts that vary across replays are auto-labeled FLAKY in runs.db, reported separately, and never published as findings.

---

## 11. Repository and open-source setup

```
lossledger/
├── README.md            # the MoJ/alloy story first, quickstart (compose up → break it → see red),
│                        # simulation GIF, limitations incl. §2.4 verbatim
├── LICENSE              # Apache-2.0
├── CONTRIBUTING.md      # dev setup, adapter guide, profile guide, PR rules
├── CODE_OF_CONDUCT.md   # Contributor Covenant 2.1
├── SECURITY.md          # harness vulns; links docs/disclosure.md
├── docs/                # methodology, adapters, profiles, disclosure, decisions/, progress/, findings/
├── profiles/ scenarios/ testbed/ cmd/ internal/
└── .github/workflows/   # ci.yml (build+lint+unit+conformance+selfcheck), soak.yml (weekly 24h refgood)
```

Conventions: semver from v0.1.0; `main` protected by CI incl. selfcheck; labels `adapter`, `profile`, `scenario`, `injector`, `docs`, `good-first-issue`; PR template asks "does this change what gets classified as MISSING/DUP? (y/n + why)". Launch venues (owner posts): Show HN titled on the finding, r/devops, r/kubernetes, r/selfhosted, Grafana community, Vector Discord / Fluent Bit Slack (contributor tone), lobste.rs.

Five seed good-first-issues: (1) OpenSearch SigV4 auth option; (2) `journald` writer; (3) Grafana dashboard JSON for §7.8 metrics; (4) new shape profile: Windows-style CRLF + BOM lines; (5) CSV export for gap reports.

---

## 12. Responsible disclosure & publication policy (normative)

1. A **finding** = MISSING or DUP_BUG against a named collector/store version, reproducing 10/10 from a committed scenario on default or explicitly-stated config.
2. Findings go to the maintainer PRIVATELY first (security contact if the project designates one, else issue tracker) with scenario, seed, versions.lock, and repro command. Public matrix withholds the detail for **14 days or until fixed/acknowledged, whichever is sooner**; the aggregate cell may read "1 issue under disclosure."
3. ORDER/LATE findings may be filed publicly immediately (bugs, not hazards).
4. Config-default findings (e.g., a cooldown default) are framed as "default-config behavior," never "X is broken"; the report shows the tuned-config result beside it.
5. Re-test posts after upstream fixes are mandatory follow-ups: they are the alliance-building half of the loop.
6. The agent drafts; **the owner files and publishes. No exceptions.** Repo private until employer OSS clearance is confirmed by the owner.

---

## 13. Risks and mitigations

| Risk | Mitigation |
|---|---|
| False accusation (harness bug read as collector bug): credibility death | §10.2 gate; settle+re-check; false_missing metric published with every result; 10/10 rule; maintainer-first disclosure; verifier never consumes collector self-reports |
| Demand is thinner than the GitHub issues suggest (§2.4) | M0 kill-gate on the owner's own infra; Jepsen-model success metrics (citations, upstream fixes), not DAU |
| VictoriaMetrics generalizes log-verifier to real backends | Their repo watched; our differentiators (multi-collector matrix, shape profiles, gauntlet CI, dup taxonomy) front-loaded into v1; ship the matrix first |
| Unfair MISSING from mis-tuned settle windows | SettleHint per adapter + adaptive window + mandatory re-check + publish the window in every report; maintainers may PR per-sink overrides |
| Adapter maintenance across ES/OS/CH/Loki version churn | Conformance suite pins behavior; adapters are ~200 lines and community-ownable; versions.lock makes breakage visible, not silent |
| Canary volume costs money in billed stores | Rate caps, namespacing, README warning (§8.5) |
| Competes with SyncGauntlet for owner's evenings | Sequencing is explicit (after SyncGauntlet v1); `internal/fault` designed for extraction into a shared library |
| Employer OSS policy | Owner clears repo AND public posts/talks through the firm's process before anything is public; agent keeps repo private until told |

---

## 14. Out of scope now, explicitly on the roadmap

v2: Kafka-hop tap for per-hop attribution; VictoriaLogs + Filebeat + Alloy adapters; Helm chart + operator; journald source parity; metrics/traces canaries (same wire discipline, OTLP shapes); `gauntlet` GitHub Action; public leaderboard site with historical matrix trends; integration with the shared SyncGauntlet fault library once extracted.

---

## 15. Definition of done (v1)

- [ ] M0 passed with owner sign-off recorded in docs/progress/M0.md.
- [ ] M1–M5 acceptance criteria all green; selfcheck + conformance in CI; weekly soak configured.
- [ ] `lossledger_false_missing_total` = 0 across every run in runs.db that feeds report #1.
- [ ] Matrix report #1 generated: ≥ 3 collectors × 4 faults × 2 stores, every non-clean cell one-command reproducible, latency + dup-class tables, Limitations section incl. §2.4.
- [ ] All findings drafted in docs/findings/ with disclosure state tracked; nothing filed or published by the agent.
- [ ] Adapter + profile guides validated by building a skeleton from each.
- [ ] ≥ 5 good-first-issues labeled; CONTRIBUTING/CoC/SECURITY present.
- [ ] Repo private until owner confirms employer OSS clearance.

---

## 16. Glossary

| Term | Meaning |
|---|---|
| Canary line | An emitter-written log line carrying the LL1 token (src, epoch, seq, ts, hmac) |
| Epoch | ULID minted per emitter start; seqs are monotonic within it; restarts never fake gaps |
| Settle window | Per-sink adaptive delay before absence may even be provisionally judged |
| MISSING / DUP_BUG / ORDER / LATE / CLEAN | The anomaly taxonomy (§7.6), severity-ordered |
| DUP_REPLAY | Expected at-least-once duplicate after a collector restart; informational |
| False-missing | A seq declared MISSING that later arrived: the integrity failure the tool must never publish |
| Cheap path / bisection | CountRange over wide ranges; recurse only into mismatching halves; FetchRange only at leaves |
| Shape profile | A data-defined realistic line form (stacktrace, oversize, interleave…): loss is content-dependent |
| Gauntlet | CI mode: composed pipeline + fault injection + loss budgets + replayable scenarios |
| refgood / refbad | In-tree correct and sabotaged reference pipelines powering the self-validation gate |
| The matrix | The published collectors × faults × stores loss table: the launch artifact |

---

## Appendix A: Worked example: one gauntlet run, end to end

Scenario `rotation_storm_vector.yaml`, seed 7.
1. Testbed compose up (Vector → ES + CH), versions.lock recorded; emitter starts epoch `01JC…`, 50 lines/s, shape mix per scenario.
2. At emission count 2000 (+ seeded jitter), `rotatestorm` fires: 30 rapid rotations, several files living < 400 ms: below Vector's default discovery cooldown.
3. Emission ends at seq 6000; verifier runs settle (max_wait 90 s), CountRange both sinks: ES distinct 5 812, CH distinct 5 812; bisection isolates identical gaps ⇒ attribution: source-side.
4. Mandatory re-check confirms; classifier emits MISSING ranges (e.g., seq 2 041–2 118, 2 305–2 414, …) totalling 188; dup=0; order=3 (interleave profile, informational).
5. runs.db row written; exit 40; `--replay` reproduces 10/10. Finding draft: "Vector <ver>, default glob_minimum_cooldown_ms=60000: 3.1% loss under sub-second file churn; tuned to 1000 ms: 0 loss (repro attached)." Owner notified; disclosure clock per §12.

## Appendix B: v1 matrix sizing

Minimum (G7): 3 collectors × 4 faults × 2 stores × (1 baseline no-fault + fault cell) ≈ 48 scenario cells × 2–10 seeds (reproducibility tier) ≈ 150–300 runs: one overnight. Stretch: +Alloy, +Filebeat, +Loki store ⇒ 5×4×3 = 60 cells ≈ 400–600 runs. Exact trim lives in `scenarios/suites.yaml`.

## Appendix C: Production sizing guidance (docs excerpt)

Default prod deployment: 1 emitter per node at 10 lines/s plain+stacktrace mix ⇒ ~864 k canary lines/node/day (~100–200 MB/day/node in ES before ILM; far less in CH/Loki). Verifier cheap-path cost: ≤ 1 CountRange query/source/sink/minute. Both numbers go in the README so cost objections are answered before they're raised.
