# Extensions: where else this engine fits

LossLedger's engine is domain-general. Sequence-numbered signed records travel
through an untouched pipeline, a verifier asks the endpoints which numbers
arrived, and exact gap ranges come out.

This document records which other pipelines that engine actually fits, which it
does not, and why. It exists so the question is answered once with evidence
rather than re-argued every time someone suggests a new adapter.

**None of this is scheduled.** v1 still has no measured finding, no real store
adapter and no users. Everything here is after that.

---

## The filter

One question decides it:

> **Can the canary ride the same path as real data, with no configuration
> change to the thing under test?**

If it cannot, the canary is on a different code path than production data. A
failure specific to that other path then hides from the canary, and the tool
reports CLEAN while data is being lost. For a product whose only asset is
trust, a built-in false negative is worse than not shipping.

This follows directly from the honest-boundary principle in the PRD section 5:
*nothing injected into or patched onto the pipeline under test.*

---

## Summary

| Candidate | Same path | Supply today | Verdict |
|---|---|---|---|
| SIEM and audit-log ingestion | yes | source-level only | **best fit** |
| Prometheus remote_write | yes | blind counters | strong, one open question |
| S3 event notifications | yes | documented workaround | skip |
| Kafka topic delivery | yes | fully solved | dead |
| CDC (Debezium, Flink CDC) | **no** | partially served | dead, see below |

---

## Best fit: SIEM and audit-log completeness

### The pain, and a regulator asking for it

Security teams have to prove completeness, not merely observe health:

> Proving that the event stream arriving in the SIEM is complete, current, and
> attributable to the source you intended to monitor is a key concern. The
> right question is not whether the SIEM can ingest logs, but whether each
> critical source can be independently proven to be delivering the expected
> events at the expected cadence.

> Monitoring integrity and source completeness are core continuous monitoring
> concerns under NIST CSF 2.0, and proving monitoring completeness is a
> governance and accountability requirement.

> A SIEM that appears healthy is often hiding silent failures. Agents that
> stopped reporting, source types with days of missing data, and log pipelines
> that quietly dropped events all create blind spots that look like normal
> operations.

### Why there is room

The incumbents work at source level, not event level. CardinalOps reports
telemetry health as "missing fields and stale log sources". Anvilogic covers
"source asset coverage, log delivery timeliness, log field availability". The
existing synthetic practice is "a small heartbeat event inserted at the edge
and verified at the index".

All of that answers *is anything arriving*. None of it answers *which events did
not arrive*. Stated in the research directly:

> SIEMs rarely monitor their own log ingestion beyond "source went fully
> silent", and almost none natively alert when a source's volume deviates from
> its baseline before the gap becomes weeks wide.

This is the same distinction as loki-canary versus LossLedger in PRD section
2.2, so the existing argument transfers without modification.

### Why it passes the filter

Canary events are written into the same log source the agent already collects:
the same file the forwarder monitors, the same syslog stream, the same event
channel. No allowlist, no connector configuration, no separate path.

### What it would take

A Splunk adapter, later a Sentinel one. The wire format, taxonomy, settle
window, bisection and integrity metric are all unchanged. This is closer to
positioning than to engineering, which is what makes it attractive.

It is also not a new persona. It is U5 in the PRD, the compliance and audit
owner, who is already written down as documentation-only in v1.

---

## Strong second: Prometheus remote_write

The PRD already names metrics as the v2 direction (NG1), and the loss is
documented and of exactly the kind this project exists to catch.

| Issue | Behaviour |
|---|---|
| prometheus/prometheus #19665 | A series with a duplicate label name is answered 204 No Content and silently dropped |
| Non-finite values | NaN and Infinity are dropped, success is returned, nothing is retried |
| prometheus/prometheus #5389 | An upgrade caused roughly 30 percent metric loss through remote_write |
| opentelemetry-collector-contrib #51983 | A default 5 second timeout caps retries, so all metrics are dropped during a backend outage |

The counter operators are told to watch is
`prometheus_remote_storage_samples_dropped_total`. The duplicate-label case
returns **success**, so it is never counted. That is this project's thesis
restated in a second domain: a component can only count the drops it notices.

### How the model maps

A canary gauge whose value **is** the sequence number, incrementing on each
scrape. "Which samples arrived" becomes a range query over that series, which
is `CountRange` unchanged.

### The open question

Signing. Labels are per-series, not per-sample, so the HMAC cannot travel with
each sample the way it does on a log line. The result is per-epoch signing
rather than per-record, which is a weaker guarantee than LossLedger currently
makes. That needs a decision before any code, not an assumption.

---

## Skip: S3 event notifications

Loss is real. AWS once documented that "on very rare occasions, events might be
lost" and later removed the sentence, and the delivery model is explicitly at
least once.

But AWS documents the workaround themselves: audit with the LIST Objects API or
S3 Inventory. A rare failure with a published one-line remedy is not a product.

---

## Dead: Kafka topic delivery

LinkedIn's **Xinfra Monitor** already implements this algorithm exactly:

> ProduceService produces messages with integer index in the message payload,
> and ConsumeService reads the index from message payload, comparing it with
> the last index observed from the same partition, to determine whether there
> is lost or duplicated message.

Sequence-numbered canary accounting, open source, proven at LinkedIn's scale.
There is nothing here to add.

It is worth noting for a different reason. PRD section 2.2 lists four
organisations that independently funded this harness internally: AWS, Grafana,
VictoriaMetrics and OpenTelemetry. LinkedIn is the fifth, and the fifth
independent build of the same tool is a stronger argument than the fourth.

---

## Dead: CDC, and why

Investigated separately and rejected on a structural fault rather than on
demand. The demand is real: flink-cdc #2500 (Oracle loses records under
incremental snapshot), flink-cdc #762 (Oracle 19c loses fields and data),
FLINK-37065 (MySQL CDC skips transactions during checkpoint recovery),
flink-cdc #1206 (TiDB loses data at the snapshot handover), risingwave #24854
(silent CDC source failure after snapshot), and a practitioner report of an
inventory pipeline running three weeks with no alerts and 40,000 missing
updates found only by manual reconciliation.

It fails the filter for two reasons.

**The canary cannot ride the same path.** A CDC canary needs its own table, and
that table must be added to the connector's capture allowlist. One source
states the consequence plainly: Flink CDC pipelines "capture only their
specified tables, so a canary row written to the source never reaches the
destination". Once the canary has a dedicated allowlisted table it is on a
different capture path than the production tables, and a per-table capture
failure, which is what flink-cdc #762 describes, would leave the canary
reporting CLEAN while real data is lost.

**A steady canary does not exercise where CDC fails.** The documented losses
live in the incremental snapshot, the snapshot to streaming handover, and
checkpoint recovery after a restart. All transitions. A canary emitting at a
fixed rate tests the steady state, which is the part that mostly works.

**One angle left unexplored.** Debezium's own signal table is allowlisted by
design and already carries writes. Whether a canary can ride that without a
configuration change is a narrower question than the one rejected here, and it
is the only thing that would reopen CDC.

---

## Sources

- linkedin/kafka-monitor, Xinfra Monitor design overview
- prometheus/prometheus #19665, #5389
- opentelemetry-collector-contrib #51983
- apache/flink-cdc #2500, #762, #1206; FLINK-37065
- risingwavelabs/risingwave #24854
- AWS S3 event notification delivery semantics
- NIST CSF 2.0 continuous monitoring guidance, via practitioner write-ups
- CardinalOps and Anvilogic product descriptions
