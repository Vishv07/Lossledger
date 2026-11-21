-- The canary table the ClickHouse adapter queries.
--
-- Kept here rather than only in the adapter so the testbed is self-contained,
-- and checked against adapter.Schema() by a test so the two cannot drift.
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
