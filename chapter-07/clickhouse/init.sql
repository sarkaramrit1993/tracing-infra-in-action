-- Chapter 7: ClickHouse trace store, listing 7.1.
--
-- This is the span-per-row OLAP archetype from section 7.2: one wide row per
-- span in a MergeTree table. The schema below is listing 7.1 verbatim, wrapped
-- only in a database and an IF NOT EXISTS so the container entrypoint can run it
-- idempotently. The companion stack feeds it from the same partition-by-trace-ID
-- Collector path as chapter-05/.
--
-- Three design choices make MergeTree fit traces, all visible here:
--   1. ORDER BY puts low-cardinality columns first so the sorted prefix
--      compresses by dictionary and run-length encoding; trace_id rides last.
--   2. PARTITION BY toYYYYMMDD(timestamp) makes retention a DROP PARTITION
--      metadata operation, not a row-by-row tombstone delete.
--   3. A bloom_filter skip index answers trace_id membership without sorting on
--      a random column.

CREATE DATABASE IF NOT EXISTS tracing;

-- ---- Listing 7.1: A ClickHouse trace table sized for compression and retention
CREATE TABLE IF NOT EXISTS tracing.otel_traces
(
    timestamp      DateTime64(9) CODEC(Delta, ZSTD(1)),
    trace_id       String CODEC(ZSTD(1)),
    span_id        String CODEC(ZSTD(1)),
    service_name   LowCardinality(String) CODEC(ZSTD(1)),
    span_name      LowCardinality(String) CODEC(ZSTD(1)),
    status_code    LowCardinality(String) CODEC(ZSTD(1)),
    duration_ns    UInt64 CODEC(T64, ZSTD(1)),
    -- adjusted_count: the sample-rate reciprocal from section 7.4.4. A span with
    -- no sampling carries weight 1.0; a 1-in-100 sampled span carries 100.0.
    -- Downstream query listings sum this column to weight kept spans back to
    -- the population they represent.
    adjusted_count Float64 DEFAULT 1.0 CODEC(ZSTD(1)),
    attributes     Map(LowCardinality(String), String) CODEC(ZSTD(3)),
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id)
TTL toDateTime(timestamp) + INTERVAL 15 DAY
-- storage_policy = 'tiered' binds this table to the hot+cold volume policy
-- defined in config.d/storage.xml. It is what listing 7.2's `TO VOLUME 'cold'`
-- clause (in tiering.sql) resolves against; without it, there is no volume named
-- 'cold' and the tiering ALTER fails. The policy is an operator/server concern,
-- separate from the schema's shape.
SETTINGS storage_policy = 'tiered';
-- ---- end listing 7.1
