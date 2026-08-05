-- Chapter 8: the query-tier table. Applied automatically on first boot.
--
-- This is chapter 7's listing 7.1 schema with two deliberate differences, both
-- of which chapter 8 depends on:
--
--   1. parent_span_id is present. Listing 7.1 does not carry it, and without it
--      there is no way to ask a question about REQUESTS. The table stores one
--      row per span and the producer writes seven spans per request, so a bare
--      count() over this table answers a question nobody asked. Every count and
--      rate query in chapter 8 filters to parent_span_id = '', the one root span
--      that stands for each request.
--
--   2. There is NO bloom filter on trace_id. Listing 7.1 creates one. Chapter 8
--      leaves it off on purpose so that listing 8.2 has an index to add and a
--      before-and-after EXPLAIN to show. Adding a second identical bloom next to
--      one that is already pruning demonstrates nothing, because the first one
--      has already done the work.
--
-- adjusted_count is the sampling weight from section 7.4.3: the reciprocal of
-- the probability its trace was kept at. An unsampled span carries 1.

CREATE DATABASE IF NOT EXISTS tracing;

CREATE TABLE IF NOT EXISTS tracing.otel_traces
(
    timestamp      DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    trace_id       String CODEC(ZSTD(1)),
    span_id        String CODEC(ZSTD(1)),
    parent_span_id String CODEC(ZSTD(1)),
    service_name   LowCardinality(String) CODEC(ZSTD(1)),
    span_name      LowCardinality(String) CODEC(ZSTD(1)),
    status_code    LowCardinality(String) CODEC(ZSTD(1)),
    duration_ns    UInt64 CODEC(T64, ZSTD(1)),
    adjusted_count Float64 DEFAULT 1.0 CODEC(ZSTD(1)),
    attributes     Map(LowCardinality(String), String) CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id);

-- What the generator produced before it sampled anything. This is the whole
-- reason the exercises can prove an estimate correct rather than merely
-- different: in production the population is gone, here it is one row.
CREATE TABLE IF NOT EXISTS tracing.ground_truth
(
    run_id       String,
    generated_at DateTime,
    requests     UInt64,
    p99_ms       Float64,
    errors       UInt64
)
ENGINE = MergeTree
ORDER BY run_id;

-- The keep rate per class, as a table rather than a constant buried in a
-- script, so a reader can read the policy and check that adjusted_count really
-- is 1 / keep_rate rather than take it on trust.
CREATE TABLE IF NOT EXISTS tracing.sampling_policy
(
    class           LowCardinality(String),
    keep_rate       Float64,
    adjusted_count  Float64
)
ENGINE = MergeTree
ORDER BY class;
