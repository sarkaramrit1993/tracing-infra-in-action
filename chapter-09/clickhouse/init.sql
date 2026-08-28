-- Chapter 9: the span store. Applied automatically on first boot.
--
-- This is chapter 7's listing 7.1 table, carried forward with two differences,
-- both of which chapter 9 depends on:
--
--   1. parent_span_id is present. Listing 7.1 does not carry it. Chapter 8
--      added it because a table holding one row per span cannot answer a
--      question about REQUESTS without a way to pick out the root. Chapter 9
--      needs it for a second reason: an exemplar or a log line hands you a
--      trace_id, and reassembling that trace into something a human reads
--      means knowing which span hung off which.
--
--   2. There is no storage_policy. Chapter 7 pinned this table to a hot+cold
--      volume policy defined in its own config.d/storage.xml. That file is a
--      chapter 7 artifact and it is not here, so the SETTINGS line is gone too.
--      Left in, ClickHouse fails to create the table on first boot and the
--      whole stack comes up with an empty store and no obvious reason why.
--
-- adjusted_count is the sampling weight from section 7.4.4: the reciprocal of
-- the probability its trace was kept at. An unsampled span carries 1.

CREATE DATABASE IF NOT EXISTS tracing;

-- ---- Listing 7.1 (carried forward): the span table chapter 9 reads ----------
CREATE TABLE IF NOT EXISTS tracing.otel_traces
(
    timestamp      DateTime64(9) CODEC(Delta, ZSTD(1)),
    trace_id       String CODEC(ZSTD(1)),
    span_id        String CODEC(ZSTD(1)),
    parent_span_id String CODEC(ZSTD(1)),
    service_name   LowCardinality(String) CODEC(ZSTD(1)),
    span_name      LowCardinality(String) CODEC(ZSTD(1)),
    status_code    LowCardinality(String) CODEC(ZSTD(1)),
    duration_ns    UInt64 CODEC(T64, ZSTD(1)),
    adjusted_count Float64 DEFAULT 1.0 CODEC(ZSTD(1)),
    attributes     Map(LowCardinality(String), String) CODEC(ZSTD(3)),
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id)
TTL toDateTime(timestamp) + INTERVAL 15 DAY;
-- ---- end listing 7.1
