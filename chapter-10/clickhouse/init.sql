-- Chapter 10: the span store. Applied automatically on first boot.
--
-- The chapter 9 table, unchanged. adjusted_count is the sampling weight from
-- section 7.4.3 and nothing in this stack writes it, so it keeps its default of
-- 1.0 on every row. That is what makes the stored, sampled population undercount
-- in section 10.4.

CREATE DATABASE IF NOT EXISTS tracing;

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
