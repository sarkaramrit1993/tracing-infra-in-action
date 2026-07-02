-- Chapter 5: RED metrics materialized views.
-- These run the aggregate-first pattern from section 5.4: each span contributes
-- to a per-service-per-minute rollup, and the views feed dashboards without
-- the dashboards ever needing to assemble a trace.

-- Per-service-per-operation span counts and error counts in 1-minute buckets.
-- The cardinality basis is (service_name, span_name): span_name is the
-- operation, matching the rollup key section 5.4.2 describes.
CREATE TABLE IF NOT EXISTS tracing.red_service_minute
(
    ts_bucket_start DateTime           CODEC(Delta, ZSTD(1)),
    service_name    LowCardinality(String) CODEC(ZSTD(1)),
    span_name       LowCardinality(String) CODEC(ZSTD(1)),
    span_count      AggregateFunction(count),
    error_count     AggregateFunction(countIf, UInt8),
    duration_p50    AggregateFunction(quantileTDigest(0.50), Int64),
    duration_p95    AggregateFunction(quantileTDigest(0.95), Int64),
    duration_p99    AggregateFunction(quantileTDigest(0.99), Int64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toStartOfDay(ts_bucket_start)
ORDER BY (service_name, span_name, ts_bucket_start)
TTL ts_bucket_start + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS tracing.red_service_minute_mv
TO tracing.red_service_minute
AS SELECT
    toStartOfMinute(timestamp) AS ts_bucket_start,
    service_name,
    span_name,
    countState() AS span_count,
    countIfState(toUInt8(status_code = 'STATUS_CODE_ERROR')) AS error_count,
    quantileTDigestState(0.50)(duration) AS duration_p50,
    quantileTDigestState(0.95)(duration) AS duration_p95,
    quantileTDigestState(0.99)(duration) AS duration_p99
FROM tracing.otel_traces
GROUP BY ts_bucket_start, service_name, span_name;
