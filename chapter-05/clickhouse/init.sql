-- Chapter 5: ClickHouse schema for the storage-time path.
-- Modeled after the SigNoz traces schema: one wide row per span with the
-- attributes payload kept as a Map for late binding. The sorting key
-- (trace_id, start_time) keeps trace assembly to a contiguous range scan,
-- and the partition key bucketizes by hour for cheap retention drops.

CREATE DATABASE IF NOT EXISTS tracing;

CREATE TABLE IF NOT EXISTS tracing.otel_traces
(
    timestamp           DateTime64(9, 'UTC')        CODEC(Delta, ZSTD(1)),
    trace_id            String                      CODEC(ZSTD(1)),
    span_id             String                      CODEC(ZSTD(1)),
    parent_span_id      String                      CODEC(ZSTD(1)),
    trace_state         String                      CODEC(ZSTD(1)),
    span_name           LowCardinality(String)      CODEC(ZSTD(1)),
    span_kind           LowCardinality(String)      CODEC(ZSTD(1)),
    service_name        LowCardinality(String)      CODEC(ZSTD(1)),
    resource_attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    scope_name          LowCardinality(String)      CODEC(ZSTD(1)),
    scope_version       LowCardinality(String)      CODEC(ZSTD(1)),
    span_attributes     Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    duration            Int64                       CODEC(ZSTD(1)),
    status_code         LowCardinality(String)      CODEC(ZSTD(1)),
    status_message      String                      CODEC(ZSTD(1)),
    events_timestamp    Array(DateTime64(9, 'UTC')) CODEC(ZSTD(1)),
    events_name         Array(LowCardinality(String)) CODEC(ZSTD(1)),
    events_attributes   Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),
    links_trace_id      Array(String)               CODEC(ZSTD(1)),
    links_span_id       Array(String)               CODEC(ZSTD(1)),
    links_trace_state   Array(String)               CODEC(ZSTD(1)),
    links_attributes    Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),

    INDEX idx_trace_id      trace_id      TYPE bloom_filter GRANULARITY 4,
    INDEX idx_service_name  service_name  TYPE bloom_filter GRANULARITY 4,
    INDEX idx_span_name     span_name     TYPE bloom_filter GRANULARITY 4,
    INDEX idx_status_code   status_code   TYPE set(8)       GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toStartOfHour(timestamp)
ORDER BY (trace_id, timestamp)
TTL toDateTime(timestamp) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- Sibling table for the resource tier. Keeps resource_attributes deduplicated
-- by service so the main table does not have to repeat the full map on every
-- row. Materialized views downstream join through this on service_name.
CREATE TABLE IF NOT EXISTS tracing.otel_traces_resource
(
    timestamp           DateTime64(9, 'UTC')        CODEC(Delta, ZSTD(1)),
    service_name        LowCardinality(String)      CODEC(ZSTD(1)),
    resource_attributes Map(LowCardinality(String), String) CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree(timestamp)
PARTITION BY toStartOfHour(timestamp)
ORDER BY (service_name, timestamp)
TTL toDateTime(timestamp) + INTERVAL 30 DAY;
