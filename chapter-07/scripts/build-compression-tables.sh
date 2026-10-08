#!/usr/bin/env bash
# Drops any scratch table a previous run left, then creates the two the
# compression exercise compares: listing 7.1's declarations, and none of them.
#
# Usage: ./scripts/build-compression-tables.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
for t in $COMPRESS_TABLES; do
  ch --query "DROP TABLE IF EXISTS tracing.$t"
done

ch --query "
CREATE TABLE tracing.compress_listing
(
    timestamp      DateTime64(9) CODEC(Delta, ZSTD(1)),
    trace_id       String CODEC(ZSTD(1)),
    span_id        String CODEC(ZSTD(1)),
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
ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id)"

ch --query "
CREATE TABLE tracing.compress_plain
(
    timestamp      DateTime64(9),
    trace_id       String,
    span_id        String,
    service_name   String,
    span_name      String,
    status_code    String,
    duration_ns    UInt64,
    adjusted_count Float64 DEFAULT 1.0,
    attributes     Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id)"

echo "created tracing.compress_listing and tracing.compress_plain, both empty"
