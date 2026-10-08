#!/usr/bin/env bash
# Copies compress_listing's rows into three tables that each change the sort key,
# the timestamp codec, or both, and compares the timestamp column across all four.
#
# Usage: ./scripts/try-clock-first-sort-key.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_compression_loaded
ch --query "DROP TABLE IF EXISTS tracing.compress_clock_first"
ch --query "DROP TABLE IF EXISTS tracing.compress_clock_first_no_delta"
ch --query "DROP TABLE IF EXISTS tracing.compress_no_delta"

ch --query "
CREATE TABLE tracing.compress_clock_first AS tracing.compress_listing
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, service_name, span_name, trace_id)"
ch --query "CREATE TABLE tracing.compress_clock_first_no_delta AS tracing.compress_clock_first"
ch --query "CREATE TABLE tracing.compress_no_delta AS tracing.compress_listing"
ch --query "
ALTER TABLE tracing.compress_clock_first_no_delta
MODIFY COLUMN timestamp DateTime64(9) CODEC(ZSTD(1))"
ch --query "
ALTER TABLE tracing.compress_no_delta
MODIFY COLUMN timestamp DateTime64(9) CODEC(ZSTD(1))"

ch --query "INSERT INTO tracing.compress_clock_first SELECT * FROM tracing.compress_listing"
ch --query "INSERT INTO tracing.compress_clock_first_no_delta SELECT * FROM tracing.compress_listing"
ch --query "INSERT INTO tracing.compress_no_delta SELECT * FROM tracing.compress_listing"
ch --query "OPTIMIZE TABLE tracing.compress_clock_first FINAL"
ch --query "OPTIMIZE TABLE tracing.compress_clock_first_no_delta FINAL"
ch --query "OPTIMIZE TABLE tracing.compress_no_delta FINAL"

ch_table "
SELECT
  if(table LIKE '%clock_first%', 'timestamp first', 'listing 7.1') AS sort_key,
  if(compression_codec LIKE '%Delta%', 'Delta, ZSTD(1)', 'ZSTD(1)') AS timestamp_codec,
  formatReadableSize(data_compressed_bytes) AS timestamp_on_disk
FROM system.columns
WHERE database = 'tracing' AND name = 'timestamp'
  AND table IN ('compress_listing', 'compress_no_delta',
                'compress_clock_first', 'compress_clock_first_no_delta')
ORDER BY sort_key DESC, timestamp_codec"
