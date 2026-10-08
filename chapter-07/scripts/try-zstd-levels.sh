#!/usr/bin/env bash
# Copies compress_listing's rows into two tables that change only the ZSTD
# level on attributes, and compares that column at levels 1, 3 and 9.
#
# Usage: ./scripts/try-zstd-levels.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_compression_loaded
ch --query "DROP TABLE IF EXISTS tracing.compress_attr_zstd1"
ch --query "DROP TABLE IF EXISTS tracing.compress_attr_zstd9"
ch --query "CREATE TABLE tracing.compress_attr_zstd1 AS tracing.compress_listing"
ch --query "CREATE TABLE tracing.compress_attr_zstd9 AS tracing.compress_listing"
ch --query "
ALTER TABLE tracing.compress_attr_zstd1
MODIFY COLUMN attributes Map(LowCardinality(String), String) CODEC(ZSTD(1))"
ch --query "
ALTER TABLE tracing.compress_attr_zstd9
MODIFY COLUMN attributes Map(LowCardinality(String), String) CODEC(ZSTD(9))"
ch --query "INSERT INTO tracing.compress_attr_zstd1 SELECT * FROM tracing.compress_listing"
ch --query "INSERT INTO tracing.compress_attr_zstd9 SELECT * FROM tracing.compress_listing"
ch --query "OPTIMIZE TABLE tracing.compress_attr_zstd1 FINAL"
ch --query "OPTIMIZE TABLE tracing.compress_attr_zstd9 FINAL"

ch_table "
SELECT compression_codec AS attributes_codec,
       formatReadableSize(data_compressed_bytes) AS attributes_on_disk
FROM system.columns
WHERE database = 'tracing' AND name = 'attributes'
  AND table IN ('compress_attr_zstd1', 'compress_listing', 'compress_attr_zstd9')
ORDER BY data_compressed_bytes DESC"
