#!/usr/bin/env bash
# Whole-table bytes for both compression tables, on disk and before compression.
#
# Usage: ./scripts/compare-table-sizes.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_compression_loaded
ch_table "
SELECT table,
       formatReadableSize(sum(data_compressed_bytes))   AS on_disk,
       formatReadableSize(sum(data_uncompressed_bytes)) AS raw,
       sum(rows) AS rows
FROM system.parts
WHERE database = 'tracing' AND active
  AND table IN ('compress_listing', 'compress_plain')
GROUP BY table ORDER BY table"
