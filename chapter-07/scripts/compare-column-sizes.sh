#!/usr/bin/env bash
# Listing 7.3's per-column query, widened to read both compression tables at once.
#
# Usage: ./scripts/compare-column-sizes.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_compression_loaded
ch_table "
SELECT
  name AS column,
  formatReadableSize(sumIf(data_compressed_bytes, table = 'compress_listing')) AS listing_7_1,
  formatReadableSize(sumIf(data_compressed_bytes, table = 'compress_plain'))   AS plain,
  round(sumIf(data_compressed_bytes, table = 'compress_plain')
      / sumIf(data_compressed_bytes, table = 'compress_listing'), 1)          AS smaller_by
FROM system.columns
WHERE database = 'tracing' AND table IN ('compress_listing', 'compress_plain')
GROUP BY name
ORDER BY sumIf(data_compressed_bytes, table = 'compress_plain') DESC"
