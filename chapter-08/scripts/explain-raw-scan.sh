#!/usr/bin/env bash
# Asks ClickHouse how many granules the raw request count reads, step by step.
#
# Usage: ./scripts/explain-raw-scan.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

ch --query "
EXPLAIN indexes = 1
SELECT sum(adjusted_count) FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''" | awk '
  /^ *(MinMax|Partition|PrimaryKey|Skip)$/ { step = $1 }
  /Granules:/ { printf "%-12s granules read %s\n", step, $2 }'
