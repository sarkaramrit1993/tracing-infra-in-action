#!/usr/bin/env bash
# Shows how much of the kept set, and how much of the real traffic, is slow or
# failing requests.
#
# Usage: ./scripts/show-survivor-mix.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

ch_table "
SELECT round(100 * countIf(adjusted_count < 100) / count(), 1) AS pct_of_kept,
       round(100 * sumIf(adjusted_count, adjusted_count < 100)
             / sum(adjusted_count), 1)                        AS pct_of_traffic
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''"
