#!/usr/bin/env bash
# Counts distinct trace IDs next to the weighted request count.
#
# Usage: ./scripts/count-distinct-traces.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

ch_table "
SELECT uniqExact(trace_id) AS distinct_traces,
       sum(adjusted_count) AS weighted_requests
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''"
