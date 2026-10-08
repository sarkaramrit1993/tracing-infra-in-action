#!/usr/bin/env bash
# Asks listing 8.1's weighted questions as if every weight were 1, without
# touching the table.
#
# Usage: ./scripts/ignore-the-weight.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

ch_table "
SELECT service_name,
       sum(1) AS requests,
       round(quantileExactWeighted(0.99)(duration_ns, toUInt64(1)) / 1e6, 1) AS p99_ms
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''
GROUP BY service_name"
