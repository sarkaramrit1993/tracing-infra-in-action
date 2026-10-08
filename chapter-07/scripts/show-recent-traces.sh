#!/usr/bin/env bash
# Prints the three most recent checkout traces, one row per span, with `>`
# where a new trace begins.
#
# Usage: ./scripts/show-recent-traces.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_ready
ch_table "
SELECT
  if(trace_id != lagInFrame(trace_id) OVER (ORDER BY trace_id, timestamp), '>', '') AS t,
  concat(substring(trace_id, 1, 8), '...') AS trace,
  formatDateTime(timestamp, '%H:%i:%S.%f') AS time,
  span_name AS span,
  round(duration_ns / 1000000.0, 3) AS took_ms,
  adjusted_count AS weight,
  status_code
FROM tracing.otel_traces
WHERE trace_id IN (
  SELECT trace_id FROM tracing.otel_traces
  WHERE span_name = 'GET /checkout' ORDER BY timestamp DESC LIMIT 3)
ORDER BY trace_id, timestamp"
