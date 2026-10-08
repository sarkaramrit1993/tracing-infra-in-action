#!/usr/bin/env bash
# Prints every span of the most recent failed checkout, root span first.
#
# Usage: ./scripts/show-failing-trace.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require clickhouse

rows=$(ch --query "
SELECT
  if(parent_span_id = '', '>', ' ') AS root,
  substring(trace_id, 1, 8) AS trace,
  span_name AS span,
  concat(toString(round(duration_ns / 1e6, 1)), 'ms') AS took,
  status_code
FROM tracing.otel_traces
WHERE trace_id = (
  SELECT trace_id FROM tracing.otel_traces
  WHERE status_code = 'STATUS_CODE_ERROR' ORDER BY timestamp DESC LIMIT 1)
ORDER BY parent_span_id = '' DESC, timestamp")

[ -n "$rows" ] || die "no failed trace in ClickHouse yet. Run ./scripts/send-traffic.sh, then ./scripts/wait-until-ready.sh"
printf '%s\n' "$rows" | q table
