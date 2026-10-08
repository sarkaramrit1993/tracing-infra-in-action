#!/usr/bin/env bash
# Looks up the last checkout trace send-traffic.sh made, by its trace id.
#
# Usage: ./scripts/look-up-one-trace.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_ready
TID=$(last_trace)
echo "trace_id = $TID"
echo
ch_table "
SELECT span_name, round(duration_ns / 1000000.0, 1) AS took_ms
FROM tracing.otel_traces WHERE trace_id = '$TID' ORDER BY timestamp"
