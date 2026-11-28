#!/usr/bin/env bash
# Walks the critical path of one ordinary agent run and prints each step's share
# of the run, then names the step that holds the most time.
#
# Shares only, never seconds: the stand-in model sleeps for a set time per call,
# so the seconds belong to the stand-in and the shares are what the walk shows.
#
# Usage: ./scripts/show-agent-critical-path.sh
# shellcheck source=scripts/agent-lib.sh
source "$(dirname "$0")/agent-lib.sh"
require clickhouse
OK_TRACE=$(agent_state OK_TRACE)

spans=$(ch --query "
SELECT span_id, parent_span_id, span_name,
  toUnixTimestamp64Nano(timestamp) AS start_ns, duration_ns
FROM tracing.otel_traces
WHERE trace_id = '$OK_TRACE'
FORMAT JSONEachRow")
[ -n "$spans" ] || die "trace $OK_TRACE is not in ClickHouse yet. Run ./scripts/send-agent-traffic.sh again"
printf '%s\n' "$spans" | python3 scripts/critical_path.py
