# shellcheck shell=bash
# Sourced by the agent scripts. Not a reader step.
# shellcheck source=scripts/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NO_AGENT_TRAFFIC="no agent traffic yet: run ./scripts/send-agent-traffic.sh first"
AGENT_GRAPH='traces_service_graph_request_total{client="support-agent"'

# run_agent FAIL: one agent run through the Collector. FAIL=1 makes the refund
# fail on every attempt. Sets TRACE_ID to the run's trace.
run_agent() {
  local out
  out=$(AGENT_FAIL_LAST_TOOL=$1 docker compose --profile agent run --rm -T agent 2>&1) \
    || die "The agent run failed: $out"
  TRACE_ID=$(printf '%s\n' "$out" | sed -n 's/.*trace_id=\([0-9a-f]*\).*/\1/p' | tail -n 1)
  [ -n "$TRACE_ID" ] || die "The agent printed no trace ID: $out"
}

# edge_count [EXTRA]: calls the service graph has counted from the agent, 0 when
# it has counted none. EXTRA narrows the series, for example ,failed=\"true\".
edge_count() {
  local n
  n=$(q sum "sum($AGENT_GRAPH$1})") || return 1
  [ "$n" = none ] && n=0
  echo "$n"
}

# stored_spans IDS: how many spans of the comma-separated quoted trace IDs
# ClickHouse holds, optionally narrowed by a SQL condition in $2.
stored_spans() {
  ch --query "SELECT count() FROM tracing.otel_traces WHERE trace_id IN ($1) ${2:+AND $2}"
}

# stored_traces IDS: how many of the comma-separated quoted trace IDs have any
# span in ClickHouse.
stored_traces() {
  ch --query "SELECT uniqExact(trace_id) FROM tracing.otel_traces WHERE trace_id IN ($1)"
}

# agent_state KEY: one value send-agent-traffic.sh saved for this Collector.
agent_state() {
  [ "$(state agent COLLECTOR_ID "$NO_AGENT_TRAFFIC")" = "$(q instance "$COLLECTOR")" ] \
    || die "$NO_AGENT_TRAFFIC"
  state agent "$1" "$NO_AGENT_TRAFFIC"
}
