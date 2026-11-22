#!/usr/bin/env bash
# Prints the service graph edges that start at the agent, then the spans that
# ended in error in the run whose refund failed every time.
#
# The service graph connector writes its counts out once a minute, so this waits
# until the graph has counted every call the agent made.
#
# Usage: ./scripts/show-agent-dependencies.sh
# shellcheck source=scripts/agent-lib.sh
source "$(dirname "$0")/agent-lib.sh"
require prometheus otel-collector clickhouse
TARGET_ALL=$(agent_state TARGET_ALL)
TARGET_FAILED=$(agent_state TARGET_FAILED)
FAIL_TRACE=$(agent_state FAIL_TRACE)

graph_counted_every_call() {
  local all failed
  all=$(edge_count "") || return 1
  failed=$(edge_count ',failed="true"') || return 1
  ge "$all" "$TARGET_ALL" && ge "$failed" "$TARGET_FAILED"
}

poll "waiting for the service graph to reach Prometheus" 240 graph_counted_every_call
q edges support-agent
echo
echo "Spans with an error status in the run whose refund failed every time:"
ch --query "
SELECT s.service_name AS service, s.span_name AS span, p.span_name AS parent
FROM tracing.otel_traces AS s
LEFT JOIN tracing.otel_traces AS p ON p.trace_id = s.trace_id AND p.span_id = s.parent_span_id
WHERE s.trace_id = '$FAIL_TRACE' AND s.status_code = 'STATUS_CODE_ERROR'
ORDER BY s.timestamp, s.span_id
FORMAT TSVWithNames" | q table
