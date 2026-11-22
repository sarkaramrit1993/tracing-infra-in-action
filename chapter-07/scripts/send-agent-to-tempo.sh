#!/usr/bin/env bash
# Sends one AI agent run with prompt capture on through the Collector to Tempo
# and ClickHouse. Each model call puts its whole prompt on its span.
#
# Usage: ./scripts/send-agent-to-tempo.sh
# shellcheck source=scripts/agent-lib.sh
source "$(dirname "$0")/agent-lib.sh"

require_stack
require_clickhouse
echo "Running one AI agent run with 4 model calls and prompt capture on."
run_ai_agent 8k span_only 4 1
save_agent_trace "$TRACE_ID"
echo "Waiting for the trace to settle in Tempo. This takes about 20 seconds."
SPANS=$(tempo_spans_settled "$TRACE_ID")
[ "$SPANS" -gt 0 ] || die "Tempo has none of the trace. $HINT"
echo "Spans of that trace in Tempo: $SPANS"
echo "Next: ./scripts/read-agent-prompt-back.sh"
