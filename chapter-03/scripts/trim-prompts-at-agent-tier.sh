#!/usr/bin/env bash
# Adds a transform processor to the agent tier that cuts string attributes on GenAI spans
# to 2048 bytes, then repeats the oversized AI agent run from send-oversized-agent-run.sh.
#
# Usage: ./scripts/trim-prompts-at-agent-tier.sh
# shellcheck source=scripts/agent-lib.sh
source "$(dirname "$0")/agent-lib.sh"

require_stack
use_configs agent-genai-trim-config.yaml gateway-default-limit-config.yaml
snapshot
echo "Trim: $(grep -o 'truncate_all(attributes, [0-9]*)' collector/agent-genai-trim-config.yaml), on spans that carry gen_ai.operation.name."
echo "Running the same AI agent run, 90 model calls, with the trim in place. This takes about 40 seconds."
run_ai_agent 32k span_only 90 1
wait_for_export
report
echo "Waiting for the trace to reach Jaeger."
SENT=$(($(agent_sent) - SENT0))
COUNT=0
for _ in $(seq 1 "${POLL_TRIES:-15}"); do
  COUNT=$(jaeger_spans "$TRACE_ID")
  [ "$COUNT" -gt 0 ] && [ "$COUNT" -ge "$SENT" ] && break
  sleep "${POLL_SECONDS:-2}"
done
echo "Spans of that trace in Jaeger: $COUNT"
[ "$COUNT" -gt 0 ] && [ "$COUNT" -ge "$SENT" ] \
  || die "Jaeger has $COUNT of the $SENT spans the agent tier sent. See: docker compose logs otel-gateway-1 jaeger"
echo "Open: $JAEGER/trace/$TRACE_ID"
echo "To go back to the shipped configs: docker compose up -d"
