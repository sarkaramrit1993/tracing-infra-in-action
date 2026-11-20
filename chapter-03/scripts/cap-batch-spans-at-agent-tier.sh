#!/usr/bin/env bash
# Caps the agent tier's batch at 100 spans with send_batch_max_size, then repeats the oversized
# AI agent run from send-oversized-agent-run.sh.
#
# Usage: ./scripts/cap-batch-spans-at-agent-tier.sh
# shellcheck source=scripts/agent-lib.sh
source "$(dirname "$0")/agent-lib.sh"

require_stack
use_configs agent-genai-batchcap-config.yaml gateway-default-limit-config.yaml
snapshot
echo "Cap: $(grep -o 'send_batch_max_size: [0-9]*' collector/agent-genai-batchcap-config.yaml) spans per request, prompts left whole."
echo "Running the same AI agent run, 90 model calls, with the cap in place. This takes about 40 seconds."
run_ai_agent 32k span_only 90 1
wait_for_export
report
echo "Waiting for the trace to reach Jaeger."
COUNT=0
for _ in $(seq 1 "${POLL_TRIES:-15}"); do
  COUNT=$(jaeger_spans "$TRACE_ID")
  [ "$COUNT" -ge $(($(agent_in) - IN0)) ] && break
  sleep "${POLL_SECONDS:-2}"
done
echo "Spans of that trace in Jaeger: $COUNT"
echo "Open: $JAEGER/trace/$TRACE_ID"
echo "Next: ./scripts/trim-prompts-at-agent-tier.sh"
