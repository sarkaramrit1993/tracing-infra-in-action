#!/usr/bin/env bash
# Sends one long AI agent run, 90 model calls at a 32 KB prompt, so the agent tier's batch
# is larger than the gateway's 4 MiB receive limit. Shows where the refusal is visible.
#
# Usage: ./scripts/send-oversized-agent-run.sh
# shellcheck source=scripts/agent-lib.sh
source "$(dirname "$0")/agent-lib.sh"

require_stack
use_configs agent-genai-config.yaml gateway-default-limit-config.yaml
START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
snapshot
echo "Running one AI agent run with 90 model calls. This takes about 40 seconds."
run_ai_agent 32k span_only 90 1
wait_for_export
report
[ $(($(agent_failed) - FAILED0)) -gt 0 ] \
  || die "The run fit under the gateway's 4 MiB limit, so nothing was refused. Run it again."
echo "Agent tier log:"
{ docker logs otel-agent --since "$START" 2>&1 || true; } \
  | { grep -o '"error": "not retryable[^}]*' || true; } | tail -n 1
echo "Agent tier metric:"
{ curl -s http://localhost:8888/metrics || true; } | { grep '^otelcol_exporter_send_failed_spans' || true; }
echo "Gateway 1 metric:"
{ curl -s http://localhost:8889/metrics || true; } | { grep '^otelcol_receiver_refused_spans' || true; }
echo "Spans of that trace in Jaeger: $(jaeger_spans "$TRACE_ID")"
echo "Next: ./scripts/cap-batch-spans-at-agent-tier.sh"
