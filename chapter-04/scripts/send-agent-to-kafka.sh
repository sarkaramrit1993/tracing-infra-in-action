#!/usr/bin/env bash
# Sends one long AI agent run, 24 model calls at a 32 KB prompt with capture on, through
# the gateway to Kafka. Shows the exporter dropping the oversized record while the
# gateway still accepts every span.
#
# Usage: ./scripts/send-agent-to-kafka.sh
# shellcheck source=scripts/agent-lib.sh
source "$(dirname "$0")/agent-lib.sh"

require_stack
use_gateway_config gateway-agent-config.yaml
snapshot
echo "Running one AI agent run with 24 model calls. This takes about 40 seconds."
run_ai_agent 32k span_only 24 1
wait_for_export
report
echo "Gateway log:"
{ docker compose logs --no-log-prefix otel-gateway 2>&1 || true; } \
  | { grep -o 'record size [0-9]* exceeds max [0-9]*: MESSAGE_TOO_LARGE' || true; } | tail -n 1
echo "Spans of that trace in Jaeger: $(jaeger_spans_settled "$TRACE_ID")"
echo "Next: ./scripts/raise-kafka-limits.sh"
