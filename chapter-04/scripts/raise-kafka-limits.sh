#!/usr/bin/env bash
# Raises the topic's max.message.bytes, then the exporter's max_message_bytes, and sends
# the same AI agent run again. The topic goes first, because a record the exporter can
# now emit has to be one the broker accepts.
#
# Usage: ./scripts/raise-kafka-limits.sh
# shellcheck source=scripts/agent-lib.sh
source "$(dirname "$0")/agent-lib.sh"

require_stack
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka-1:9093 \
  --alter --entity-type topics --entity-name otlp_spans --add-config max.message.bytes=2097152 \
  < /dev/null 2>&1 | tail -n 1 || die "Could not raise the topic limit. Is Kafka up? See: docker compose logs kafka-1"
use_gateway_config gateway-agent-raised-config.yaml
snapshot
echo "Running the same AI agent run, 24 model calls. This takes about 40 seconds."
run_ai_agent 32k span_only 24 1
wait_for_export
report
echo "MESSAGE_TOO_LARGE lines in the gateway log: $({ docker compose logs --no-log-prefix otel-gateway 2>&1 || true; } | { grep -c MESSAGE_TOO_LARGE || true; })"
echo "Spans of that trace in Jaeger: $(jaeger_spans_settled "$TRACE_ID")"
