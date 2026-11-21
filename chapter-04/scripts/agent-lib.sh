# shellcheck shell=bash
# Sourced by the agent scripts. Not a reader step.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

JAEGER=http://localhost:16686

die() { echo "$*" >&2; exit 1; }

# Prints the sum of the metric lines matching a pattern on one Collector port, or 0.
metric() {
  { curl -s "http://localhost:$1/metrics" 2> /dev/null || true; } \
    | { grep -E "$2" || true; } \
    | awk '{ s += $NF } END { printf "%.0f\n", s }'
}

gateway_accepted() { metric 8889 '^otelcol_processor_outgoing_items[{].*filter/drop_health'; }
gateway_refused() { metric 8889 '^otelcol_receiver_refused_spans[{]'; }
kafka_sent() { metric 8889 '^otelcol_exporter_sent_spans[{].*exporter="kafka"'; }
kafka_failed() { metric 8889 '^otelcol_exporter_send_failed_spans[{].*exporter="kafka"'; }

require_stack() {
  curl -sf http://localhost:8889/metrics > /dev/null 2>&1 \
    || die "The stack is not running. Run: docker compose up -d"
}

# Starts the gateway on the given config file and waits for its metrics port.
use_gateway_config() {
  export GATEWAY_CONFIG=$1
  docker compose up -d --no-deps --force-recreate otel-gateway > /dev/null 2>&1 \
    || die "Could not switch the gateway config. Is the stack up? Run: docker compose up -d"
  for _ in $(seq 1 30); do
    curl -sf http://localhost:8889/metrics > /dev/null 2>&1 && return 0
    sleep 1
  done
  die "The gateway did not come back. See: docker compose logs otel-gateway"
}

# Runs the AI agent with the given prompt size, capture setting, model calls and runs.
# Prints the run line. Sets TRACE_ID to the last trace.
run_ai_agent() {
  export AGENT_PROMPT_SIZE=$1 OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=$2 AGENT_MODEL_CALLS=$3 AGENT_RUNS=$4
  docker compose --profile agent build agent > /dev/null 2>&1 \
    || die "Could not build the agent image. Run: docker compose --profile agent build agent"
  RUN_OUT=$(docker compose run --rm -T agent 2> /dev/null) \
    || die "The AI agent run failed. See: docker compose --profile agent logs stand-in-model"
  echo "$RUN_OUT"
  TRACE_ID=$(echo "$RUN_OUT" | sed -n 's/.*trace_id=\([0-9a-f]*\).*/\1/p' | tail -n 1)
  [ -n "$TRACE_ID" ] || die "The AI agent printed no trace ID."
}

snapshot() {
  ACCEPTED0=$(gateway_accepted)
  REFUSED0=$(gateway_refused)
  SENT0=$(kafka_sent)
  FAILED0=$(kafka_failed)
}

# Waits until the Kafka exporter has sent or dropped every span the gateway accepted.
# The demo batch timer is 30 seconds, so this takes up to about 40.
wait_for_export() {
  local tries=${EXPORT_TRIES:-90} accepted exported
  for _ in $(seq 1 "$tries"); do
    accepted=$(($(gateway_accepted) - ACCEPTED0))
    exported=$(($(kafka_sent) + $(kafka_failed) - SENT0 - FAILED0))
    if [ "$accepted" -gt 0 ] && [ "$exported" -eq "$accepted" ]; then
      return 0
    fi
    sleep 1
  done
  die "The gateway did not finish exporting in $tries seconds. Is the stack up? See: docker compose logs otel-gateway"
}

report() {
  echo "Gateway: took in $(($(gateway_accepted) - ACCEPTED0)) spans, receiver refused $(($(gateway_refused) - REFUSED0)) spans"
  echo "Kafka exporter: sent $(($(kafka_sent) - SENT0)) spans, failed $(($(kafka_failed) - FAILED0)) spans"
}

jaeger_spans() {
  { curl -s "$JAEGER/api/v3/traces/$1" 2> /dev/null || true; } | { grep -o '"spanId"' || true; } | wc -l | tr -d ' '
}

# Prints the span count of a trace once it stops growing, or after about 30 seconds.
jaeger_spans_settled() {
  local prev=-1 cur
  for _ in $(seq 1 15); do
    cur=$(jaeger_spans "$1")
    if [ "$cur" -gt 0 ] && [ "$cur" = "$prev" ]; then
      break
    fi
    prev=$cur
    sleep 2
  done
  echo "$cur"
}
