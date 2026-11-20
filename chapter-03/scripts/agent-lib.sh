# shellcheck shell=bash
# Sourced by the agent scripts. Not a reader step.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

JAEGER=http://localhost:16686

die() { echo "$*" >&2; exit 1; }

# Docker output stays off the screen unless a step fails, then its tail is shown.
ERR_LOG=$(mktemp)
trap 'rm -f "$ERR_LOG"' EXIT
fail_with_log() { tail -n 20 "$ERR_LOG" >&2; die "$1"; }

# Prints the sum of the metric lines matching a pattern on one Collector port, or 0.
metric() {
  { curl -s "http://localhost:$1/metrics" 2> /dev/null || true; } \
    | { grep -E "$2" || true; } \
    | awk '{ s += $NF } END { printf "%.0f\n", s }'
}

agent_in() { metric 8888 '^otelcol_processor_outgoing_items[{].*filter/drop_health'; }
agent_batched() { metric 8888 '^otelcol_processor_batch_batch_send_size_sum'; }
agent_batches() { metric 8888 '^otelcol_processor_batch_batch_send_size_bytes_count'; }
agent_bytes() { metric 8888 '^otelcol_processor_batch_batch_send_size_bytes_sum'; }
agent_sent() { metric 8888 '^otelcol_exporter_sent_spans[{]'; }
agent_failed() { metric 8888 '^otelcol_exporter_send_failed_spans[{]'; }
gateway_accepted() { metric 8889 '^otelcol_receiver_accepted_spans[{]'; }
gateway_refused() { metric 8889 '^otelcol_receiver_refused_spans[{]'; }

# Builds the agent image here, before use_configs restarts the agent tier, so a first
# build cannot push the run across a batch tick.
require_stack() {
  curl -sf http://localhost:8888/metrics > /dev/null 2>&1 \
    || die "The stack is not running. Run: docker compose up --build -d"
  docker compose --profile agent build agent > "$ERR_LOG" 2>&1 \
    || fail_with_log "Could not build the agent image. The last lines of the build are above."
}

# Points the agent tier and the gateways at the given config files, then restarts the
# agent tier. The batch timer ticks every 30 seconds from the moment the process starts,
# so a fresh start keeps a whole run inside one tick.
use_configs() {
  export AGENT_TIER_CONFIG=$1 GATEWAY_CONFIG=$2
  docker compose up -d --no-deps otel-agent otel-gateway-1 otel-gateway-2 > /dev/null 2>&1 \
    || die "Could not switch the Collector configs. Is the stack up? Run: docker compose up --build -d"
  docker compose restart otel-agent > /dev/null 2>&1 \
    || die "Could not restart the agent tier. See: docker compose logs otel-agent"
  for port in 8888 8889 8890; do
    for _ in $(seq 1 30); do
      curl -sf "http://localhost:$port/metrics" > /dev/null 2>&1 && break
      sleep 1
    done
    curl -sf "http://localhost:$port/metrics" > /dev/null 2>&1 \
      || die "The Collector on port $port did not come back. See: docker compose logs otel-agent"
  done
}

# Runs the AI agent with the given prompt size, capture setting, model calls and runs.
# Prints the run lines. Sets TRACE_ID to the last trace.
run_ai_agent() {
  export AGENT_PROMPT_SIZE=$1 OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=$2 AGENT_MODEL_CALLS=$3 AGENT_RUNS=$4
  RUN_OUT=$(docker compose run --rm -T agent 2> "$ERR_LOG") \
    || fail_with_log "The AI agent run failed. Its last output is above."
  echo "$RUN_OUT"
  TRACE_ID=$(echo "$RUN_OUT" | sed -n 's/.*trace_id=\([0-9a-f]*\).*/\1/p' | tail -n 1)
  [ -n "$TRACE_ID" ] || die "The AI agent printed no trace ID."
}

# Waits until every span that reached the batch processor has been sent or dropped.
# The demo batch timer is 30 seconds, so this takes up to about 40.
wait_for_export() {
  local tries=${EXPORT_TRIES:-90} spans
  for _ in $(seq 1 "$tries"); do
    spans=$(($(agent_in) - IN0))
    if [ "$spans" -gt 0 ] \
      && [ $(($(agent_batched) - BATCHED0)) -eq "$spans" ] \
      && [ $(($(agent_sent) + $(agent_failed) - EXPORTED0)) -eq "$spans" ]; then
      return 0
    fi
    sleep 1
  done
  die "The agent tier did not finish exporting in $tries seconds. Is the stack up? See: docker compose logs otel-agent"
}

snapshot() {
  IN0=$(agent_in)
  BATCHED0=$(agent_batched)
  BATCHES0=$(agent_batches)
  BYTES0=$(agent_bytes)
  SENT0=$(agent_sent)
  FAILED0=$(agent_failed)
  EXPORTED0=$((SENT0 + FAILED0))
  ACCEPTED0=$(gateway_accepted)
  REFUSED0=$(gateway_refused)
}

report() {
  local batches noun=requests
  batches=$(($(agent_batches) - BATCHES0))
  [ "$batches" -eq 1 ] && noun=request
  echo "Agent tier batch: $(($(agent_in) - IN0)) spans in $batches $noun, $(($(agent_bytes) - BYTES0)) bytes in total"
  echo "Agent tier exporter: sent $(($(agent_sent) - SENT0)) spans, failed $(($(agent_failed) - FAILED0)) spans"
  echo "Gateway 1 receiver: accepted $(($(gateway_accepted) - ACCEPTED0)) spans, refused $(($(gateway_refused) - REFUSED0)) spans"
}

jaeger_spans() {
  { curl -s "$JAEGER/api/v3/traces/$1" 2> /dev/null || true; } | { grep -o '"spanId"' || true; } | wc -l | tr -d ' '
}
