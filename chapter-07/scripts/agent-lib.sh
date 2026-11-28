# shellcheck shell=bash
# Sourced by the agent scripts. Not a reader step.
# shellcheck source=scripts/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

COLLECTOR_METRICS=http://localhost:8888/metrics
TEMPO_METRICS=$TEMPO/metrics

# Sums the metric lines matching a pattern at one metrics URL. A series that
# has never counted anything is not exposed at all, so no match is a 0.
metric() {
  { curl -s -m 5 "$1" 2> /dev/null || true; } \
    | { grep -E "$2" || true; } \
    | awk '{ s += $NF } END { printf "%.0f\n", s }'
}

collector_refused() { metric "$COLLECTOR_METRICS" '^otelcol_receiver_refused_spans[{]'; }
collector_failed_to_tempo() { metric "$COLLECTOR_METRICS" '^otelcol_exporter_send_failed_spans[{].*exporter="otlp_grpc/tempo"'; }
tempo_refused() { metric "$TEMPO_METRICS" '^tempo_receiver_refused_spans[{]'; }
tempo_discarded() { metric "$TEMPO_METRICS" '^tempo_discarded_spans_total[{]'; }
tempo_cut() { metric "$TEMPO_METRICS" '^tempo_distributor_attributes_truncated_total[{]'; }

require_stack() {
  { curl -sf -m 5 "$COLLECTOR_METRICS" > /dev/null 2>&1 && curl -sf -m 5 "$TEMPO/ready" > /dev/null 2>&1; } \
    || die "The stack is not running. From chapter-07/ run: docker compose up -d --build --wait"
}

# Runs the AI agent with the given prompt size, capture setting, model calls and runs.
# A fifth argument of "upload" turns on the upload hook, writing to /uploads in the container.
# Prints the run line. Sets TRACE_ID to the last trace.
run_ai_agent() {
  export AGENT_PROMPT_SIZE=$1 OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT=$2 AGENT_MODEL_CALLS=$3 AGENT_RUNS=$4
  if [ "${5:-}" = upload ]; then
    export OTEL_INSTRUMENTATION_GENAI_COMPLETION_HOOK=upload OTEL_INSTRUMENTATION_GENAI_UPLOAD_BASE_PATH=/uploads
  fi
  docker compose --profile agent build agent > /dev/null 2>&1 \
    || die "Could not build the agent image. Run: docker compose --profile agent build agent"
  RUN_OUT=$(docker compose run --rm -T agent 2> /dev/null) \
    || die "The AI agent run failed. See: docker compose --profile agent logs stand-in-model"
  echo "$RUN_OUT"
  TRACE_ID=$(echo "$RUN_OUT" | sed -n 's/.*trace_id=\([0-9a-f]*\).*/\1/p' | tail -n 1)
  [ -n "$TRACE_ID" ] || die "The AI agent printed no trace ID."
}

# Remembers the trace for the next script. The stack id ties it to these containers.
save_agent_trace() {
  printf 'STACK_ID=%s\nTRACE_ID=%s\n' "$(stack_id)" "$1" | save_state agent
}

NO_AGENT_RUN="no agent run yet: run ./scripts/send-agent-to-tempo.sh first"

agent_trace() {
  [ "$(state agent STACK_ID "$NO_AGENT_RUN")" = "$(stack_id)" ] || die "$NO_AGENT_RUN"
  state agent TRACE_ID "$NO_AGENT_RUN"
}

# tempo_trace ID: Tempo's JSON for the trace, or nothing.
tempo_trace() {
  { curl -s -m 10 "$TEMPO/api/traces/$1" 2> /dev/null || true; }
}

tempo_span_count() {
  tempo_trace "$1" | { grep -o '"spanId"' || true; } | wc -l | tr -d ' '
}

# tempo_string_attrs ID KEY: one line per span that carries KEY as a string:
# span id (hex), start time, the string's length in bytes and the string itself,
# tab separated, with any tab or line break in the string turned into a space.
tempo_string_attrs() {
  tempo_trace "$1" | python3 -c '
import base64, json, sys
try:
    batches = json.load(sys.stdin)["batches"]
except (ValueError, KeyError):
    sys.exit(0)
for batch in batches:
    for scope in batch["scopeSpans"]:
        for span in scope["spans"]:
            for attr in span["attributes"]:
                value = attr["value"].get("stringValue")
                if attr["key"] == sys.argv[1] and value is not None:
                    flat = value.replace("\t", " ").replace("\n", " ")
                    print(base64.b64decode(span["spanId"]).hex(), span["startTimeUnixNano"],
                          len(value.encode()), flat, sep="\t")
' "$2" | sort -t "$(printf '\t')" -k2n
}

clickhouse_span_count() {
  ch --query "SELECT count() FROM tracing.otel_traces WHERE trace_id = '$1'" 2> /dev/null || echo 0
}

# Prints the span count once it has held for three polls, or whatever it is
# after about a minute. The checkout service's spans leave on its own
# five-second timer, so a count that held is the only sign the trace is whole.
tempo_spans_settled() {
  local prev=-1 cur=0 same=0
  for _ in $(seq 1 "${POLL_TRIES:-30}"); do
    cur=$(tempo_span_count "$1")
    if [ "$cur" -gt 0 ] && [ "$cur" = "$prev" ]; then
      same=$((same + 1))
      [ "$same" -ge 3 ] && break
    else
      same=0
    fi
    prev=$cur
    sleep "${POLL_SECONDS:-2}"
  done
  echo "$cur"
}
