#!/usr/bin/env bash
# Sends one long AI agent run (about 90 seconds) through both assembly paths and shows
# when each of its spans reached Kafka, which wait Flink ran against, and where Flink put
# each span: the assembled trace or spans.late.
#
# Usage: ./scripts/send-long-agent-run.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require clickhouse flink
require_ready

WAIT_MS=$(docker compose exec -T flink-taskmanager printenv DECISION_WAIT_MS < /dev/null 2> /dev/null || true)
[ -n "$WAIT_MS" ] || die "could not read DECISION_WAIT_MS from the Flink task manager. $HINT"

docker compose --profile agent build agent > /dev/null 2>&1 \
  || die "could not build the agent image. Run: docker compose --profile agent build agent"

printf '%-34s %s\n' "Flink assembly window" "${WAIT_MS} ms (DECISION_WAIT_MS)"
echo "Running one AI agent run in long mode. This takes about 90 seconds."
RUN_OUT=$(docker compose run --rm -T agent 2> /dev/null) \
  || die "the AI agent run failed. See: docker compose --profile agent logs stand-in-model"
TRACE_ID=$(echo "$RUN_OUT" | sed -n 's/.*trace_id=\([0-9a-f]*\).*/\1/p' | tail -n 1)
[ -n "$TRACE_ID" ] || die "the AI agent printed no trace ID."
echo "$RUN_OUT" | sed -n '/^run /p'

timeline() {
  docker compose exec -T consumer-clickhouse python - "$TRACE_ID" < scripts/trace-timeline.py 2> /dev/null
}

# Every span is on Kafka, and Flink has put each one in the assembled trace or in spans.late.
accounted() {
  TIMELINE=$(timeline) || return 1
  [ -n "$TIMELINE" ] || return 1
  printf '%s' "$TIMELINE" | python3 -c '
import json, sys
d = json.load(sys.stdin)
n = lambda t: sum(len(r["spans"]) for r in d[t])
sys.exit(0 if n("otlp_spans") > 0 and n("otlp_spans") == n("traces.assembled") + n("spans.late") else 1)'
}
POLL_HINT="Flink has not finished with the trace. $HINT"
poll "waiting for Flink to assemble the trace" 180 accounted

# Both stores hold what Kafka held: every span in ClickHouse and in the query-time copy,
# and the assembled spans in the stream-time copy.
stored() {
  local total assembled
  total=$(printf '%s' "$TIMELINE" | q count-spans otlp_spans)
  assembled=$(printf '%s' "$TIMELINE" | q count-spans traces.assembled)
  SPANS=$(ch --query "SELECT count() FROM tracing.otel_traces WHERE trace_id = '$TRACE_ID'") || return 1
  SOURCES=$(q jaeger-sources "$TRACE_ID") || return 1
  QUERY_TIME=$(printf '%s\n' "$SOURCES" | sed -n 's/^query-time //p')
  STREAM_TIME=$(printf '%s\n' "$SOURCES" | sed -n 's/^stream-time //p')
  [ "$SPANS" = "$total" ] && [ "$QUERY_TIME" = "$total" ] && [ "$STREAM_TIME" = "$assembled" ]
}
POLL_HINT="the stores have not caught up. $HINT"
poll "waiting for ClickHouse and Jaeger to hold the trace" 120 stored

echo
printf '%s' "$TIMELINE" | q timeline
echo
printf '%-44s %s\n' "where" "spans"
printf '%-44s %s\n' "ClickHouse tracing.otel_traces" "$SPANS"
printf '%-44s %s\n' "Jaeger, assembly.source=query-time" "$QUERY_TIME"
printf '%-44s %s\n' "Jaeger, assembly.source=stream-time" "$STREAM_TIME"
