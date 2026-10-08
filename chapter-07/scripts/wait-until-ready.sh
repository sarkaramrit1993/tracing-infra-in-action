#!/usr/bin/env bash
# Waits until every span send-traffic.sh caused is in ClickHouse, and the last
# checkout trace is in Tempo. Checks the data itself, never sleeps a fixed time.
#
# Usage: ./scripts/wait-until-ready.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_traffic
STARTED=$(state traffic STARTED "$NO_TRAFFIC")
OK=$(state traffic OK "$NO_TRAFFIC")
SPANS=$((OK * 7))

clickhouse_has_every_span() {
  local got
  got=$(ch --query "
    SELECT count() FROM tracing.otel_traces
    WHERE timestamp >= toDateTime64('$STARTED', 3) AND span_name != 'GET /health'" 2> /dev/null) \
    || return 1
  [ "$got" -ge "$SPANS" ]
}
poll "waiting for all $SPANS spans to reach ClickHouse" 120 clickhouse_has_every_span

LAST=$(ch --query "
  SELECT trace_id FROM tracing.otel_traces
  WHERE timestamp >= toDateTime64('$STARTED', 3) AND span_name = 'GET /checkout'
  ORDER BY timestamp DESC LIMIT 1")

tempo_has_last_trace() {
  local spans
  spans=$(curl -sf -m 5 "$TEMPO/api/traces/$LAST" 2> /dev/null | grep -o '"spanId"' | wc -l) \
    || return 1
  [ "$spans" -ge 7 ]
}
poll "waiting for the last checkout trace to reach Tempo" 120 tempo_has_last_trace

save_state traffic <<STATE
STACK_ID=$(stack_id)
STARTED=$STARTED
OK=$OK
LAST_TRACE=$LAST
READY=1
STATE
echo "ready"
