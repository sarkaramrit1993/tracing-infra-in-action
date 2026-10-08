#!/usr/bin/env bash
# Sends one failing checkout under a trace id this script picks itself, so no
# query below can find it by accident, then waits for its spans and log lines.
#
# Usage: ./scripts/send-traced-checkout.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
await_services 180 checkout-service otel-collector clickhouse loki

TRACE_ID=$(python3 -c 'import os; print(os.urandom(16).hex())')
SPAN_ID=$(python3 -c 'import os; print(os.urandom(8).hex())')
SENT=$(date +%s)
body=$(curl -sf -m 30 -H "traceparent: 00-$TRACE_ID-$SPAN_ID-01" "$APP/checkout?fail=1") \
  || die "checkout-service did not answer. $HINT"
CART=$(printf '%s' "$body" | python3 -c 'import json, sys; print(json.load(sys.stdin)["cart_id"])')

save_state traced <<STATE
TRACE_ID=$TRACE_ID
CART=$CART
SENT=$SENT
STATE
echo "trace id: $TRACE_ID"
echo "cart:     $CART"

trace_stored() {
  local n
  n=$(ch --query "SELECT count() FROM tracing.otel_traces WHERE trace_id = '$TRACE_ID'" 2>/dev/null) || return 1
  ge "$n" 7
}
logs_arrived() {
  local n
  n=$(q loki-count "{service_name=\"checkout-service\"} |= \"$CART\"" "$((SENT - 1))" 2>/dev/null) || return 1
  ge "$n" 2
}
poll "waiting for its 7 spans to reach ClickHouse" 180 trace_stored
poll "waiting for its 2 log lines to reach Loki" 120 logs_arrived
await_span_metrics "waiting for its spans to reach the span metrics in Prometheus"
