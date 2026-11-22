#!/usr/bin/env bash
# Sends five AI agent runs through the Collector: four ordinary ones, where the
# refund fails once and the agent retries it, and one where the refund fails on
# every attempt. Notes what it sent so the next two scripts know what to wait for.
#
# Usage: ./scripts/send-agent-traffic.sh
# shellcheck source=scripts/agent-lib.sh
source "$(dirname "$0")/agent-lib.sh"
require checkout-service otel-collector prometheus clickhouse

ORDINARY=4
RUNS=$((ORDINARY + 1))
docker compose --profile agent build agent stand-in-model > /dev/null 2>&1 \
  || die "Could not build the agent image. Run: docker compose --profile agent build agent stand-in-model"

COLLECTOR_ID=$(q instance "$COLLECTOR")
BASE_ALL=$(edge_count "")
BASE_FAILED=$(edge_count ',failed="true"')

echo "sending $ORDINARY ordinary agent runs and 1 run whose refund fails every time..."
IDS=""
i=0
while [ "$i" -lt "$ORDINARY" ]; do
  run_agent ""
  OK_TRACE=$TRACE_ID
  IDS="$IDS'$TRACE_ID',"
  i=$((i + 1))
done
run_agent 1
FAIL_TRACE=$TRACE_ID
IDS="$IDS'$TRACE_ID'"

PREV=""
SAME=0
traces_settled() {
  local now
  [ "$(stored_traces "$IDS")" = "$RUNS" ] || return 1
  now=$(stored_spans "$IDS") || return 1
  if [ "$now" -gt 0 ] && [ "$now" = "$PREV" ]; then SAME=$((SAME + 1)); else SAME=0; fi
  PREV=$now
  [ "$SAME" -ge 3 ]
}
poll "waiting for the runs to reach ClickHouse" 120 traces_settled

CLIENT_SPANS="service_name = 'support-agent' AND NOT startsWith(span_name, 'invoke_agent') AND NOT startsWith(span_name, 'execute_tool') AND NOT startsWith(span_name, 'retrieval')"
WANT_ALL=$(stored_spans "$IDS" "$CLIENT_SPANS")
WANT_FAILED=$(stored_spans "$IDS" "$CLIENT_SPANS AND status_code = 'STATUS_CODE_ERROR'")

save_state agent <<STATE
COLLECTOR_ID=$COLLECTOR_ID
OK_TRACE=$OK_TRACE
FAIL_TRACE=$FAIL_TRACE
IDS=$IDS
TARGET_ALL=$((BASE_ALL + WANT_ALL))
TARGET_FAILED=$((BASE_FAILED + WANT_FAILED))
STATE

echo "sent $RUNS agent runs, $WANT_ALL calls from the agent, $WANT_FAILED of them failed"
echo "Next: ./scripts/show-agent-dependencies.sh"
