#!/usr/bin/env bash
# Runs the support agent once against the stand-in model and prints where to
# see its trace. Uses whatever capture setting capture-on.sh or capture-off.sh
# last chose, and capture-off.sh's setting if neither was run.
#
# Usage: ./scripts/run-agent.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

echo "Building the agent image if needed and running one agent run. This takes under a minute the first time."
build_agent
OUT=$(docker compose run --rm -T agent 2> "$ERR_LOG") || fail_with_log "The agent run failed. Its last output is above."
echo "$OUT"
TRACE_ID=$(echo "$OUT" | sed -n 's/.*trace_id=\([0-9a-f]*\).*/\1/p' | tail -n 1)
[ -n "$TRACE_ID" ] || die "The agent printed no trace ID."

echo "Waiting for the trace to reach Jaeger."
# checkout-service batches its spans for 5 seconds, so the count has to hold
# still for longer than that before the trace is complete.
POLL_SECONDS=${POLL_SECONDS:-2}
POLL_TRIES=${POLL_TRIES:-30}
STABLE_POLLS=${STABLE_POLLS:-5}
PREV=0
STABLE=0
for _ in $(seq 1 "$POLL_TRIES"); do
  BODY=$(curl -s "$JAEGER/api/v3/traces/$TRACE_ID" || true)
  COUNT=$(echo "$BODY" | { grep -o '"spanId"' || true; } | wc -l | tr -d ' ')
  if [ "$COUNT" -gt 0 ] && [ "$COUNT" = "$PREV" ]; then
    STABLE=$((STABLE + 1))
  else
    STABLE=0
  fi
  if [ "$STABLE" -ge "$((STABLE_POLLS - 1))" ]; then
    echo "Trace $TRACE_ID has $COUNT spans in Jaeger."
    echo "Open: $JAEGER/trace/$TRACE_ID"
    exit 0
  fi
  PREV=$COUNT
  sleep "$POLL_SECONDS"
done
die "The trace did not reach Jaeger in $((POLL_TRIES * POLL_SECONDS)) seconds. Is the stack up? Run: docker compose up -d"
