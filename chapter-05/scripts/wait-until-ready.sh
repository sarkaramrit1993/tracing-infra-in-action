#!/usr/bin/env bash
# Waits until every checkout send-traffic.sh sent has reached both paths: all
# spans of each in ClickHouse, and the stream-time copy of the newest one
# in Jaeger. Every wait is a check on the data itself, never a fixed sleep.
#
# Usage: ./scripts/wait-until-ready.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

docker compose ps --status running --services 2>/dev/null | grep -qx clickhouse || die "the stack is not running. $HINT"
require_traffic
COUNT=$(state traffic COUNT "$SEND")
STARTED=$(state traffic STARTED "$SEND")
poll "waiting for ClickHouse" 300 answers clickhouse
poll "waiting for Jaeger" 300 answers jaeger
poll "waiting for the Flink assembly job to start" 300 answers flink

# Spans an app still held in its export buffer when the stack stopped never
# reach Kafka, so after a restart some checkouts can be missing for good.
MISSING_HINT="The rest will not come: send fresh ones with ./scripts/send-traffic.sh, then run this again."
await_both_paths "$STARTED" "$COUNT"

kept=$(grep -v -e '^LAST_TRACE=' -e '^READY=' "$STATE_DIR/traffic")
save_state traffic <<STATE
$kept
LAST_TRACE=$LAST
READY=1
STATE
echo ready
