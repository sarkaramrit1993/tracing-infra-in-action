#!/usr/bin/env bash
# Waits until every checkout send-traffic.sh sent has reached both paths: all
# spans of each in ClickHouse, and the stream-time copy of the newest one
# in Jaeger. Every wait is a check on the data itself, never a fixed sleep.
#
# Usage: ./scripts/wait-until-ready.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require clickhouse
require_traffic
COUNT=$(state traffic COUNT "$SEND")
STARTED=$(state traffic STARTED "$SEND")
require jaeger flink

await_both_paths "$STARTED" "$COUNT"

kept=$(grep -v -e '^LAST_TRACE=' -e '^READY=' "$STATE_DIR/traffic")
save_state traffic <<STATE
$kept
LAST_TRACE=$LAST
READY=1
STATE
echo ready
