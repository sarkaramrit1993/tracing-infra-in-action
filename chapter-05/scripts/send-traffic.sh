#!/usr/bin/env bash
# Sends checkouts and notes what it sent, so wait-until-ready.sh knows exactly
# how much data to wait for. Waits for the stack first, because the Flink job
# is the last thing to come up and takes a couple of minutes on a fresh start.
#
# Usage: ./scripts/send-traffic.sh [checkouts, default 120]
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

COUNT=${1:-120}
case "$COUNT" in '' | *[!0-9]*) die "usage: ./scripts/send-traffic.sh [number of checkouts]" ;; esac
[ "$COUNT" -gt 0 ] || die "usage: ./scripts/send-traffic.sh [number of checkouts]"

docker compose ps --status running --services 2>/dev/null | grep -qx clickhouse || die "the stack is not running. $HINT"
poll "waiting for checkout-service" 300 answers checkout-service
poll "waiting for ClickHouse" 300 answers clickhouse
poll "waiting for Jaeger" 300 answers jaeger
poll "waiting for the Flink assembly job to start" 300 answers flink

STARTED=$(ch --query "SELECT toUnixTimestamp64Nano(now64(9))")

echo "sending $COUNT checkouts..."
i=0
while [ "$i" -lt "$COUNT" ]; do
  curl -sf -m 30 -o /dev/null "$APP/checkout" \
    || die "checkout-service stopped answering after $i checkouts. $HINT"
  i=$((i + 1))
done

save_state traffic <<STATE
STACK_ID=$(stack_id)
COUNT=$COUNT
STARTED=$STARTED
STATE

echo "sent $COUNT checkouts"
