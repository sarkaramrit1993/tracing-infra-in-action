#!/usr/bin/env bash
# Stops one of the three Kafka brokers, sends checkouts while it is down, and
# checks that both paths still deliver every one of them whole. Starts the
# broker again on the way out, including when something fails.
#
# Usage: ./scripts/stop-a-broker.sh [checkouts, default 20]
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

COUNT=${1:-20}
case "$COUNT" in '' | *[!0-9]*) die "usage: ./scripts/stop-a-broker.sh [number of checkouts]" ;; esac
[ "$COUNT" -gt 0 ] || die "usage: ./scripts/stop-a-broker.sh [number of checkouts]"
require checkout-service clickhouse jaeger flink

restart_broker() {
  echo "starting kafka-2 again..."
  docker compose start kafka-2 > /dev/null 2>&1 || echo "could not start kafka-2: run docker compose start kafka-2" >&2
}
trap restart_broker EXIT

STARTED=$(ch --query "SELECT toUnixTimestamp64Nano(now64(9))")
echo "stopping kafka-2..."
docker compose stop kafka-2 > /dev/null 2>&1

echo "sending $COUNT checkouts with kafka-2 down..."
ok=0
while [ "$ok" -lt "$COUNT" ]; do
  curl -sf -m 30 -o /dev/null "$APP/checkout" \
    || die "checkout $((ok + 1)) failed with kafka-2 down"
  ok=$((ok + 1))
done
echo "all $ok checkouts answered"

await_both_paths "$STARTED" "$COUNT"

restart_broker
trap - EXIT
broker_healthy() {
  [ "$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q kafka-2)" 2>/dev/null)" = healthy ]
}
poll "waiting for kafka-2 to rejoin" 180 broker_healthy
echo "no checkout failed and no trace lost a span while a broker was down"
