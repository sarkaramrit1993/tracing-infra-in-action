#!/usr/bin/env bash
# Sends checkouts to the app, one after another, and notes when it started so
# wait-until-ready.sh knows exactly what to wait for.
#
# Usage: ./scripts/send-traffic.sh [checkouts, default 300]
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

COUNT=${1:-300}
case "$COUNT" in
  '' | *[!0-9]* | 0) die "usage: ./scripts/send-traffic.sh [checkouts, default 300]" ;;
esac

require_clickhouse
curl -sf -m 5 -o /dev/null "$APP/health" || die "checkout-service is not answering. $HINT"

rm -f "$STATE_DIR/traffic"
STARTED=$(ch --query "SELECT toString(now64(3))")

echo "sending $COUNT checkouts, one at a time (about $((COUNT / 5 + 1)) seconds)..."
sent=0
failed=0
while [ "$sent" -lt "$COUNT" ]; do
  curl -sf -m 30 -o /dev/null "$APP/checkout" || failed=$((failed + 1))
  sent=$((sent + 1))
done
[ "$failed" -lt "$COUNT" ] || die "every checkout failed. $HINT"

save_state traffic <<STATE
STACK_ID=$(stack_id)
STARTED=$STARTED
OK=$((COUNT - failed))
STATE
echo "sent $COUNT checkouts, $failed did not answer"
