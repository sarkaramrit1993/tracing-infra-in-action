#!/usr/bin/env bash
# Sends a fixed, countable workload through checkout -> payment and prints what
# it sent. Request i fails when i*29 % 1000 < 29, so exactly 2.9% of every
# thousand requests fail, and each request carries one of 6000 user IDs.
#
# Usage: ./scripts/run-workload.sh [requests, default 20000] [requests per second, default 200]
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

N=${1:-20000}
RATE=${2:-200}
case "$N" in '' | *[!0-9]* | 0) die "usage: ./scripts/run-workload.sh [requests] [requests per second]" ;; esac
case "$RATE" in '' | *[!0-9]* | 0) die "usage: ./scripts/run-workload.sh [requests] [requests per second]" ;; esac

require checkout-service edge-collector
START=$(date +%s)

echo "sending $N checkout requests at $RATE per second (about $(( N / RATE )) s)..."
RESULT=$(docker compose run --rm --no-deps -T loadgen python loadgen.py --count "$N" --rate "$RATE" < /dev/null 2> /dev/null) \
  || die "the load generator failed. Run: docker compose logs checkout-service payment-service"
echo "$RESULT"

ERRORS=$(echo "$RESULT" | sed -n 's/.*"errors": \([0-9]*\).*/\1/p')
save_state workload <<STATE
INSTANCE=$(q instance)
REQUESTS=$N
ERRORS=$ERRORS
START=$START
FINISHED=$(date +%s)
STATE

echo "sent $N requests, $ERRORS failed"
echo "span metrics flush every 60 s: wait about 70 s before reading them"
