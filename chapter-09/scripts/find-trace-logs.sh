#!/usr/bin/env bash
# Asks Loki for the log lines of the checkout send-traced-checkout.sh sent, by
# its trace id. With --label-selector it asks the way that looks obvious and
# does not work: trace_id as a stream label.
#
# Usage: ./scripts/find-trace-logs.sh [--label-selector]
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
TRACE_ID=$(state traced TRACE_ID "no traced checkout yet: run ./scripts/send-traced-checkout.sh first")
case "${1:-}" in
  "") LOGQL="{service_name=\"checkout-service\"} | trace_id=\"$TRACE_ID\"" ;;
  --label-selector) LOGQL="{trace_id=\"$TRACE_ID\"}" ;;
  *) die "usage: ./scripts/find-trace-logs.sh [--label-selector]" ;;
esac
require loki

echo "logql: $LOGQL"
q loki "$LOGQL" "$(( $(date +%s) - 900 ))"
