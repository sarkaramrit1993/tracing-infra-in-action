#!/usr/bin/env bash
# Waits for the Collector to log that Loki refused a batch of log records, and
# says so. Reads the Collector's log since send-traffic.sh last ran.
#
# Usage: ./scripts/show-loki-rejections.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
STARTED=$(state traffic STARTED "nothing sent yet: run ./scripts/send-traffic.sh first")
require otel-collector
SINCE=$(python3 -c 'import datetime, sys; print(datetime.datetime.fromtimestamp(int(sys.argv[1]), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))' "$STARTED")

rejections() { docker compose logs --since "$SINCE" otel-collector 2>/dev/null | grep -c 'not retryable error' || true; }
rejected() { [ "$(rejections)" -gt 0 ]; }
poll "waiting for the Collector to log what Loki did with the logs" 120 rejected
echo "not retryable error"
