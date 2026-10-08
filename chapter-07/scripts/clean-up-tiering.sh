#!/usr/bin/env bash
# Drops the demo table, and with it every part the exercise moved, then shows
# that otel_traces still carries listing 7.2's rule.
#
# Usage: ./scripts/clean-up-tiering.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
ch --query "DROP TABLE IF EXISTS tracing.tiering_demo"
ch --query "SHOW TABLES FROM tracing"
ch --format TSVRaw --query "
SELECT engine_full FROM system.tables
WHERE database = 'tracing' AND name = 'otel_traces'" | grep -o 'TTL .*' | sed 's/ SETTINGS.*//'
