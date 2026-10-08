#!/usr/bin/env bash
# Applies listing 7.2 from clickhouse/tiering.sql and prints the TTL rules the
# table now carries.
#
# Usage: ./scripts/apply-tiering-rule.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
ch_file clickhouse/tiering.sql
ch --format TSVRaw --query "
SELECT engine_full FROM system.tables
WHERE database = 'tracing' AND name = 'otel_traces'" | grep -o 'TTL .*' | sed 's/ SETTINGS.*//'
