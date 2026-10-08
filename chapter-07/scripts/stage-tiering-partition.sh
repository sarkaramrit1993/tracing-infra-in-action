#!/usr/bin/env bash
# Makes tracing.tiering_demo, a fresh copy of otel_traces carrying listing 7.2's
# rule, writes 50,000 spans into it dated yesterday at midday, and shows where
# its partitions sit. Running it again starts the exercise over.
#
# Usage: ./scripts/stage-tiering-partition.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
ch --format TSVRaw --query "SELECT engine_full FROM system.tables
  WHERE database = 'tracing' AND name = 'otel_traces'" | grep -q "TO VOLUME 'cold'" \
  || die "listing 7.2 is not on otel_traces: run ./scripts/apply-tiering-rule.sh first"

ch --query "DROP TABLE IF EXISTS tracing.tiering_demo"
ch --query "CREATE TABLE tracing.tiering_demo AS tracing.otel_traces"
stage_tiering_rows 1
show_parts
