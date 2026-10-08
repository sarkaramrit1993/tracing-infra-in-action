#!/usr/bin/env bash
# Writes another 50,000 spans into the demo table, dated a week back this time,
# past listing 7.2's two-day boundary, and shows where every partition sits.
#
# Usage: ./scripts/stage-week-old-partition.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

tiering_partition > /dev/null
OLD=$(ch --query "SELECT toYYYYMMDD(today() - 7)")
ch --query "ALTER TABLE tracing.tiering_demo DROP PARTITION '$OLD'"
stage_tiering_rows 7
show_parts
