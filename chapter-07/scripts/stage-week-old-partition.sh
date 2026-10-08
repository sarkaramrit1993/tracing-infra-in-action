#!/usr/bin/env bash
# Writes another 50,000 tiering-demo spans, dated a week back this time, past
# listing 7.2's two-day boundary, and shows where every partition sits.
#
# Usage: ./scripts/stage-week-old-partition.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
tiering_partition > /dev/null
stage_tiering_rows 7
show_parts
