#!/usr/bin/env bash
# Sends a fixed workload and reads what each placement counted for it: the
# Collector edge connector, the Flink window job, the ClickHouse view and the
# query-time query. Compare them with the load generator's own count.
#
# Usage: ./scripts/compare-placements.sh [requests, default 20000]
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

N=$(requests_arg "${1:-}")
measure "$N"
require flink

START=$(m START)

flink_counted_everything() {
  local totals
  totals=$(kafka_read red_metrics | q red-totals "GET /checkout" "$START")
  FLINK_CALLS=${totals%% *}
  [ "$FLINK_CALLS" = "$N" ]
}
poll "waiting for Flink to close the windows (a window closes 5 s after its minute ends)" 240 flink_counted_everything
FLINK_ERRORS=$(kafka_read red_metrics | q red-totals "GET /checkout" "$START" | cut -d' ' -f2)

SINCE="toStartOfMinute(toDateTime($START))"
VIEW=$(ch --query "SELECT sumMerge(weighted_calls), sumMergeIf(weighted_calls, status = 'STATUS_CODE_ERROR') FROM red_by_service WHERE service = 'checkout-service' AND operation = 'GET /checkout' AND minute >= $SINCE FORMAT TSV")
QUERY=$(ch --query "SELECT sum(weighted_calls), sum(weighted_errors) FROM ($(sed -e '/^--/d' -e 's/;$//' clickhouse/red_query.sql)) WHERE service = 'checkout-service' AND operation = 'GET /checkout' AND minute >= $SINCE FORMAT TSV")

TRUTH_E=$(m ERRORS)
row() { printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$(q percent "$3" "$2")" "$(q ratio "$2" "$N")x"; }
{
  printf 'placement\tcalls\terrors\terror rate\tcalls vs truth\n'
  row "ground truth (load generator)" "$N" "$TRUTH_E"
  row "1 Collector edge, before the sampler" "$(m edge_calls)" "$(m edge_errors)"
  row "2 Flink 60 s windows (Listing 10.2)" "$FLINK_CALLS" "$FLINK_ERRORS"
  row "3 ClickHouse view (Listing 10.3)" "$(echo "$VIEW" | cut -f1)" "$(echo "$VIEW" | cut -f2)"
  row "4 ClickHouse query (Listing 10.4)" "$(echo "$QUERY" | cut -f1)" "$(echo "$QUERY" | cut -f2)"
} | q table
