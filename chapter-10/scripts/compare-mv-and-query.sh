#!/usr/bin/env bash
# Sends a fixed workload, then reads the stored result twice: from the
# materialized view (Listing 10.3) and from the query that needs no view
# (Listing 10.4). Same answer, different cost. Then loads a few rows that carry a
# real weight (adjusted_count = 100) under their own service name, reads them
# both ways, and removes them again.
#
# Usage: ./scripts/compare-mv-and-query.sh [requests]
#   With no count it reads whatever workload the stack has already counted, at
#   any size and under any sampler setting, and sends 20,000 if there is none.
#   With a count it sends that workload.
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

if [ -n "${1:-}" ]; then
  N=$(requests_arg "$1")
  measure "$N"
else
  measure_settled
  N=$(m REQUESTS)
fi

SINCE="toStartOfMinute(toDateTime($(m START)))"
FILTER="service = 'checkout-service' AND operation = 'GET /checkout' AND minute >= $SINCE"

VIEW=$(ch --log_comment=ch10-view --query "SELECT sumMerge(weighted_calls), sumMergeIf(weighted_calls, status = 'STATUS_CODE_ERROR') FROM red_by_service WHERE $FILTER FORMAT TSV")
QUERY=$(ch --log_comment=ch10-query --query "SELECT sum(weighted_calls), sum(weighted_errors) FROM ($(sed -e '/^--/d' -e 's/;$//' clickhouse/red_query.sql)) WHERE $FILTER FORMAT TSV")
ch --query "SYSTEM FLUSH LOGS"

cost() {
  ch --query "SELECT read_rows, query_duration_ms FROM system.query_log WHERE type = 'QueryFinish' AND log_comment = '$1' ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT TSV"
}
VIEW_COST=$(cost ch10-view)
QUERY_COST=$(cost ch10-query)

VIEW_CALLS=$(echo "$VIEW" | cut -f1)
QUERY_CALLS=$(echo "$QUERY" | cut -f1)
STORED=$(m stored_rows)
{
  printf 'read from\tcalls\terrors\trows read\tserver time\n'
  printf 'view (Listing 10.3)\t%s\t%s\t%s\t%sms\n' "$VIEW_CALLS" "$(echo "$VIEW" | cut -f2)" "$(echo "$VIEW_COST" | cut -f1)" "$(echo "$VIEW_COST" | cut -f2)"
  printf 'query (Listing 10.4)\t%s\t%s\t%s\t%sms\n' "$QUERY_CALLS" "$(echo "$QUERY" | cut -f2)" "$(echo "$QUERY_COST" | cut -f1)" "$(echo "$QUERY_COST" | cut -f2)"
} | q table
if eq "$VIEW_CALLS" "$STORED" && eq "$QUERY_CALLS" "$STORED"; then
  echo "the load generator sent $N requests; both read the $STORED stored rows the sampler kept"
else
  echo "the load generator sent $N requests and the sampler kept $STORED rows; the view counts $VIEW_CALLS of them, the query counts $QUERY_CALLS"
  echo "the query only looks at the time window written in clickhouse/red_query.sql; stored rows older than that are not in its count"
fi
echo
echo "Listing 10.4 as printed gives a row per operation per minute; GET /checkout, newest minutes:"
NEWEST=$(ch --query "SELECT operation, minute, weighted_calls, weighted_errors, concat(toString(round(p99_ms, 1)), 'ms') AS p99 FROM ($(sed -e '/^--/d' -e 's/;$//' clickhouse/red_query.sql)) WHERE service = 'checkout-service' AND operation = 'GET /checkout' ORDER BY minute DESC LIMIT 3 FORMAT TSV")
if [ -n "$NEWEST" ]; then
  echo "$NEWEST" | q table
else
  echo "(none: every stored row is older than the query's time window)"
fi

# ---------------------------------------------------------------------------
# Weighted rows. Behind the sampler adjusted_count stays 1.0, so everything
# above counts survivors. Fifty rows that carry a weight of 100 show the
# population-correct read: two of them are errors, one is slow.
WC="service = 'weight-check'"
forget_weight_check() {
  ch --query "ALTER TABLE tracing.otel_traces DELETE WHERE service_name = 'weight-check' SETTINGS mutations_sync = 1" > /dev/null
  ch --query "ALTER TABLE red_by_service DELETE WHERE service = 'weight-check' SETTINGS mutations_sync = 1" > /dev/null
}
trap forget_weight_check EXIT
forget_weight_check
ch --query "INSERT INTO tracing.otel_traces (timestamp, trace_id, span_id, parent_span_id, service_name, span_name, status_code, duration_ns, adjusted_count)
  SELECT now64(9), lower(hex(number)), lower(hex(number)), '', 'weight-check', 'GET /checkout',
         if(number IN (3, 17), 'STATUS_CODE_ERROR', 'STATUS_CODE_UNSET'),
         if(number = 49, 500000000, 10000000), 100
  FROM numbers(50)"

RAW=$(ch --query "SELECT count(), countIf(status_code = 'STATUS_CODE_ERROR'), round(quantileTDigest(0.99)(duration_ns / 1e6), 1) FROM tracing.otel_traces WHERE service_name = 'weight-check' FORMAT TSV")
WVIEW=$(ch --query "SELECT countMerge(calls), sumMerge(weighted_calls), sumMergeIf(weighted_calls, status = 'STATUS_CODE_ERROR'), round(quantileTDigestWeightedMerge(0.99)(p99_state), 1) FROM red_by_service WHERE $WC FORMAT TSV")
WQUERY=$(ch --query "SELECT sum(weighted_calls), sum(weighted_errors), round(max(p99_ms), 1) FROM ($(sed -e '/^--/d' -e 's/;$//' clickhouse/red_query.sql)) WHERE $WC FORMAT TSV")

echo
echo "weight check: 50 stored rows, each standing for 100 requests (adjusted_count = 100)"
{
  printf 'read as\trows\tcalls\terrors\tp99\n'
  printf 'each row once\t%s\t%s\t%s\t%sms\n' "$(echo "$RAW" | cut -f1)" "$(echo "$RAW" | cut -f1)" "$(echo "$RAW" | cut -f2)" "$(echo "$RAW" | cut -f3)"
  printf 'view at its weight (Listing 10.3)\t%s\t%s\t%s\t%sms\n' "$(echo "$WVIEW" | cut -f1)" "$(echo "$WVIEW" | cut -f2)" "$(echo "$WVIEW" | cut -f3)" "$(echo "$WVIEW" | cut -f4)"
  printf 'query at its weight (Listing 10.4)\t%s\t%s\t%s\t%sms\n' "$(echo "$RAW" | cut -f1)" "$(echo "$WQUERY" | cut -f1)" "$(echo "$WQUERY" | cut -f2)" "$(echo "$WQUERY" | cut -f3)"
} | q table
echo "the column holds real weights, so the view and the query count the population: 50 rows are 5000 calls"
