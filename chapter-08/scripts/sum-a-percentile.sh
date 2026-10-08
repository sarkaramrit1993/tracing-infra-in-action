#!/usr/bin/env bash
# Puts a weighted p99 into a SummingMergeTree view and into an
# AggregatingMergeTree view, sends two batches into one minute, and reads both
# before and after a merge. Drops both views and deletes their rows at the end.
#
# Usage: ./scripts/sum-a-percentile.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data
[ "$(ch --query "EXISTS TABLE tracing.red_by_service")" = 0 ] \
  || die "listing 8.3's view is still built, and it would keep this demo's rows after the demo deletes them. Run ./scripts/clean-up-rollup.sh first"

cleanup() {
  ch --query "DROP VIEW IF EXISTS tracing.red_p99_summed" || true
  ch --query "DROP VIEW IF EXISTS tracing.red_p99_merged" || true
  ch --query "ALTER TABLE tracing.otel_traces DELETE WHERE service_name = 'rollup-demo-p99'
              SETTINGS mutations_sync = 2" || true
}
trap cleanup EXIT

ch --query "DROP VIEW IF EXISTS tracing.red_p99_summed"
ch --query "DROP VIEW IF EXISTS tracing.red_p99_merged"
ch --query "
CREATE MATERIALIZED VIEW tracing.red_p99_summed
ENGINE = SummingMergeTree
ORDER BY (minute, service_name)
AS SELECT service_name, toStartOfMinute(timestamp) AS minute,
          quantileExactWeighted(0.99)(duration_ns, toUInt64(round(adjusted_count))) / 1e6 AS p99_ms
FROM tracing.otel_traces WHERE parent_span_id = '' AND service_name = 'rollup-demo-p99'
GROUP BY service_name, minute"
ch --query "
CREATE MATERIALIZED VIEW tracing.red_p99_merged
ENGINE = AggregatingMergeTree
ORDER BY (minute, service_name)
AS SELECT service_name, toStartOfMinute(timestamp) AS minute,
          quantileExactWeightedState(0.99)(duration_ns, toUInt64(round(adjusted_count))) AS p99_state
FROM tracing.otel_traces WHERE parent_span_id = '' AND service_name = 'rollup-demo-p99'
GROUP BY service_name, minute"
ch --query "SYSTEM STOP MERGES tracing.red_p99_summed"
ch --query "SYSTEM STOP MERGES tracing.red_p99_merged"

minute=$(ch --query "SELECT toStartOfMinute(now())")
insert_batch rollup-demo-p99 "$minute"
insert_batch rollup-demo-p99 "$minute"
echo "sent two batches into the minute $minute, every span 180 ms"

read_both() {
  ch_table "
SELECT 'SummingMergeTree, p99_ms read raw' AS view, toString(p99_ms) AS p99_ms
FROM tracing.red_p99_summed
UNION ALL
SELECT 'AggregatingMergeTree, ...Merge' AS view,
       toString(quantileExactWeightedMerge(0.99)(p99_state) / 1e6) AS p99_ms
FROM tracing.red_p99_merged
ORDER BY view DESC"
}

echo
echo "before a merge:"
read_both
ch --query "SYSTEM START MERGES tracing.red_p99_summed"
ch --query "SYSTEM START MERGES tracing.red_p99_merged"
ch --query "OPTIMIZE TABLE tracing.red_p99_summed FINAL"
ch --query "OPTIMIZE TABLE tracing.red_p99_merged FINAL"
echo
echo "after OPTIMIZE TABLE ... FINAL:"
read_both
