#!/usr/bin/env bash
# Builds listing 8.3's view without POPULATE, stops its merges, sends two
# batches into one minute, and reads the view raw and re-summed, before and
# after a merge. Drops the view and deletes its rows at the end.
#
# Usage: ./scripts/show-unmerged-batches.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data
[ "$(ch --query "EXISTS TABLE tracing.red_by_service")" = 0 ] \
  || die "listing 8.3's view is still built, and it would keep this demo's rows after the demo deletes them. Run ./scripts/clean-up-rollup.sh first"

cleanup() {
  ch --query "DROP VIEW IF EXISTS tracing.red_unmerged" || true
  ch --query "ALTER TABLE tracing.otel_traces DELETE WHERE service_name = 'rollup-demo-merge'
              SETTINGS mutations_sync = 2" || true
}
trap cleanup EXIT

ch --query "DROP VIEW IF EXISTS tracing.red_unmerged"
ch --query "
CREATE MATERIALIZED VIEW tracing.red_unmerged
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, service_name, status_code)
TTL minute + INTERVAL 90 DAY
AS SELECT service_name, status_code, toStartOfMinute(timestamp) AS minute,
          sum(adjusted_count) AS requests
FROM tracing.otel_traces WHERE parent_span_id = ''
GROUP BY service_name, status_code, minute"
ch --query "SYSTEM STOP MERGES tracing.red_unmerged"

minute=$(ch --query "SELECT toStartOfMinute(now())")
insert_batch rollup-demo-merge "$minute"
insert_batch rollup-demo-merge "$minute"
echo "sent two batches of 50000 requests into the minute $minute, merges stopped"

echo
echo "read raw, merges stopped:"
ch_table "SELECT minute, requests FROM tracing.red_unmerged ORDER BY requests"
echo
echo "read the way listing 8.3 does, GROUP BY and re-sum:"
ch_table "SELECT minute, sum(requests) AS requests FROM tracing.red_unmerged GROUP BY minute"

ch --query "SYSTEM START MERGES tracing.red_unmerged"
ch --query "OPTIMIZE TABLE tracing.red_unmerged FINAL"
echo
echo "read raw, after SYSTEM START MERGES and OPTIMIZE TABLE ... FINAL:"
ch_table "SELECT minute, requests FROM tracing.red_unmerged ORDER BY requests"
