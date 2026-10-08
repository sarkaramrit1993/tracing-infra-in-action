#!/usr/bin/env bash
# Builds listing 8.3's view with the root-span filter left out, and compares
# its request count with listing 8.3's.
#
# Usage: ./scripts/build-view-without-root-filter.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data
[ "$(ch --query "EXISTS TABLE tracing.red_by_service")" = 1 ] \
  || die "there is no rollup to compare with yet. Run ./scripts/build-rollup.sh"

ch --query "DROP VIEW IF EXISTS tracing.red_no_root"
ch --query "
CREATE MATERIALIZED VIEW tracing.red_no_root
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, service_name, status_code)
TTL minute + INTERVAL 90 DAY
POPULATE
AS SELECT service_name, status_code, toStartOfMinute(timestamp) AS minute,
          sum(adjusted_count) AS requests
FROM tracing.otel_traces
GROUP BY service_name, status_code, minute"
ch_table "
SELECT
  (SELECT sum(requests) FROM tracing.red_no_root   WHERE service_name = 'checkout-service') AS no_root_filter,
  (SELECT sum(requests) FROM tracing.red_by_service WHERE service_name = 'checkout-service') AS listing_8_3,
  round(no_root_filter / listing_8_3, 2) AS ratio"
