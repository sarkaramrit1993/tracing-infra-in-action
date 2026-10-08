#!/usr/bin/env bash
# Builds listing 8.3's view without POPULATE on a table that already holds the
# population, then reads what the dashboard would show.
#
# Usage: ./scripts/build-view-without-populate.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data

ch --query "DROP VIEW IF EXISTS tracing.red_no_populate"
ch --query "
CREATE MATERIALIZED VIEW tracing.red_no_populate
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (minute, service_name, status_code)
TTL minute + INTERVAL 90 DAY
AS SELECT service_name, status_code, toStartOfMinute(timestamp) AS minute,
          sum(adjusted_count) AS requests
FROM tracing.otel_traces WHERE parent_span_id = ''
GROUP BY service_name, status_code, minute"
ch_table "
SELECT (SELECT count() FROM tracing.red_no_populate)      AS rollup_rows,
       (SELECT sum(requests) FROM tracing.red_no_populate) AS dashboard_requests,
       (SELECT count() FROM tracing.otel_traces)           AS spans_in_table"
