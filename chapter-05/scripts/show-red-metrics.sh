#!/usr/bin/env bash
# Reads RED metrics (rate, errors, duration) off the materialized view that
# rolls every span into a per-minute bucket as it is inserted. No trace is
# assembled to produce them.
#
# Usage: ./scripts/show-red-metrics.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require clickhouse
require_ready

ch --format TSVWithNames --query "
SELECT span_name,
       countMerge(span_count) AS spans,
       countIfMerge(error_count) AS errors,
       round(quantileTDigestMerge(0.99)(duration_p99) / 1e6, 1) AS p99_ms
FROM tracing.red_service_minute
WHERE service_name = 'checkout-service'
  AND ts_bucket_start >= now() - INTERVAL 1 HOUR
GROUP BY span_name
ORDER BY spans DESC, span_name" | q table
