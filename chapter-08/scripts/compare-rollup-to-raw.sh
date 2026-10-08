#!/usr/bin/env bash
# Reads the request count from the rollup and from a raw scan of the span
# table over the same window, and prints the difference.
#
# Usage: ./scripts/compare-rollup-to-raw.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data
[ "$(ch --query "EXISTS TABLE tracing.red_by_service")" = 1 ] \
  || die "there is no rollup yet. Run ./scripts/build-rollup.sh"

ch_table "
SELECT
  (SELECT sum(requests) FROM tracing.red_by_service
    WHERE minute >= toStartOfMinute(now() - INTERVAL 1 HOUR)) AS from_rollup,
  (SELECT sum(adjusted_count) FROM tracing.otel_traces
    WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)
      AND parent_span_id = '') AS from_raw,
  from_rollup - from_raw AS delta"
