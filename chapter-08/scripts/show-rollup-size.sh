#!/usr/bin/env bash
# Counts the rows in the rollup against the spans it summarizes.
#
# Usage: ./scripts/show-rollup-size.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data
[ "$(ch --query "EXISTS TABLE tracing.red_by_service")" = 1 ] \
  || die "there is no rollup yet. Run ./scripts/build-rollup.sh"

ch_table "
SELECT (SELECT count() FROM tracing.red_by_service)  AS rollup_rows,
       (SELECT count() FROM tracing.otel_traces)     AS spans,
       (SELECT countDistinct(minute) FROM tracing.red_by_service) AS minutes"
