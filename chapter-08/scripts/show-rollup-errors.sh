#!/usr/bin/env bash
# Reads the error half of the dashboard out of the rollup.
#
# Usage: ./scripts/show-rollup-errors.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data
[ "$(ch --query "EXISTS TABLE tracing.red_by_service")" = 1 ] \
  || die "there is no rollup yet. Run ./scripts/build-rollup.sh"

ch_table "
SELECT status_code, sum(requests) AS requests
FROM tracing.red_by_service
WHERE minute >= toStartOfMinute(now() - INTERVAL 1 HOUR)
GROUP BY status_code ORDER BY requests DESC"
