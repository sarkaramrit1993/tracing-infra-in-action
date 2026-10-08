#!/usr/bin/env bash
# Reads the request count per service out of the view built without POPULATE
# and out of listing 8.3's view, which watch the same table.
#
# Usage: ./scripts/compare-the-two-views.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data
[ "$(ch --query "EXISTS TABLE tracing.red_no_populate")" = 1 ] \
  || die "there is no view without POPULATE yet. Run ./scripts/build-view-without-populate.sh"
[ "$(ch --query "EXISTS TABLE tracing.red_by_service")" = 1 ] \
  || die "there is no rollup yet. Run ./scripts/build-rollup.sh"

ch_table "
SELECT 'red_no_populate' AS view, service_name, sum(requests) AS requests
FROM tracing.red_no_populate GROUP BY service_name
UNION ALL
SELECT 'red_by_service' AS view, service_name, sum(requests) AS requests
FROM tracing.red_by_service GROUP BY service_name
ORDER BY view DESC, service_name"
