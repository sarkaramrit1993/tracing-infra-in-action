#!/usr/bin/env bash
# Reads the error half of the dashboard out of the view without the root-span
# filter.
#
# Usage: ./scripts/show-no-root-errors.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require_data
[ "$(ch --query "EXISTS TABLE tracing.red_no_root")" = 1 ] \
  || die "there is no view without the root filter yet. Run ./scripts/build-view-without-root-filter.sh"

ch_table "
SELECT status_code, sum(requests) AS requests
FROM tracing.red_no_root WHERE service_name = 'checkout-service'
GROUP BY status_code ORDER BY requests DESC"
