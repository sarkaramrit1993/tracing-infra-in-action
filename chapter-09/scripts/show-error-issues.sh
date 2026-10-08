#!/usr/bin/env bash
# Reads the listing 9.2 index: one row per fingerprint, busiest first.
#
# Usage: ./scripts/show-error-issues.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require clickhouse

[ "$(ch --query "EXISTS TABLE tracing.exceptions")" = 1 ] \
  || die "the error index does not exist. Run ./scripts/arm-error-index.sh, then send traffic"
rows=$(ch --query "
SELECT any(error_type)      AS error_type,
       any(msg_template)    AS message,
       sum(error_count)     AS errors,
       any(sample_trace_id) AS trace
FROM tracing.exceptions GROUP BY fingerprint ORDER BY errors DESC")
[ -n "$rows" ] || die "the error index is empty. Run ./scripts/send-traffic.sh, then ./scripts/wait-until-ready.sh"
printf '%s\n' "$rows" | q table
