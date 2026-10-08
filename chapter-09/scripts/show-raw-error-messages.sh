#!/usr/bin/env bash
# Prints the raw exception message on the three most recent error spans.
#
# Usage: ./scripts/show-raw-error-messages.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require clickhouse

rows=$(ch --query "
SELECT attributes['exception.message'] AS raw
FROM tracing.otel_traces WHERE status_code = 'STATUS_CODE_ERROR'
ORDER BY timestamp DESC LIMIT 3")
[ -n "$rows" ] || die "no error spans in ClickHouse yet. Run ./scripts/send-traffic.sh, then ./scripts/wait-until-ready.sh"
printf '%s\n' "$rows"
