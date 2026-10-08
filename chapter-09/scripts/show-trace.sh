#!/usr/bin/env bash
# Prints the spans of the checkout send-traced-checkout.sh sent.
#
# Usage: ./scripts/show-trace.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
TRACE_ID=$(state traced TRACE_ID "no traced checkout yet: run ./scripts/send-traced-checkout.sh first")
require clickhouse

rows=$(ch --query "
SELECT span_name, status_code, concat(toString(round(duration_ns / 1e6, 1)), 'ms') AS took
FROM tracing.otel_traces WHERE trace_id = '$TRACE_ID' ORDER BY timestamp")
[ -n "$rows" ] || die "trace $TRACE_ID is not in ClickHouse. Run ./scripts/send-traced-checkout.sh again"
printf '%s\n' "$rows" | q table
