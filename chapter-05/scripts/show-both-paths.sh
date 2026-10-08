#!/usr/bin/env bash
# Takes the newest checkout send-traffic.sh sent and counts its spans in each
# place the two assembly paths deliver it.
#
# Usage: ./scripts/show-both-paths.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require clickhouse jaeger
require_ready
TRACE_ID=$(state traffic LAST_TRACE "$ARRIVING")

rows=$(ch --query "SELECT uniqExact(span_id) FROM tracing.otel_traces WHERE trace_id = '$TRACE_ID'")
sources=$(q jaeger-sources "$TRACE_ID")
query_time=$(printf '%s\n' "$sources" | sed -n 's/^query-time //p')
stream_time=$(printf '%s\n' "$sources" | sed -n 's/^stream-time //p')

echo "trace $TRACE_ID"
echo
printf '%-44s %s\n' "where" "spans"
printf '%-44s %s\n' "ClickHouse tracing.otel_traces" "$rows"
printf '%-44s %s\n' "Jaeger, assembly.source=query-time" "$query_time"
printf '%-44s %s\n' "Jaeger, assembly.source=stream-time" "$stream_time"
echo
echo "open http://localhost:16686/trace/$TRACE_ID to see it in Jaeger"
