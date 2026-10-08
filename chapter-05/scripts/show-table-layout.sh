#!/usr/bin/env bash
# Shows how listing 5.1's table lays spans out on disk: hourly partitions,
# immutable parts, and the newest checkout's seven spans side by side inside
# each part that holds them, because the table is sorted by trace ID.
#
# Usage: ./scripts/show-table-layout.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require clickhouse
require_ready
TRACE_ID=$(state traffic LAST_TRACE "$ARRIVING")

echo "parts (every insert writes one; background merges combine them)"
ch --format TSVWithNames --query "
SELECT partition, part_type, count() AS parts, sum(rows) AS rows,
       formatReadableSize(sum(bytes_on_disk)) AS on_disk
FROM system.parts
WHERE database = 'tracing' AND table = 'otel_traces' AND active
GROUP BY partition, part_type ORDER BY partition, part_type" | q table
echo
echo "where trace $TRACE_ID sits"
ch --format TSVWithNames --query "
SELECT _part AS part, count() AS spans,
       min(_part_offset) AS first_row, max(_part_offset) AS last_row
FROM tracing.otel_traces
WHERE trace_id = '$TRACE_ID'
GROUP BY _part ORDER BY _part" | q table
