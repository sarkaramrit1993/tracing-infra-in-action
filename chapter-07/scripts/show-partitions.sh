#!/usr/bin/env bash
# Prints where the table's bytes are: one row per daily partition.
#
# Usage: ./scripts/show-partitions.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_ready
ch_table "
SELECT partition, formatReadableSize(sum(bytes_on_disk)) AS on_disk,
       sum(rows) AS rows, count() AS parts, disk_name AS disk
FROM system.parts
WHERE database = 'tracing' AND table = 'otel_traces' AND active
GROUP BY partition, disk_name ORDER BY partition, disk_name"
