#!/usr/bin/env bash
# Moves the tiering exercise's partition back to local disk and times the same
# aggregate there.
#
# Usage: ./scripts/move-partition-to-hot.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
PART=$(tiering_partition)
if [ "$(partition_disk "$PART")" = default ]; then
  echo "partition $PART is already on default"
else
  echo "moving partition $PART"
  ch --query "ALTER TABLE tracing.otel_traces MOVE PARTITION '$PART' TO DISK 'default'"
fi
show_parts
echo
time_demo_query
