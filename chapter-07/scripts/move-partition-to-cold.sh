#!/usr/bin/env bash
# Moves the tiering exercise's partition to the S3-backed cold volume by hand.
#
# Usage: ./scripts/move-partition-to-cold.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
PART=$(tiering_partition)
if [ "$(partition_disk "$PART")" = s3_cold ]; then
  echo "partition $PART is already on s3_cold"
else
  echo "moving partition $PART"
  ch --query "ALTER TABLE tracing.otel_traces MOVE PARTITION '$PART' TO VOLUME 'cold'"
fi
show_parts
