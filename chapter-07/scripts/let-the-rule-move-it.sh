#!/usr/bin/env bash
# Lowers the demo table's move boundary to one hour so the staged rows cross
# it, tells ClickHouse to re-evaluate its TTL, and watches the partition until
# the background mover takes it to the cold volume. Only the demo table changes.
#
# Usage: ./scripts/let-the-rule-move-it.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

PART=$(tiering_partition)
DISK=$(partition_disk "$PART")
[ "$DISK" = default ] \
  || die "partition $PART is on $DISK, not default: run ./scripts/move-partition-to-hot.sh first"

ch --query "
ALTER TABLE tracing.tiering_demo MODIFY TTL
  toDateTime(timestamp) + INTERVAL 1 HOUR TO VOLUME 'cold',
  toDateTime(timestamp) + INTERVAL 15 DAY DELETE"
ch --query "ALTER TABLE tracing.tiering_demo MATERIALIZE TTL"

START=$(date +%s)
moved_to_cold() {
  [ "$(partition_disk "$PART")" = s3_cold ]
}
poll "waiting for the background mover to take partition $PART to s3_cold" 180 moved_to_cold
echo "moved after $(( $(date +%s) - START ))s, with nobody asking"
show_parts
