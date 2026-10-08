#!/usr/bin/env bash
# Lowers the move boundary to one hour so the staged rows cross it, tells
# ClickHouse to re-evaluate its TTL, and watches the partition until the
# background mover takes it to the cold volume. Leaves the one-hour boundary
# in place: ./scripts/clean-up-tiering.sh puts listing 7.2's back.
#
# Usage: ./scripts/let-the-rule-move-it.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
PART=$(tiering_partition)
DISK=$(partition_disk "$PART")
[ "$DISK" = default ] \
  || die "partition $PART is already on $DISK: run ./scripts/move-partition-to-hot.sh first"

ch --query "
ALTER TABLE tracing.otel_traces MODIFY TTL
  toDateTime(timestamp) + INTERVAL 1 HOUR TO VOLUME 'cold',
  toDateTime(timestamp) + INTERVAL 15 DAY DELETE"
ch --query "ALTER TABLE tracing.otel_traces MATERIALIZE TTL"

START=$(date +%s)
moved_to_cold() {
  [ "$(ch --query "
    SELECT countIf(disk_name = 'default') = 0 FROM system.parts
    WHERE database = 'tracing' AND table = 'otel_traces' AND active AND partition = '$PART'")" = 1 ]
}
poll "waiting for the background mover to take partition $PART to s3_cold" 180 moved_to_cold
echo "moved after $(( $(date +%s) - START ))s, with nobody asking"
show_parts
