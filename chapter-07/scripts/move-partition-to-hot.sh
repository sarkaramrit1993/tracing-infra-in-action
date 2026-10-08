#!/usr/bin/env bash
# Moves the demo partition back to local disk and times the same aggregate there.
#
# Usage: ./scripts/move-partition-to-hot.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

PART=$(tiering_partition)
! one_hour_rule_on \
  || die "the one-hour boundary from let-the-rule-move-it.sh is still on, so the mover would take it straight back: run ./scripts/stage-tiering-partition.sh to start again"
if [ "$(partition_disk "$PART")" = default ]; then
  echo "partition $PART is already on default"
else
  echo "moving partition $PART"
  ch --query "ALTER TABLE tracing.tiering_demo MOVE PARTITION '$PART' TO DISK 'default'"
fi
show_parts
echo
time_demo_query
