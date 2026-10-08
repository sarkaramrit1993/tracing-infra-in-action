#!/usr/bin/env bash
# Drops the demo partition, times it, and counts what is left.
#
# Usage: ./scripts/drop-tiering-partition.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

PART=$(tiering_partition)
ROWS=$(ch --query "SELECT count() FROM tracing.tiering_demo WHERE toYYYYMMDD(timestamp) = $PART")
START=$(python3 -c 'import time; print(time.time())')
ch --query "ALTER TABLE tracing.tiering_demo DROP PARTITION '$PART'"
python3 -c 'import sys, time; print("dropped %s rows in %.3fs" % (sys.argv[2], time.time() - float(sys.argv[1])))' "$START" "$ROWS"
echo "rows left in tiering_demo: $(ch --query "SELECT count() FROM tracing.tiering_demo")"
