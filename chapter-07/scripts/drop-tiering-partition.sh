#!/usr/bin/env bash
# Drops the tiering exercise's partition, times it, and counts what is left.
#
# Usage: ./scripts/drop-tiering-partition.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
PART=$(tiering_partition)
ROWS=$(ch --query "SELECT count() FROM tracing.otel_traces WHERE toYYYYMMDD(timestamp) = $PART")
START=$(python3 -c 'import time; print(time.time())')
ch --query "ALTER TABLE tracing.otel_traces DROP PARTITION '$PART'"
python3 -c 'import sys, time; print("dropped %s rows in %.3fs" % (sys.argv[2], time.time() - float(sys.argv[1])))' "$START" "$ROWS"
echo "tiering-demo rows left: $(ch --query "SELECT count() FROM tracing.otel_traces WHERE service_name = 'tiering-demo'")"
