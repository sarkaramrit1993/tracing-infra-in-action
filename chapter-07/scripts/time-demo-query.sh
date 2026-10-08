#!/usr/bin/env bash
# Runs one aggregate over the tiering exercise's rows and prints what it took.
#
# Usage: ./scripts/time-demo-query.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

require_clickhouse
PART=$(tiering_partition)
echo "partition $PART is on $(partition_disk "$PART")"
time_demo_query
