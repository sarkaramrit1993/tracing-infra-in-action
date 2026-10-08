#!/usr/bin/env bash
# Runs one aggregate over the demo table and prints what it took.
#
# Usage: ./scripts/time-demo-query.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

PART=$(tiering_partition)
echo "partition $PART is on $(partition_disk "$PART")"
time_demo_query
