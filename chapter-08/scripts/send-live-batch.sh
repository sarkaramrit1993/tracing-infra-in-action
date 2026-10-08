#!/usr/bin/env bash
# Writes 500 root spans into the current minute under the service name given,
# each with a weight of 100 and a duration of 180 ms, so one batch stands for
# exactly 50,000 requests. Any view watching the table sees it as an insert.
#
# Usage: ./scripts/send-live-batch.sh <service name starting rollup-demo>
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

case "${1:-}" in
  rollup-demo*) ;;
  *) die "usage: ./scripts/send-live-batch.sh rollup-demo-a (the name must start with rollup-demo, so the clean-up can find the rows)" ;;
esac
require_data

insert_batch "$1" "$(ch --query "SELECT toStartOfMinute(now())")"
echo "sent 500 root spans as $1, worth 50000 requests"
