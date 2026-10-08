#!/usr/bin/env bash
# Counts the exemplars Prometheus stored on the post-sampler latency histogram
# since the Collector last started.
#
# Usage: ./scripts/count-exemplars.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require prometheus otel-collector

uptime=$(collector_metric otelcol_process_uptime_seconds_total)
since=$(awk -v now="$(date +%s)" -v up="$uptime" 'BEGIN { printf "%d", now - up }')
q exemplar-count post_duration_milliseconds_bucket "$since"
