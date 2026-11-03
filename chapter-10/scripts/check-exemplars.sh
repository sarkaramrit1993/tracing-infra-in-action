#!/usr/bin/env bash
# Sends a fixed workload, then takes the trace IDs Prometheus holds as exemplars
# on the span-metrics histograms, before the sampler and after it, and looks each
# one up in ClickHouse.
#
# Usage: ./scripts/check-exemplars.sh [requests, default 20000]
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

N=$(requests_arg "${1:-}")
require sampler-collector
config_is_running sampler-collector collector/sampler-config.yaml
if [ "$(sampler_exemplars_enabled)" != "true" ]; then
  die "Exemplars are turned off on the sampler tier (spanmetrics/uniform in collector/sampler-config.yaml), so Prometheus will never hold any. Turn them back on, run docker compose restart sampler-collector, run again."
fi
measure "$N"

START=$(m START)
BEFORE_IDS=$(q exemplars 'traces_span_metrics_duration_milliseconds_bucket{span_name="GET /checkout"}' "$START")
AFTER_IDS=$(q exemplars 'uniform_duration_milliseconds_bucket{span_name="GET /checkout"}' "$START")
if [ -z "$BEFORE_IDS" ] || [ -z "$AFTER_IDS" ]; then
  if [ "$(sampler_exemplars_enabled)" != "true" ]; then
    die "Exemplars are turned off for the sampler tier (spanmetrics/uniform in collector/sampler-config.yaml), so Prometheus will never hold any for it. Turn them back on, restart sampler-collector, and run this again."
  fi
  die "Prometheus holds no exemplars for this workload yet. They arrive with the next flush: run this script again in a minute."
fi

in_list() { echo "$1" | sed "s/.*/'&'/" | paste -sd, -; }
resolved() {
  ch --query "SELECT uniqExact(trace_id) FROM tracing.otel_traces WHERE trace_id IN ($(in_list "$1"))"
}
every_after_id_resolves() {
  [ "$(resolved "$AFTER_IDS")" = "$(echo "$AFTER_IDS" | wc -l | tr -d ' ')" ]
}
poll "waiting for the kept traces to reach ClickHouse" 90 every_after_id_resolves

report() {
  local total found
  total=$(echo "$2" | wc -l | tr -d ' ')
  found=$(resolved "$2")
  printf '%s\t%s\t%s\t%s\n' "$1" "$total" "$found" "$(q percent "$found" "$total")"
}
echo
{
  printf 'exemplar taken\ttrace IDs\tfound in ClickHouse\tshare\n'
  report "before the sampler (edge, :8889)" "$BEFORE_IDS"
  report "after the sampler (:8890)" "$AFTER_IDS"
} | q table
