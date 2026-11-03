#!/usr/bin/env bash
# Counts the Prometheus series one label set costs, and what the whole edge
# scrape holds. Reads the checkout-service GET /cart label set, which the
# background traffic keeps alive.
#
# Usage: ./scripts/count-series.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require prometheus edge-collector
config_is_running edge-collector collector/edge-config.yaml collector/edge-base.yaml

LABELS='service_name="checkout-service",span_name="GET /cart",span_kind="SPAN_KIND_SERVER",status_code="STATUS_CODE_UNSET"'
ONE="count({__name__=~\"traces_span_metrics_.*\",$LABELS})"
BUCKETS="count(traces_span_metrics_duration_milliseconds_bucket{$LABELS})"
SETS='count(traces_span_metrics_calls_total)'
ALL='count({__name__=~"traces_span_metrics_.*"})'

label_set_is_scraped() {
  local here
  here=$(q endpoint-count http://localhost:8889/metrics traces_span_metrics_ 'service_name="checkout-service"' 'span_name="GET /cart"' 'span_kind="SPAN_KIND_SERVER"' 'status_code="STATUS_CODE_UNSET"')
  [ "$here" != "0" ] && [ "$(q sum "$ONE")" = "$here" ]
}
poll "waiting for Prometheus to scrape the edge Collector" 300 label_set_is_scraped

NAMES=$(q series-names "{__name__=~\"traces_span_metrics_.*\",$LABELS}")

{
  printf 'what\tseries\n'
  printf 'one label set (checkout-service, GET /cart, SERVER, UNSET)\t%s\n' "$(q sum "$ONE")"
  printf '  of which histogram buckets\t%s\n' "$(q sum "$BUCKETS")"
  printf 'label sets the edge Collector holds\t%s\n' "$(q sum "$SETS")"
  printf 'span-metrics series the edge Collector holds\t%s\n' "$(q sum "$ALL")"
} | q table
echo
echo "the $(echo "$NAMES" | wc -l | tr -d ' ') series of that one label set:"
echo "$NAMES"
