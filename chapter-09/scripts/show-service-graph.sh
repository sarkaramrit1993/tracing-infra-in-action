#!/usr/bin/env bash
# Prints every edge of the service graph and how many calls crossed it.
#
# The service graph connector writes its counts out once a minute, its default
# metrics_flush_interval, so this waits until the graph has counted every
# checkout the span metrics saw. When that can never happen (a checkout that
# arrived with a parent span from outside has no edge from "user", and a graph
# that drops spans never catches up), it waits instead for one full flush and
# scrape after the Collector's last new edge.
#
# Usage: ./scripts/show-service-graph.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require prometheus otel-collector

EDGE='traces_service_graph_request_total'
EXPORTER=http://localhost:8889/metrics
GRAPH_FLUSH=60

graph_counted_every_checkout() {
  local graph checkouts
  graph=$(q sum "sum($EDGE{client=\"user\",server=\"checkout-service\"})") || return 1
  checkouts=$(q sum 'sum(pre_calls_total{service_name="checkout-service",span_kind="SPAN_KIND_SERVER"})') || return 1
  [ "$graph" != none ] && [ "$checkouts" != none ] && ge "$graph" "$checkouts"
}

EDGES_SEEN=""
QUIET_SINCE=0
graph_flushed_after_last_edge() {
  local edges scraped exposed now
  edges=$(collector_metric otelcol_connector_servicegraph_total_edges_total) || return 1
  if [ "$edges" != "$EDGES_SEEN" ]; then
    EDGES_SEEN=$edges
    QUIET_SINCE=$(q time) || return 1
  fi
  now=$(q scrape-time otel-spanmetrics) || return 1
  ge "$now" "$(awk -v t="$QUIET_SINCE" -v f="$GRAPH_FLUSH" 'BEGIN { printf "%.3f", t + f + 2 }')" || return 1
  scraped=$(q sum "sum($EDGE)") || return 1
  exposed=$(q exposition "$EXPORTER" "$EDGE") || return 1
  [ "$scraped" != none ] && [ "$scraped" = "$exposed" ]
}

graph_ready() {
  graph_counted_every_checkout 2>/dev/null || graph_flushed_after_last_edge 2>/dev/null
}

poll "waiting for the service graph to reach Prometheus" 240 graph_ready
q edges
DROPPED=$(collector_metric otelcol_connector_servicegraph_dropped_spans_total)
[ "$DROPPED" = 0 ] || echo "spans the service graph dropped: $DROPPED"
