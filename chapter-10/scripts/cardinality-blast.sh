#!/usr/bin/env bash
# Starts a second edge Collector that adds user.id as a span-metrics dimension,
# sends a workload with 6000 distinct user IDs through it, and shows where the
# series go: the connector's cardinality limit folds them into an overflow
# series, and Prometheus refuses the scrape at its sample_limit.
#
# Usage: ./scripts/cardinality-blast.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"
require checkout-service edge-collector prometheus

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.cardinality.yml"

group_is_stable() {
  $COMPOSE exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9093 \
    --describe --group edge-userid --state < /dev/null 2> /dev/null | grep -q Stable
}
# Counts calls, not series: a lower cardinality limit folds series into the
# overflow series but every call is still counted somewhere. Each request
# makes at least two server spans (checkout and payment).
USERID_REQUESTS=7000
userid_collector_counted_everything() {
  ge "$(q exposition http://localhost:8891/metrics traces_span_metrics_calls_total 2> /dev/null || echo 0)" "$(( 2 * USERID_REQUESTS ))"
}
# Prometheus has made up its mind about the scrape: refused it (over its
# sample_limit), or accepted it and holds the same calls the endpoint shows.
# Which of the two depends on the connector's limit, so the wait accepts both.
scrape_settled() {
  local line here held
  line=$(q targets | grep '^edge-userid-spanmetrics ') || return 1
  case "$line" in
    'edge-userid-spanmetrics down'*) return 0 ;;
    'edge-userid-spanmetrics up'*)
      here=$(q exposition http://localhost:8891/metrics traces_span_metrics_calls_total)
      held=$(q sum 'sum(traces_span_metrics_calls_total{job="edge-userid-spanmetrics"})')
      eq "$here" "$held" ;;
    *) return 1 ;;
  esac
}

CONFIG_FIX=./scripts/cardinality-down.sh config_is_running edge-collector-userid collector/edge-config.yaml collector/edge-base.yaml collector/edge-userid.yaml
$COMPOSE up -d edge-collector-userid 2> /dev/null
poll "waiting for the new Collector to join Kafka (it reads only spans sent after it starts)" 120 group_is_stable
echo "This takes about 3 minutes: it starts a Collector, sends a fresh $USERID_REQUESTS-request workload, and waits for the 60 s metric flush."
./scripts/run-workload.sh "$USERID_REQUESTS" "$RATE"
poll "waiting for the new Collector to flush its span metrics" 180 userid_collector_counted_everything
poll "waiting for Prometheus to scrape it (every 15 s)" 120 scrape_settled

echo
echo "series on the edge Collector without user.id:   $(q series-count http://localhost:8889/metrics)"
echo "series on the edge Collector with user.id:      $(q series-count http://localhost:8891/metrics)"
echo
echo "calls series per service on the user.id Collector, and the overflow series among them:"
q label-sets http://localhost:8891/metrics | q table
echo
DOWN=$(q targets | grep ' down' || true)
if [ -n "$DOWN" ]; then
  echo "the scrape job Prometheus refused, and why:"
  echo "$DOWN" | q table
  echo "the other $(q targets | grep -c ' up') scrape jobs are up"
else
  echo "Prometheus accepted every scrape job"
fi
