#!/usr/bin/env bash
# Waits until every service answers, the Flink job is running, and the first span
# metrics have reached the Collectors. Every wait checks the thing itself.
#
# Usage: ./scripts/wait-until-ready.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

service_answers() { answers "$1" 2> /dev/null; }

flink_job_running() {
  curl -sf -m 5 http://localhost:18081/jobs/overview | grep -q '"state":"RUNNING"'
}
tables_exist() {
  [ "$(ch --query "SELECT count() FROM system.tables WHERE (database = 'tracing' AND name = 'otel_traces') OR (database = 'default' AND name IN ('red_by_service', 'red_by_service_mv'))" 2> /dev/null)" = "3" ]
}
edge_has_span_metrics() {
  [ "$(q exposition http://localhost:8889/metrics traces_span_metrics_calls_total 2> /dev/null)" != "0" ]
}
sampler_has_span_metrics() {
  [ "$(q exposition http://localhost:8890/metrics uniform_calls_total 2> /dev/null)" != "0" ]
}
flink_has_written_a_window() {
  [ -n "$(kafka_read red_metrics | head -c 1)" ]
}

[ -n "$(docker compose ps -q 2> /dev/null)" ] || die "the stack is not running. $HINT"

for s in checkout-service edge-collector sampler-collector prometheus clickhouse flink; do
  poll "waiting for $s" 180 service_answers "$s"
done
poll "waiting for the Flink job to run" 120 flink_job_running
poll "waiting for the ClickHouse tables" 60 tables_exist
poll "waiting for the edge Collector's first span metrics (it flushes every 60 s)" 240 edge_has_span_metrics
poll "waiting for the sampler tier's first span metrics" 240 sampler_has_span_metrics
poll "waiting for Flink to close its first window" 240 flink_has_written_a_window
echo ready
