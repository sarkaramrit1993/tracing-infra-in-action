#!/usr/bin/env bash
# Waits until everything send-traffic.sh sent has been counted everywhere the
# next steps read it: the Collector, the tail sampler, Prometheus and ClickHouse.
# Every wait is a check on the data itself, never a fixed sleep.
#
# Usage: ./scripts/wait-until-ready.sh
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

HOW="nothing to wait for yet: run ./scripts/send-traffic.sh first"
COLLECTOR_ID=$(state traffic COLLECTOR_ID "$HOW")
EMITTED0=$(state traffic EMITTED0 "$HOW")
EMITTED1=$(state traffic EMITTED1 "$HOW")
ACCEPTED0=$(state traffic ACCEPTED0 "$HOW")
TRACES0=$(state traffic TRACES0 "$HOW")
DECIDED0=$(state traffic DECIDED0 "$HOW")
KAFKA0=$(state traffic KAFKA0 "$HOW")
STORED0=$(state traffic STORED0 "$HOW")

await_services 180 otel-collector prometheus clickhouse checkout-service

[ "$(q instance "$COLLECTOR")" = "$COLLECTOR_ID" ] \
  || die "the Collector restarted after send-traffic.sh ran, so its counts start from zero. Run ./scripts/send-traffic.sh again"
ge "$(q exposition "$APP/metrics" checkout_spans_emitted_total)" "$EMITTED1" \
  || die "checkout-service restarted after send-traffic.sh ran. Run ./scripts/send-traffic.sh again"

SPANS=$(awk -v a="$EMITTED1" -v b="$EMITTED0" 'BEGIN { printf "%d", a - b }')

collector_has_every_span() {
  local now
  now=$(collector_metric otelcol_receiver_accepted_spans_total 2>/dev/null) || return 1
  ge "$(awk -v a="$now" -v b="$ACCEPTED0" 'BEGIN { printf "%d", a - b }')" "$SPANS"
}
poll "waiting for the Collector to receive all $SPANS spans the app sent" 120 collector_has_every_span

TRACES=$(awk -v a="$(collector_metric otelcol_processor_tail_sampling_new_trace_id_received_total)" \
             -v b="$TRACES0" 'BEGIN { printf "%d", a - b }')
sampler_decided_every_trace() {
  local now
  now=$(collector_metric otelcol_processor_tail_sampling_global_count_traces_sampled_total 2>/dev/null) || return 1
  ge "$(awk -v a="$now" -v b="$DECIDED0" 'BEGIN { printf "%d", a - b }')" "$TRACES"
}
poll "waiting for the tail sampler to decide all $TRACES traces" 120 sampler_decided_every_trace

await_span_metrics "waiting for span metrics to reach Prometheus"

KEPT=$(awk -v a="$(collector_metric otelcol_exporter_sent_spans_total 'exporter="kafka"')" \
           -v b="$KAFKA0" 'BEGIN { printf "%d", a - b }')
clickhouse_has_every_kept_span() {
  local now
  now=$(ch --query "SELECT count() FROM tracing.otel_traces" 2>/dev/null) || return 1
  ge "$(awk -v a="$now" -v b="$STORED0" 'BEGIN { printf "%d", a - b }')" "$KEPT"
}
poll "waiting for the $KEPT kept spans to reach ClickHouse" 120 clickhouse_has_every_kept_span

echo READY=1 >> "$STATE_DIR/traffic"
echo ready
