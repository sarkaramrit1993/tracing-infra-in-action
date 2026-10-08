#!/usr/bin/env bash
# Sends ordinary checkouts, then forced failures, and notes what it sent so
# wait-until-ready.sh knows exactly how much data to wait for.
#
# Usage: ./scripts/send-traffic.sh [ordinary checkouts, default 300] [forced failures, default 6]
# shellcheck source=scripts/lib.sh
source "$(dirname "$0")/lib.sh"

NORMAL=${1:-300}
FORCED=${2:-6}
case "$NORMAL" in '' | *[!0-9]*) die "usage: ./scripts/send-traffic.sh [ordinary checkouts] [forced failures]" ;; esac
case "$FORCED" in '' | *[!0-9]*) die "usage: ./scripts/send-traffic.sh [ordinary checkouts] [forced failures]" ;; esac

await_services 180 checkout-service otel-collector clickhouse

COLLECTOR_ID=$(q instance "$COLLECTOR")
EMITTED0=$(q exposition "$APP/metrics" checkout_spans_emitted_total)
ACCEPTED0=$(collector_metric otelcol_receiver_accepted_spans_total)
TRACES0=$(collector_metric otelcol_processor_tail_sampling_new_trace_id_received_total)
DECIDED0=$(collector_metric otelcol_processor_tail_sampling_global_count_traces_sampled_total)
KAFKA0=$(collector_metric otelcol_exporter_sent_spans_total 'exporter="kafka"')
STORED0=$(ch --query "SELECT count() FROM tracing.otel_traces")
STARTED=$(date +%s)

bodies=$(mktemp)
trap 'rm -f "$bodies"' EXIT

send() {
  local url=$1 count=$2 i=0
  while [ "$i" -lt "$count" ]; do
    curl -sf -m 30 "$url" >> "$bodies" \
      || die "checkout-service stopped answering after $(( $(wc -l < "$bodies") )) requests. $HINT"
    echo >> "$bodies"
    i=$((i + 1))
  done
}

echo "sending $NORMAL ordinary checkouts and $FORCED forced failures..."
send "$APP/checkout" "$NORMAL"
send "$APP/checkout?fail=1" "$FORCED"
FAILED=$(grep -c '"fraud_failed":true' "$bodies" || true)

save_state traffic <<STATE
COLLECTOR_ID=$COLLECTOR_ID
EMITTED0=$EMITTED0
EMITTED1=$(q exposition "$APP/metrics" checkout_spans_emitted_total)
ACCEPTED0=$ACCEPTED0
TRACES0=$TRACES0
DECIDED0=$DECIDED0
KAFKA0=$KAFKA0
STORED0=$STORED0
STARTED=$STARTED
STATE

echo "sent $((NORMAL + FORCED)) checkouts, $FAILED of them failed"
