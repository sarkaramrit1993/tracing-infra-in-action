#!/usr/bin/env bash
# Chapter 3 stack test. Asserts three things against the LIVE stack:
#   1. the agent and both gateways are running and scraped,
#   2. spans accepted by the agent are exported onward,
#   3. trace-aware routing spreads spans across BOTH gateways, the
#      three-tier claim this chapter makes.
#
# Assertion 1 scopes the Prometheus up-check to the three jobs this chapter
# is actually about (otel-agent, otel-gateway-1, otel-gateway-2) and requires
# exactly those three, not merely "at least 3". This file's prometheus.yml
# scrapes eight jobs total (it also carries agent variants for the
# routing/backpressure demos elsewhere in the chapter), so an unscoped
# "count(up == 1) >= 3" could be satisfied by unrelated targets while one of
# the three that matter here is down.
#
# Assertion 2 reads otelcol_receiver_accepted_spans and
# otelcol_exporter_sent_spans, both monotonic counters since collector
# start. It takes a before/after reading around the 50 checkouts below and
# asserts the delta grew, rather than asserting the raw totals are non-zero
# (which leftover traffic from any earlier run would satisfy forever).
#
# Assertion 3 is directional, not a fixed split: it asserts each gateway
# received a non-zero share, never an exact ratio. The precise distribution
# is a property of the trace-id mix, not a constant. The full skew
# measurement (Gini coefficient, skew ratio) lives in benchmarks/bench_routing.py.
#
# Prereq: `docker compose up -d` has settled and Prometheus has scraped at
# least twice (~30s).
#
# Usage: bash tests/test_stack.sh
set -euo pipefail
cd "$(dirname "$0")/.."

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

# Runs a PromQL query and prints the summed scalar result, or 0 if empty.
promql() {
    curl -s --data-urlencode "query=$1" http://localhost:9090/api/v1/query \
        | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(sum(float(x['value'][1]) for x in r) if r else 0)"
}

echo "== 1. agent and both gateways running and scraped =="
RUNNING=$(docker compose ps --status running --services)
for svc in otel-agent otel-gateway-1 otel-gateway-2; do
    echo "$RUNNING" | grep -qx "$svc" || fail "service '$svc' not running"
done
UP=$(promql 'count(up{job=~"otel-agent|otel-gateway-1|otel-gateway-2"} == 1)')
[ "$(printf '%.0f' "$UP")" -eq 3 ] || fail "expected exactly 3 of otel-agent/otel-gateway-1/otel-gateway-2 up in Prometheus (got $UP)"
pass "agent and both gateways are running and scraped"

echo "== 2. spans accepted by the agent are exported onward =="
ACCEPTED_BEFORE=$(printf '%.0f' "$(promql 'sum(otelcol_receiver_accepted_spans)')")
SENT_BEFORE=$(printf '%.0f' "$(promql 'sum(otelcol_exporter_sent_spans)')")
for _ in $(seq 1 50); do curl -s -o /dev/null http://localhost:8080/checkout; done
echo "waiting for batch export and two Prometheus scrapes (40s)..."
sleep 40

ACCEPTED_AFTER=$(printf '%.0f' "$(promql 'sum(otelcol_receiver_accepted_spans)')")
SENT_AFTER=$(printf '%.0f' "$(promql 'sum(otelcol_exporter_sent_spans)')")
ACCEPTED_DELTA=$((ACCEPTED_AFTER - ACCEPTED_BEFORE))
SENT_DELTA=$((SENT_AFTER - SENT_BEFORE))
[ "$ACCEPTED_DELTA" -gt 0 ] || fail "no new spans accepted by any receiver during this run (before=$ACCEPTED_BEFORE, after=$ACCEPTED_AFTER)"
[ "$SENT_DELTA" -gt 0 ] || fail "no new spans exported by any exporter during this run (before=$SENT_BEFORE, after=$SENT_AFTER)"
pass "spans accepted (+$ACCEPTED_DELTA) and exported (+$SENT_DELTA) during this run"

echo "== 3. both gateways received spans (trace-aware routing spreads load) =="
G1=$(promql 'sum(otelcol_receiver_accepted_spans{job="otel-gateway-1"})')
G2=$(promql 'sum(otelcol_receiver_accepted_spans{job="otel-gateway-2"})')
G1I=$(printf '%.0f' "$G1")
G2I=$(printf '%.0f' "$G2")
[ "$G1I" -gt 0 ] || fail "gateway-1 accepted no spans"
[ "$G2I" -gt 0 ] || fail "gateway-2 accepted no spans"
pass "both gateways received spans (gateway-1=$G1I, gateway-2=$G2I)"

echo
echo "ALL TESTS PASSED"
