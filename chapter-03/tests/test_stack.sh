#!/usr/bin/env bash
# Chapter 3 stack test. Asserts three things against the LIVE stack:
#   1. the agent and both gateways are running and scraped,
#   2. spans accepted by the agent are exported onward,
#   3. trace-aware routing spreads spans across BOTH gateways, the
#      three-tier claim this chapter makes.
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
UP=$(promql 'count(up == 1)')
[ "$(printf '%.0f' "$UP")" -ge 3 ] || fail "fewer than 3 Prometheus targets are up (got $UP)"
pass "agent and both gateways are running and scraped"

echo "== 2. spans accepted by the agent are exported onward =="
for _ in $(seq 1 50); do curl -s -o /dev/null http://localhost:8080/checkout; done
echo "waiting for batch export and two Prometheus scrapes (40s)..."
sleep 40

ACCEPTED=$(promql 'sum(otelcol_receiver_accepted_spans)')
SENT=$(promql 'sum(otelcol_exporter_sent_spans)')
[ "$(printf '%.0f' "$ACCEPTED")" -gt 0 ] || fail "no spans accepted by any receiver"
[ "$(printf '%.0f' "$SENT")" -gt 0 ] || fail "no spans exported by any exporter"
pass "spans accepted ($(printf '%.0f' "$ACCEPTED")) and exported ($(printf '%.0f' "$SENT"))"

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
