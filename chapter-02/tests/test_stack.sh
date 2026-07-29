#!/usr/bin/env bash
# Chapter 2 stack test. Asserts three things against the LIVE stack:
#   1. all three services are running,
#   2. the instrumented endpoint answers,
#   3. a trace it produced is retrievable from Jaeger by service name.
#
# Step 3 takes a baseline count of /checkout traces before sending the
# request and asserts growth past that baseline, not just presence. Jaeger
# v2 keeps traces for the container's lifetime, and the healthcheck polls
# /health every 10s, so a stale trace from an earlier run (or a health-check
# trace under the same service name) would otherwise satisfy a plain
# "any trace exists" check even if export is now broken. It then polls on a
# short interval because the SDK batches spans before export, so a trace is
# not queryable the instant the curl below returns.
#
# Prereq: `docker compose up -d` has settled.
#
# Usage: bash tests/test_stack.sh
set -euo pipefail
cd "$(dirname "$0")/.."

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

# Counts traces for checkout-service's /checkout operation specifically,
# excluding the container's periodic /health check traffic.
trace_count() {
  curl -s "http://localhost:16686/api/traces?service=checkout-service&operation=%2Fcheckout&limit=1000" \
    | python3 -c "import sys,json;print(len(json.load(sys.stdin).get('data') or []))" 2>/dev/null || echo 0
}

echo "== 1. services running =="
RUNNING=$(docker compose ps --status running --services)
for svc in checkout-service otel-collector jaeger; do
  echo "$RUNNING" | grep -qx "$svc" || fail "service '$svc' not running"
done
pass "all three services running"

echo "== 2. instrumented endpoint answers =="
BASE=$(trace_count)
CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/checkout)
[ "$CODE" = "200" ] || fail "GET /checkout returned HTTP $CODE"
pass "GET /checkout returned 200"

echo "== 3. trace reached Jaeger =="
FOUND=0
for _ in $(seq 1 30); do
  COUNT=$(trace_count)
  if [ "${COUNT:-0}" -gt "$BASE" ]; then
    FOUND=1
    break
  fi
  sleep 2
done
[ "$FOUND" = "1" ] || fail "trace count for checkout-service's GET /checkout did not grow past baseline ($BASE) in Jaeger after 60s"
pass "trace for checkout-service is queryable in Jaeger (baseline $BASE, now $COUNT)"

echo
echo "ALL TESTS PASSED"
