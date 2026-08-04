#!/usr/bin/env bash
# Chapter 7 stack test. Asserts four things against the LIVE stack:
#   1. the listing 7.1 table exists,
#   2. a trace round-trips (insert -> point lookup by trace_id),
#   3. the row policy blocks cross-tenant reads while leaving the operator
#      login able to read the whole table,
#   4. a span the checkout service really emitted comes back out of Tempo.
#
# The order mirrors the README walkthrough rather than being forced by the row
# policy. Listing 7.4 exempts the operator login, so nothing here has to run
# before the policy exists or after it is gone.
#
# Step 4 is split in two: the request that produces the span is fired right
# after step 2, and the assertion runs last, so the batch timers between the app
# and Tempo elapse while the tenancy checks are working rather than in a sleep.
#
# Prereq: `docker compose up -d` has settled.
#
# Usage:  bash tests/test_stack.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# clickhouse-client reads stdin for INSERT data even when the row is inline in
# VALUES, and `docker compose exec -T` hands it the caller's stdin. If that stdin
# stays open without reaching EOF, the INSERT blocks forever. So CH always feeds
# /dev/null, and CH_FILE is the single path that feeds stdin a .sql file.
CH()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
CH_FILE() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

# Step 4 talks to the two published HTTP ports rather than going through
# `docker compose exec`, because that is what a reader does and what the
# Collector's Tempo exporter is actually reachable on.
TEMPO_URL="http://localhost:3200"
CHECKOUT_URL="http://localhost:8080/checkout"

# One checkout request, carrying a trace id this script chose. The W3C
# traceparent header makes the id an input rather than something to go hunting
# for afterwards, and the 01 flag marks the trace sampled so the SDK keeps it.
emit_live_trace() {
  curl -sf -o /dev/null -H "traceparent: 00-$1-1111222233334444-01" "$CHECKOUT_URL" \
    || fail "checkout service did not answer on $CHECKOUT_URL"
}

# The span crosses two batch timers before Tempo can answer for it: the SDK's
# (OTEL_BSP_SCHEDULE_DELAY, 5s) and the Collector's (batch timeout, 1s). Polling
# waits those out instead of racing them, and gives up loudly instead of hanging.
# A 404 means Tempo has no such trace, which is what a broken fan-out looks like.
wait_for_tempo_trace() {
  local tid="$1" deadline=$((SECONDS + 90)) code="" body=""
  while [ "$SECONDS" -lt "$deadline" ]; do
    body=$(curl -s -w '\n%{http_code}' "$TEMPO_URL/api/traces/$tid" || true)
    code="${body##*$'\n'}"
    if [ "$code" = "200" ] \
       && printf '%s' "$body" | grep -q '"stringValue":"checkout-service"' \
       && printf '%s' "$body" | grep -q '"name":"validate_cart"'; then
      return 0
    fi
    sleep 3
  done
  echo "last answer from $TEMPO_URL/api/traces/$tid was HTTP ${code:-none}" >&2
  fail "no checkout span for trace $tid reached Tempo in 90s (fan-out to Tempo is broken)"
}

echo "== 1. table exists =="
EXISTS=$(CH --query "EXISTS TABLE tracing.otel_traces")
[ "$EXISTS" = "1" ] || fail "tracing.otel_traces does not exist"
pass "tracing.otel_traces exists (listing 7.1)"

echo "== 2. trace round-trips (insert + point lookup by trace_id) =="
# One id per run, used twice: once for the synthetic row below, once for the
# live checkout request that step 4 chases into Tempo. It has to be fresh each
# run. Both stores keep what they are given, so a fixed id would let a second
# run find the first run's trace in Tempo and pass with the fan-out already
# dead, and would let the leftover live spans confuse the lookup below.
TID=$(od -An -tx1 -N16 /dev/urandom | tr -d ' \n')
CH --query "INSERT INTO tracing.otel_traces \
  (timestamp, trace_id, span_id, service_name, span_name, status_code, duration_ns, attributes) \
  VALUES (now64(9), '$TID', 'ffff1111', 'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 21000000, {'k':'v'})"
GOT=$(CH --query "SELECT span_name FROM tracing.otel_traces WHERE trace_id = '$TID' LIMIT 1")
[ "$GOT" = "validate_cart" ] || fail "point lookup by trace_id returned '$GOT'"
pass "trace round-trip: inserted and read back by trace_id"

echo "== fire one live trace, asserted against Tempo in step 4 =="
emit_live_trace "$TID"
echo "sent trace $TID into checkout -> collector -> tempo"

echo "== apply tenancy.sql (row policy + tenant map + tenant logins) =="
CH_FILE clickhouse/tenancy.sql
pass "tenancy.sql applied (listing 7.4)"

echo "== 3. row policy blocks cross-tenant reads =="
# acme_reader is mapped to tenant_a in tracing.tenant_users, so it must see
# tenant_a rows...
A_SEES_A=$(CH --user acme_reader --query \
  "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_a'")
[ "$A_SEES_A" -ge 1 ] || fail "acme_reader sees zero of its own rows (expected >=1)"
# ...and zero tenant_b rows, even when explicitly asking for them.
A_SEES_B=$(CH --user acme_reader --query \
  "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'")
[ "$A_SEES_B" = "0" ] || fail "acme_reader saw $A_SEES_B tenant_b rows (row policy leaked)"
# symmetric check
B_SEES_A=$(CH --user globex_reader --query \
  "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_a'")
[ "$B_SEES_A" = "0" ] || fail "globex_reader saw $B_SEES_A tenant_a rows (row policy leaked)"
pass "row policy isolates tenants (acme sees a, acme sees 0 of b, globex sees 0 of a)"

# Annotation #D: the operator is in the exempt list, so the policy that isolates
# the tenants does not blind the login every other assertion, benchmark and
# exercise runs as. Asserted while the policy is in place, which is the only
# time it means anything.
ADMIN_SEES=$(CH --query "SELECT count() FROM tracing.otel_traces")
[ "$ADMIN_SEES" -ge 1 ] \
  || fail "the operator login reads $ADMIN_SEES rows with the policy in place (TO ALL EXCEPT is not exempting it)"
pass "operator login reads the whole table with the policy in place ($ADMIN_SEES rows)"

# Nothing above depends on this. It is hygiene: the tenant logins are
# passwordless and hold SELECT, and dropping the column puts the table back to
# listing 7.1's nine so a re-run starts where the last one did.
echo "== cleanup: undo everything tenancy.sql created =="
CH --query "DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces"
CH --query "DROP USER IF EXISTS acme_reader, globex_reader"
CH --query "DROP TABLE IF EXISTS tracing.tenant_users"
CH --query "ALTER TABLE tracing.otel_traces DROP COLUMN IF EXISTS tenant_id"
echo "dropped the policy, the tenant logins, the map and the tenant_id column"

echo "== 4. a real span reaches Tempo (the Collector's other export) =="
# ClickHouse got this trace over Kafka. Tempo got the same spans over the other
# leg of the Collector's fan-out. Asking Tempo for the id is the only check in
# this repo that proves that leg carries spans; the rest read the config.
wait_for_tempo_trace "$TID"
pass "trace $TID read back from Tempo by id (section 7.3 block archetype)"

echo
echo "ALL TESTS PASSED"
