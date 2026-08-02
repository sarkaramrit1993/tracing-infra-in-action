#!/usr/bin/env bash
# Chapter 7 stack test. Asserts three things against the LIVE stack:
#   1. the listing 7.1 table exists,
#   2. a trace round-trips (insert -> point lookup by trace_id),
#   3. the row policy blocks cross-tenant reads (tenant_a cannot see tenant_b).
#
# Ordering matters and mirrors the README walkthrough: the admin round-trip in
# step 2 runs BEFORE the listing 7.4 row policy, because that policy is TO ALL
# and filters the default admin login to zero rows. Step 3 then applies the
# policy and checks isolation as the tenant users. The script drops any existing
# policy up front so it is safe to re-run from any prior state.
#
# Prereq: `docker compose up -d` has settled.
#
# Usage:  bash tests/test_stack.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# clickhouse-client reads stdin for INSERT data even when the row is inline in
# VALUES, and `docker compose exec -T` hands it the caller's stdin. From a
# terminal that never reaches EOF, so an INSERT would block forever. Feed it
# /dev/null when stdin is a terminal, and otherwise pass the caller's stdin
# straight through, so `CH --multiquery < file.sql` still works.
CH() {
  if [ -t 0 ]; then
    docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null
  else
    docker compose exec -T clickhouse clickhouse-client "$@"
  fi
}
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

echo "== 1. table exists =="
EXISTS=$(CH --query "EXISTS TABLE tracing.otel_traces")
[ "$EXISTS" = "1" ] || fail "tracing.otel_traces does not exist"
pass "tracing.otel_traces exists (listing 7.1)"

echo "== reset: drop the row policy so the admin round-trip sees its own write =="
CH --query "DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces"

echo "== 2. trace round-trips (insert + point lookup by trace_id) =="
TID="ffff0000ffff0000ffff0000ffff0000"
CH --query "INSERT INTO tracing.otel_traces \
  (timestamp, trace_id, span_id, service_name, span_name, status_code, duration_ns, attributes) \
  VALUES (now64(9), '$TID', 'ffff1111', 'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 21000000, {'k':'v'})"
GOT=$(CH --query "SELECT span_name FROM tracing.otel_traces WHERE trace_id = '$TID' LIMIT 1")
[ "$GOT" = "validate_cart" ] || fail "point lookup by trace_id returned '$GOT'"
pass "trace round-trip: inserted and read back by trace_id"

echo "== apply tenancy.sql (row policy + tenant users) =="
CH --multiquery < clickhouse/tenancy.sql
pass "tenancy.sql applied (listing 7.4)"

echo "== 3. row policy blocks cross-tenant reads =="
# tenant_a must see tenant_a rows...
A_SEES_A=$(CH --user tenant_a --query \
  "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_a'")
[ "$A_SEES_A" -ge 1 ] || fail "tenant_a sees zero of its own rows (expected >=1)"
# ...and zero tenant_b rows, even when explicitly asking for them.
A_SEES_B=$(CH --user tenant_a --query \
  "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'")
[ "$A_SEES_B" = "0" ] || fail "tenant_a saw $A_SEES_B tenant_b rows (row policy leaked)"
# symmetric check
B_SEES_A=$(CH --user tenant_b --query \
  "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_a'")
[ "$B_SEES_A" = "0" ] || fail "tenant_b saw $B_SEES_A tenant_a rows (row policy leaked)"
pass "row policy isolates tenants (a sees a, a sees 0 of b, b sees 0 of a)"

# The policy is TO ALL, so leaving it behind would filter the default admin to
# zero rows for everything the reader does next (walkthrough, benchmarks).
echo "== cleanup: drop the row policy =="
CH --query "DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces"
echo "dropped row policy tenant_filter; admin reads are unfiltered again"

echo
echo "ALL TESTS PASSED"
