#!/usr/bin/env bash
# Chapter 7 adversarial tenancy test (section 7.5.2). Extends test_stack.sh with
# the trap the section warns about: the listing 7.4 row policy gates SELECT, but
# it does NOT gate INSERT or DROP PARTITION. A shared-table deployment that leans
# on the row policy alone still has to validate tenant_id at the ingest boundary,
# or one tenant's writer can land rows in another tenant's view.
#
# Asserts against the LIVE stack:
#   1. read isolation holds: tenant_a asking for tenant_b rows gets zero,
#   2. the ingest gap is real: a writer with INSERT rights can tag a row with
#      someone else's tenant_id and it lands, unfiltered, in that tenant's reads,
#   3. cleanup: the mislabeled row is removed so the demo is repeatable.
#
# Prereq: `docker compose up -d` has settled. This script applies tenancy.sql if
# it has not run yet. Run it LAST, after test_stack.sh, because the TO ALL policy
# this test relies on filters the default admin the other tests need unfiltered.
#
# Usage:  bash tests/test_tenancy.sh
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

echo "== ensure tenancy.sql applied (idempotent) =="
HAS_POLICY=$(CH --query \
  "SELECT count() FROM system.row_policies WHERE short_name = 'tenant_filter'")
if [ "$HAS_POLICY" = "0" ]; then
  CH_FILE clickhouse/tenancy.sql
  echo "  applied tenancy.sql"
fi

echo "== 1. read isolation: tenant_a cannot read tenant_b rows =="
A_SEES_B=$(CH --user tenant_a --query \
  "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'")
[ "$A_SEES_B" = "0" ] || fail "tenant_a read $A_SEES_B tenant_b rows (policy leaked)"
pass "row policy blocks the cross-tenant read (tenant_a sees 0 of tenant_b)"

echo "== 2. ingest gap: the row policy does not gate INSERT =="
# A trusted writer (here the default admin, which holds INSERT) mislabels a row
# with someone else's tenant_id. The row policy only rewrites SELECT, so nothing
# stops the write. This is the section 7.5.2 trap.
POISON="deadbeefdeadbeefdeadbeefdeadbeef"

# reset: an interrupted earlier run can leave its poison row behind, which would
# make the exact count below read 2 and report a leak that did not happen.
CH --query "ALTER TABLE tracing.otel_traces DELETE WHERE trace_id = '$POISON'" \
   --mutations_sync 1

CH --query "INSERT INTO tracing.otel_traces \
  (timestamp, trace_id, tenant_id, span_id, service_name, span_name, status_code, duration_ns, attributes) \
  VALUES (now64(9), '$POISON', 'tenant_b', 'deadbeef', 'checkout-service', 'validate_cart', 'STATUS_CODE_OK', 1000000, {'injected':'true'})"

# tenant_b now sees a row it never wrote: the ingest path was not gated.
B_SEES_POISON=$(CH --user tenant_b --query \
  "SELECT count() FROM tracing.otel_traces WHERE trace_id = '$POISON'")
if [ "$B_SEES_POISON" = "1" ]; then
  pass "INSERT is ungated: a mislabeled row landed in tenant_b's reads (7.5.2 trap)"
else
  fail "expected the injected row to reach tenant_b (got count $B_SEES_POISON)"
fi

echo "== 3. cleanup: remove the injected row =="
CH --query "ALTER TABLE tracing.otel_traces DELETE WHERE trace_id = '$POISON'" \
   --mutations_sync 1
STILL=$(CH --user tenant_b --query \
  "SELECT count() FROM tracing.otel_traces WHERE trace_id = '$POISON'")
[ "$STILL" = "0" ] || fail "cleanup left $STILL injected rows behind"
pass "injected row removed; demo is repeatable"

# The policy is TO ALL, so leaving it behind would filter the default admin to
# zero rows for everything the reader does next (walkthrough, benchmarks).
echo "== 4. cleanup: undo everything tenancy.sql created =="
CH --query "DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces"
# The users outlive the policy otherwise, and without it they are passwordless
# logins holding SELECT on all of tracing. The column goes too, so the table is
# back to listing 7.1's nine.
CH --query "DROP USER IF EXISTS tenant_a, tenant_b"
CH --query "ALTER TABLE tracing.otel_traces DROP COLUMN IF EXISTS tenant_id"
echo "dropped the policy, the tenant users and the tenant_id column"

echo
echo "The lesson from section 7.5.2: the row policy secures reads, not writes."
echo "A real multi-tenant ingest path must bind tenant_id to the authenticated"
echo "principal before the INSERT, never trust the value on the wire."
echo
echo "ALL TENANCY TESTS PASSED"
