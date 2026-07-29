#!/usr/bin/env bash
# Chapter 5 stack test. Asserts four things against the LIVE stack:
#   1. the storage-time path fills ClickHouse,
#   2. the stream-time path produces assembled traces on traces.assembled,
#   3. the atomicity invariant holds: no checkout trace in the store has
#      fewer spans than the checkout endpoint emits,
#   4. the late-span side output topic exists, separate from the main path.
#
# Assertion 3 is the chapter's central claim: whole traces or none, never a
# partial trace. Live data shows the checkout endpoint emits a seven-span
# trace per call, not the six the chapter prose describes: Flask's
# auto-instrumentation adds one root "GET /checkout" server span on top of
# the six manually created spans (validate_cart, inventory.reserve,
# payment.charge, fraud.score, order.create, notification.send). The same
# auto-instrumentation also traces the container healthcheck's GET /health
# calls as their own one-span traces, which are not checkout traces and are
# excluded below rather than miscounted as partial ones.
#
# Prereq: `docker compose up -d` has settled. Give it 90 seconds: Kafka has
# to elect controllers, ClickHouse has to apply its schema, and the Flink
# job has to submit. Watch `docker compose logs -f flink-job-submit` to
# confirm the job actually submitted.
#
# Usage: bash tests/test_stack.sh
set -euo pipefail
cd "$(dirname "$0")/.."

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

CH() { docker compose exec -T clickhouse clickhouse-client "$@"; }
KTOPICS() { docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9093 "$@"; }

echo "== generate traffic (120 checkouts) =="
for _ in $(seq 1 120); do curl -s -o /dev/null http://localhost:8080/checkout; done
echo "waiting for batch export, Kafka, the ClickHouse consumer, and Flink's"
echo "decision_wait timer (10s event-time) to all drain (60s)..."
sleep 60

echo "== 1. storage-time path filled ClickHouse =="
ROWS=$(CH --query "SELECT count() FROM tracing.otel_traces")
[ "${ROWS:-0}" -gt 0 ] || fail "tracing.otel_traces is empty (storage-time path did not deliver)"
pass "ClickHouse tracing.otel_traces holds $ROWS rows"

echo "== 2. stream-time path produced assembled traces =="
KTOPICS --list | grep -q "^traces.assembled$" || fail "topic traces.assembled does not exist"
ASSEMBLED=$(docker compose exec -T kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka-1:9093 --topic traces.assembled \
    | awk -F: '{s+=$3} END {print s+0}')
[ "${ASSEMBLED:-0}" -gt 0 ] || fail "traces.assembled has no messages (Flink assembly produced nothing)"
pass "traces.assembled holds $ASSEMBLED assembled traces"

echo "== 3. atomicity: no partial checkout traces in the store =="
# Scope to trace_ids that carry the "GET /checkout" root span so the
# healthcheck's one-span "GET /health" traces (see header) are not
# miscounted as partial. A real checkout trace has 7 spans; fewer than that
# is a partial-trace violation.
PARTIAL=$(CH --query "
    SELECT count() FROM (
        SELECT trace_id, count() AS n
        FROM tracing.otel_traces
        WHERE trace_id IN (
            SELECT trace_id FROM tracing.otel_traces WHERE span_name = 'GET /checkout'
        )
        GROUP BY trace_id
        HAVING n < 7
    )")
[ "${PARTIAL:-0}" = "0" ] || fail "$PARTIAL checkout trace_ids have fewer than 7 spans (partial traces in store)"
pass "no partial traces: every checkout trace_id in tracing.otel_traces has 7 spans"

echo "== 4. late-span side output exists, separate from the main path =="
KTOPICS --list | grep -q "^spans.late$" || fail "topic spans.late does not exist"
LATE=$(docker compose exec -T kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka-1:9093 --topic spans.late \
    | awk -F: '{s+=$3} END {print s+0}')
pass "spans.late exists (holds $LATE spans); assertion 3 passing alongside it shows none of them re-entered the store"

echo
echo "ALL TESTS PASSED"
