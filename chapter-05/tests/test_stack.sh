#!/usr/bin/env bash
# Chapter 5 stack test. Asserts five things against the LIVE stack:
#   1. the storage-time path fills ClickHouse,
#   2. the stream-time path assembles MORE traces than it had before,
#   3. the atomicity invariant holds: no checkout trace in the store has
#      fewer spans than the checkout endpoint emits,
#   4. nothing consumes the late-span side output, so it cannot feed back,
#   5. the assembly job is still alive at the end, so none of the above was
#      measured over a stack that had already died.
#
# Assertions 2 and 5 exist because of a specific false pass. On a Docker
# allocation that is too small the Flink taskmanager is OOM-killed partway
# through, the job fails, and the traces it assembled before dying stay in
# the topic. A cumulative "has anything ever been assembled" check keeps
# reading green over a dead stack, and so does everything else here. Hence a
# growth check rather than a total, and an explicit liveness check last.
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
# Assertion 3's grouped query is scoped to trace_ids carrying a "GET
# /checkout" root span; if that population is empty, the grouping returns no
# rows and the partial-trace count reads zero having examined nothing. The
# script takes that population's count before sending any traffic and again
# afterward, and requires both an absolute floor and a real increase over the
# baseline: an absolute floor alone would let leftover checkout trace_ids
# from an earlier run (the stack was not torn down with `docker compose down
# -v` in between) satisfy the check even while this run's export is broken.
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

CHECKOUT_TRACES_BEFORE=$(CH --query "
    SELECT count(DISTINCT trace_id) FROM tracing.otel_traces
    WHERE span_name = 'GET /checkout'")

# Sums the end offsets of every partition of a topic, which is its total
# message count.
topic_count() {
    docker compose exec -T kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
        --bootstrap-server kafka-1:9093 --topic "$1" \
        | awk -F: '{s+=$3} END {print s+0}'
}

# Offsets are cumulative, so a bare "greater than zero" would stay true
# forever once anything had ever been assembled, and would pass on a re-run
# even with the stream-time path dead. Take a baseline and require growth.
ASSEMBLED_BEFORE=$(topic_count traces.assembled)

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
ASSEMBLED_AFTER=$(topic_count traces.assembled)
ASSEMBLED_GROWTH=$((ASSEMBLED_AFTER - ASSEMBLED_BEFORE))
# The endpoint's own healthcheck traces are assembled too, so the growth is
# not exactly the 120 checkouts sent. Require a clear majority of them rather
# than an exact figure.
MIN_ASSEMBLED_GROWTH=60
[ "$ASSEMBLED_GROWTH" -ge "$MIN_ASSEMBLED_GROWTH" ] || fail "traces.assembled grew by only $ASSEMBLED_GROWTH (before=$ASSEMBLED_BEFORE, after=$ASSEMBLED_AFTER), expected at least $MIN_ASSEMBLED_GROWTH from the 120 checkouts sent; the Flink assembly job may not be running"
pass "traces.assembled grew by $ASSEMBLED_GROWTH (before=$ASSEMBLED_BEFORE, after=$ASSEMBLED_AFTER)"

echo "== 3. atomicity: no partial checkout traces in the store =="
# The grouped query below only examines trace_ids that carry a "GET
# /checkout" root span. If that population is empty (checkout traffic never
# reached ClickHouse) the grouping returns no rows, PARTIAL reads 0, and the
# assertion would pass having checked nothing. An absolute floor alone is
# not enough: if the stack was not torn down between runs, leftover checkout
# trace_ids from an earlier good run can clear the floor even while this
# run's export is broken. Require the count to have grown by a real margin
# since the baseline taken before traffic was sent, on top of the floor.
CHECKOUT_TRACES_AFTER=$(CH --query "
    SELECT count(DISTINCT trace_id) FROM tracing.otel_traces
    WHERE span_name = 'GET /checkout'")
CHECKOUT_TRACES_GROWTH=$((CHECKOUT_TRACES_AFTER - CHECKOUT_TRACES_BEFORE))
MIN_CHECKOUT_TRACES=30
[ "${CHECKOUT_TRACES_AFTER:-0}" -ge "$MIN_CHECKOUT_TRACES" ] || fail "only $CHECKOUT_TRACES_AFTER checkout trace_ids found in the store, expected at least $MIN_CHECKOUT_TRACES of the 120 checkouts sent"
[ "$CHECKOUT_TRACES_GROWTH" -ge "$MIN_CHECKOUT_TRACES" ] || fail "checkout trace_ids grew by only $CHECKOUT_TRACES_GROWTH (before=$CHECKOUT_TRACES_BEFORE, after=$CHECKOUT_TRACES_AFTER), expected growth of at least $MIN_CHECKOUT_TRACES from the 120 checkouts sent this run"
pass "checkout trace_ids grew by $CHECKOUT_TRACES_GROWTH (before=$CHECKOUT_TRACES_BEFORE, after=$CHECKOUT_TRACES_AFTER) to check for partial traces"

# Scope to trace_ids that carry the "GET /checkout" root span so the
# healthcheck's one-span "GET /health" traces (see header) are not
# miscounted as partial. A real checkout trace has 7 spans; fewer than that
# is a partial-trace violation.
#
# The max(timestamp) guard excludes traces that are still being written. Under
# live traffic a checkout can land mid-query, and its spans are legitimately
# incomplete for that moment. Without the guard the assertion false-fails on
# timing rather than on the invariant it exists to test.
PARTIAL=$(CH --query "
    SELECT count() FROM (
        SELECT trace_id, count() AS n, max(timestamp) AS last_span
        FROM tracing.otel_traces
        WHERE trace_id IN (
            SELECT trace_id FROM tracing.otel_traces WHERE span_name = 'GET /checkout'
        )
        GROUP BY trace_id
        HAVING n < 7 AND last_span < now() - INTERVAL 30 SECOND
    )")
[ "${PARTIAL:-0}" = "0" ] || fail "$PARTIAL settled checkout trace_ids have fewer than 7 spans (partial traces in store)"
pass "no partial traces: every settled checkout trace_id in tracing.otel_traces has 7 spans"

echo "== 4. the late-span side output is a dead end, not a feedback loop =="
KTOPICS --list | grep -q "^spans.late$" || fail "topic spans.late does not exist"
# The topic existing proves nothing on its own, since kafka-init creates it at
# startup before any span is produced. The invariant worth asserting is that
# nothing reads it: late spans are preserved for audit and never re-injected
# into assembly, which is what stops a straggler reopening a shipped trace.
LATE_CONSUMERS=$(docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka-1:9093 --all-groups --describe 2>/dev/null \
    | awk '$2 == "spans.late"' | wc -l | tr -d ' ')
[ "${LATE_CONSUMERS:-0}" = "0" ] || fail "$LATE_CONSUMERS consumer group assignments read spans.late; late spans must never re-enter the assembly path"
LATE=$(topic_count spans.late)
pass "spans.late exists with no consumer reading it (holds $LATE spans, audit only)"

echo "== 5. the assembly job is still alive at the end of the run =="
# Everything above can pass over a stack that is already dying. The Flink
# taskmanager is the first thing to be OOM-killed on a small Docker
# allocation, and when it goes the job fails while the assembled traces it
# already wrote stay in the topic, so assertions 1 to 4 still read as green.
# Check liveness last, or a dead stack looks like a healthy one.
TM_OOM=$(docker inspect -f '{{.State.OOMKilled}}' \
    "$(docker compose ps -aq flink-taskmanager)" 2>/dev/null || echo unknown)
[ "$TM_OOM" != "true" ] || fail "the Flink taskmanager was OOM-killed during this run; raise Docker's memory limit (see troubleshooting.md) and re-run, the results above are not trustworthy"
JOB_STATE=$(curl -s http://localhost:8081/jobs/overview \
    | python3 -c "import sys,json;print(next((j['state'] for j in json.load(sys.stdin).get('jobs',[])),'NONE'))" 2>/dev/null || echo UNREACHABLE)
[ "$JOB_STATE" = "RUNNING" ] || fail "the Flink assembly job is in state $JOB_STATE, expected RUNNING; the stream-time results above are not trustworthy"
pass "the Flink assembly job is still RUNNING and the taskmanager was not OOM-killed"

echo
echo "ALL TESTS PASSED"
