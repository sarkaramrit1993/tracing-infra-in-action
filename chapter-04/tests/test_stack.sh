#!/usr/bin/env bash
# Chapter 4 stack test. Asserts four things against the LIVE stack:
#   1. the otlp_spans topic exists with the expected partitions and RF,
#   2. a trace round-trips through Kafka to Jaeger,
#   3. with ONE broker down the trace still lands (RF=2 tolerates it),
#   4. with TWO brokers down it does not.
#
# Assertion 3/4 is the point of the chapter: curl returns 200 either way, so
# the signal is whether the trace arrives, never the HTTP status. Two down
# is a total outage, not a partial one: all three brokers also form the
# KRaft controller quorum, so losing two of three voters loses controller
# majority and the cluster stops accepting writes cluster-wide, independent
# of which partition a trace's ID happens to hash to.
#
# Steps 3 and 4 wait for the trace count to hold steady before taking a
# baseline, and again after stopping a broker. The collector queues spans
# while Kafka is unreachable and flushes the backlog once it can reach a
# broker again, so measuring against a queue that is still draining would
# count old backlog as growth caused by the checkout this step is about to
# send.
#
# The trap restores all three brokers on any exit path, so a failure part-way
# through does not leave the stack crippled.
#
# Prereq: `docker compose up -d` has settled (Kafka needs ~60s to elect).
#
# Usage:  bash tests/test_stack.sh
set -euo pipefail
cd "$(dirname "$0")/.."

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

restore_brokers() {
    echo "== restoring brokers =="
    docker compose start kafka-1 kafka-2 kafka-3 >/dev/null 2>&1 || true
}
trap restore_brokers EXIT

# Counts traces for the checkout-service's GET /checkout operation
# specifically. Filtering by operation excludes the container's periodic
# health-check traffic (every route is instrumented, including /health,
# and Docker polls it every 10s), which would otherwise never let the
# count hold steady.
trace_count() {
    curl -s "http://localhost:16686/api/traces?service=checkout-service&operation=GET%20%2Fcheckout&limit=1000" \
        | python3 -c "import sys,json;print(len(json.load(sys.stdin).get('data') or []))" 2>/dev/null || echo 0
}

# Polls for the trace count to exceed a baseline. Returns 0 if it does.
wait_for_growth() {
    local baseline=$1 tries=$2
    for _ in $(seq 1 "$tries"); do
        [ "$(trace_count)" -gt "$baseline" ] && return 0
        sleep 2
    done
    return 1
}

# Waits until trace_count reads the same value on two consecutive checks
# 5 seconds apart, then prints that value. Failing means the count never
# held still, so any baseline taken now would not be trustworthy.
wait_for_quiet() {
    local tries=$1 prev cur i
    prev=$(trace_count)
    for i in $(seq 1 "$tries"); do
        sleep 5
        cur=$(trace_count)
        if [ "$cur" = "$prev" ]; then
            echo "$cur"
            return 0
        fi
        prev=$cur
    done
    return 1
}

echo "== 1. otlp_spans topic exists with expected partitions and RF =="
DESC=$(docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:9093 --describe --topic otlp_spans)
echo "$DESC" | grep -q "PartitionCount" || fail "otlp_spans topic not found"
PARTS=$(echo "$DESC" | grep -o "PartitionCount: [0-9]*" | head -1 | awk '{print $2}')
RF=$(echo "$DESC" | grep -o "ReplicationFactor: [0-9]*" | head -1 | awk '{print $2}')
[ "${PARTS:-0}" -ge 1 ] || fail "otlp_spans has no partitions"
[ "${RF:-0}" -ge 2 ] || fail "otlp_spans ReplicationFactor is $RF, expected >= 2"
pass "otlp_spans exists (partitions=$PARTS, RF=$RF)"

echo "== 2. a trace round-trips through Kafka to Jaeger =="
BASE=$(trace_count)
curl -s -o /dev/null http://localhost:8080/checkout
wait_for_growth "$BASE" 30 || fail "trace did not reach Jaeger with all brokers up"
pass "trace reached Jaeger through the full path"

echo "== 3. one broker down: the trace still lands (RF=2) =="
wait_for_quiet 6 >/dev/null || fail "trace count did not settle before stopping kafka-2"
docker compose stop kafka-2 >/dev/null
sleep 20
BASE=$(wait_for_quiet 6) || fail "trace count did not settle after stopping kafka-2 (queue still draining)"
curl -s -o /dev/null http://localhost:8080/checkout
wait_for_growth "$BASE" 30 || fail "trace did not reach Jaeger with one broker down (RF=2 should tolerate this)"
pass "trace still reached Jaeger with kafka-2 down"

echo "== 4. two brokers down: the trace does NOT land =="
wait_for_quiet 6 >/dev/null || fail "trace count did not settle before stopping kafka-1"
docker compose stop kafka-1 >/dev/null
sleep 25
BASE=$(wait_for_quiet 6) || fail "trace count did not settle after stopping kafka-1 (queue still draining, measurement not trustworthy)"
CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/checkout)
[ "$CODE" = "200" ] || echo "note: checkout returned HTTP $CODE (the assertion is about the trace, not this)"
if wait_for_growth "$BASE" 15; then
    fail "trace reached Jaeger with two brokers down (expected the pipeline to stall)"
fi
pass "trace did not reach Jaeger with two brokers down, while checkout still answered"

echo
echo "ALL TESTS PASSED"
