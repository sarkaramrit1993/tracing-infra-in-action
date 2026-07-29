#!/usr/bin/env bash
# Chapter 4 stack test. Asserts four things against the LIVE stack:
#   1. the otlp_spans topic exists with the expected partitions and RF,
#   2. a trace round-trips through Kafka to Jaeger,
#   3. with ONE broker down the trace still lands (RF=2 tolerates it),
#   4. with TWO brokers down it does not.
#
# Assertion 3/4 is the point of the chapter: curl returns 200 either way, so
# the signal is whether the trace arrives, never the HTTP status. One broker
# down is tolerated (RF=2). With two down, new traces stop arriving while the
# endpoint keeps answering 200; that gap between "request succeeded" and
# "trace captured" is the lesson. The two stopped brokers also sit in the
# KRaft controller quorum, and any partition they led is leaderless until a
# new controller is elected, and the consumer group's coordinator may have
# been on one of them too, so those are plausible contributing factors, but
# the exact mechanism has not been isolated to a single cause.
#
# Steps 2 through 4 wait for the trace count to hold steady before taking a
# baseline. Step 2's baseline needs this too: this script's own prior run
# ends by restarting all three brokers, and the gateway can flood its queued
# backlog for up to a minute afterward, so a run started in that window would
# take step 2's baseline mid-flood. The collector queues spans while Kafka is
# unreachable and flushes the backlog once it can reach a broker again, so
# measuring against a queue that is still draining would count old backlog
# as growth caused by the checkout that step is about to send.
#
# wait_for_quiet requires three consecutive equal readings, sampled 7 seconds
# apart, not two readings 5 seconds apart. OTEL_BSP_SCHEDULE_DELAY=5000 means
# the SDK exports on a 5-second cadence, so a draining queue empties in
# bursts roughly 5 seconds apart; two readings exactly 5 seconds apart can
# land in the same inter-burst gap and read as quiet while more spans are
# still queued behind it. Three readings 7 seconds apart (out of phase with
# the 5-second cadence) make that far less likely.
#
# Step 4's negative window (does the trace NOT arrive) is the same length as
# steps 2 and 3's positive window (does it arrive), not half of it. A
# shorter negative window would score any delivery that just happens to
# land in the gap between the two windows as absence.
#
# The trap restores all three brokers on any exit path, so a failure part-way
# through does not leave the stack crippled. It waits for all three brokers
# to answer before returning: `docker compose start` returns as soon as the
# containers start, well before Kafka is ready to serve `kafka-topics.sh
# --describe`, and a second run launched immediately after would otherwise
# hit assertion 1 against a broker that is not listening yet. If a broker
# does not come back, the trap prints a loud warning rather than swallowing
# the failure, and it preserves the exit status this test run actually
# earned rather than letting a restore hiccup override it.
#
# Prereq: `docker compose up -d` has settled (Kafka needs ~60s to elect).
#
# Usage:  bash tests/test_stack.sh
set -euo pipefail
cd "$(dirname "$0")/.."

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

# Polls a broker with the same command its compose healthcheck uses, until
# it answers or the tries run out.
wait_for_broker() {
    local svc=$1 tries=$2
    for _ in $(seq 1 "$tries"); do
        docker compose exec -T "$svc" /opt/kafka/bin/kafka-broker-api-versions.sh \
            --bootstrap-server localhost:9093 >/dev/null 2>&1 && return 0
        sleep 3
    done
    return 1
}

restore_brokers() {
    local rc=$?
    echo "== restoring brokers =="
    docker compose start kafka-1 kafka-2 kafka-3 >/dev/null 2>&1 || true

    local all_ok=1
    for b in kafka-1 kafka-2 kafka-3; do
        wait_for_broker "$b" 20 || all_ok=0
    done

    if [ "$all_ok" = "1" ]; then
        echo "restored: all three brokers responding"
    else
        echo "WARNING: restore did not confirm all three brokers responding; run 'docker compose ps' and check before the next test run" >&2
    fi

    exit "$rc"
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

# Waits until trace_count reads the same value on three consecutive checks,
# 7 seconds apart, then prints that value. Failing means the count never
# held still, so any baseline taken now would not be trustworthy. See the
# header comment for why three readings 7 seconds apart, not two 5 seconds
# apart.
wait_for_quiet() {
    local tries=$1 interval=7 prev cur streak=0 i
    prev=$(trace_count)
    for i in $(seq 1 "$tries"); do
        sleep "$interval"
        cur=$(trace_count)
        if [ "$cur" = "$prev" ]; then
            streak=$((streak + 1))
            if [ "$streak" -ge 2 ]; then
                echo "$cur"
                return 0
            fi
        else
            streak=0
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
BASE=$(wait_for_quiet 6) || fail "trace count did not settle before taking the baseline (still draining from a prior run)"
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
if wait_for_growth "$BASE" 30; then
    fail "trace reached Jaeger with two brokers down (expected the pipeline to stall)"
fi
pass "trace did not reach Jaeger with two brokers down, while checkout still answered"

echo
echo "ALL TESTS PASSED"
