#!/usr/bin/env bash
# Chapter 10 live tests. With the stack up:  docker compose up -d --build
# This grades the stack against a workload whose size and error count are
# known, through the same scripts the README runs. Section numbers are the
# README's steps:
#
#   1. Every service is running; the two one-shot jobs exited 0.
#   2. The edge connector counted exactly the requests sent, errors included;
#      Flink's windows add up; the view and the query agree with the rows.
#   3. The policy sampler kept every error trace, so its error count is exact.
#      The uniform sampler kept about 1 percent, and exactly what it kept is in
#      ClickHouse. The probabilistic sampler's survivors count at weight 100.
#      Exemplars taken after the sampler resolve; the edge's mostly do not.
#   4. A late span is dropped by Flink and counted, and the window row holds.
#   5. Fifty rows that carry adjusted_count = 100 read back as 5,000 calls from
#      both the view and the query.
#   6. One label set costs sixteen series; a Try this edit is undone byte for byte.
#   7. The user.id Collector overflows its limit and Prometheus refuses it.
#   3 again. A workload sent right after the sampler restarts is counted in
#      full.
#
# Every wait polls the data itself, never a fixed sleep.
#
# Usage: bash tests/test_stack.sh
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib.sh
source scripts/lib.sh

N=5000
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

echo "== 1. services =="
for s in checkout-service payment-service loadgen edge-collector sampler-collector kafka \
         span-consumer flink-jobmanager flink-taskmanager clickhouse prometheus; do
  state=$(docker compose ps -a --format '{{.Service}}|{{.State}}' | awk -F'|' -v s="$s" '$1 == s { print $2 }')
  [ "$state" = "running" ] || fail "service $s is '${state:-absent}', not running"
done
for s in kafka-init flink-sql-submit; do
  exit_code=$(docker compose ps -a --format '{{.Service}}|{{.State}}|{{.ExitCode}}' | awk -F'|' -v s="$s" '$1 == s { print $2 ":" $3 }')
  [ "$exit_code" = "exited:0" ] || fail "$s is '${exit_code:-absent}', expected exited:0"
done
pass "eleven services running, kafka-init and flink-sql-submit exited 0"

./scripts/wait-until-ready.sh > /dev/null
pass "wait-until-ready.sh"

echo "== 2-3. a measured workload of $N requests =="
measure "$N"
ERRORS=$(m ERRORS)
[ "$ERRORS" = "145" ] || fail "the load generator reports $ERRORS errors for $N requests, expected 145 (2.9%)"
eq "$(m edge_calls)" "$N" || fail "edge counted $(m edge_calls) calls, sent $N"
eq "$(m edge_errors)" "$ERRORS" || fail "edge counted $(m edge_errors) errors, sent $ERRORS"
pass "edge connector: $N calls, $ERRORS errors, exact"

eq "$(m policy_errors)" "$ERRORS" || fail "the policy sampler kept $(m policy_errors) of $ERRORS error traces"
pass "policy sampler kept every one of the $ERRORS error traces"

KEPT=$(m uniform_calls)
ge "$KEPT" 10 || fail "the uniform sampler kept $KEPT of $N traces; expected about 50"
ge 150 "$KEPT" || fail "the uniform sampler kept $KEPT of $N traces; expected about 50"
eq "$(m stored_rows)" "$KEPT" || fail "ClickHouse holds $(m stored_rows) rows, the sampler kept $KEPT"
pass "uniform sampler kept $KEPT of $N; ClickHouse holds the same $KEPT"

WEIGHTED=$(m weighted_calls)
ge "$WEIGHTED" "$(( N / 2 ))" || fail "weighted calls $WEIGHTED is under half of $N"
ge "$(( N * 2 ))" "$WEIGHTED" || fail "weighted calls $WEIGHTED is over twice $N"
pass "weighted calls $WEIGHTED against $N sent"

echo "== 2. Flink, the view and the query =="
OUT=$(./scripts/compare-placements.sh "$N")
echo "$OUT"
echo "$OUT" | awk -F'  +' -v n="$N" '$1 ~ /^2 Flink/ { exit !($2 + 0 == n) }' || fail "Flink's windows do not add up to $N"
pass "Flink windows add up to $N"
VIEW=$(echo "$OUT" | awk -F'  +' '$1 ~ /^3 ClickHouse/ { print $2 }')
QUERY=$(echo "$OUT" | awk -F'  +' '$1 ~ /^4 ClickHouse/ { print $2 }')
eq "$VIEW" "$KEPT" || fail "the view holds $VIEW calls, the stored rows are $KEPT"
eq "$QUERY" "$KEPT" || fail "the query counts $QUERY calls, the stored rows are $KEPT"
./scripts/compare-mv-and-query.sh "$N" > /dev/null
WEIGHT_OUT=$(./scripts/compare-mv-and-query.sh "$N")
echo "$WEIGHT_OUT"
pass "view and query-time query both count the $KEPT stored rows"

echo "== 3. the sampling table =="
BIAS=$(./scripts/show-sampling-bias.sh "$N")
echo "$BIAS"
echo "$BIAS" | awk -F'  +' '/^after a [0-9]+% probabilistic sampler/ { exit !($2 + 0 > 0) }' || fail "the probabilistic sampler row is empty"
echo "$BIAS" | awk -F'  +' -v e="$ERRORS" '/^after keep-every-error/ { exit !($3 + 0 == e) }' || fail "the keep-every-error row does not hold all $ERRORS errors"
WEIGHT=$(q weight-per-span)
ge "$WEIGHT" 95 && ge 105 "$WEIGHT" || fail "a kept span counted at weight $WEIGHT, expected 100"
pass "error rate skews, and a kept span counts at weight $WEIGHT"

echo "== 3. exemplars =="
OUT=$(./scripts/check-exemplars.sh "$N")
echo "$OUT"
echo "$OUT" | awk -F'  +' '/^after the sampler/ { exit !($4 == "100.00%") }' || fail "an exemplar taken after the sampler did not resolve"
pass "every exemplar taken after the sampler resolves in ClickHouse"

echo "== 4. a late span =="
./scripts/send-late-span.sh > /tmp/ch10-late.$$ || { cat /tmp/ch10-late.$$; rm -f /tmp/ch10-late.$$; fail "send-late-span.sh failed"; }
BEFORE_ROW=$(sed -n 's/^window already closed: GET \/cart  //p' /tmp/ch10-late.$$)
AFTER_ROW=$(sed -n 's/^window after:  GET \/cart  //p' /tmp/ch10-late.$$)
rm -f /tmp/ch10-late.$$
[ -n "$BEFORE_ROW" ] && [ "$BEFORE_ROW" = "$AFTER_ROW" ] || fail "the closed window changed: '$BEFORE_ROW' became '$AFTER_ROW'"
pass "late span counted by Flink; the closed window row did not change"

echo "== 5. fifty weighted rows =="
for label in view query; do
  ROW=$(echo "$WEIGHT_OUT" | awk -F'  +' -v l="^$label at its weight" '$1 ~ l { print $3 " " $4 " " $5 }')
  [ "$ROW" = "5000 200 500ms" ] || fail "the $label read the weighted rows as '$ROW', expected '5000 200 500ms'"
done
[ "$(ch --query "SELECT count() FROM tracing.otel_traces WHERE service_name = 'weight-check'")" = "0" ] || fail "the weighted rows were left behind"
pass "50 rows at adjusted_count = 100 read as 5000 calls, 200 errors, p99 500ms in the view and the query; removed again"

echo "== 6. series per label set =="
SERIES=$(./scripts/count-series.sh | awk -F'  +' '/^one label set/ { print $2 }')
[ "$SERIES" = "16" ] || fail "one label set costs '$SERIES' series, expected 16"
pass "one label set: 16 series"

BEFORE=$(cksum < collector/edge-config.yaml)
SERIES=$(./scripts/try-edge.sh bucket | awk -F'  +' '/^one label set/ { print $2 }')
[ "$SERIES" = "17" ] || fail "a 300ms boundary gave '$SERIES' series per label set, expected 17"
./scripts/try-edge.sh reset > /dev/null
[ "$(cksum < collector/edge-config.yaml)" = "$BEFORE" ] || fail "try-edge.sh reset did not restore collector/edge-config.yaml"
pass "a 300ms boundary makes 17 series; reset restores the file byte for byte"

echo "== 7. cardinality =="
OUT=$(./scripts/cardinality-blast.sh)
echo "$OUT"
echo "$OUT" | grep -q 'edge-userid-spanmetrics down sample limit exceeded' || fail "Prometheus did not refuse the user.id scrape"
echo "$OUT" | grep -q '^overflow series.* [1-9]$' || fail "no overflow series on the user.id Collector"
[ "$(echo "$OUT" | grep -c ' down ')" = "1" ] || fail "more than the user.id job went down"
AFTER=$(./scripts/cardinality-down.sh)
echo "$AFTER" | grep -q '^edge-spanmetrics up' || fail "the edge job is not up after removing the user.id Collector"
pass "user.id Collector overflowed and was refused; removed again"

echo "== 3. a workload right after the sampler restarts =="
docker compose restart sampler-collector > /dev/null 2>&1
require sampler-collector
measure 1000
KEPT=$(m uniform_calls)
ge "$KEPT" 1 || fail "the uniform sampler kept $KEPT of 1000 traces after a restart"
eq "$(m stored_rows)" "$KEPT" || fail "ClickHouse holds $(m stored_rows) rows, the sampler kept $KEPT"
eq "$(m policy_errors)" "$(m ERRORS)" || fail "the policy sampler kept $(m policy_errors) of $(m ERRORS) error traces after a restart"
pass "after a sampler restart, the uniform sampler kept $KEPT of 1000 and ClickHouse holds the same $KEPT"

echo "ALL PASSED"
