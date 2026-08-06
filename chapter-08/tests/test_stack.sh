#!/usr/bin/env bash
# Chapter 8 live tests. The stack must be up and generate/generate.py must have run.
#
# The interesting assertion is step 3. Chapter 8's whole claim is that the same
# bytes return a precise lie or a correct estimate depending on how the query
# weights what it reads. That is testable, because the generator records the
# population it produced before it sampled anything. So this script does not
# check that the two answers differ, which would pass on any pair of wrong
# numbers. It checks that the weighted answer equals the truth and the biased
# one does not.
#
# Usage:  bash tests/test_stack.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# stdin is /dev/null so a query cannot hang on it. NOTES.md has the why, and why
# CH_FILE has to be a separate helper.
CH()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
CH_FILE() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

WINDOW="timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR) AND parent_span_id = ''"

echo "== 1. the table exists and carries chapter 8's two additions =="
EXISTS=$(CH --query "EXISTS TABLE tracing.otel_traces")
[ "$EXISTS" = "1" ] || fail "tracing.otel_traces is missing; has the stack finished starting?"
HAS_PARENT=$(CH --query "SELECT count() FROM system.columns
  WHERE database='tracing' AND table='otel_traces' AND name='parent_span_id'")
[ "$HAS_PARENT" = "1" ] || fail "parent_span_id is missing, so no query can pick out a request"
pass "tracing.otel_traces exists and has parent_span_id"

echo "== 2. the generator has run and recorded what it produced =="
TRUTH=$(CH --query "SELECT count() FROM tracing.ground_truth")
if [ "$TRUTH" = "0" ]; then
  fail "tracing.ground_truth is empty. Run: python3 generate/generate.py"
fi
POP=$(CH --query "SELECT requests FROM tracing.ground_truth LIMIT 1")
TRUE_P99=$(CH --query "SELECT p99_ms FROM tracing.ground_truth LIMIT 1")
TRUE_ERRORS=$(CH --query "SELECT errors FROM tracing.ground_truth LIMIT 1")
SPANS=$(CH --query "SELECT count() FROM tracing.otel_traces")
[ "$SPANS" != "0" ] || fail "no spans in tracing.otel_traces. Run: python3 generate/generate.py"

# Every assertion below filters to the last hour, and the generator writes rows
# spanning the twenty minutes before it ran. So this suite has a shelf life: an
# hour after generating, every row is still on disk and none is in the window.
# Check it here. Left to surface downstream it arrives as a weighted total of
# zero, which reads like the sampling weights are broken and sends you into
# adjusted_count when the answer is that the clock moved.
IN_WINDOW=$(CH --query "SELECT count() FROM tracing.otel_traces
             WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)")
if [ "$IN_WINDOW" = "0" ]; then
  AGE=$(CH --query "SELECT dateDiff('minute', max(timestamp), now())
         FROM tracing.otel_traces")
  fail "all $SPANS spans are on disk but none fall inside the one-hour window every
      query uses; the newest is $AGE minutes old. This is age, not a broken query.
      Run: python3 generate/generate.py"
fi
pass "population $POP requests recorded, $SPANS spans on disk, $IN_WINDOW in the window"

echo "== 3. listing 8.1: the weighted answers are right, the biased ones are not =="
BIASED=$(CH --query "SELECT count() FROM tracing.otel_traces WHERE $WINDOW")
WEIGHTED=$(CH --query "SELECT toUInt64(sum(adjusted_count)) FROM tracing.otel_traces WHERE $WINDOW")
[ "$WEIGHTED" = "$POP" ] \
  || fail "sum(adjusted_count) returned $WEIGHTED, but the generator produced $POP"
pass "sum(adjusted_count) reproduces the population exactly ($WEIGHTED)"
[ "$BIASED" != "$POP" ] \
  || fail "count() returned the population exactly; the data is not sampled, so listing 8.1 shows nothing"
# 10,000,000 over 154,200 is 64.85, which shell arithmetic would truncate to 64.
RATIO=$(awk -v pop="$POP" -v kept="$BIASED" 'BEGIN { printf "%.0f", pop / kept }')
pass "count() reads $BIASED, low by ${RATIO}x, which is the bug the chapter opens with"

W_P99=$(CH --query "SELECT round(quantileExactWeighted(0.99)(duration_ns,
          toUInt64(round(adjusted_count))) / 1e6, 1) FROM tracing.otel_traces WHERE $WINDOW")
U_P99=$(CH --query "SELECT round(quantile(0.99)(duration_ns) / 1e6, 1)
          FROM tracing.otel_traces WHERE $WINDOW")
[ "$W_P99" = "$TRUE_P99" ] \
  || fail "weighted p99 read $W_P99 ms against a true p99 of $TRUE_P99 ms"
pass "weighted p99 $W_P99 ms matches the true p99 exactly"
awk -v u="$U_P99" -v t="$TRUE_P99" 'BEGIN { exit !(u > t * 2) }' \
  || fail "unweighted p99 $U_P99 ms is not meaningfully above the true $TRUE_P99 ms; the demo shows nothing"
pass "unweighted p99 $U_P99 ms reads far above the truth, which is the other half of the bug"

echo "== 4. listing 8.2: the bloom index prunes granules the primary key left =="
# The last Granules line of the plan is the last thing that got to prune. Before
# the index that is the primary key; after, it is the bloom. Comparing the two is
# the difference between proving the index prunes and proving it merely appears,
# and an index that appears while pruning nothing is exactly the failure listing
# 8.2 warns about.
EXPLAIN_8_2() {
  CH --query "EXPLAIN indexes = 1 SELECT * FROM tracing.otel_traces
              WHERE trace_id = '4bf92f3577b34da6a3ce929d0e0e4736'"
}
SURVIVORS() { EXPLAIN_8_2 | grep -oE 'Granules: [0-9]+' | tail -1 | grep -oE '[0-9]+' || true; }

CH --query "ALTER TABLE tracing.otel_traces DROP INDEX IF EXISTS idx_trace_id"
BEFORE=$(EXPLAIN_8_2 | grep -c "Name: idx_trace_id" || true)
[ "$BEFORE" = "0" ] || fail "the index still exists before listing 8.2 adds it"
BEFORE_G=$(SURVIVORS)
CH_FILE clickhouse/skipindex.sql > /dev/null
AFTER=$(EXPLAIN_8_2 | grep -c "Name: idx_trace_id" || true)
[ "$AFTER" = "1" ] || fail "listing 8.2 ran but EXPLAIN does not report the index"
AFTER_G=$(SURVIVORS)
[ -n "$BEFORE_G" ] && [ -n "$AFTER_G" ] \
  || fail "could not read granule counts out of EXPLAIN; has the output format changed?"
[ "$AFTER_G" -lt "$BEFORE_G" ] \
  || fail "the index is in the plan but pruned nothing: $BEFORE_G granules before, $AFTER_G after"
pass "listing 8.2 adds the index and it prunes $BEFORE_G granules to $AFTER_G"

echo "== 5. listing 8.3: the rollup agrees with the raw scan, to the unit =="
CH_FILE clickhouse/rollup.sql > /dev/null
ROLLUP=$(CH --query "SELECT toUInt64(sum(requests)) FROM tracing.red_by_service
          WHERE minute >= toStartOfMinute(now() - INTERVAL 1 HOUR)")
[ "$ROLLUP" = "$WEIGHTED" ] \
  || fail "the rollup reads $ROLLUP where listing 8.1 reads $WEIGHTED; the two windows have drifted apart"
pass "rollup $ROLLUP matches listing 8.1 exactly"
ROLLUP_ERRORS=$(CH --query "SELECT toUInt64(sum(requests)) FROM tracing.red_by_service
                 WHERE status_code = 'STATUS_CODE_ERROR'")
[ "$ROLLUP_ERRORS" = "$TRUE_ERRORS" ] \
  || fail "the rollup counts $ROLLUP_ERRORS errors against a true $TRUE_ERRORS"
pass "the error half of RED reads $ROLLUP_ERRORS, exactly what the generator produced"

echo "== 6. cleanup: put the table back the way the stack starts =="
CH --query "DROP VIEW IF EXISTS tracing.red_by_service"
CH --query "ALTER TABLE tracing.otel_traces DROP INDEX IF EXISTS idx_trace_id"
echo "dropped the rollup view and the index listing 8.2 added"

echo
echo "ALL TESTS PASSED"
