#!/usr/bin/env bash
# Chapter 9 live tests. The stack must be up: docker compose up -d --build
#
# What this proves, against the running stack rather than against a fixture:
#
#   1. All eight services are where they should be. Seven run; kafka-init is a
#      one-shot that must have exited 0. A suite that only checks the service it
#      is about passes on a stack that is half down.
#   2. The store carries parent_span_id and it is populated, so a trace can be
#      reassembled rather than just counted.
#   3. record_exception() detail reaches the span. The SDK writes it to a span
#      EVENT and the consumer stores span ATTRIBUTES, so without the Collector's
#      transform processor this is empty and everything below it is empty too.
#   4. The listing 9.2 error-issue index collapses many raw error spans into one
#      fingerprint, with a real innermost frame.
#   5. The pre/post divergence (section 9.2.4): the sampler keeps every error and
#      a tenth of the successes, so the post error rate reads inflated against
#      the pre rate, which is the truth.
#   6. The service graph has edges, derived from the pre-sample stream.
#   7. Listing 9.5's two counters exist and were reached by two different paths.
#   8. Logs arrived in Loki carrying the trace context of the span they were
#      written inside.
#
# It polls for every one of those rather than sleeping and hoping. A fixed sleep
# is a guess about a machine you are not sitting at.
#
# Step 9 puts the store back the way a fresh stack starts, so the suite can be
# re-run in any order.
#
# Usage:  bash tests/test_stack.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# clickhouse-client reads stdin when it is a terminal, so a helper called inside
# a loop that also owns stdin will sit there forever with no output. Redirecting
# from /dev/null is what stops that. It has to be a SEPARATE helper from the one
# that pipes a .sql file in, because that one needs its stdin for the file.
CH() { docker compose exec -T clickhouse clickhouse-client --database tracing "$@" < /dev/null; }
CH_FILE() { docker compose exec -T clickhouse clickhouse-client --database tracing --multiquery < "$1"; }

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

PROMQ() {
  curl -s --data-urlencode "query=$1" http://localhost:9090/api/v1/query \
    | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(sum(float(x['value'][1]) for x in r) if r else 0)"
}

# gt0 <promql> succeeds when the query sums to something above zero. Every poll
# below goes through it rather than through an inline test, because a quoted
# PromQL selector nested inside a polling snippet is where these scripts go
# wrong: the quoting breaks, the snippet never succeeds, and the failure reads
# as a timeout on data that was there the whole time.
gt0() { awk -v v="$(PROMQ "$1")" 'BEGIN { exit !(v > 0) }'; }

CH_COUNT() { CH --query "$1"; }
ge() { [ "$(CH_COUNT "$1")" -ge "$2" ]; }

NS_NOW() { python3 -c 'import time;print(int(time.time()*1e9))'; }
NS_AGO() { python3 -c "import time;print(int((time.time()-$1)*1e9))"; }

LOKI_STREAMS() {
  curl -s -G 'http://localhost:3100/loki/api/v1/query_range' \
    --data-urlencode "query=$1" \
    --data-urlencode "start=$(NS_AGO 900)" \
    --data-urlencode "end=$(NS_NOW)" \
    --data-urlencode 'limit=100' \
  | python3 -c "import sys,json;print(len(json.load(sys.stdin)['data']['result']))"
}

# wait_for <budget_seconds> <label> <shell snippet that exits 0 when satisfied>
wait_for() {
  local budget="$1" label="$2" snippet="$3" i=0
  while [ "$i" -lt "$budget" ]; do
    if eval "$snippet" >/dev/null 2>&1; then
      echo "   satisfied after ${i}s: $label"
      return 0
    fi
    i=$((i + 1)); sleep 1
  done
  fail "timed out after ${budget}s waiting for: $label"
}

SVC="checkout-service"
SPAN="fraud.score"
CHECKOUTS="${CHECKOUTS:-200}"
FORCED_ERRORS="${FORCED_ERRORS:-4}"

echo "== 1. all eight services are up =="
for s in checkout-service otel-collector kafka clickhouse consumer-clickhouse prometheus loki; do
  state=$(docker compose ps -a --format '{{.Service}}|{{.State}}' | awk -F'|' -v s="$s" '$1==s {print $2}')
  [ "$state" = "running" ] || fail "service $s is '${state:-absent}', not running"
done
init=$(docker compose ps -a --format '{{.Service}}|{{.State}}|{{.ExitCode}}' | awk -F'|' '$1=="kafka-init" {print $2":"$3}')
[ "$init" = "exited:0" ] || fail "kafka-init is '${init:-absent}', expected exited:0 (the otlp_spans topic may not exist)"
pass "seven services running, kafka-init exited 0"

wait_for 60 "clickhouse answering queries" "CH --query 'SELECT 1'"
wait_for 90 "loki /ready returning 200" \
  "[ \"\$(curl -s -o /dev/null -w '%{http_code}' http://localhost:3100/ready)\" = 200 ]"
wait_for 60 "prometheus ready" \
  "[ \"\$(curl -s -o /dev/null -w '%{http_code}' http://localhost:9090/-/ready)\" = 200 ]"
pass "clickhouse, loki and prometheus all answering"

echo "== 2. the store carries parent_span_id =="
[ "$(CH --query "EXISTS TABLE otel_traces")" = "1" ] \
  || fail "tracing.otel_traces is missing; did clickhouse finish its first-boot init.sql?"
HAS_PARENT=$(CH --query "SELECT count() FROM system.columns WHERE database='tracing' AND table='otel_traces' AND name='parent_span_id'")
[ "$HAS_PARENT" = "1" ] \
  || fail "parent_span_id is missing. init.sql runs on FIRST BOOT ONLY, so an older volume keeps the older table. Run: docker compose down -v && docker compose up -d --build"
pass "tracing.otel_traces exists and declares parent_span_id"

echo "== 3. arm the listing 9.2 error-issue index BEFORE traffic (the MV fires on insert) =="
CH --query "DROP VIEW IF EXISTS exc_mv" >/dev/null
CH --query "DROP TABLE IF EXISTS exceptions" >/dev/null
CH_FILE clickhouse/error_index.sql >/dev/null
pass "error_index.sql applied against an empty index"

echo "== 4. drive $CHECKOUTS checkouts plus $FORCED_ERRORS forced failures =="
# Baseline first. The store survives a re-run, so polling for an absolute count
# is satisfied instantly by the PREVIOUS run's rows and every assertion below
# then reads a half-arrived store. The target has to be relative to this run.
Q_ERRSPANS="SELECT count() FROM otel_traces WHERE status_code='STATUS_CODE_ERROR'"
ERR_BEFORE=$(CH_COUNT "$Q_ERRSPANS")
for _ in $(seq 1 "$CHECKOUTS"); do curl -s -o /dev/null http://localhost:8080/checkout || true; done
for _ in $(seq 1 "$FORCED_ERRORS"); do curl -s -o /dev/null "http://localhost:8080/checkout?fail=1" || true; done
# The sampler keeps every error trace, so the forced failures are guaranteed to
# land. The successes are sampled at 10%, which is the whole point of step 6.
ERR_TARGET=$((ERR_BEFORE + FORCED_ERRORS))
wait_for 180 "this run's $FORCED_ERRORS error spans to reach ClickHouse" 'ge "$Q_ERRSPANS" "$ERR_TARGET"'
SPANS=$(CH --query "SELECT count() FROM otel_traces")
ROOTS=$(CH --query "SELECT count() FROM otel_traces WHERE parent_span_id = ''")
CHILDREN=$(CH --query "SELECT count() FROM otel_traces WHERE parent_span_id != ''")
[ "$ROOTS" -gt 0 ] || fail "no root spans: every row has a parent, so parent_span_id is not being written"
[ "$CHILDREN" -gt 0 ] || fail "no child spans carry a parent_span_id, so the column is declared but never filled"
pass "$SPANS spans stored, $ROOTS roots and $CHILDREN children, so parent_span_id is populated both ways"

echo "== 5. record_exception detail reached the span attributes =="
# The SDK writes exception.* onto a span EVENT and the consumer stores span
# ATTRIBUTES. If the Collector's transform processor is not moving them across,
# these are empty and the index in step 6 has nothing to fingerprint.
ETYPE=$(CH --query "SELECT attributes['exception.type'] FROM otel_traces WHERE status_code='STATUS_CODE_ERROR' AND attributes['exception.type'] != '' LIMIT 1")
[ -n "$ETYPE" ] || fail "no error span carries attributes['exception.type']; the transform processor is not flattening the exception event"
STACK_LINES=$(CH --query "SELECT length(splitByChar('\n', attributes['exception.stacktrace'])) FROM otel_traces WHERE status_code='STATUS_CODE_ERROR' AND attributes['exception.stacktrace'] != '' LIMIT 1")
[ -n "$STACK_LINES" ] && [ "$STACK_LINES" -ge 3 ] \
  || fail "exception.stacktrace is ${STACK_LINES:-absent} lines; expected a real multi-line traceback"
FRAMES=$(CH --query "SELECT length(extractAll(attributes['exception.stacktrace'], 'File \"[^\"]*\", line [0-9]+, in [A-Za-z_0-9<>.]+')) FROM otel_traces WHERE status_code='STATUS_CODE_ERROR' AND attributes['exception.stacktrace'] != '' LIMIT 1")
[ -n "$FRAMES" ] && [ "$FRAMES" -ge 1 ] \
  || fail "the stacktrace parses to ${FRAMES:-0} frames; listing 9.2 fingerprints on the innermost one"
pass "exception.type=$ETYPE with a $STACK_LINES-line traceback carrying $FRAMES frames"

echo "== 6. listing 9.2: many raw error spans collapse to one issue =="
# The index was recreated empty in step 3, so it holds only this run's errors.
# Waiting for the busiest issue to reach the forced-error count is what stops the
# dedup assertion below from reading a partially-filled index.
Q_BUSIEST="SELECT max(c) FROM (SELECT sum(error_count) AS c FROM exceptions GROUP BY fingerprint)"
wait_for 90 "the error-issue index to fold this run's errors" 'ge "$Q_BUSIEST" "$FORCED_ERRORS"'
RAW_ERR=$(CH --query "SELECT count() FROM otel_traces WHERE status_code='STATUS_CODE_ERROR'")
FPS=$(CH --query "SELECT uniqExact(fingerprint) FROM exceptions")
TOPCOUNT=$(CH --query "SELECT sum(error_count) FROM exceptions GROUP BY fingerprint ORDER BY sum(error_count) DESC LIMIT 1")
TOPFRAME=$(CH --query "SELECT any(top_frame) FROM exceptions GROUP BY fingerprint ORDER BY sum(error_count) DESC LIMIT 1")
TEMPLATE=$(CH --query "SELECT any(msg_template) FROM exceptions GROUP BY fingerprint ORDER BY sum(error_count) DESC LIMIT 1")
[ -n "$TOPFRAME" ] \
  || fail "the busiest issue has an empty top_frame, so the fingerprint is hashing over nothing"
[ "$RAW_ERR" -gt "$FPS" ] \
  || fail "$RAW_ERR raw error spans produced $FPS fingerprints; nothing was deduplicated"
[ "$TOPCOUNT" -ge 2 ] \
  || fail "the busiest issue folds only $TOPCOUNT span; the normalization regex is not collapsing the varying message"
echo "   template: $TEMPLATE"
echo "   frame:    $TOPFRAME"
pass "$RAW_ERR raw error spans collapsed to $FPS issue(s); the busiest folds $TOPCOUNT"

echo "== 7. section 9.2.4: the post error rate reads higher than the pre error rate =="
Q_PRE="sum(pre_calls_total{service_name=\"$SVC\",span_name=\"$SPAN\"})"
Q_POST="sum(post_calls_total{service_name=\"$SVC\",span_name=\"$SPAN\"})"
Q_PRE_ERR="sum(pre_calls_total{service_name=\"$SVC\",span_name=\"$SPAN\",status_code=\"STATUS_CODE_ERROR\"})"
Q_POST_ERR="sum(post_calls_total{service_name=\"$SVC\",span_name=\"$SPAN\",status_code=\"STATUS_CODE_ERROR\"})"
# All FOUR series, not just the two totals. The totals appear a scrape before
# the error series does, and a poll that stops at the totals reads post errors
# as zero and reports a divergence failure that is really a stopwatch failure.
wait_for 150 "all four spanmetrics series to reach Prometheus" \
  'gt0 "$Q_PRE" && gt0 "$Q_POST" && gt0 "$Q_PRE_ERR" && gt0 "$Q_POST_ERR"'
PRE=$(PROMQ "$Q_PRE")
POST=$(PROMQ "$Q_POST")
PRE_ERR=$(PROMQ "$Q_PRE_ERR")
POST_ERR=$(PROMQ "$Q_POST_ERR")
python3 - "$PRE" "$POST" "$PRE_ERR" "$POST_ERR" <<'PY'
import sys
pre, post, pre_e, post_e = (float(x) for x in sys.argv[1:5])
assert pre > post, f"expected pre total {pre:.0f} > post total {post:.0f}; the sampler is not dropping anything"
pre_rate, post_rate = pre_e / pre, post_e / post
assert post_rate > pre_rate, \
    f"expected post error rate {post_rate:.3%} > pre error rate {pre_rate:.3%}"
print(f"   pre  total={pre:.0f} errors={pre_e:.0f} rate={pre_rate:.3%}")
print(f"   post total={post:.0f} errors={post_e:.0f} rate={post_rate:.3%}  inflation x{post_rate/pre_rate:.1f}")
PY
pass "the survivors over-report errors, which is why insights are derived before the sampler"

echo "== 8. the service graph has edges, derived from the pre-sample stream =="
wait_for 90 "service graph edges" 'gt0 "sum(traces_service_graph_request_total)"'
EDGES=$(PROMQ "sum(traces_service_graph_request_total)")
pass "traces_service_graph_request_total sums to $EDGES"

echo "== 9. listing 9.5: two span counts that took different paths =="
UPJOBS=$(curl -s http://localhost:9090/api/v1/targets \
  | python3 -c "import sys,json;print(sum(1 for t in json.load(sys.stdin)['data']['activeTargets'] if t['health']=='up'))")
[ "$UPJOBS" -ge 4 ] || fail "only $UPJOBS Prometheus targets are up; expected 4"
PRODUCER_UP=$(curl -s http://localhost:9090/api/v1/targets \
  | python3 -c "import sys,json;print(next((t['health'] for t in json.load(sys.stdin)['data']['activeTargets'] if t['labels']['job']=='checkout-producer'), 'absent'))")
[ "$PRODUCER_UP" = "up" ] \
  || fail "the checkout-producer job is '$PRODUCER_UP'. Listing 9.5 needs the producer scraped DIRECTLY, not through the Collector"
wait_for 120 "both listing 9.5 recording rules to evaluate" 'gt0 "spans:expected:rate5m" && gt0 "spans:received:rate5m"'
EXPECTED=$(PROMQ "spans:expected:rate5m")
RECEIVED=$(PROMQ "spans:received:rate5m")
RULE_ERRS=$(curl -s http://localhost:9090/api/v1/rules \
  | python3 -c "import sys,json;print(sum(1 for g in json.load(sys.stdin)['data']['groups'] for r in g['rules'] if r.get('health')!='ok'))")
[ "$RULE_ERRS" = "0" ] || fail "$RULE_ERRS recording or alerting rules are unhealthy"
RULE_COUNT=$(curl -s http://localhost:9090/api/v1/rules \
  | python3 -c "import sys,json;print(sum(1 for g in json.load(sys.stdin)['data']['groups'] for r in g['rules'] if r['type']=='recording'))")
[ "$RULE_COUNT" -ge 7 ] \
  || fail "only $RULE_COUNT recording rules loaded; burn_rate.yml must carry all four windows or its alerts reference nothing"
echo "   expected=$EXPECTED spans/s (producer, scraped directly)   received=$RECEIVED spans/s (collector)"
pass "$RULE_COUNT recording rules healthy, both sides of listing 9.5 reporting"

echo "== 10. logs reached Loki carrying the trace context of their span =="
Q_LOGS="{service_name=\"checkout-service\"}"
wait_for 120 "log lines in Loki" '[ "$(LOKI_STREAMS "$Q_LOGS")" != 0 ]'
LOKI_TIDS=$(curl -s -G 'http://localhost:3100/loki/api/v1/query_range' \
  --data-urlencode "query=$Q_LOGS" \
  --data-urlencode "start=$(NS_AGO 900)" \
  --data-urlencode "end=$(NS_NOW)" \
  --data-urlencode 'limit=100' \
 | python3 -c "
import sys,json
d=json.load(sys.stdin)['data']['result']
print(len({s['stream'].get('trace_id') for s in d if s['stream'].get('trace_id')}))")
[ "$LOKI_TIDS" -ge 1 ] \
  || fail "Loki has log lines but none carry a trace_id; the OTel logging handler is not reading the active span context"
pass "$LOKI_TIDS distinct trace ids present on log lines in Loki"

echo "== 11. cleanup: put the store back the way a fresh stack starts =="
CH --query "DROP VIEW IF EXISTS exc_mv"
CH --query "DROP TABLE IF EXISTS exceptions"
echo "dropped the listing 9.2 view and index, so this suite re-runs from any state"

echo
echo "ALL TESTS PASSED"
