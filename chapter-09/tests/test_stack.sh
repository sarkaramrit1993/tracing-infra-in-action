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
#      one success in a hundred, so the post error rate reads inflated against
#      the pre rate, which is the truth.
#   6. The service graph has edges, derived from the pre-sample stream.
#   7. The ingest-gap rule's two counters exist and were reached by two
#      different paths.
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
CH() { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
CH_FILE() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }

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

# PROMQ answers 0 both for a series reading zero and for a selector that matches
# nothing, and those are opposite facts: the first is a healthy counter before
# anything happened, the second is a rule wired to a name that does not exist.
# SERIES counts matching series instead, so the two can be told apart.
SERIES() {
  curl -s -G http://localhost:9090/api/v1/series \
    --data-urlencode "match[]=$1" \
    --data-urlencode "start=$(( $(date +%s) - 900 ))" \
    --data-urlencode "end=$(date +%s)" \
  | python3 -c "import sys,json;print(len(json.load(sys.stdin).get('data',[])))"
}
has_series() { [ "$(SERIES "$1")" -gt 0 ]; }

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
[ "$(CH --query "EXISTS TABLE tracing.otel_traces")" = "1" ] \
  || fail "tracing.otel_traces is missing; did clickhouse finish its first-boot init.sql?"
HAS_PARENT=$(CH --query "SELECT count() FROM system.columns WHERE database='tracing' AND table='otel_traces' AND name='parent_span_id'")
[ "$HAS_PARENT" = "1" ] \
  || fail "parent_span_id is missing. init.sql runs on FIRST BOOT ONLY, so an older volume keeps the older table. Run: docker compose down -v && docker compose up -d --build"
pass "tracing.otel_traces exists and declares parent_span_id"

echo "== 3. arm the listing 9.2 error-issue index BEFORE traffic (the MV fires on insert) =="
CH --query "DROP VIEW IF EXISTS tracing.exc_mv" >/dev/null
CH --query "DROP TABLE IF EXISTS tracing.exceptions" >/dev/null
CH_FILE clickhouse/error_index.sql >/dev/null
pass "error_index.sql applied against an empty index"

echo "== 4. drive $CHECKOUTS checkouts plus $FORCED_ERRORS forced failures =="
# Baseline first. The store survives a re-run, so polling for an absolute count
# is satisfied instantly by the PREVIOUS run's rows and every assertion below
# then reads a half-arrived store. The target has to be relative to this run.
Q_ERRSPANS="SELECT count() FROM tracing.otel_traces WHERE status_code='STATUS_CODE_ERROR'"
ERR_BEFORE=$(CH_COUNT "$Q_ERRSPANS")
for _ in $(seq 1 "$CHECKOUTS"); do curl -s -o /dev/null http://localhost:8080/checkout || true; done
for _ in $(seq 1 "$FORCED_ERRORS"); do curl -s -o /dev/null "http://localhost:8080/checkout?fail=1" || true; done
# The sampler keeps every error trace, so the forced failures are guaranteed to
# land. The successes are sampled at one in a hundred, which is the whole point
# of step 7. Each failed checkout stores TWO error spans, fraud.score and the
# server span it propagated to, so the target below is the conservative one.
ERR_TARGET=$((ERR_BEFORE + FORCED_ERRORS))
wait_for 180 "this run's $FORCED_ERRORS error spans to reach ClickHouse" 'ge "$Q_ERRSPANS" "$ERR_TARGET"'
SPANS=$(CH --query "SELECT count() FROM tracing.otel_traces")
ROOTS=$(CH --query "SELECT count() FROM tracing.otel_traces WHERE parent_span_id = ''")
CHILDREN=$(CH --query "SELECT count() FROM tracing.otel_traces WHERE parent_span_id != ''")
[ "$ROOTS" -gt 0 ] || fail "no root spans: every row has a parent, so parent_span_id is not being written"
[ "$CHILDREN" -gt 0 ] || fail "no child spans carry a parent_span_id, so the column is declared but never filled"
pass "$SPANS spans stored, $ROOTS roots and $CHILDREN children, so parent_span_id is populated both ways"

echo "== 5. record_exception detail reached the span attributes =="
# The SDK writes exception.* onto a span EVENT and the consumer stores span
# ATTRIBUTES. If the Collector's transform processor is not moving them across,
# these are empty and the index in step 6 has nothing to fingerprint.
ETYPE=$(CH --query "SELECT attributes['exception.type'] FROM tracing.otel_traces WHERE status_code='STATUS_CODE_ERROR' AND attributes['exception.type'] != '' LIMIT 1")
[ -n "$ETYPE" ] || fail "no error span carries attributes['exception.type']; the transform processor is not flattening the exception event"
STACK_LINES=$(CH --query "SELECT length(splitByChar('\n', attributes['exception.stacktrace'])) FROM tracing.otel_traces WHERE status_code='STATUS_CODE_ERROR' AND attributes['exception.stacktrace'] != '' LIMIT 1")
[ -n "$STACK_LINES" ] && [ "$STACK_LINES" -ge 3 ] \
  || fail "exception.stacktrace is ${STACK_LINES:-absent} lines; expected a real multi-line traceback"
FRAMES=$(CH --query "SELECT length(extractAll(attributes['exception.stacktrace'], 'File \"[^\"]*\", line [0-9]+, in [A-Za-z_0-9<>.]+')) FROM tracing.otel_traces WHERE status_code='STATUS_CODE_ERROR' AND attributes['exception.stacktrace'] != '' LIMIT 1")
[ -n "$FRAMES" ] && [ "$FRAMES" -ge 1 ] \
  || fail "the stacktrace parses to ${FRAMES:-0} frames; listing 9.2 fingerprints on the innermost one"
pass "exception.type=$ETYPE with a $STACK_LINES-line traceback carrying $FRAMES frames"

echo "== 6. listing 9.2: many raw error spans collapse to one issue =="
# The index was recreated empty in step 3, so it holds only THIS run's errors.
# The span table does not: it survives a re-run. Counting every error span ever
# stored against an index that starts empty compares two different populations,
# and the comparison passes on volume alone -- on the second run the table holds
# twice the errors, the index holds one run's, and "N spans collapsed to 1 issue"
# is printed with an N that was never folded. Baseline it the way step 4 does.
Q_BUSIEST="SELECT max(c) FROM (SELECT sum(error_count) AS c FROM tracing.exceptions GROUP BY fingerprint)"
Q_FOLDED="SELECT sum(error_count) FROM tracing.exceptions"
wait_for 120 "the error-issue index to hold every error span this run stored" \
  'ge "$Q_BUSIEST" "$FORCED_ERRORS" && [ "$(CH_COUNT "$Q_FOLDED")" = "$(( $(CH_COUNT "$Q_ERRSPANS") - ERR_BEFORE ))" ]'
RAW_ERR=$(( $(CH --query "$Q_ERRSPANS") - ERR_BEFORE ))
FPS=$(CH --query "SELECT uniqExact(fingerprint) FROM tracing.exceptions")
FOLDED=$(CH --query "$Q_FOLDED")
[ "$FOLDED" = "$RAW_ERR" ] \
  || fail "the index folded $FOLDED spans but this run stored $RAW_ERR error spans; the two are not the same population, so the collapse below would be arithmetic across two different sets"
TOPCOUNT=$(CH --query "SELECT sum(error_count) FROM tracing.exceptions GROUP BY fingerprint ORDER BY sum(error_count) DESC LIMIT 1")
TOPFRAME=$(CH --query "SELECT any(top_frame) FROM tracing.exceptions GROUP BY fingerprint ORDER BY sum(error_count) DESC LIMIT 1")
TEMPLATE=$(CH --query "SELECT any(msg_template) FROM tracing.exceptions GROUP BY fingerprint ORDER BY sum(error_count) DESC LIMIT 1")
[ -n "$TOPFRAME" ] \
  || fail "the busiest issue has an empty top_frame, so the fingerprint is hashing over nothing"
[ "$RAW_ERR" -gt "$FPS" ] \
  || fail "$RAW_ERR raw error spans produced $FPS fingerprints; nothing was deduplicated"
[ "$TOPCOUNT" -ge 2 ] \
  || fail "the busiest issue folds only $TOPCOUNT span; the normalization regex is not collapsing the varying message"
echo "   template: $TEMPLATE"
echo "   frame:    $TOPFRAME"
pass "this run's $RAW_ERR raw error spans collapsed to $FPS issue(s); the busiest folds $TOPCOUNT"

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

echo "== 9. the ingest gap: two span counts that took different paths =="
UPJOBS=$(curl -s http://localhost:9090/api/v1/targets \
  | python3 -c "import sys,json;print(sum(1 for t in json.load(sys.stdin)['data']['activeTargets'] if t['health']=='up'))")
[ "$UPJOBS" -ge 4 ] || fail "only $UPJOBS Prometheus targets are up; expected 4"
PRODUCER_UP=$(curl -s http://localhost:9090/api/v1/targets \
  | python3 -c "import sys,json;print(next((t['health'] for t in json.load(sys.stdin)['data']['activeTargets'] if t['labels']['job']=='checkout-producer'), 'absent'))")
[ "$PRODUCER_UP" = "up" ] \
  || fail "the checkout-producer job is '$PRODUCER_UP'. The ingest-gap rule needs the producer scraped DIRECTLY, not through the Collector"
wait_for 120 "both ingest-gap recording rules to evaluate" 'gt0 "spans:expected:rate5m" && gt0 "spans:received:rate5m"'
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
pass "$RULE_COUNT recording rules healthy, both sides of the ingest gap reporting"

echo "== 9b. every recording rule reads from a selector that matches something =="
# A rule whose selector matches nothing evaluates to 0, reports health ok, and
# counts toward RULE_COUNT above. It is indistinguishable from a rule reporting
# good news. Checking the rules' INPUTS is what separates them: a rule may
# legitimately compute zero, but it may never legitimately read from a name that
# does not exist. This is how the connector-namespace trap gets caught, where a
# rule written against traces_span_metrics_calls_total matches nothing here
# because the connectors are namespaced pre and post.
DEAD=$(curl -s http://localhost:9090/api/v1/rules \
  | python3 - "$(date +%s)" <<'PYEOF'
import json, re, sys, urllib.parse, urllib.request
now = int(sys.argv[1])
groups = json.load(sys.stdin)["data"]["groups"]
# Metric names this stack records; anything a rule reads that is not one of
# these and not a recorded name has to exist as a raw series.
recorded = {r["name"] for g in groups for r in g["rules"] if r["type"] == "recording"}
selectors = set()
for g in groups:
    for r in g["rules"]:
        if r["type"] != "recording":
            continue
        for m in re.finditer(r'\b([a-zA-Z_][a-zA-Z0-9_]*)(\{[^}]*\})?', r["query"]):
            name, labels = m.group(1), m.group(2) or ""
            if name in recorded or name in {
                "sum", "rate", "clamp_min", "clamp_max", "vector", "or", "and",
                "unless", "by", "without", "min_over_time", "max_over_time",
                "avg_over_time", "increase", "offset", "on", "ignoring", "le",
            }:
                continue
            selectors.add(name + labels)
dead = []
for sel in sorted(selectors):
    q = urllib.parse.urlencode(
        {"match[]": sel, "start": now - 900, "end": now})
    with urllib.request.urlopen(
            "http://localhost:9090/api/v1/series?" + q, timeout=10) as fh:
        if not json.load(fh).get("data"):
            dead.append(sel)
print(" ".join(dead))
PYEOF
)
[ -z "$DEAD" ] \
  || fail "recording rules read from selectors that match nothing: $DEAD"
pass "every recording-rule selector matches at least one live series"

echo "== 9c. the burn-rate ratio is per request, not per span =="
# spanmetrics counts spans, and one checkout produces seven of them, so a ratio
# over every span divides the failures by seven times the request count. The
# span_kind selector is what makes it a request error rate again. Without it a
# 1.44 percent threshold does not fire until real requests fail at about ten
# percent, under a label promising 99.9 percent availability.
has_series 'pre_calls_total{service_name="checkout-service",span_kind="SPAN_KIND_SERVER"}' \
  || fail "no pre_calls_total series carries span_kind=SPAN_KIND_SERVER; the burn-rate selector matches nothing and every window records a constant 0"
wait_for 180 "the burn-rate ratio to carry the driven errors" 'gt0 "slo:checkout_errors:ratio_rate5m"'
RATIO=$(PROMQ "slo:checkout_errors:ratio_rate5m")
SPAN_GRAIN=$(PROMQ 'sum(rate(pre_calls_total{status_code="STATUS_CODE_ERROR"}[5m])) / sum(rate(pre_calls_total[5m]))')
awk -v r="$RATIO" 'BEGIN { exit !(r > 0.015 && r < 0.06) }' \
  || fail "burn-rate ratio is $RATIO, outside the request-grain band; the same errors at span grain read about $SPAN_GRAIN, so a ratio down there means the span_kind selector stopped matching"
pass "burn-rate ratio $RATIO is request grain (span grain reads $SPAN_GRAIN)"

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
CH --query "DROP VIEW IF EXISTS tracing.exc_mv"
CH --query "DROP TABLE IF EXISTS tracing.exceptions"
echo "dropped the listing 9.2 view and index, so this suite re-runs from any state"

echo
echo "ALL TESTS PASSED"
