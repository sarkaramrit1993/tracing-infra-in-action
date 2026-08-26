#!/usr/bin/env bash
# Chapter 9, section 9.3: the three bridges between signals.
#
# A trace, a log line and a metric are three separate stores. What makes them one
# picture is that each carries an identifier the others can be looked up by. This
# script fires ONE request with a trace id it chose itself, then walks all three
# crossings and proves each one lands:
#
#   BRIDGE 1  trace -> log. The producer never writes a trace id into a log
#             message. The OTel logging handler reads it off the active span
#             context, and Loki stores it as structured metadata. So a log line
#             is findable by a trace id that only the caller knew.
#
#   BRIDGE 2  metric -> trace. A latency bucket is a number with no way back to
#             the request that produced it, unless the histogram carries
#             exemplars. This reads them out of Prometheus and checks the trace
#             they point at actually exists in the store.
#
#             It reads POST exemplars, not pre. Both connectors emit them, but a
#             pre-sampler exemplar is minted before the sampling decision, so
#             ninety-nine times in a hundred it points at a trace that was then
#             discarded and
#             the jump dead-ends. Only an exemplar minted after the decision is a
#             pointer to something that is still there.
#
#   BRIDGE 3  the pre-sample series exists at all. Everything section 9.2 claims
#             rests on pre_calls_total being a population count rather than a
#             count of survivors.
#
# The request is fired with ?fail=1 on purpose. The tail sampler keeps every
# trace carrying an error and only one in a hundred of the rest, so a success
# trace would be dropped ninety-nine runs in a hundred and this suite would be a
# lottery.
#
# Prereq: docker compose up -d --build, settled.
# Usage:  bash tests/test_correlation.sh
set -euo pipefail
cd "$(dirname "$0")/.."

CH() { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

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

NS_NOW() { python3 -c 'import time;print(int(time.time()*1e9))'; }
NS_AGO() { python3 -c "import time;print(int((time.time()-$1)*1e9))"; }

# Every poll below goes through a named helper with its query in a variable.
# A quoted LogQL selector or SQL string nested inside a polling snippet is where
# these scripts break: the quoting collapses, nothing ever succeeds, and the
# failure reads as a timeout on data that was present the whole time.
CH_COUNT() { CH --query "$1"; }
ge() { [ "$(CH_COUNT "$1")" -ge "$2" ]; }

LOKI_COUNT() {  # LOKI_COUNT <logql>
  curl -s -G 'http://localhost:3100/loki/api/v1/query_range' \
    --data-urlencode "query=$1" \
    --data-urlencode "start=$(NS_AGO 900)" \
    --data-urlencode "end=$(NS_NOW)" \
    --data-urlencode 'limit=100' \
  | python3 -c "import sys,json;print(sum(len(s['values']) for s in json.load(sys.stdin)['data']['result']))"
}

echo "== 0. the script picks the trace id, so nothing downstream can have guessed it =="
TRACE_ID=$(python3 -c 'import os;print(os.urandom(16).hex())')
SPAN_ID=$(python3 -c 'import os;print(os.urandom(8).hex())')
echo "   trace_id=$TRACE_ID"
curl -s -o /dev/null -H "traceparent: 00-$TRACE_ID-$SPAN_ID-01" \
  "http://localhost:8080/checkout?fail=1" \
  || fail "the /checkout request failed; is checkout-service up?"
pass "one checkout fired under a caller-chosen trace id"

echo "== 1. those spans reach the store, stitched to the caller's span =="
# The caller supplied the traceparent, so this trace has NO row with
# parent_span_id = ''. Its root is the span the script named, which lives
# nowhere. What must be true instead is stronger and is the actual claim: the
# service's own span points back at the caller's span id, which proves both that
# the incoming context was continued and that parent_span_id is being written.
Q_TRACE="SELECT count() FROM tracing.otel_traces WHERE trace_id='$TRACE_ID'"
Q_STITCH="SELECT count() FROM tracing.otel_traces WHERE trace_id='$TRACE_ID' AND parent_span_id='$SPAN_ID'"
Q_TRACE_ERR="SELECT count() FROM tracing.otel_traces WHERE trace_id='$TRACE_ID' AND status_code='STATUS_CODE_ERROR'"
wait_for 150 "the trace to arrive in ClickHouse" 'ge "$Q_TRACE" 1'
wait_for 60 "the span that continues the caller's span id" 'ge "$Q_STITCH" 1'
wait_for 60 "the error span the tail sampler kept the trace for" 'ge "$Q_TRACE_ERR" 1'
NSPANS=$(CH_COUNT "$Q_TRACE")
NERR=$(CH_COUNT "$Q_TRACE_ERR")
NSTITCH=$(CH_COUNT "$Q_STITCH")
pass "$NSPANS spans stored for this trace, $NSTITCH continuing the caller's span, $NERR in error"

echo "== 2. BRIDGE 1, trace -> log: Loki finds the line by trace id =="
# The stream selector has to name a real index label. trace_id is structured
# metadata, not a label, and {trace_id="..."} on its own returns zero rows with
# no error at all, which is the quietest way to conclude the bridge is broken
# when it is not.
LOGQL="{service_name=\"checkout-service\"} | trace_id=\"$TRACE_ID\""
wait_for 150 "a log line carrying this trace id" '[ "$(LOKI_COUNT "$LOGQL")" -ge 1 ]'
NLOG=$(LOKI_COUNT "$LOGQL")
CONTROL=$(LOKI_COUNT "{service_name=\"checkout-service\"} | trace_id=\"$(python3 -c 'import os;print(os.urandom(16).hex())')\"")
[ "$CONTROL" = "0" ] \
  || fail "a trace id that was never emitted matched $CONTROL lines, so the filter is not filtering"
pass "$NLOG log lines carry trace_id=$TRACE_ID, and an unused id matches none"

echo "== 3. BRIDGE 2, metric -> trace: an exemplar resolves in the store =="
EXEMPLAR_TIDS() {
  curl -s -G http://localhost:9090/api/v1/query_exemplars \
    --data-urlencode 'query=post_duration_milliseconds_bucket' \
    --data-urlencode "start=$(python3 -c 'import time;print(time.time()-900)')" \
    --data-urlencode "end=$(python3 -c 'import time;print(time.time())')" \
  | python3 -c "
import sys,json
r=json.load(sys.stdin).get('data',[])
t=sorted({e['labels'].get('trace_id') for s in r for e in s.get('exemplars',[]) if e['labels'].get('trace_id')})
print('\n'.join(t))"
}
wait_for 150 "exemplars to appear on the post histogram" '[ -n "$(EXEMPLAR_TIDS)" ]'
RESOLVED=0
CHECKED=0
for tid in $(EXEMPLAR_TIDS); do
  CHECKED=$((CHECKED + 1))
  n=$(CH --query "SELECT count() FROM tracing.otel_traces WHERE trace_id='$tid'")
  if [ "${n:-0}" -gt 0 ]; then
    RESOLVED=$((RESOLVED + 1))
    [ "$RESOLVED" = "1" ] && echo "   exemplar $tid -> $n spans in ClickHouse"
  fi
done
[ "$RESOLVED" -ge 1 ] \
  || fail "none of the $CHECKED exemplar trace ids resolve in ClickHouse; the jump from a latency bucket dead-ends"
pass "$RESOLVED of $CHECKED exemplar trace ids resolve to real spans"

echo "== 4. BRIDGE 3, the pre-sample series exists =="
PRE=$(curl -s --data-urlencode 'query=sum(pre_calls_total)' http://localhost:9090/api/v1/query \
  | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(r[0]['value'][1] if r else 0)")
python3 -c "import sys;sys.exit(0 if float('$PRE')>0 else 1)" \
  || fail "pre_calls_total is empty. The namespace: pre setting on the spanmetrics connector is what puts it under this name; without it the series is traces_span_metrics_calls_total and every rule referencing pre_calls_total silently evaluates to nothing"
POST=$(curl -s --data-urlencode 'query=sum(post_calls_total)' http://localhost:9090/api/v1/query \
  | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(r[0]['value'][1] if r else 0)")
python3 -c "import sys;sys.exit(0 if float('$PRE')>float('$POST') else 1)" \
  || fail "pre_calls_total ($PRE) is not above post_calls_total ($POST); the pre series is not counting the full population"
pass "pre_calls_total=$PRE above post_calls_total=$POST, so pre is the population count"

echo
echo "ALL CORRELATION BRIDGES HOLD"
