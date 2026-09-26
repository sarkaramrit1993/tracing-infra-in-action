#!/usr/bin/env bash
# Chapter 9, section 9.3: the three bridges between signals.
#
# A trace, a log line and a metric are three separate stores. What makes them one
# picture is that each carries an identifier the others can be looked up by. This
# script fires a request with a trace id it chose itself and follows that id
# across bridges 1 and 2. Alongside it, it sends a hundred ordinary requests,
# which bridges 2 and 3 need as traffic the sampler drops. Each crossing has to
# land:
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
#   BRIDGE 3  the pre-sample series counts the population. Everything section
#             9.2 claims rests on pre_calls_total being a population count
#             rather than a count of survivors, so this step sends ordinary
#             traffic the sampler drops, waits for post to settle, and checks
#             post rose, but by less than half as much as pre.
#
# The traced request is fired with ?fail=1 on purpose. The tail sampler keeps every
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

# Bridges 2 and 3 both need traffic the sampler drops: 3b needs pre exemplars
# that dangle, bridge 3 needs pre to move further than post. So the script
# sends a hundred ordinary requests of its own here rather than depending on
# what ran before it. Each series is read as a delta across these requests, so
# the assertions hold whatever the store already held.
#
# The delta is taken per collector_instance_id. Every Collector restart starts
# a new set of series under a new id, and Prometheus keeps answering with the
# old set until the new process has exported its first span. A plain before
# and after on sum() would subtract the old process's total from the new one's.
PROM_SNAP() {  # PROM_SNAP <metric>: one "instance value" line per Collector process
  curl -s --data-urlencode "query=sum by (collector_instance_id) ($1)" http://localhost:9090/api/v1/query \
  | python3 -c "
import sys,json
for r in json.load(sys.stdin)['data']['result']:
    print(r['metric'].get('collector_instance_id', '-'), int(float(r['value'][1])))"
}
DELTA() {  # DELTA <before> <after>: growth summed over processes present after
  python3 -c "
import sys
parse = lambda s: dict((k, int(v)) for k, v in (l.split() for l in s.splitlines() if l.strip()))
b, a = parse(sys.argv[1]), parse(sys.argv[2])
print(sum(v - b.get(k, 0) for k, v in a.items()))" "$1" "$2"
}
PRE0=$(PROM_SNAP pre_calls_total)
POST0=$(PROM_SNAP post_calls_total)
PLAIN=100
for _ in $(seq 1 "$PLAIN"); do
  curl -s -o /dev/null http://localhost:8080/checkout \
    || fail "an ordinary /checkout request failed; is checkout-service up?"
done

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

# 3b. The post side resolving is only half the claim. Section 9.3.1 says the
# side matters, and a check that reads one side cannot tell a working bridge
# from a bridge that would work either way. The pre-sampler pointer is minted
# before the sampler has decided anything, so most of them dangle. Direction
# is the claim, so direction is what this asserts; the ratio is a draw and is
# recorded by benchmarks/exemplar_resolution.py rather than asserted here.
PRE_TIDS() {
  curl -s -G http://localhost:9090/api/v1/query_exemplars \
    --data-urlencode 'query=pre_duration_milliseconds_bucket' \
    --data-urlencode "start=$(python3 -c 'import time;print(time.time()-900)')" \
    --data-urlencode "end=$(python3 -c 'import time;print(time.time())')" \
  | python3 -c "
import sys,json
r=json.load(sys.stdin).get('data',[])
t=sorted({e['labels'].get('trace_id') for s in r for e in s.get('exemplars',[]) if e['labels'].get('trace_id')})
print('\n'.join(t))"
}
# Pre's exemplars ride the same flush and scrape as its counts, so once pre has
# counted all of step 0's requests their exemplars are readable too. Without
# this wait, an idle stack gives 3b only exemplars from traces that were kept.
i=0
until [ "$(DELTA "$PRE0" "$(PROM_SNAP pre_calls_total)")" -ge $((PLAIN * 7)) ]; do
  i=$((i + 1))
  [ "$i" -le 150 ] || fail "pre_calls_total never counted the $PLAIN requests at seven spans each. The namespace: pre setting on the spanmetrics connector is what puts it under this name; without it the series is traces_span_metrics_calls_total and every rule referencing pre_calls_total silently evaluates to nothing"
  sleep 1
done
echo "   satisfied after ${i}s: pre_calls_total counted all $PLAIN requests"
wait_for 150 "exemplars to appear on the pre histogram" '[ -n "$(PRE_TIDS)" ]'
PRE_RESOLVED=0
PRE_CHECKED=0
for tid in $(PRE_TIDS); do
  PRE_CHECKED=$((PRE_CHECKED + 1))
  n=$(CH --query "SELECT count() FROM tracing.otel_traces WHERE trace_id='$tid'")
  [ "${n:-0}" -gt 0 ] && PRE_RESOLVED=$((PRE_RESOLVED + 1))
done
PRE_PCT=$((PRE_RESOLVED * 100 / PRE_CHECKED))
POST_PCT=$((RESOLVED * 100 / CHECKED))
[ "$POST_PCT" -gt "$PRE_PCT" ] \
  || fail "the post side resolves $POST_PCT% against the pre side's $PRE_PCT%; an exemplar minted behind the sampler is supposed to be the one that still lands"
pass "post resolves $RESOLVED/$CHECKED ($POST_PCT%) against pre $PRE_RESOLVED/$PRE_CHECKED ($PRE_PCT%), which is why bridge 2 reads the post connector"

echo "== 4. BRIDGE 3, the pre-sample series counts the whole population =="
# Step 0 sent the requests and step 3b saw pre count them all. Post trails pre
# by the sampler's 5 s decision_wait and the connector's 15 s flush. Wait those
# out, then read every 20 s, one scrape interval plus margin, until two reads
# agree. Reading post the moment pre is complete would compare
# the whole population against survivors not yet counted, and pass even if the
# sampler kept everything.
sleep 25
POST_D=$(DELTA "$POST0" "$(PROM_SNAP post_calls_total)")
j=0
while :; do
  sleep 20
  NEXT=$(DELTA "$POST0" "$(PROM_SNAP post_calls_total)")
  [ "$NEXT" = "$POST_D" ] && break
  POST_D=$NEXT
  j=$((j + 1))
  [ "$j" -le 8 ] || fail "post_calls_total never settled after the $PLAIN requests; last delta $POST_D"
done
echo "   settled: post_calls_total rose by $POST_D on two reads 20 s apart"
PRE_D=$(DELTA "$PRE0" "$(PROM_SNAP pre_calls_total)")
# keep-errors plus probabilistic-1-percent: every 100th checkout fails, so post keeps ~2-3 of these ~100 traces (never 0); half of pre means a sampler keeping everything.
[ "$POST_D" -gt 0 ] \
  || fail "post_calls_total did not move over $PLAIN requests that include an error trace; keep-errors kept nothing"
[ $((POST_D * 2)) -lt "$PRE_D" ] \
  || fail "pre_calls_total rose by $PRE_D and post_calls_total by $POST_D over $PLAIN ordinary requests; post should count 2 or 3 traces in 100, so the sampler is keeping far more than its policy says"
pass "over $PLAIN ordinary requests pre_calls_total rose by $PRE_D and post_calls_total by $POST_D, so pre is the population count and post the sample"

echo
echo "ALL CORRELATION BRIDGES HOLD"
