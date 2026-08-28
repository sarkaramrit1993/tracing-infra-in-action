# Correlation: one trace id, three signals, three silent ways the join breaks

Run this from `chapter-09/`. It does not depend on the other two exercises, and
every edit below restores the file it touched, so the directory ends where it
started.

## The question

Section 9.3.1 says there are exactly three bridges between signals and all three
ride the same key, the trace id in the W3C `traceparent` header. Section 9.3.2
takes each in turn with the mechanism that makes it work and the way it silently
breaks.

Silently is the word to hold onto. None of the three failures below produces an
error, a warning, or a non-2xx response anywhere the person running the query can
see. Each one produces an empty result with `"status": "success"`, which is what
a correct query over a system with no matching data also produces. There is no
way to tell those two apart from the outside, which is why the fix in every case
is to make the bridge testable rather than to watch it.

## The starting state

Three helpers, one per store:

```bash
ch()    { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
promq() { curl -s -G http://localhost:9090/api/v1/query --data-urlencode "query=$1" \
            | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(r[0]['value'][1] if r else 'no data')"; }
loki()  { curl -s -G http://localhost:3100/loki/api/v1/query_range --data-urlencode "query=$1" \
            --data-urlencode "start=$(python3 -c 'import time;print(int((time.time()-900)*1e9))')" \
            --data-urlencode "end=$(python3 -c 'import time;print(int(time.time()*1e9))')" \
            --data-urlencode 'limit=20' \
            | python3 -c "
import sys,json
d = json.load(sys.stdin)
print('status:', d['status'], ' lines:', sum(len(s['values']) for s in d['data']['result']))
for s in d['data']['result']:
    for _, line in s['values']: print('   ', line)"; }
```

Three more that poll instead of sleeping. Nothing in this file waits a fixed
number of seconds: a trace has to clear the tail sampler's `decision_wait`, then
Kafka, then the storage consumer's batch, and an exemplar has to clear a
15-second connector flush and a 15-second scrape on top of that. A sleep tuned
to one machine is a guess on any other, and every silent failure in this file
looks exactly like a wait that was too short.

```bash
await()      { for _ in $(seq 1 180); do
                 awk -v v="$(promq "$1")" -v t="$2" 'BEGIN { exit !(v + 0 >= t) }' && return 0
                 sleep 2
               done
               echo "timed out: $1 never reached $2" >&2; return 1; }
await_rows() { for _ in $(seq 1 180); do
                 [ "$(ch --query "$1" 2>/dev/null)" -ge "$2" ] 2>/dev/null && return 0
                 sleep 2
               done
               echo "timed out: $1 never reached $2" >&2; return 1; }
await_collector() { for _ in $(seq 1 90); do
                      curl -sf -o /dev/null http://localhost:8888/metrics && return 0
                      sleep 1
                    done
                    echo "the collector did not come back" >&2; return 1; }
```

The `< /dev/null` on `ch` is not decoration. Without it the client waits on a
stdin that never reaches EOF. NOTES has the detail.

```bash
docker compose up -d --build
docker compose ps
```

Wait for the health column to settle, then give the exemplar buffer something to
hold. Exemplars ride histogram buckets, and a bucket with no observations has
nothing to attach one to:

```bash
for _ in $(seq 1 400); do curl -s -o /dev/null http://localhost:8080/checkout; done
for _ in $(seq 1 10); do curl -s -o /dev/null "http://localhost:8080/checkout?fail=1"; done
await 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})' 14
```

The ten forced failures are there because the sampler keeps one successful trace
in a hundred. Four hundred ordinary requests leave about four survivors between
them, which is not enough post-sampler traces for the exemplar buffer to be
worth reading. The forced ones are kept unconditionally, so they are what puts
pointers on the histogram.

## One request, chosen from outside

The trick that makes this exercise falsifiable is picking the trace id yourself
and handing it in on the wire. Nothing downstream guessed it, nothing generated
it, and no query below can accidentally find it by matching something else:

```bash
TID=$(python3 -c 'import os;print(os.urandom(16).hex())')
SID=$(python3 -c 'import os;print(os.urandom(8).hex())')
echo "$TID"
curl -s -o /dev/null -H "traceparent: 00-$TID-$SID-01" "http://localhost:8080/checkout?fail=1"
await_rows "SELECT count() FROM tracing.otel_traces WHERE trace_id = '$TID'" 7
```

```
3f90e0804176e99a4753224139b33ac0
```

The poll is on ClickHouse and it covers Loki too. A log record leaves the
producer on a two-second batch delay and reaches Loki in one hop; a span waits
five seconds for the exporter's batch, then the sampler's `decision_wait`, then
Kafka and the storage consumer. By the time the seventh span is in the store the
log line has been in Loki for a while.

`?fail=1` is deliberate. The tail sampler keeps every trace carrying an error and
one in a hundred of the rest, so a successful trace would be gone ninety-nine runs
in a hundred and this whole exercise would be a lottery.

The trace itself, so there is something for the bridges to land on:

```bash
ch --query "
SELECT rpad(span_name, 18) AS span, status_code,
       concat(toString(round(duration_ns/1e6,1)), 'ms') AS took
FROM tracing.otel_traces WHERE trace_id = '$TID' ORDER BY timestamp"
```

```
GET /checkout       STATUS_CODE_ERROR  181.2ms
validate_cart       STATUS_CODE_UNSET  21.1ms
inventory.reserve   STATUS_CODE_UNSET  32.6ms
payment.charge      STATUS_CODE_UNSET  93.9ms
fraud.score         STATUS_CODE_ERROR  41.4ms
order.create        STATUS_CODE_UNSET  20.5ms
notification.send   STATUS_CODE_UNSET  11.1ms
```

## Bridge 1, trace to log

The application never writes the trace id into the log message. It calls
`log.error(...)` and the OpenTelemetry logging handler reads the id off the
active span context, so the line is findable by an id only the caller knew.

Try it the way that looks obvious first:

```bash
loki "{trace_id=\"$TID\"}"
```

```
status: success  lines: 0
```

`success`, zero lines, no error and no warning. This is the first silent failure
and the most common one, because the selector is not wrong in any way LogQL can
detect. Loki stores the OTLP `TraceId` as **structured metadata**, not as a
stream label, and a stream selector naming something that is not a label matches
no streams. Matching no streams is not an error condition.

The selector has to lead with a real label and filter on the metadata afterwards:

```bash
loki "{service_name=\"checkout-service\"} | trace_id=\"$TID\""
```

```
status: success  lines: 2
    fraud scoring failed for cart-4780: fraud scoring backend timed out after 30873ms (req 559e8201)
    checkout complete cart=cart-4780 order=ord-87481 amount=292.40 fraud_failed=True
```

Two lines, from a request that finished moments ago, retrieved by an id chosen
before it existed. Promoting `trace_id` to a real label would make the first
selector work and would also be the one thing section 9.3.3 says never to do: a
label per trace id is one stream per trace, and the cardinality bill multiplies
instead of adding.

## Bridge 2, metric to trace

A latency bucket is a number with no way back to the request that produced it,
unless the histogram carries exemplars. Read them off the post-sampler histogram:

```bash
exemplars() {
  curl -s -G http://localhost:9090/api/v1/query_exemplars \
    --data-urlencode "query=$1" \
    --data-urlencode "start=$(python3 -c 'import time;print(time.time()-900)')" \
    --data-urlencode "end=$(python3 -c 'import time;print(time.time())')" \
  | python3 -c "
import sys,json
seen = set()
for s in json.load(sys.stdin).get('data', []):
    for e in s.get('exemplars', []):
        t = e['labels'].get('trace_id')
        if t and t not in seen:
            seen.add(t); print(t)"
}
for tid in $(exemplars 'post_duration_milliseconds_bucket'); do
  echo "$tid -> $(ch --query "SELECT count() FROM tracing.otel_traces WHERE trace_id='$tid'") spans"
done
```

```
4ce2583888e94371d6043a784f63ddf5 -> 7 spans
72cf6c74283185b5d2de295c9fbb32b1 -> 7 spans
abeab00dfc096a0a058884ec132525a6 -> 7 spans
f50f67efe947002b39af604a9d7b212b -> 7 spans
597e80a81c22a6a9ab79614f2002b654 -> 7 spans
18c9e14609f623d88dff5b85508cc609 -> 7 spans
03cea38c5acad73b1141b512f94deebc -> 7 spans
0f4423616521a1c3fccba52b2a68742a -> 7 spans
566ac9c84aaaf80de3431eb2f45c6b56 -> 7 spans
7e82f0ab0b233fd2418b5ed812492ccd -> 7 spans
479714f979ca5ed6e83118c3213f2757 -> 7 spans
2f2d1c7b41f412056e323fe9d3b3950f -> 7 spans
6fb208fd809c242e0b4594e5dc0ede74 -> 7 spans
2cfff298383555fad84c662ea94530ca -> 7 spans
f7d60b2722f37559dbb02a41a03599e4 -> 7 spans
a97e05ba800424eaef44f488ac82f9e3 -> 7 spans
8e5cbe096f230ace9fea23343274fad1 -> 7 spans
```

Seventeen exemplars, seventeen whole traces, including the one this exercise
picked. Now run exactly the same loop against the pre-sampler histogram:

```bash
for tid in $(exemplars 'pre_duration_milliseconds_bucket'); do
  echo "$tid -> $(ch --query "SELECT count() FROM tracing.otel_traces WHERE trace_id='$tid'") spans"
done
```

```
4ce2583888e94371d6043a784f63ddf5 -> 7 spans
72cf6c74283185b5d2de295c9fbb32b1 -> 7 spans
ae282b8d98020e0ca35faae9af5ef844 -> 7 spans
...
2357781d34e090348f6d41fb04a2a013 -> 0 spans
03ad1fe35c42fe6602199aece896e0ab -> 0 spans
2214d41e2b09a6a69ae27742b7984154 -> 0 spans
...
```

Thirty-five exemplars on the run that produced this file, abridged here because
the interesting part is the tally rather than the ids: ten resolved and
twenty-five pointed at nothing. That is the second silent failure. A pre-sampler exemplar is minted before the sampler has
decided anything, and the sampler then throws away ninety-nine successful traces
in every hundred. The pointer is still a valid trace id. `query_exemplars`
returns it without complaint, the drill-down runs, and the trace viewer says the
trace does not exist.

Which is the reason `tests/test_correlation.sh` reads `post` and not `pre`. It is
also the reason to keep bridge 1: logs and traces share the key without sharing
the sampling decision, so the log join still works on exactly the traces whose
exemplar dangles.

## Bridge 3, trace to metric

The third bridge is the one section 9.1 built and section 9.2.4 measured, and it
is the reason the other two are worth having at this grain:

```bash
promq 'sum(pre_calls_total{service_name="checkout-service"})'
promq 'sum(post_calls_total{service_name="checkout-service"})'
```

```
6111
266
```

The pre series is a population count, derived from every span before anything was
dropped. The post series describes the sample. Everything section 9.2 claims
rests on reading the first and not the second.

## Try this

Two edits, each changing one variable, each backing up the file it touches and
restoring it in the same section.

**Strip the trace id off the log record in transit.** This is the failure section
9.3.2 names for bridge 1, a shipper that drops the field, and one OTTL statement
stands in for the shipper:

```bash
cp collector/gateway-config.yaml collector/gateway-config.yaml.bak
python3 - <<'PY'
from pathlib import Path
p = Path("collector/gateway-config.yaml")
t = p.read_text()
t = t.replace("""  batch:
    send_batch_size: 8192""",
"""  transform/strip_trace_id:
    error_mode: ignore
    log_statements:
      - context: log
        statements:
          - set(log.trace_id.string, "00000000000000000000000000000000")

  batch:
    send_batch_size: 8192""", 1)
t = t.replace("""      processors: [memory_limiter, batch]
      exporters: [otlphttp/loki]""",
"""      processors: [memory_limiter, transform/strip_trace_id, batch]
      exporters: [otlphttp/loki]""", 1)
p.write_text(t)
PY
docker compose restart otel-collector
await_collector
TID=$(python3 -c 'import os;print(os.urandom(16).hex())')
SID=$(python3 -c 'import os;print(os.urandom(8).hex())')
curl -s -o /dev/null -H "traceparent: 00-$TID-$SID-01" "http://localhost:8080/checkout?fail=1"
await_rows "SELECT count() FROM tracing.otel_traces WHERE trace_id = '$TID'" 7
loki "{service_name=\"checkout-service\"} | trace_id=\"$TID\""
loki '{service_name="checkout-service"}'
```

```
status: success  lines: 0
status: success  lines: 20
    fraud scoring failed for cart-4334: fraud scoring backend timed out after 30192ms (req b959942b)
    fraud scoring failed for cart-7054: fraud scoring backend timed out after 30186ms (req fab8ecf6)
    fraud scoring failed for cart-3310: fraud scoring backend timed out after 30187ms (req c475baf8)
    fraud scoring failed for cart-5974: fraud scoring backend timed out after 30188ms (req 9c1b02c7)
    fraud scoring failed for cart-5045: fraud scoring backend timed out after 30189ms (req 4fcf8636)
    fraud scoring failed for cart-2034: fraud scoring backend timed out after 30190ms (req b482e2b4)
    fraud scoring failed for cart-8906: fraud scoring backend timed out after 30191ms (req a7560b23)
    checkout complete cart=cart-4334 order=ord-90485 amount=47.70 fraud_failed=True
    checkout complete cart=cart-3064 order=ord-56350 amount=208.69 fraud_failed=False
    checkout complete cart=cart-6650 order=ord-44105 amount=378.14 fraud_failed=False
    checkout complete cart=cart-4612 order=ord-33934 amount=448.72 fraud_failed=False
    checkout complete cart=cart-7092 order=ord-61110 amount=314.24 fraud_failed=False
    checkout complete cart=cart-2282 order=ord-59237 amount=357.12 fraud_failed=False
    checkout complete cart=cart-8526 order=ord-47349 amount=93.30 fraud_failed=False
    checkout complete cart=cart-7054 order=ord-67334 amount=319.27 fraud_failed=True
    checkout complete cart=cart-3310 order=ord-78862 amount=410.71 fraud_failed=True
    checkout complete cart=cart-5974 order=ord-19698 amount=345.89 fraud_failed=True
    checkout complete cart=cart-5045 order=ord-73311 amount=17.07 fraud_failed=True
    checkout complete cart=cart-2034 order=ord-24892 amount=152.06 fraud_failed=True
    checkout complete cart=cart-8906 order=ord-97764 amount=347.16 fraud_failed=True
```

The number that moved is the first one, from 2 to 0. The second is the `loki`
helper's own `limit=20`, which is the point: the log lines are all still there,
still readable, still carrying the cart id and the order id and the error text.
This request's two are the `cart-8906` pair at the end of each group. Only the
join is gone.

That is the worst version of this failure, worse than losing the logs entirely. A
missing log is noticed within a day. A log that is present, correct and no longer
reachable from the trace looks exactly like a request that happened not to log
anything, and the on-call engineer concludes there is nothing to see. Restore:

```bash
mv collector/gateway-config.yaml.bak collector/gateway-config.yaml
docker compose restart otel-collector
```

**Shrink the service-graph store to one item.** The service graph is the other
thing derived off the pre-sample stream, and its store is where client and server
halves wait to be paired:

```bash
edges() { curl -s -G http://localhost:9090/api/v1/query \
            --data-urlencode 'query=traces_service_graph_request_total' \
            | python3 -c "
import sys,json
r = json.load(sys.stdin)['data']['result']
print(len(r), 'edges')
for x in r:
    m = x['metric']; print('  ', m.get('client','?'), '->', m.get('server','?'), x['value'][1])"; }
cp collector/gateway-config.yaml collector/gateway-config.yaml.bak
sed -i.tmp 's/      max_items: 1000/      max_items: 1/' collector/gateway-config.yaml
rm -f collector/gateway-config.yaml.tmp
docker compose restart otel-collector
await_collector
for _ in $(seq 1 200); do curl -s -o /dev/null http://localhost:8080/checkout; done
await 'sum(traces_service_graph_request_total)' 1
edges
promq 'sum(pre_calls_total{service_name="checkout-service"})'
promq 'otelcol_connector_servicegraph_dropped_spans_total'
```

```
3 edges
   checkout-service -> fraud-service 3
   checkout-service -> inventory-service 3
   checkout-service -> notification-service 2
1400
992
```

Three edges where there were seven, and the three that survived carry 3, 3 and 2
requests out of 200. The dependency graph is now wrong in a way no one would
question: it is a plausible graph of a service with three downstreams and light
traffic. Read it a scrape too early and you get no edges at all, which is the
same failure wearing a more obvious face, and the reason `await` above polls the
service graph itself rather than the span metrics: the connector's store flushes
on a schedule of its own, several scrapes behind `pre_calls_total`.

The number that did not move is `pre_calls_total`, still 1,400 for 200 requests
at seven spans each. RED is flat. Every latency panel, every error rate, every
burn-rate rule reads exactly what it read before, because none of them goes
through the service-graph store.

The third number is the one worth taking away. `otelcol_connector_servicegraph_dropped_spans_total`
is 992, and unlike the other two failures in this file, this one does announce
itself. It announces itself on a series nobody has a panel for. Restore:

```bash
mv collector/gateway-config.yaml.bak collector/gateway-config.yaml
docker compose restart otel-collector
```

## Clean up

Both edits restore in place, so this is a confirmation:

```bash
grep -c 'strip_trace_id' collector/gateway-config.yaml
grep -o 'max_items: [0-9]*' collector/gateway-config.yaml
ls collector/*.bak collector/*.tmp 2>/dev/null | wc -l
```

```
0
max_items: 1000
       0
```

No `strip_trace_id` processor anywhere, the service-graph store back at 1,000,
and nothing with a `.bak` or `.tmp` suffix left in `collector/`.

If the last number is not zero, some edit was interrupted between its `cp` and
its `mv`. It does not have to have been one of yours: `exercises/divergence.md`
backs up the same file, so an abandoned run of either exercise leaves the same
`.bak` behind, and the remedy is the same either way.

```bash
mv collector/gateway-config.yaml.bak collector/gateway-config.yaml
```

Then confirm all three bridges are back, with one request and one id:

```bash
docker compose restart otel-collector
await_collector
TID=$(python3 -c 'import os;print(os.urandom(16).hex())')
SID=$(python3 -c 'import os;print(os.urandom(8).hex())')
curl -s -o /dev/null -H "traceparent: 00-$TID-$SID-01" "http://localhost:8080/checkout?fail=1"
await_rows "SELECT count() FROM tracing.otel_traces WHERE trace_id = '$TID'" 7
ch --query "SELECT count() FROM tracing.otel_traces WHERE trace_id = '$TID'"
loki "{service_name=\"checkout-service\"} | trace_id=\"$TID\""
```

```
7
status: success  lines: 2
    fraud scoring failed for cart-9255: fraud scoring backend timed out after 30793ms (req b98179c5)
    checkout complete cart=cart-9255 order=ord-12190 amount=30.14 fraud_failed=True
```

Seven spans in the store and two log lines reachable from the same id. Or run the
packaged version, which walks all three crossings and cleans up after itself:

```bash
bash tests/test_correlation.sh
```

This exercise wrote nothing to ClickHouse beyond the traffic it drove, which ages
out on the table's 15-day TTL, so there is nothing to delete.

## Going deeper

`collector/gateway-config.yaml` is listings 9.1 and 9.3 with their annotations,
including why the logs pipeline has no sampler of its own. That is the coherence
question from section 9.3.3 answered in the direction the chapter recommends for
a tail decision: keep every log, accept that some exemplars dangle, and rely on
the log join as the fallback that still works.

**Cause the third failure on purpose.** Switch the histograms from explicit
buckets to exponential, which is the better choice everywhere except here:

```bash
cp collector/gateway-config.yaml collector/gateway-config.yaml.bak
python3 - <<'PY'
from pathlib import Path
p = Path("collector/gateway-config.yaml")
t = p.read_text()
t = t.replace("""      explicit:
        buckets: [2ms, 5ms, 10ms, 20ms, 50ms, 100ms, 200ms, 500ms, 1s, 2s, 5s, 10s]""",
              """      exponential:
        max_size: 160""")
p.write_text(t)
PY
docker compose restart otel-collector
await_collector
for _ in $(seq 1 200); do curl -s -o /dev/null http://localhost:8080/checkout; done
await 'sum(pre_calls_total{service_name="checkout-service"})' 1400
curl -s -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=post_duration_milliseconds_bucket' \
  | python3 -c "
import sys,json
r = json.load(sys.stdin)['data']['result']
print(len(r), 'series; le values:', sorted({x['metric'].get('le') for x in r}))"
curl -s -G http://localhost:9090/api/v1/query_exemplars \
  --data-urlencode 'query=post_duration_milliseconds_bucket' \
  --data-urlencode "start=$(python3 -c 'import time;print(time.time()-60)')" \
  --data-urlencode "end=$(python3 -c 'import time;print(time.time())')" \
  | python3 -c "
import sys,json
d = json.load(sys.stdin).get('data', [])
print('exemplar series:', len(d), ' exemplars:', sum(len(s.get('exemplars', [])) for s in d))"
```

```
7 series; le values: ['+Inf']
exemplar series: 0  exemplars: 0
```

Seven bucket series and one distinct `le` between them. The prometheus exporter
renders classic exposition, an exponential histogram has no classic rendering,
and the whole distribution comes out as a single `+Inf` bucket.
`histogram_quantile` over one bucket cannot return a quantile, and an exemplar
has no bucket to attach to, so bridge 2 has nothing to hand back. The Collector
logged nothing about either.

Give it a minute before believing a `histogram_quantile` reading here: a five
minute rate window still contains the explicit buckets from before the restart,
so the p99 keeps looking sane for a while after the histogram it is computed from
has stopped existing.

Both halves of section 9.3's metric-to-trace jump die from one setting that is
valid, modern, and better in almost any other pipeline. Restore:

```bash
mv collector/gateway-config.yaml.bak collector/gateway-config.yaml
docker compose restart otel-collector
```

Two more if the bridges themselves interest you.

Take `--enable-feature=exemplar-storage` off the Prometheus command in
`docker-compose.yml`. Prometheus keeps accepting exemplars on every scrape and
stores none of them, `query_exemplars` returns an empty list, and there is no
setting anywhere that reads as "off". It is the same empty result as a connector
with exemplars disabled, from the other end of the wire.

And set `allow_structured_metadata: false` in `loki/loki.yaml`. This one does not
fail silently, which makes it the useful contrast: Loki rejects the entire write
with a 400, the Collector logs `not retryable error` and drops the batch, and
every log line disappears rather than just the join. Read
`docker compose logs otel-collector` to see it, then put the setting back. A
bridge that breaks loudly is the easy case, and it is the only one of the four
failures in this file that anybody would catch the same day.
