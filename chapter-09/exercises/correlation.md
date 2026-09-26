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
docker compose restart otel-collector
await_collector
for _ in $(seq 1 400); do curl -s -o /dev/null http://localhost:8080/checkout; done
for _ in $(seq 1 10); do curl -s -o /dev/null "http://localhost:8080/checkout?fail=1"; done
await 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})' 14
```

The ten forced failures are there because the sampler keeps one successful trace
in a hundred. Four hundred ordinary requests leave about four survivors between
them, which is not enough post-sampler traces for the exemplar buffer to be
worth reading. The forced ones are kept unconditionally, so they are what puts
pointers on the histogram.

The restart zeroes the connector counters, so the poll waits for this traffic
rather than for whatever an earlier walk left on the counter, and the totals
under bridge 3 count this file's requests alone.

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
99cd424c32817c88f4193174fd126bdc
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
GET /checkout       STATUS_CODE_ERROR  181.5ms
validate_cart       STATUS_CODE_UNSET  21.4ms
inventory.reserve   STATUS_CODE_UNSET  31.3ms
payment.charge      STATUS_CODE_UNSET  93.3ms
fraud.score         STATUS_CODE_ERROR  42.3ms
order.create        STATUS_CODE_UNSET  20.5ms
notification.send   STATUS_CODE_UNSET  12.7ms
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
    fraud scoring failed for cart-4647: fraud scoring backend timed out after 30579ms (req 4969f476)
    checkout complete cart=cart-4647 order=ord-62656 amount=308.38 fraud_failed=True
```

Two lines, from a request that finished moments ago, retrieved by an id chosen
before it existed. Promoting `trace_id` to a real label would make the first
selector work and would also be the one thing section 9.3.2 says never to do: a
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
c4a3eb9685b88de2a6757eb7adbfe4f5 -> 7 spans
4795f509cff872c67ed71553e6712642 -> 7 spans
5e8207565a54867ce870cdee5013baad -> 7 spans
49f90976af1afb3578d3efe7cf061868 -> 7 spans
796a9d380f33390eb609301d0a47a4a7 -> 7 spans
...
```

Every exemplar resolves to a whole trace. How many there are follows the traffic
in the query's fifteen-minute window: 51 on the walk printed here, abridged to
its first five, and 37 and 40 on two earlier walks. All three ran the README and
the other two exercises first, so expect fewer if you start here. The trace this
exercise picked is usually not among them. Prometheus keeps one exemplar per
series per scrape, and the other failures share its buckets. Now run exactly the
same loop against the pre-sampler histogram:

```bash
for tid in $(exemplars 'pre_duration_milliseconds_bucket'); do
  echo "$tid -> $(ch --query "SELECT count() FROM tracing.otel_traces WHERE trace_id='$tid'") spans"
done
```

```
ff184ca0c25767f18e2d658d734a16e7 -> 7 spans
63e9112f28f75f0dad9a0c87706f98ff -> 7 spans
1b61d19f4c9cd18bb42f2c4adea64aea -> 7 spans
...
1a961363b6317c36f1a4fd356e6b8b18 -> 0 spans
bd5338a9e6cd556ff8d7297bca7edd4d -> 0 spans
58fbc7766a9b50091e8c944a236973e7 -> 0 spans
...
```

The same walk had 101 pre exemplars, abridged here because the interesting part
is the tally rather than the ids: 27 resolved and 74 pointed at nothing. Two earlier
walks got 23 of 95 and 25 of 89. The count follows the traffic; what holds is that about three in four
dangle. That is the second silent failure. A pre-sampler exemplar is minted before the sampler has
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
2877
140
```

The first number is exact: 411 requests since the restart, at seven spans each.
The second is a draw. It counts the fifteen error traces (the fourteen above and
the one this exercise picked) plus whichever successes the sampler happened to
keep, so it moves between runs: 140 on the walk printed here, 119 and 133 on
two earlier ones.

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
    fraud scoring failed for cart-4647: fraud scoring backend timed out after 30579ms (req 4969f476)
    fraud scoring failed for cart-1593: fraud scoring backend timed out after 30580ms (req 3078086b)
    fraud scoring failed for cart-4975: fraud scoring backend timed out after 30571ms (req 82391892)
    fraud scoring failed for cart-8942: fraud scoring backend timed out after 30572ms (req c1ad723a)
    fraud scoring failed for cart-3483: fraud scoring backend timed out after 30573ms (req 03786303)
    fraud scoring failed for cart-8285: fraud scoring backend timed out after 30574ms (req 5381af3b)
    fraud scoring failed for cart-4369: fraud scoring backend timed out after 30575ms (req 2f22b6c5)
    fraud scoring failed for cart-2744: fraud scoring backend timed out after 30576ms (req 9aa84aba)
    fraud scoring failed for cart-9077: fraud scoring backend timed out after 30577ms (req 92de9a45)
    fraud scoring failed for cart-4370: fraud scoring backend timed out after 30578ms (req fa574eeb)
    checkout complete cart=cart-4647 order=ord-62656 amount=308.38 fraud_failed=True
    checkout complete cart=cart-1593 order=ord-96610 amount=327.97 fraud_failed=True
    checkout complete cart=cart-4975 order=ord-33289 amount=420.05 fraud_failed=True
    checkout complete cart=cart-8942 order=ord-45029 amount=448.16 fraud_failed=True
    checkout complete cart=cart-3483 order=ord-90361 amount=358.53 fraud_failed=True
    checkout complete cart=cart-8285 order=ord-85775 amount=240.09 fraud_failed=True
    checkout complete cart=cart-4369 order=ord-69032 amount=95.74 fraud_failed=True
    checkout complete cart=cart-2744 order=ord-31180 amount=54.35 fraud_failed=True
    checkout complete cart=cart-9077 order=ord-30568 amount=293.13 fraud_failed=True
    checkout complete cart=cart-4370 order=ord-28334 amount=326.83 fraud_failed=True
```

The number that moved is the first one, from 2 to 0. The second is the `loki`
helper's own `limit=20`, which is the point: the log lines are all still there,
still readable, still carrying the cart id and the order id and the error text.
This request's own pair is in there, the `cart-1593` lines, second in each group.
Only the join is gone.

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
2 edges
   checkout-service -> fraud-service 4
   checkout-service -> inventory-service 4
1400
992
```

Two edges where there were seven, and the two that survived carry 4 and 4
requests out of 200. Which edges survive, and what they carry, depends on which
halves happened to meet in the one free slot, so it moves between runs: two
earlier walks kept three edges each, carrying 3, 4 and 1 and then 3, 2 and 3.
The dependency graph is now wrong in a way no one would question: it is a
plausible graph of a service with a couple of downstreams and light traffic. Read it a scrape too early and you get no edges at all, which is the
same failure wearing a more obvious face, and the reason `await` above polls the
service graph itself rather than the span metrics: the connector's store flushes
on a schedule of its own, several scrapes behind `pre_calls_total`.

Now `pre_calls_total`, which reads 1,400: exactly 200 requests at seven spans
each, and the whole of what this Collector process has seen. Restarting it to
apply the edit zeroed that counter, so 1,400 is not a number that survived the
failure, it is a number taken cleanly after it. That is the stronger version of
the point. The service graph lost about 990 spans under the same config, on the same
traffic, in the same process, and the span metrics counted every one of them.
RED is flat here not because nothing was measured but because nothing RED
measures goes through the service-graph store.

The third number is the one worth taking away. `otelcol_connector_servicegraph_dropped_spans_total`
is about 990 (992 on four walks, 991 on a fifth, for the same reason the edges
move), and unlike the other two failures in this file, this one does announce
itself. It announces itself on a series nobody has a panel for. Restore:

```bash
mv collector/gateway-config.yaml.bak collector/gateway-config.yaml
docker compose restart otel-collector
```

## Going deeper

`collector/gateway-config.yaml` is listings 9.1 and 9.3 with their annotations,
including why the logs pipeline has no sampler of its own. That is the coherence
question from section 9.3.2 answered in the direction the chapter recommends for
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
await 'sum(pre_calls_total{service_name="checkout-service"})' 1050
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
9 series; le values: ['+Inf']
exemplar series: 0  exemplars: 0
```

Nine bucket series and one distinct `le` between them. You may see seven: the
1-in-100 cadence puts two failures in those 200 requests, and their two error
series join the other seven only once the sampler has let those traces through. The prometheus exporter
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

**Turn the exemplar store off.** `--enable-feature=exemplar-storage` is what
makes Prometheus keep the exemplars it is handed. Without it Prometheus keeps
accepting them on every scrape and stores none, and there is no setting anywhere
that reads as "off".

This edit is the only one in this file that touches `docker-compose.yml`, so it
is the only one that needs the container replaced rather than restarted, and
replacing Prometheus destroys its TSDB. Every counter you have read in this
exercise goes back to zero and the rate windows start refilling from empty:

```bash
cp docker-compose.yml docker-compose.yml.bak
sed -i.tmp '/--enable-feature=exemplar-storage/d' docker-compose.yml
rm -f docker-compose.yml.tmp
docker compose up -d prometheus
for _ in $(seq 1 200); do curl -s -o /dev/null http://localhost:8080/checkout; done
await 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})' 1
curl -s -G http://localhost:9090/api/v1/query_exemplars \
  --data-urlencode 'query=post_duration_milliseconds_bucket' \
  --data-urlencode "start=$(python3 -c 'import time;print(time.time()-900)')" \
  --data-urlencode "end=$(python3 -c 'import time;print(time.time())')" \
  | python3 -c "
import sys,json
d = json.load(sys.stdin).get('data', [])
print('exemplar series:', len(d), ' exemplars:', sum(len(s.get('exemplars', [])) for s in d))"
```

```
exemplar series: 0  exemplars: 0
```

The same empty list as the previous edit produced, from the other end of the
wire: there, the connector minted no exemplar; here, it minted one per scrape
and Prometheus dropped every one. Restore, which replaces the container a second
time and wipes the TSDB again:

```bash
mv docker-compose.yml.bak docker-compose.yml
docker compose up -d prometheus
```

**And break one loudly, for the contrast.** Set `allow_structured_metadata` to
`false` in `loki/loki.yaml`. This one does not fail silently, which is the whole
point of ending on it:

```bash
cp loki/loki.yaml loki/loki.yaml.bak
sed -i.tmp 's/allow_structured_metadata: true/allow_structured_metadata: false/' loki/loki.yaml
rm -f loki/loki.yaml.tmp
docker compose restart loki
for _ in $(seq 1 20); do curl -s -o /dev/null http://localhost:8080/checkout; done
sleep 20
docker compose logs otel-collector | grep -o 'not retryable error' | head -1
```

```
not retryable error
```

Loki rejects the entire write with a 400, the Collector says so and drops the
batch, and every log line disappears rather than just the join. The grep asks
whether the phrase is there rather than how many times, because how many times
is a function of how long you left it running. A bridge that breaks loudly is the easy case, and it is the only one of the
four failures in this file that anybody would catch the same day. Restore:

```bash
mv loki/loki.yaml.bak loki/loki.yaml
docker compose restart loki
```

## Clean up

Every edit above restores in place, so this is a confirmation rather than a
step:

```bash
grep -c 'strip_trace_id' collector/gateway-config.yaml
grep -o 'max_items: [0-9]*' collector/gateway-config.yaml
grep -c 'explicit:' collector/gateway-config.yaml
grep -c 'exemplar-storage' docker-compose.yml
grep -c 'allow_structured_metadata: true' loki/loki.yaml
ls collector/*.bak collector/*.tmp docker-compose.yml.bak loki/*.bak 2>/dev/null | wc -l
```

```
0
max_items: 1000
2
2
2
       0
```

No `strip_trace_id` processor anywhere, the service-graph store back at 1,000,
both histograms back on explicit buckets, the exemplar-storage flag back on the
Prometheus command, Loki accepting structured metadata again, and nothing with a
`.bak` or `.tmp` suffix left behind. The two counts of two are not a coincidence
worth reading into: there is one `explicit:` block per `spanmetrics` connector,
and `allow_structured_metadata: true` appears in `loki/loki.yaml` twice, once in
the comment that explains it and once as the setting.

Three files rather than one. `collector/gateway-config.yaml` is the only file
Try this touches, and Going deeper goes on to edit `docker-compose.yml` and
`loki/loki.yaml` as well, so a check that greps `collector/` alone reports green
over a stack with two bridges still broken.

If the last number is not zero, some edit was interrupted between its `cp` and
its `mv`. It does not have to have been one of yours: `exercises/divergence.md`
backs up the same file, so an abandoned run of either exercise leaves the same
`.bak` behind, and the remedy is the same either way. This restores whichever of
the three is there and leaves the other two alone:

```bash
if [ -f collector/gateway-config.yaml.bak ]; then
  mv collector/gateway-config.yaml.bak collector/gateway-config.yaml
  docker compose restart otel-collector
fi
if [ -f docker-compose.yml.bak ]; then
  mv docker-compose.yml.bak docker-compose.yml
  docker compose up -d prometheus
fi
if [ -f loki/loki.yaml.bak ]; then
  mv loki/loki.yaml.bak loki/loki.yaml
  docker compose restart loki
fi
```

The guard matters because the block above just told you the count was zero. A
bare `mv` on a path that is not there fails with `No such file or directory`,
which reads like a broken instruction rather than the all-clear it is.

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
    fraud scoring failed for cart-2418: fraud scoring backend timed out after 30201ms (req f9236f92)
    checkout complete cart=cart-2418 order=ord-63457 amount=431.98 fraud_failed=True
```

Seven spans in the store and two log lines reachable from the same id. Or run the
packaged version, which walks all three crossings and cleans up after itself. It
sends a hundred ordinary requests of its own for the pre-exemplar check and
bridge 3, because the one request above failed on purpose and the sampler keeps
every failure, so on that traffic alone pre and post count the same spans:

```bash
bash tests/test_correlation.sh
```

This exercise wrote nothing to ClickHouse beyond the traffic it drove, which ages
out on the table's 15-day TTL, so there is nothing to delete.
