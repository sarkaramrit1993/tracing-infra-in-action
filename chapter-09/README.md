# Chapter 9: Trace-Driven Insights

Runnable companion to chapter 9 of *Tracing Infrastructure in Action*. Chapter 7
built the store and chapter 8 made it answerable. This chapter asks a different
question: what can you learn from traces that a metric alone could not tell you?

Answering it needs the whole path rather than the store, because every claim in
chapter 9 is about something that happens **before the sampler** or **across two
signals**. So the Collector here runs span metrics twice. `spanmetrics/pre`
counts every span that arrives. The same stream is then handed to a tail sampler
that keeps every error trace and one in a hundred of the rest, and
`spanmetrics/post`
counts what survives. Two series, one workload, one configuration file. Their
disagreement is section 9.2.4's argument, and it is not a thing you can stage
with a fixture.

The same pipeline carries logs. The producer writes them through the
OpenTelemetry logs SDK, so a line written inside a span already carries that
span's TraceId and SpanId, and the Collector forwards them to Loki over OTLP.
Nothing parses a log file and nothing greps for a hex string. That is why the
trace-to-log jump in section 9.3 is exact rather than approximate, and it is
also what lets you break it on purpose in `exercises/correlation.md` and watch
the join return nothing with no error anywhere.

Three commands get you to the chapter's central comparison: `docker compose up
-d --build`, some traffic through `/checkout`, and the pre-versus-post read in
[Look at your trace data](#look-at-your-trace-data). Everything after that is
optional. [NOTES.md](NOTES.md) holds the why behind the design and is worth
opening when something surprises you.

## Listings

| Listing | File | What it shows |
|---------|------|---------------|
| 9.1 | `collector/gateway-config.yaml` | Span metrics and the service graph derived before the sampler runs |
| 9.2 | `clickhouse/error_index.sql` | An error-issue index as a materialized view, fingerprinting on a normalized message |
| 9.3 | `collector/gateway-config.yaml` | The three bridges between signals, declared in one config |

Listings 9.1 and 9.3 name the same file, and that is deliberate rather than an
oversight. They are two readings of one Collector config: 9.1 asks what gets
counted and on which side of the sampler, 9.3 asks which signal can be reached
from which. Duplicating the config into two files so each listing could have one
would have created two things to keep in step. The fences in the file overlap
the way the listings do.

## Prerequisites

- Docker and Docker Compose v2
- **About 5 GB of memory given to Docker**, and read the peak before you set it.
  On macOS and Windows that is Docker Desktop's own setting under Settings,
  Resources, not free host RAM. Measured with `docker stats --no-stream` across a
  full pass through this file: the seven containers settle at about 2.2 GB with
  traffic driven and the store loaded, ClickHouse 1.2 GB of that and Kafka
  700 MB. The number that matters is the other one. `benchmarks/fingerprint_compression.py`
  builds two million rows server-side and takes the stack to **3.9 GB peak, with
  ClickHouse alone at 2.9 GB**. Size Docker for the settled figure and that
  benchmark gets ClickHouse OOM-killed partway through, which looks like a query
  that hung rather than like a memory limit
- **About 4 GB of free disk inside the Docker VM.** This one is not the usual
  boilerplate. If the VM's disk fills, Loki reports `Up` in `docker compose ps`
  and returns 503 to every write, with a single `warn` line in its log as the
  only clue that anything is wrong. Nothing else in the stack notices, and the
  trace-to-log bridge simply comes back empty. Check with
  `docker run --rm alpine df -h /` before you start, and reclaim with
  `docker system prune --volumes` if it is tight
- Python 3 on the host, for the benchmarks. Nothing to install for them: they
  shell out to `clickhouse-client` inside the container and read Prometheus over
  HTTP. The offline test suite needs PyYAML, in a venv
- A POSIX shell, plus `curl`. On Windows, run this inside WSL2

Tear down any other chapter's stack first. This one binds host ports 8080, 3100,
4317, 4318, 8123, 8888, 8889, 9000, 9090 and 9363, all on `127.0.0.1`, and
chapters 5, 7 and 8 bind several of those. The [Ports](#ports) table below says
what each one is for.

```bash
docker compose ls
```

## Bring it up

```bash
docker compose up -d --build
docker compose ps
```

About 90 seconds to settle, plus whatever the first image pull and the app build
cost. Seven services run and one, `kafka-init`, is a one-shot that creates the
`otlp_spans` topic and must have exited 0:

```
SERVICE               STATUS
checkout-service      Up 11 minutes (healthy)
clickhouse            Up 11 minutes (healthy)
consumer-clickhouse   Up 11 minutes
kafka                 Up 11 minutes (healthy)
loki                  Up 11 minutes
otel-collector        Up 11 minutes
prometheus            Up 11 minutes
```

`consumer-clickhouse`, `loki`, `otel-collector` and `prometheus` show no health
state because their images ship no shell to run a check in. That is expected;
NOTES has the detail, and it is why the test scripts poll endpoints from outside
the containers instead of trusting `docker compose ps`.

ClickHouse applies `clickhouse/init.sql` on the way up, which creates
`tracing.otel_traces`: chapter 7's listing 7.1 table carried forward, with
`parent_span_id` present so a trace can be reassembled and chapter 7's storage
policy removed because that chapter's volume config is not here.

Now give it a workload. Ordinary traffic fails one checkout in a hundred, so the
`?fail=1` requests are there to make the error path deterministic rather than to
change its shape:

```bash
for _ in $(seq 1 300); do curl -s -o /dev/null http://localhost:8080/checkout; done
for _ in $(seq 1 6); do curl -s -o /dev/null "http://localhost:8080/checkout?fail=1"; done
```

Nothing below waits a fixed number of seconds. Every read polls for the series or
the rows it needs, because the chain in front of them is longer than it looks:
the tail sampler holds a trace for `decision_wait`, `spanmetrics` flushes every
15 seconds, Prometheus scrapes every 15, and the service graph pairs a client
span with a server span in a store of its own. A sleep long enough for the first
of those is not long enough for the last, and a series that has not arrived yet
looks exactly like a connector that is broken.

---

## Look at your trace data

Five helpers for everything below. `ch` runs a query, `ch_file` applies a `.sql`
file, `promq` reads one scalar out of Prometheus, and `await` and `await_rows`
block until a number reaches a target instead of sleeping and hoping:

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
promq()   { curl -s -G http://localhost:9090/api/v1/query --data-urlencode "query=$1" \
              | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(r[0]['value'][1] if r else 'no data')"; }
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
```

NOTES says why `ch` closes stdin, and why merging it with `ch_file` breaks both.
`await` treats a series Prometheus has never seen as zero, which is what you want
here: `no data` and a genuine zero mean the same thing to a poll that is waiting
for a number to come up.

Start with the shape of one trace, because a wide table of spans does not look
much like a trace until you group it. Wait for this run's nine error spans to
reach the store first, then pull the most recent failing trace, root first:

```bash
await_rows "SELECT count() FROM tracing.otel_traces WHERE status_code = 'STATUS_CODE_ERROR'" 9
ch --query "
SELECT
  if(parent_span_id = '', '>', ' ') AS root,
  substring(trace_id, 1, 8) AS trace,
  rpad(span_name, 18) AS span,
  concat(toString(round(duration_ns / 1e6, 1)), 'ms') AS took,
  status_code
FROM tracing.otel_traces
WHERE trace_id = (
  SELECT trace_id FROM tracing.otel_traces
  WHERE status_code = 'STATUS_CODE_ERROR' ORDER BY timestamp DESC LIMIT 1)
ORDER BY parent_span_id = '' DESC, timestamp"
```

```
>  d3035323  GET /checkout       177.9ms  STATUS_CODE_ERROR
   d3035323  validate_cart       20.8ms   STATUS_CODE_UNSET
   d3035323  inventory.reserve   30.7ms   STATUS_CODE_UNSET
   d3035323  payment.charge      92.5ms   STATUS_CODE_UNSET
   d3035323  fraud.score         41.5ms   STATUS_CODE_ERROR
   d3035323  order.create        21ms     STATUS_CODE_UNSET
   d3035323  notification.send   10.9ms   STATUS_CODE_UNSET
```

Your trace id and durations differ; the shape does not. Seven spans, one root,
and two of them in error. The root is in error because it propagated: the
request failed, so the span that answers the caller says so, and that is what
makes an error ratio over server spans a ratio of requests. `fraud.score` is the
one that actually threw, and it is the deepest span in the trace. That depth is
the point of section 9.2.2: an origin query wants the deepest error span, not
the first one it meets on the way down, and here there is a real pair to tell
apart.

Now the comparison the whole chapter turns on. Same service, same span, same
15-second window, two series. The poll goes on the error series rather than on
the totals, because the totals reach Prometheus a scrape before the error
breakdown does and a poll that stops at them reads the errors as zero:

```bash
await 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})' 9
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})'
```

```
306
12
9
9
```

306 calls became 12. The sampler dropped the other 294. But the error counts are
identical, 9 against 9, because the `keep-errors` policy keeps every trace that
carries an error and drops nothing from that class. So the numerator survived
whole while the denominator was cut by a factor of twenty-two, and the two error
rates come out:

```bash
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"}) / sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"}) / sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})'
```

```
0.029411764705882353
0.75
```

2.9 percent against 64. One of those is the service's error rate and the other is
an artifact of how the traces were selected. Seventy-five is loud enough to
disbelieve; the dangerous version is the opener's, where the same arithmetic over
a service failing one request in a hundred lands on 50 and reads as a real outage
rather than as a broken denominator. Either way the panel has an axis, a line and
a plausible number. `exercises/divergence.md` takes it apart, including what
happens to the gap when you change the sample rate.

The service graph is derived off the same pre-sample stream. It needs its own
poll: the connector pairs a client span with a server span in a store of its own,
so its edges converge several scrapes after the span metrics have settled, and
reading it early is what makes the arithmetic below come out one edge short:

```bash
await 'sum(traces_service_graph_request_total{client="user",server="checkout-service"})' 306
curl -s -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=traces_service_graph_request_total' \
  | python3 -c "
import sys,json
for r in json.load(sys.stdin)['data']['result']:
    m = r['metric']
    print(m.get('client','?'), '->', m.get('server','?'), r['value'][1])"
```

```
checkout-service -> fraud-service 297
checkout-service -> inventory-service 306
checkout-service -> notification-service 306
checkout-service -> payment-service 306
user -> checkout-service 297
checkout-service -> fraud-service 9
user -> checkout-service 9
```

Seven edges over five destinations. `checkout-service -> fraud-service` and
`user -> checkout-service` each appear twice because the connector carries a
`failed` label the command above does not print, and the 9 on each is the same 9
errors the RED series counted: once where the call failed, once where the request
that made it failed. 297 plus 9 is 306, which is every call, on both edges.

---

## The exercises

Three separate things live in this chapter and none of them needs the others. So
they are three separate files. Open any one in any order. Each brings the stack
up, puts it into the state it needs, and clears up after itself, so none assumes
you ran another and none leaves a mess behind.

| Exercise | Listing | The question |
|---|---|---|
| [exercises/divergence.md](exercises/divergence.md) | 9.1 | Two series over one workload disagree by a factor of twenty-two. Which one is lying, and what does the survivors' version look like on a dashboard? |
| [exercises/fingerprints.md](exercises/fingerprints.md) | 9.2 | Normalization turns two million error spans into a triage list. Graded against the number of bugs the generator was told to seed. |
| [exercises/correlation.md](exercises/correlation.md) | 9.3 | One trace id, three signals, three joins. All three break silently, and each one breaks differently. |

If you only do one, do divergence: it is the chapter's thesis, and it is the one
whose wrong answer is most likely to already be on a dashboard somewhere.

## Beyond the book's listings

Three things here back no printed listing: `rules/burn_rate.yml`,
`rules/span_ingest_gap.yml`, and the span counter in `app/checkout.py` that
feeds the second of them. They ship anyway, and the reason is the same in both
cases. The chapter's argument is that a pre-sample series and a post-sample
series disagree and that only one of them is the truth. That argument is worth
nothing until something reads the right series and pages a human when it moves,
and these two files are that something.

They are also where the argument got expensive. Each file had an earlier version
that parsed, loaded clean, evaluated without error, and was wrong: one needed a
10.1% request failure rate before it would fire, under a label promising 99.9%
availability, and the other reported every span in a healthy stack lost.
NOTES.md, under "Before you change either rule file", has both measurements.
Read it before you edit either one. Neither defect announces itself.

Both files load automatically. `rules/` is mounted into Prometheus and
`prometheus.yml` globs it, so the seven recording rules and three alerts are
live from the moment the stack is up:

```bash
curl -s http://localhost:9090/api/v1/rules \
  | python3 -c "
import sys,json
for g in json.load(sys.stdin)['data']['groups']:
    for r in g['rules']:
        print(g['name'], r['type'], r.get('name'), r['health'])"
```

```
checkout_slo_burn_rate recording slo:checkout_errors:ratio_rate5m ok
checkout_slo_burn_rate recording slo:checkout_errors:ratio_rate30m ok
checkout_slo_burn_rate recording slo:checkout_errors:ratio_rate1h ok
checkout_slo_burn_rate recording slo:checkout_errors:ratio_rate6h ok
checkout_slo_burn_rate alerting CheckoutErrorBudgetBurnFast ok
checkout_slo_burn_rate alerting CheckoutErrorBudgetBurnSlow ok
span_ingest_gap recording spans:received:rate5m ok
span_ingest_gap recording spans:expected:rate5m ok
span_ingest_gap recording spans:ingest_gap:ratio5m ok
span_ingest_gap alerting SpanIngestGap ok
```

Ten rules, all `ok`.

### The burn-rate rule

Two things in `rules/burn_rate.yml` are worth reading, and both are about what
the ratio is a ratio of.

It burns against `pre_calls_total`, never `post_calls_total`. On this stack the
post error rate reads twenty-two times the true one, so a burn-rate alert built
on the survivors would page on a healthy service every time the sampler did its
job. That is not a hypothetical: it is the 2.9-against-75 above, wired to a
pager.

It also selects `span_kind="SPAN_KIND_SERVER"`. `spanmetrics` counts spans and a
checkout makes seven of them, so a ratio over every span is a seventh of the
request error rate, and the fast threshold of 1.44% would need a real request
error rate above 10% before it fired, under a label that promises 99.9%. The
server span is opened once per request, which is what makes the selector count
requests. It is also why `app/checkout.py` marks that span failed rather than
leaving the error on `fraud.score` alone.

All four windows are here, not just the 5m and 1h pair you would keep if you
were trimming this for print. Both alerts read a short window against a long
one, and a Prometheus alert whose expression names a recording rule that does
not exist does not error. It evaluates to an empty vector and never fires, which
is the quietest possible way for an alert to be broken.

### The ingest gap

The useful thing about `rules/span_ingest_gap.yml` is where its two numbers come
from. The expected side is read a minute behind the received side, so the
poll waits for a minute of producer history to exist before there is anything to
read:

```bash
await 'spans:expected:rate5m' 1
promq 'spans:expected:rate5m'
promq 'spans:received:rate5m'
promq 'spans:ingest_gap:ratio5m'
```

```
7.14
7.14
0
```

Equal, and a zero gap. That is what a healthy stack has to read: at this point the
producer's counter and the Collector's both stood at 2,142, so any number other
than zero would have been an artifact of the arithmetic rather than a
measurement. Getting there took two departures from `rate()`, and
`rules/span_ingest_gap.yml` carries both with the reasons. Sampled every ten
seconds through a burst and the ten idle minutes after it, the gap peaked at
3.7 percent while the windows drained and the alert never left `inactive`. `received` is `otelcol_receiver_accepted_spans_total`,
the Collector's own telemetry off its `:8888` endpoint. `expected` is
`checkout_spans_emitted_total`, a counter the producer increments from a
`SpanProcessor` inside its own process, scraped by Prometheus **directly off the
application container** and never through the Collector.

That independence is the whole signal. Comparing the Collector's received count
against a number that also travelled through the Collector proves nothing: an
outage takes both to zero and the ratio sits at 1.0 while every span in the
system is on the floor.

## Run tests

Offline, no Docker needed: the schema carries `parent_span_id`, the three
listings match what the book prints, both rule files still say what the stack
needs them to say, the connectors sit on the right side of the sampler,
the histograms are explicit rather than exponential, and every window an alert
names is a rule that exists. It reads YAML, so it needs PyYAML:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r tests/requirements.txt
python3 tests/test_static.py
```

Live, so the stack must be up and must have had traffic:

```bash
bash tests/test_stack.sh
bash tests/test_correlation.sh
```

`test_stack.sh` checks all eight services, then walks the chapter's claims: that
`record_exception` detail reaches the span attributes, that the listing 9.2 index
folds many raw error spans into one issue, that the post error rate reads above
the pre one, that the service graph has edges, and that both sides of the
ingest-gap rule report. `test_correlation.sh` fires one request with a trace id it chose itself
and walks all three of section 9.3's crossings for that one id. Both poll for
every condition rather than sleeping, and both put the store back the way a fresh
stack starts, so either can be run in any order and re-run from any state.

The benchmarks are in [benchmarks/README.md](benchmarks/README.md), and
[RESULTS.md](RESULTS.md) is the rendered record of the last committed run.

## Tear down

The `-v` flag drops the named volumes holding the spans, the Kafka log and Loki's
chunks. The last two lines undo the virtual environment from the test step, and
are harmless if you never made one.

```bash
docker compose down -v
deactivate 2>/dev/null
rm -rf .venv
```

## Notes on running the book's listings

Listing 9.2 differs from the file that backs it in a way worth knowing before
you paste it into your own stack, and both rule files differ from the version
you would write straight from the chapter's description. All of it is in
[NOTES.md](NOTES.md), and the short version is:

- **Listing 9.2's `top_frame`.** The book slices the first line off the
  stacktrace. On a Python traceback that line is `Traceback (most recent call
  last):`, identical for every exception ever raised, so every issue in the
  service folds into one. The file parses the innermost frame instead.
- **The burn-rate rule's series name.** `traces_span_metrics_calls_total` is
  what the connector emits with no `namespace` set. This stack sets
  `namespace: pre` and `namespace: post`, so the two series are `pre_calls_total`
  and `post_calls_total`. A rule naming the connector default here matches
  nothing and never fires.
- **The ingest gap's `expected` side.** Baselining against the same hour a week
  earlier is the better input for a system that has been running a week. A stack
  you brought up ten minutes ago has no week of history, so the file uses an
  upstream emit counter. The alert arithmetic is identical.

## Reference

Nothing below is needed to run anything above it.

### Ports

| Port | What |
|---|---|
| 8080 | `checkout-service`, and its own `/metrics` |
| 4317, 4318 | Collector OTLP gRPC and HTTP |
| 8888 | Collector internal telemetry, the `received` half of the ingest gap |
| 8889 | Collector span-metrics and service-graph scrape endpoint |
| 8123, 9000 | ClickHouse HTTP and native |
| 9363 | ClickHouse Prometheus endpoint |
| 9090 | Prometheus |
| 3100 | Loki HTTP |

Everything binds to `127.0.0.1` rather than `0.0.0.0`, so nothing here is
reachable from another machine on your network.

### Version manifest (one tag per image)

| Component | Version | Role |
|---|---|---|
| OpenTelemetry Collector (contrib) | `otel/opentelemetry-collector-contrib:0.154.0` | span metrics before and after the sampler, service graph, tail sampling, the logs path to Loki |
| ClickHouse | `clickhouse/clickhouse-server:25.8` (LTS) | the span store and the listing 9.2 error-issue index |
| Apache Kafka | `apache/kafka:4.3.0` | the `otlp_spans` topic between Collector and consumer, single-broker KRaft |
| Prometheus | `prom/prometheus:v3.12.0` | span metrics, exemplar storage, the two rule files under `rules/` |
| Loki | `grafana/loki:3.7.6` | logs, with `trace_id` as structured metadata |
| Python | 3.12 in the app image, 3 on the host for benchmarks | producer, storage consumer, benchmark scripts |

The ClickHouse tag matches `chapter-07/` and `chapter-08/`, so any difference
between the three chapters is in the schema and the queries, never in the server.

### File tree

```
chapter-09/
├── docker-compose.yml
├── README.md
├── NOTES.md
├── RESULTS.md
├── app/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── checkout.py              producer: spans, logs and its own span counter
│   └── consumer_clickhouse.py   Kafka otlp_spans -> ClickHouse otel_traces
├── collector/
│   └── gateway-config.yaml      listings 9.1 and 9.3
├── clickhouse/
│   ├── init.sql                 the span table, auto-applied on first boot
│   ├── error_index.sql          listing 9.2, applied by hand before traffic
│   ├── config.d/
│   │   ├── network.xml
│   │   └── prometheus.xml
│   └── users.d/
│       └── z-allow-network.xml
├── loki/
│   └── loki.yaml                structured metadata on, so trace_id survives
├── prometheus.yml
├── rules/
│   ├── burn_rate.yml            burn-rate alerting on the pre-sample series
│   └── span_ingest_gap.yml      emitted against received, two separate paths
├── exercises/
│   ├── divergence.md            listing 9.1: what the survivors' error rate is
│   ├── fingerprints.md          listing 9.2: what normalization is worth
│   └── correlation.md           listing 9.3: three joins, three silent breaks
├── benchmarks/
│   ├── README.md
│   ├── sampler_divergence.py    pre against post, read out of Prometheus
│   ├── fingerprint_compression.py   measured F against declared P
│   └── results/                 dated JSON, gitignored except the committed runs
└── tests/
    ├── requirements.txt
    ├── test_static.py           offline: listings, connectors, rules, compose
    ├── test_stack.sh            live: the chapter's claims against the stack
    └── test_correlation.sh      live: one trace id across all three bridges
```
