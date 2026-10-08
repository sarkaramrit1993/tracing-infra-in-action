# Chapter 5: Trace Assembly and Processing Patterns

Code for Chapter 5 of *Tracing Infrastructure in Action*.

A checkout service and the four services it calls (inventory, payment, fraud,
notification) send traces down both assembly paths the chapter compares, at the
same time, from the same Kafka topic:

1. **Query-time assembly.** Spans land in ClickHouse one by one, as they arrive.
   A trace only exists as a whole when you read it.
2. **Stream-time assembly.** A Flink job holds each trace's spans in keyed state
   for 10 seconds, then emits the whole trace at once.
3. **Whole traces or nothing.** Both paths deliver every checkout with all eleven
   spans, and a small audit shows which failures break that rule and which don't.

## Listings

| Listing | File | What it shows |
|---------|------|---------------|
| 5.1 | `clickhouse/init.sql` | ClickHouse spans table for query-time assembly |
| 5.2 | `app/scatter_gather_query.py` | ClickHouse trace-assembly query |
| 5.3 | `flink/assembly_job.py` | KeyedProcessFunction skeleton for keyed trace assembly |
| 5.5 | `flink/assembly_job.py` | Bounded watermark strategy and late-span side-output routing |
| 5.6 | `clickhouse/service_graph.sql` | Service graph derivation from the spans table |

Listing 5.4 (`loadbalancingexporter`) is not in this stack: it routes by trace
ID without Kafka, and this stack routes through Kafka instead. The book prints
readable excerpts, so the files differ from it in small ways;
[NOTES.md](NOTES.md) lists how, under "Running the book's listings verbatim".

## Before you start

- Docker with Docker Compose v2, with **about 6 GB of memory** for Docker (Docker
  Desktop: Settings, Resources). With less, Flink is killed partway through and
  the stream-time path stops.
- Python 3 and `curl` on your machine. On Windows, use WSL2.
- Stop any other chapter's stack first (`docker compose ls`). The ports this one
  uses are listed under [Reference](#ports).
- If something else on your machine already has port 8081, give Flink another
  one: save the snippet below as `docker-compose.override.yml` in this directory
  (Docker Compose 2.24 or later reads it on its own), and run `export FLINK_PORT=18081` in the
  terminal you run the scripts from.

  ```yaml
  services:
    flink-jobmanager:
      ports: !override
        - "18081:8081"
        - "9249:9249"
  ```

Run every command from this `chapter-05/` directory. Each step runs a small
script from `scripts/` that prints what it found, and the query inside it is
shown under the step, so you can run it yourself.

## 1. Start the stack

```bash
docker compose up -d --build
docker compose ps
```

The first build takes a few minutes: the Flink image installs PyFlink.
`kafka-init` creates the three topics and exits, so `docker compose ps` does
not list it. `flink-job-submit` submits the assembly job and stays `Up` as long
as the job runs. Flink is the last thing to come up; the next script waits for
it, so you can go straight on.

`docker compose down -v` throws everything away, and the next start is fresh.
A plain restart (`docker compose stop`, then `docker compose up -d`) keeps
Kafka and ClickHouse, and the Flink job picks up where it left off in the
topic. Jaeger keeps traces in memory and comes back empty, so after a restart
start again from step 2.

## 2. Send some traffic

120 checkouts, then wait until they have arrived on both paths:

```bash
./scripts/send-traffic.sh
./scripts/wait-until-ready.sh
```

```
waiting for checkout-service... ok
waiting for ClickHouse... ok
waiting for Jaeger... ok
waiting for the Flink assembly job to start... ok
sending 120 checkouts...
sent 120 checkouts
query-time path: waiting for ClickHouse to hold all 11 spans of all 120 checkouts... ok
stream-time path: waiting for Flink to assemble the newest one and Jaeger to store it... ok
ready
```

Each checkout is one trace of eleven spans: six in `checkout-service`, and five
more recorded by the services it calls, each under its own service name.
`payment-service` calls `fraud-service` in turn. The first wait is the query-time path
filling ClickHouse. The second is the stream-time path: Flink holds each trace
until its 10-second timer fires, so the newest checkout comes out last.

What it sends:

```text
curl -s http://localhost:8080/checkout    120 times
```

## 3. Verify it works: one trace, both paths

Take the newest checkout and count its spans everywhere it was delivered:

```bash
./scripts/show-both-paths.sh
```

```
trace d7cbd91fb342829338b3ba2ab7a75c06

where                                        spans
ClickHouse tracing.otel_traces               11
Jaeger, assembly.source=query-time           11
Jaeger, assembly.source=stream-time          11

open http://localhost:16686/trace/d7cbd91fb342829338b3ba2ab7a75c06 to see it in Jaeger
```

Your trace ID will differ. Eleven spans in each place. ClickHouse holds them as separate rows, written as
they arrived. Jaeger holds the trace twice: one copy labelled
`assembly.source=query-time`, forwarded span by span, and one labelled
`stream-time`, which Flink emitted as a whole. Open the link the script prints
to see both copies in the Jaeger UI.

What it runs, for the Jaeger side:

```text
curl -s http://localhost:16686/api/traces/<trace_id>
```

and counts the spans under each `assembly.source` label.

## 4. Assemble the trace at read time

Query-time assembly in two steps: fetch every span with the trace ID (listing
5.2), then rebuild the parent-child tree in memory:

```bash
./scripts/assemble-trace.sh
```

```
scatter-gather across 1 shard(s) for trace_id=d7cbd91fb342829338b3ba2ab7a75c06
shards: ['clickhouse']
  shard=clickhouse returned=11 elapsed_ms=20.9

assembled 11 spans in 111.3ms (tail-shard bound)

waterfall:
  checkout-service         GET /checkout                +     0.0us 175.25ms [STATUS_CODE_UNSET]
    checkout-service         validate_cart                +   341.8us  20.93ms [STATUS_CODE_UNSET]
    checkout-service         inventory.reserve            + 21358.9us  31.05ms [STATUS_CODE_UNSET]
      inventory-service        POST /inventory/reserve      + 21404.6us  30.97ms [STATUS_CODE_UNSET]
    checkout-service         payment.charge               + 52511.8us  91.46ms [STATUS_CODE_UNSET]
      payment-service          POST /payments/charge        + 52572.1us  91.39ms [STATUS_CODE_UNSET]
        payment-service          fraud.score                  +103050.9us  40.90ms [STATUS_CODE_UNSET]
          fraud-service            POST /fraud/score            +103162.7us  40.74ms [STATUS_CODE_UNSET]
    checkout-service         order.create                 +144109.8us  20.77ms [STATUS_CODE_UNSET]
    checkout-service         notification.send            +164959.3us  10.15ms [STATUS_CODE_UNSET]
      notification-service     process notification         +164995.4us  10.09ms [STATUS_CODE_UNSET]
```

Each call shows up twice: the caller's client span, then the called service's
own span inside it. Your timings will differ. This stack has one ClickHouse, so the "scatter" is one request. The script still
prints each shard's time separately, because a real query is only as fast as its
slowest shard (Figure 5.5).

What it runs (listing 5.2), with the trace ID filled in:

```sql
SELECT span_id, parent_span_id, service_name, span_name,
       toUnixTimestamp64Nano(timestamp) AS start_ns, duration, status_code
FROM tracing.otel_traces
WHERE trace_id = '<trace_id>'
  AND timestamp >= now() - INTERVAL 24 HOUR
ORDER BY start_ns ASC
SETTINGS optimize_read_in_order = 1
```

## 5. See how the table stores spans

```bash
./scripts/show-table-layout.sh
```

```
parts (every insert writes one; background merges combine them)
partition            part_type  parts  rows  on_disk
2026-10-08 05:00:00  Compact    5      1327  52.21 KiB

where trace d7cbd91fb342829338b3ba2ab7a75c06 sits
part              spans  first_row  last_row
1791435600_8_8_0  11     121        131
```

Your partition, part names, part count and row numbers will differ, since they
depend on the hour you run in and how far the background merges have got. What
stays the same: in each part that holds the trace, its spans sit on
consecutive rows.

Listing 5.1's table is partitioned by hour and sorted by `(trace_id,
timestamp)`. Every consumer flush writes a new part, and ClickHouse merges small
parts in the background, so the part count goes up and down as you watch. Inside
a part, the newest checkout's eleven spans are eleven neighbouring rows, which is
why the query in step 4 is a seek, not a scan. Small parts are stored in the
`Compact` format, all columns in one file; past a size threshold they switch to
`Wide`, one file per column.

What it runs:

```sql
SELECT partition, part_type, count() AS parts, sum(rows) AS rows,
       formatReadableSize(sum(bytes_on_disk)) AS on_disk
FROM system.parts
WHERE database = 'tracing' AND table = 'otel_traces' AND active
GROUP BY partition, part_type ORDER BY partition, part_type;

SELECT _part AS part, count() AS spans,
       min(_part_offset) AS first_row, max(_part_offset) AS last_row
FROM tracing.otel_traces
WHERE trace_id = '<trace_id>'
GROUP BY _part ORDER BY _part;
```

## 6. Read RED metrics without assembling anything

```bash
./scripts/show-red-metrics.sh
```

```
service_name          span_name                spans  errors  p99_ms
checkout-service      GET /checkout            120    0       182
checkout-service      GET /health              7      0       4.4
fraud-service         POST /fraud/score        120    8       42.3
inventory-service     POST /inventory/reserve  120    0       33.3
notification-service  process notification     120    0       13.5
payment-service       POST /payments/charge    120    0       94.9
```

Rate, errors and duration per service and operation, read off a materialized
view that rolls each receiving span (kind server or consumer) into a one-minute
bucket as it is inserted. No trace was assembled to get them. Counting only
receiving spans counts each request once, in the service that handled it; the
caller's client span for the same call is left out. A fraud check fails about
one time in twenty, so a few errors show up on `POST /fraud/score`.
`GET /health` is the container healthcheck, traced like any request.

Your errors, p99 values and `GET /health` count will differ: the fraud score is
random, timings depend on your machine, and the healthcheck runs every 10
seconds for as long as the stack is up. The 120 in every other row stays the
same, until you send more traffic. The view covers the last hour, so if it is
more than an hour since you sent traffic, the script tells you to send some.

What it runs:

```sql
SELECT service_name, span_name,
       countMerge(span_count) AS spans,
       countIfMerge(error_count) AS errors,
       round(quantileTDigestMerge(0.99)(duration_p99) / 1e6, 1) AS p99_ms
FROM tracing.red_service_minute
WHERE ts_bucket_start >= now() - INTERVAL 1 HOUR
GROUP BY service_name, span_name
ORDER BY service_name, spans DESC, span_name
```

The server-or-consumer filter lives in the view itself, in
`clickhouse/materialized_views.sql`:
`WHERE span_kind IN ('SPAN_KIND_SERVER', 'SPAN_KIND_CONSUMER')`.

## 7. Derive the service graph

```bash
./scripts/show-service-graph.sh
```

```
parent_service    child_service         call_count  p99_duration_ns  error_count
checkout-service  payment-service       120         94863300         0
checkout-service  inventory-service     120         33255750         0
payment-service   fraud-service         120         42314830         8
checkout-service  notification-service  120         13460041         0
```

Listing 5.6 joins each span to its parent and keeps the pairs where the two sit
in different services. Each such pair is one call: `checkout-service` calls
three services, and `payment-service` calls `fraud-service`. A pair inside one
service, such as `validate_cart` under `GET /checkout`, is internal work and is
filtered out. `p99_duration_ns` is the callee's p99 in nanoseconds, so 94863300
is about 95 ms.

Your p99 values and fraud errors will differ, and so will the order of the four
rows, since the listing sorts only by `call_count` and all four are tied at 120.
The four edges themselves are always the same. Like step 6, the query reads the
last hour only.

What it runs: `clickhouse/service_graph.sql`, which is listing 5.6 as printed.

## 8. Look inside the stream-time path

```bash
./scripts/show-flink-job.sh
```

```
job state                      RUNNING
spans into trace-assembly      1328
watermark behind wall clock    11.6s
checkpoints completed          2
last checkpoint size           16.9 KiB
checkouts in traces.assembled  120
spans in spans.late            0
```

`checkouts in traces.assembled` is the number of checkouts you have sent, 120
after step 2, and `spans.late` stays at 0. Those two print the same every time
you run the script. The topic also holds one trace per healthcheck, which the
count leaves out. `spans into trace-assembly` is the 1,320 checkout spans plus
one healthcheck span every 10 seconds, so it creeps up from run to run. The
watermark lag and checkpoint size depend on when you run the script.

The `trace-assembly` operator (listing 5.3) reads every span. Its watermark
(listing 5.5) trails the clock by the 5-second out-of-order bound, plus the
seconds spans spend in export batches on the way, plus the gap since the last
span arrived. A trace is emitted when the watermark passes its
first span plus 10 seconds. `spans.late` is where spans that arrive after their
trace has shipped go. On a clean local run it stays empty.

What it reads: `http://localhost:8081/jobs/<job_id>` and its `watermarks` and
`checkpoints` endpoints, then counts the committed records on the two output
topics, reading each to its end. Open `http://localhost:8081` for the Flink UI.

## 9. Stop a broker

Stop one of the three Kafka brokers, send 20 checkouts, and check both paths
still deliver every one whole:

```bash
./scripts/stop-a-broker.sh
```

```
stopping kafka-2...
sending 20 checkouts with kafka-2 down...
all 20 checkouts answered
query-time path: waiting for ClickHouse to hold all 11 spans of all 20 checkouts... ok
stream-time path: waiting for Flink to assemble the newest one and Jaeger to store it... ok
starting kafka-2 again...
waiting for kafka-2 to rejoin... ok
no checkout failed and no trace lost a span while a broker was down
```

Every topic keeps two copies of each partition, so one broker down loses
nothing. The script starts `kafka-2` again before it exits, even if a check
fails.

What it runs:

```text
docker compose stop kafka-2
curl -s http://localhost:8080/checkout    20 times
docker compose start kafka-2
```

## 10. Break atomicity on purpose

This step needs no stack. `benchmarks/atomicity_audit.py` builds 1,000 synthetic
traces of 8 spans, applies one of four failure modes at a 5 percent rate
(`FAILURE_RATE`), and fails if any trace comes out partial. `none` drops
nothing, `drop-whole-trace` drops 5 percent of the traces whole,
`producer-crash` drops 5 percent of the producer batches, and
`buffer-overflow` drops 5 percent of the spans:

```bash
./scripts/run-atomicity-audit.sh
```

```
failure mode        whole  absent  partial  verdict
none                 1000       0        0  PASS
drop-whole-trace      950      50        0  PASS
producer-crash        534       0      466  FAIL
buffer-overflow       678       0      322  FAIL
```

Dropping whole traces is allowed: 50 are gone, but none is half there. The two
failures lose single spans. `producer-crash` loses whole producer batches, and
since a batch carries spans of many traces, each lost batch leaves many traces
partial; the assembler cannot prevent that. `buffer-overflow` evicts random
spans inside the assembler, which is the mistake section 5.3.4 warns against.

What it runs:

```text
FAILURE_MODE=none              python3 benchmarks/atomicity_audit.py
FAILURE_MODE=drop-whole-trace  python3 benchmarks/atomicity_audit.py
FAILURE_MODE=producer-crash    python3 benchmarks/atomicity_audit.py
FAILURE_MODE=buffer-overflow   python3 benchmarks/atomicity_audit.py
```

## Run the tests

Offline, no Docker needed:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r tests/requirements.txt
python3 tests/test_static.py
python3 -m pytest -q app/test_checkout.py app/test_consumer_clickhouse.py flink/test_assembly_helpers.py benchmarks/test_atomicity_audit.py
```

Against the running stack:

```bash
bash tests/test_stack.sh
```

It sends its own 120 checkouts and checks both paths, that no stored checkout is
partial, that nothing reads `spans.late`, and that the Flink job is still alive
at the end.

Benchmarks are in [benchmarks/README.md](benchmarks/README.md). The last
recorded run is in [RESULTS.md](RESULTS.md).

## Tear down

```bash
docker compose down -v
deactivate 2>/dev/null
rm -rf .venv
```

## Reference

### Ports

| Port | What |
|---|---|
| 8080 | `checkout-service` |
| 4317, 4318 | OTel agent OTLP gRPC and HTTP |
| 8888 | OTel agent internal telemetry |
| 4327, 8889 | OTel gateway OTLP gRPC and internal telemetry |
| 8890 | Query-time consumer internal telemetry |
| 8891 | Stream-time consumer internal telemetry |
| 8123, 9000 | ClickHouse HTTP and native |
| 9363 | ClickHouse Prometheus endpoint |
| 8081 | Flink UI and REST API |
| 9249, 9250 | Flink jobmanager and taskmanager metrics |
| 16686 | Jaeger UI and API |
| 4319 | Jaeger OTLP gRPC |
| 9090 | Prometheus |

### Versions

| Component | Image |
|---|---|
| OpenTelemetry Collector (contrib) | `otel/opentelemetry-collector-contrib:0.154.0` |
| Apache Kafka (KRaft) | `apache/kafka:4.3.0` |
| ClickHouse | `clickhouse/clickhouse-server:25.8` |
| Apache Flink | `flink:2.2.1-scala_2.12-java17`, with `apache-flink==2.2.1` |
| Jaeger | `jaegertracing/jaeger:2.19.0` |
| Prometheus | `prom/prometheus:v3.12.0` |
| Python | 3.12 in the app image |
