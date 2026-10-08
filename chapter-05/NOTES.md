# Notes

Why this directory is built the way it is. Nothing here is needed to run
anything in the README. Read it when something surprises you, or before you
change the stack.

The short list:

- The stack starts and the Flink job dies a few minutes later:
  [give it the memory it asks for](#give-the-stack-the-memory-it-asks-for).
- Listing 5.6 prints no edges: [one process emits every span](#why-listing-56-finds-no-edges-here).
- A trace that never comes out of Flink: [what moves the watermark](#what-moves-the-watermark).
- Counting a topic with `kafka-get-offsets.sh`: [offsets overcount](#why-the-scripts-count-records-not-offsets).
- No stream-time traces in Jaeger: [why the sink streams are typed as raw bytes](#why-the-sink-streams-are-typed-as-raw-bytes).
- Changing `DECISION_WAIT_MS` and nothing happens: [tuning knobs](#tuning-knobs).

## The two paths

```text
                                       +--> consumer-clickhouse --> ClickHouse   (query-time)
                                       |
checkout -> otel-agent -> otel-gateway --> kafka otlp_spans (16 partitions, RF=2)
                                       |
                                       +--> otel-consumer --> Jaeger              (query-time)
                                       |
                                       +--> Flink assembly job                    (stream-time)
                                                 |
                                                 +--> traces.assembled --> otel-stream-consumer --> Jaeger
                                                 |
                                                 +--> spans.late  (audit only, nothing reads it)
```

Three readers consume the same `otlp_spans` topic at once, each in its own
consumer group, so each sees every span.

- **Query-time.** `app/consumer_clickhouse.py` writes each span to
  `tracing.otel_traces` as it arrives and commits the Kafka offset only after
  the insert succeeds. `otel-consumer` forwards the same spans to Jaeger and
  labels them `assembly.source=query-time`. Nothing is assembled until someone
  reads the trace.
- **Stream-time.** `flink/assembly_job.py` keys spans by trace ID, holds them in
  keyed state until an event-time timer fires `DECISION_WAIT_MS` (10 seconds)
  after the trace's first span, then emits the whole trace to
  `traces.assembled` and clears the state in the same step. `otel-stream-consumer`
  forwards those to Jaeger labelled `assembly.source=stream-time`.

So the same trace reaches Jaeger twice, once per label. That is what README step
3 counts. The query-time copy never has a hole because each span is written on
its own. The stream-time copy never has a hole because the whole trace is
emitted or none of it is. That is Figure 5.1's decision tree and Figure 5.7's
atomicity boundaries, side by side on one span stream.

Both paths depend on the gateway's `partition_traces_by_id: true` from chapter 4,
which puts every span of a trace on one partition. Without it Flink would see
split traces.

### Components

- **app/**: the Flask checkout service, the ClickHouse consumer, a reference
  Jaeger consumer, the scatter-gather query, a standalone agent helper.
- **flink/**: the PyFlink 2.2 assembly job and its Dockerfile.
- **clickhouse/**: the spans table (listing 5.1), the `red_service_minute`
  materialized view, and the service-graph query (listing 5.6).
- **collector/**: OTel agent, gateway, query-time consumer, stream-time consumer.
- **benchmarks/**: query-time write cost, stream-time buffer cost, the atomicity
  audit. See [benchmarks/README.md](benchmarks/README.md).
- **scripts/**: the README steps. `lib.sh` and `query.py` are shared plumbing.

## Metrics

Prometheus scrapes the four collectors, the Flink jobmanager and taskmanager,
and ClickHouse. Open `http://localhost:9090/targets` and every target should
read UP. Each collector reports its own pipeline counters there, which is the
first place to look when one path delivers and the other does not.

## Give the stack the memory it asks for

Both paths deliver every checkout with all seven spans when Flink has room. On a
Docker allocation under roughly 6 GB the Flink taskmanager is OOM-killed partway
through a run and the job fails with it. The traces it assembled before dying
stay in `traces.assembled`, so a dead stack looks healthy if you only count what
came out.

`tests/test_stack.sh` checks for exactly that: it requires `traces.assembled` to
grow during its own run rather than be non-empty, and it checks last that the
job is still `RUNNING` and the taskmanager was not OOM-killed. If either fails,
raise Docker's memory limit rather than trust the earlier passes. The repo-wide
[troubleshooting.md](../troubleshooting.md) has more.

## Why the checkout trace has seven spans

The checkout endpoint creates six spans itself: `validate_cart`,
`inventory.reserve`, `payment.charge`, `fraud.score` (a child of
`payment.charge`), `order.create` and `notification.send`. Flask's
auto-instrumentation adds a seventh, the root `GET /checkout` server span.

The same instrumentation traces the container healthcheck's `GET /health` every
10 seconds as a one-span trace. Those are not checkouts, so every check that
counts spans per trace scopes itself to trace IDs that carry a `GET /checkout`
span.

`fraud.score` is marked as an error when its random score is above 0.95, so
about one checkout in twenty carries an error span. That is what the error
columns in steps 6 and 7 count.

## Why listing 5.6 finds no edges here

Listing 5.6 joins each span to its parent and names both ends by
`service_name`. In this stack one process, `checkout-service`, emits every span.
`inventory-service`, `payment-service`, `fraud-service` and
`notification-service` exist only as the `peer.service` attribute on the client
and producer spans that call them. So every parent-child pair is inside
`checkout-service`, and the listing's last filter, `parent_service !=
child_service`, removes all of them, which is what it is meant to do with
internal work.

That is why `scripts/show-service-graph.sh` runs the listing as printed, then
runs the same self-join with the callee named by `peer.service`. Section 5.4.1
names both routes: the service identity of the callee is either on the child
span or derivable from the client span that called it. In a deployment where
each service runs its own SDK, the listing as printed is the one that finds the
edges.

## What moves the watermark

The job assigns one watermark after the source: the highest span start time
seen, minus `OUT_OF_ORDER_SEC` (5 seconds). A trace's timer fires when the
watermark passes its first span's start plus `DECISION_WAIT_MS`. The watermark
only moves when a new span arrives, so the newest trace waits for later
traffic. Here the healthcheck's one-span trace every 10 seconds keeps it moving,
so even the last checkout you sent comes out soon after the next healthcheck
span pushes the watermark past its timer. That is why `wait-until-ready.sh` waits on the newest checkout's
stream-time copy: Flink emits in event-time order, so it is the last one out.

With no traffic at all, nothing would move the watermark and the last traces
would sit in keyed state until the next span arrived. The chapter's idle
partition discussion is the same effect one level down.

## Why the sink streams are typed as raw bytes

Both Kafka sinks serialize with `ByteArraySchema`, which writes the Java
`byte[]` it is handed. Until this was fixed, the assembled-trace stream and the
late-span side output were typed `PICKLED_BYTE_ARRAY`, so the sinks were handed
a pickle of the Python bytes. Every record on `traces.assembled` started with a
pickle header, and `otel-stream-consumer` rejected all of them as malformed
OTLP. The topic's offsets kept growing the whole time, so the stack test, which
only counted offsets, passed. Both streams are now typed
`PRIMITIVE_ARRAY(BYTE())`, `tests/test_static.py` pins that, and
`tests/test_stack.sh` now follows one checkout all the way to Jaeger.

## Why the scripts count records, not offsets

Both Flink sinks write inside Kafka transactions (`DeliveryGuarantee.EXACTLY_ONCE`).
Every transaction commit writes a control record to each partition it touched,
and a control record takes an offset like any other record. So the sum of end
offsets from `kafka-get-offsets.sh` is higher than the number of traces on
`traces.assembled`. `scripts/lib.sh` reads the topic with
`--isolation-level read_committed` and counts records instead. `tests/test_stack.sh`
still uses end offsets, which is fine there: it only checks that the topic grew
by at least half the checkouts it sent.

Downstream readers of `traces.assembled` and `spans.late` must read with
`isolation.level=read_committed`, or they see records from transactions that
were later aborted.

## Late spans

Under a clean local run with one clock, `spans.late` stays empty. A span goes
there when its start time is behind the watermark when it arrives, or when its
trace has already shipped. The job keeps an `emitted` tombstone per trace,
expiring after `EMITTED_TTL_MIN` (10 minutes), so a straggler cannot open a
second one-span trace under the same ID. Nothing consumes `spans.late`;
`tests/test_stack.sh` checks that no consumer group reads it.

Flink's own `numLateRecordsDropped` metric does not count these. That counter
belongs to Flink's window operators, and this job routes late spans itself
through a side output, so the topic is the only count.

To provoke late spans you need event times that run behind the watermark, for
example a container whose clock is skewed, or heavy producer load that delays
one partition.

## Atomicity boundaries in this stack

Figure 5.7's four boundaries map onto the stack like this:

1. The gateway's `partition_traces_by_id` sends a trace's spans to one
   partition, but a producer batch carries spans of many traces, so a lost batch
   leaves partial traces behind. This boundary stays unprotected, which is what
   the audit's `producer-crash` mode models.
2. The Flink Kafka source commits its offsets at checkpoints, in step with the
   assembled-trace emit.
3. Flink keyed-state eviction drops whole traces, never single spans.
4. The collector OTLP exporters acknowledge on success.

## The atomicity audit

`benchmarks/atomicity_audit.py` is a self-contained model of the audit logic,
not a probe of the running stack. It generates 1,000 synthetic traces of 8
spans in memory with a fixed seed, applies one failure mode at a 5 percent rate,
and accepts only two outcomes per trace: all spans present, or none. A partial
trace is silent data loss and fails the audit.

- `none` passes: nothing was lost.
- `drop-whole-trace` passes: 50 traces are gone, but each is gone whole, which is
  controlled degradation.
- `producer-crash` fails. A trace's spans leave different hosts through
  different gateways, so a producer batch never holds a whole trace, and a lost
  batch takes part of many traces with it.
- `buffer-overflow` fails. It evicts random spans inside the assembler, the
  failure mode section 5.3.4 calls unacceptable.

The detection logic is what you would run against real assembled traces.
Wiring it to `traces.assembled` or ClickHouse is left as an exercise.
`scripts/run-atomicity-audit.sh` runs a copy in a scratch directory, because the
audit writes a dated result file next to itself, and the committed results that
`RESULTS.md` is rendered from should not change because a reader ran it.

## Scatter-gather on one shard

`app/scatter_gather_query.py` queries every host in `CLICKHOUSE_SHARDS` in
parallel, gathers the spans and rebuilds the parent-child tree in memory. This
stack has one ClickHouse, so the fan-out is one request. The script still prints
each shard's latency separately, because Figure 5.5's point is that the slowest
shard sets the latency of the whole query. Point `CLICKHOUSE_SHARDS` at a
comma-separated list of hosts to see a real fan-out.

`scripts/assemble-trace.sh` runs it inside the `consumer-clickhouse` container,
which already has the ClickHouse driver installed, so the reader does not need a
Python virtualenv for it.

## Storage layout

`tracing.otel_traces` is a plain MergeTree. Every insert writes a new immutable
part and background merges combine small parts into larger ones, so the part
count in step 5 goes up with each consumer flush and then down. Parts are
partitioned by hour and sorted by `(trace_id, timestamp)`, so one trace's spans
sit in one contiguous range in each part, and assembling a trace is a primary
key seek per part.

Plain MergeTree keeps duplicates. A span delivered twice under chapter 4's
at-least-once contract lands as two rows. The scripts count spans with
`uniqExact(span_id)` for that reason.

## RED metrics without assembly

`clickhouse/materialized_views.sql` rolls every inserted span into a
per-service, per-operation, per-minute bucket of `AggregateFunction` states.
Reading it merges the states, so dashboards get rate, errors and duration
without assembling a single trace. This is the aggregate-first pattern section
5.4.2 describes.

## Stopping a broker

Replication factor 2 with `min.insync.replicas=1` keeps every partition
writable with one broker down. The checkout service never notices either way,
because the SDK exports spans asynchronously. The real check is that every
checkout sent while the broker was down still reaches ClickHouse with all seven
spans and comes out of Flink assembled. `scripts/stop-a-broker.sh` starts the
broker again on exit even if a check fails.

## Tuning knobs

- `DECISION_WAIT_MS` (default 10000) and `OUT_OF_ORDER_SEC` (default 5) are read
  from the environment by `flink/assembly_job.py`. `docker-compose.yml` does not
  set them. To change them, add them to the `environment:` of both
  `flink-job-submit` (which builds the job) and `flink-taskmanager` (which runs
  it), then `docker compose up -d`. The chapter's text uses 30 seconds for
  `decision_wait`; this stack uses 10 so the demo turns over quickly. Raise it and
  keyed state grows in proportion.
- `state.backend.type` is `hashmap` (heap state) in the `FLINK_PROPERTIES` block
  of `docker-compose.yml`. The chapter compares RocksDB and ForSt. `rocksdb` works
  here as is. ForSt needs an S3-compatible store configured.
- `BATCH_SIZE` (default 1000) and `BATCH_TIMEOUT_S` (default 2.0) on
  `consumer-clickhouse` set the query-time path's write batching. They are not
  set in the compose file either.
- `EMITTED_TTL_MIN` (default 10) bounds how long a shipped trace's tombstone
  lives.

A note on hot partitions: if you load-test with a small pool of trace IDs, one
Kafka partition takes most of the traffic and one key range of Flink state takes
most of the memory. That is section 5.2.3's failure mode. Prometheus scrapes no
Kafka metrics in this stack, so look at per-partition end offsets instead:

```text
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:9093 --topic otlp_spans
```

## Pinned versions

The versions in `docker-compose.yml` follow chapter 4 and the chapter text.
`flink/Dockerfile` installs `apache-flink==2.2.1` from pip with the matching
Kafka connector jar. If no wheel for your platform is published yet, lower that
pin to the nearest `apache-flink==2.2.x` (the `KeyedProcessFunction` API the job
uses is the same) and rebuild with
`docker compose build flink-jobmanager flink-taskmanager flink-job-submit`.

## Running the book's listings verbatim

The book prints readable excerpts. The files differ from them in these ways:

- **5.1** is trimmed in the book to the columns the chapter's queries read.
  `clickhouse/init.sql` carries the full table.
- **5.2** is a query with a literal trace ID in the book. In
  `app/scatter_gather_query.py` it is parameterized, returns the start time as
  nanoseconds for the waterfall, and does not fetch `span_attributes`.
- **5.3** and **5.5** are sketches with simplified types. `flink/assembly_job.py`
  has the real PyFlink: byte-array state, a TTL on the tombstone, the span
  exploder ahead of the watermark, and the two exactly-once sinks.
- **5.4** (`loadbalancingexporter`) is not in this stack. It is the Kafka-free
  route to trace-aware routing; this stack routes by trace ID through Kafka's
  `partition_traces_by_id` instead, per chapter 4.
- **5.6** is in `clickhouse/service_graph.sql` exactly as printed.
