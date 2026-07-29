# Chapter 5: Trace Assembly and Processing Patterns

Code and exploration exercises for Chapter 5 of *Tracing Infrastructure in Action*.

This chapter contrasts store-then-stitch (Tempo, Jaeger, SigNoz, X-Ray) with
stream-then-store (Edgar, Refinery, Infinite Tracing, Salesforce). Both
topologies coexist in this stack so the reader can compare them on the same
span stream. The atomicity imperative governs every boundary: drop whole
traces, never partial spans.

## Prerequisites

- Docker and Docker Compose
- Python 3.10+ (only for the local benchmark runs)
- Around 6 GB of free RAM for the full stack

## Architecture

```
                                      +--> consumer-clickhouse --> ClickHouse  (storage-time path)
                                      |
checkout -> otel-agent -> otel-gateway --> kafka (otlp_spans, 16 partitions, RF=2)
                                      |
                                      +--> otel-consumer --> Jaeger v2   (storage-time control)
                                      |
                                      +--> flink-jobmanager + taskmanager (stream-time assembly)
                                                  |
                                                  v
                                          traces.assembled (kafka)
                                                  |
                                                  v
                                          otel-stream-consumer --> Jaeger v2  (stream-time path)
                                                  |
                                                  +--> spans.late (kafka)
```

Both paths key spans by trace_id through the gateway's `partition_traces_by_id`
contract from chapter 4. The Flink job is a `KeyedProcessFunction` keyed on
trace_id with a `decision_wait` event-time timer of 10 seconds. Late spans
land in `spans.late` via a side output and never re-enter the main pipeline.

### Components

- **app/**: Flask checkout producer, ClickHouse consumer, Jaeger consumer
  reference implementation, scatter-gather query, standalone agent helper.
- **flink/**: PyFlink 2.2 assembly job, runtime config, Dockerfile.
- **clickhouse/**: storage schema (`otel_traces` wide table) and
  materialized views (`red_service_minute`, `service_graph_minute`).
- **collector/**: OTel agent, gateway, storage-time consumer, stream-time consumer.
- **benchmarks/**: storage-time write cost, stream-time buffer cost,
  atomicity audit.

## Listings

| Listing | File | Pattern |
|---------|------|---------|
| 5.1 | `app/scatter_gather_query.py` | ClickHouse trace-assembly query |
| 5.2 | `flink/assembly_job.py` | KeyedProcessFunction skeleton for keyed trace assembly |
| 5.4 | `flink/assembly_job.py` | Bounded watermark strategy and late-span side-output routing |

Not mapped: 5.3 (`loadbalancingexporter` config for Kafka-free trace-aware routing — this stack routes by trace ID through Kafka's `partition_traces_by_id` instead, per chapter 4), 5.5 (service graph derivation query — the walkthrough command in step 8 below covers the same self-join pattern over a shorter demo window, but uses a different join type and quantile function than the book listing, so it is not a match).

### Pinned versions

| Component | Version |
|---|---|
| Apache Kafka | 4.3.0 (KRaft) |
| OpenTelemetry Collector contrib | 0.154.0 |
| Jaeger | 2.19.0 |
| Apache Flink | 2.2.1 |
| ClickHouse | 25.8 (LTS) |
| Prometheus | v3.12.0 |

## Running

```bash
docker compose up -d
```

This starts the full topology. Give it 60 to 90 seconds to settle: Kafka has
to elect controllers, the topics have to land, ClickHouse has to apply the
schema, and the Flink job has to submit. Watch progress with:

```bash
docker compose ps
docker compose logs -f flink-job-submit
```

## Verify it works

The stack ships as code only. Unit tests in `app/`, `flink/`, and `benchmarks/`
pass cleanly (run `python3 -m pytest` from this directory) and
`docker compose config` is a clean YAML parse, but a live end-to-end run has
not been re-verified in this commit. Pin versions in `docker-compose.yml` were chosen to match Ch4
and the chapter prose. The `flink/Dockerfile` pulls `apache-flink==2.2.1` from
pip and the matching `flink-sql-connector-kafka-5.0.0-2.2.jar`; if a wheel for
the Python version you're on is not yet published, downgrade that pin to the
closest available `apache-flink==2.2.x` (the `KeyedProcessFunction` API the
assembly job uses is identical) and re-run `docker compose build flink`.

Each step below proves one of the chapter's figures or claims with a
runnable command. Run them in order after the stack settles.

### 1. Generate ten minutes of traffic

```bash
for i in $(seq 1 600); do
    curl -s http://localhost:8080/checkout > /dev/null
    sleep 1
done &
echo "traffic loop PID: $!"
```

This runs in the background (the trailing `&`), so steps 2 through 10 below
execute concurrently against live traffic. Note the printed PID and stop the
loop when you are done with `kill <PID>` (or run `jobs` then `kill %1`).

The checkout endpoint produces a six-span trace per call across five
services, which is the depth Figure 5.7's service-graph derivation needs.

### 2. Prometheus targets are healthy (proves the metrics surface)

```bash
open http://localhost:9090/targets    # macOS; use xdg-open on Linux
```

All targets (otel-agent, otel-gateway, otel-consumer, otel-stream-consumer,
flink-jobmanager, flink-taskmanager, clickhouse) should show as UP.

### 3. Jaeger has traces from both paths (proves F5.1 + F5.6)

```bash
open http://localhost:16686
```

In the Jaeger UI, the Service dropdown will show `checkout-service`. Each
trace carries the `assembly.source` resource attribute set by the consumer
collectors. Filter by:

- `assembly.source=store-then-stitch` for the storage-time path
- `assembly.source=stream-then-store` for the stream-time path

The same logical trace appears under both labels because both consumers
read the same `otlp_spans` topic. The storage-time path emits each span as
it arrives. The stream-time path holds the whole trace in keyed state for
the decision_wait window, then emits the assembled trace at once. Figure
5.1's decision tree and Figure 5.6's atomicity boundaries both manifest
here: the storage-time row never holds a hole because spans are written
independently, and the stream-time row never holds a hole because the
whole trace emits or none of it does.

### 4. ClickHouse has spans (proves F5.3 block layout)

```bash
docker compose exec clickhouse clickhouse-client --query \
    "SELECT count() FROM tracing.otel_traces \
     WHERE timestamp > now() - INTERVAL 10 MINUTE"
```

Expect a count climbing with traffic. Inspect the block layout:

```bash
docker compose exec clickhouse clickhouse-client --query \
    "SELECT partition, name, rows, bytes_on_disk, primary_key_bytes_in_memory \
     FROM system.parts WHERE database='tracing' AND active=1 \
     ORDER BY modification_time DESC LIMIT 10 FORMAT PrettyCompact"
```

This is the MergeTree column of Figure 5.3: parts keyed by
`(trace_id, timestamp)`, partitioned by hour, with ZSTD compression on the
column codecs.

### 5. RED metrics roll up (proves F5.7 and section 5.4.2)

```bash
docker compose exec clickhouse clickhouse-client --query \
    "SELECT service_name, \
            countMerge(span_count) AS spans, \
            countIfMerge(error_count) AS errors, \
            quantileTDigestMerge(0.99)(duration_p99) AS p99_ns \
     FROM tracing.red_service_minute \
     WHERE ts_bucket_start > now() - INTERVAL 10 MINUTE \
     GROUP BY service_name ORDER BY spans DESC FORMAT PrettyCompact"
```

The materialized view aggregates spans into per-service per-minute buckets
without ever assembling a trace. This is the aggregate-first pattern that
Lightstep and Datadog Live Search run at the high end of the volume axis.

### 6. Flink keyed-state metrics (proves F5.5 watermark lifecycle)

The Flink UI shows the job, the per-task state size, watermark lag, and the
late-span counter.

```bash
open http://localhost:8081
```

Click into the `chapter5-trace-assembly` job, then the `trace-assembly`
operator. The metrics tab surfaces:

- `numRecordsIn` (spans arriving from Kafka)
- `numRecordsOut` (assembled traces emitted)
- `currentInputWatermark` (the watermark Figure 5.5 walks)
- `numLateRecordsDropped` (spans diverted to the side output)
- `lastCheckpointSize` (the keyed-state size at each checkpoint)

Confirm the late-span side output is wired by checking the `spans.late`
topic:

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka-1:9093 --topic spans.late \
    --max-messages 1 --timeout-ms 5000
```

Under a clean local run with synchronized clocks, this topic stays at zero.
It is the metric that fires when the watermark policy has work to do.

### 7. Scatter-gather query (proves F5.4)

Pick a trace_id out of ClickHouse:

```bash
TID=$(docker compose exec -T clickhouse clickhouse-client --query \
    "SELECT trace_id FROM tracing.otel_traces ORDER BY timestamp DESC LIMIT 1")
echo "trace_id=$TID"
```

Then run the scatter-gather query from the host:

```bash
pip install -r app/requirements.txt
CLICKHOUSE_HOST=localhost python app/scatter_gather_query.py "$TID"
```

Expected output: a tail-latency-bounded fan-out (one shard in the dev
stack), followed by an in-memory assembly of the parent-child waterfall.
The script prints each shard's response latency separately because Figure
5.4's claim is that the slowest shard owns the p99 of the whole query.

### 8. Service graph (proves F5.7 + listing 5.5)

```bash
docker compose exec clickhouse clickhouse-client --query "$(cat <<'SQL'
SELECT
    parent_service,
    child_service,
    count() AS call_count,
    quantileExact(0.99)(duration) AS p99_duration_ns,
    countIf(status_code = 'STATUS_CODE_ERROR') AS error_count
FROM (
    SELECT
        s.service_name AS child_service,
        p.service_name AS parent_service,
        s.duration,
        s.status_code
    FROM tracing.otel_traces AS s
    LEFT JOIN tracing.otel_traces AS p
        ON s.trace_id = p.trace_id AND s.parent_span_id = p.span_id
    WHERE s.timestamp >= now() - INTERVAL 10 MINUTE
)
WHERE parent_service != ''
GROUP BY parent_service, child_service
ORDER BY call_count DESC
FORMAT PrettyCompact
SQL
)"
```

This is the streaming-aggregation pattern of section 5.4.1 expressed as a
ClickHouse self-join. Every span emits one edge contribution and the graph
weighted by call count falls out.

### 9. Atomicity audit (illustrates the atomicity imperative)

`atomicity_audit.py` is a self-contained model of the audit logic. It does not
read the running stack; it generates synthetic traces in memory, applies one
failure mode, and asserts the only acceptable outcomes are whole traces present
or whole traces absent. A partial trace is silent data loss and fails the audit.
It demonstrates how you would detect a partial-trace violation; wiring it to the
live `traces.assembled` topic is left as an exercise.

```bash
cd benchmarks
python atomicity_audit.py                                # PASS (clean run)
FAILURE_MODE=producer-crash    python atomicity_audit.py # PASS (whole-trace drops)
FAILURE_MODE=drop-whole-trace  python atomicity_audit.py # PASS (controlled degradation)
FAILURE_MODE=buffer-overflow   python atomicity_audit.py # FAIL (random-span eviction)
```

The audit fails on `buffer-overflow` because that mode evicts random spans
inside the assembler, exactly the failure mode section 5.3.4 calls out as
unacceptable. The audit passes on `drop-whole-trace` because evicting whole
traces preserves the imperative even under controlled degradation.

### 10. Failure test: a broker drops

Stop one Kafka broker and confirm the producer keeps working and the Flink
job keeps assembling. Replication factor 2 tolerates one node down.

```bash
docker compose stop kafka-2
curl http://localhost:8080/checkout    # still succeeds, gateway holds buffer
docker compose start kafka-2
```

The blast radius of one broker loss stays inside the Kafka tier. The
producer never sees an error, the storage-time consumer never drops a
span, and the Flink keyed state never corrupts.

## Tuning knobs worth poking

- `DECISION_WAIT_MS` (Flink env): the chapter's central trade-off, set on
  the `flink-jobmanager` and `flink-taskmanager` services. Default 10000
  matches New Relic's Infinite Tracing. Raise to 30000 to match the
  OpenTelemetry tail sampler default; watch keyed-state size grow.
- `OUT_OF_ORDER_SEC` (Flink env): bounded out-of-orderness for the
  watermark. Default 5. Lower for single-AZ; raise for cross-region.
- `state.backend.type` (`flink/conf/flink-conf.yaml`): switch from RocksDB
  to ForSt to demonstrate the disaggregated-state recovery characteristic
  from footnote [^16]. ForSt needs an S3-compatible target configured.
- `BATCH_SIZE`, `BATCH_TIMEOUT_S` (`consumer-clickhouse` env): the
  storage-time path's write batching. Tune to trade ingestion latency for
  throughput.

## Operational notes

- **Resource attribute label**: `assembly.source` is set by each consumer
  collector (`store-then-stitch` or `stream-then-store`). Use it to compare
  the two paths inside Jaeger or ClickHouse.
- **Atomicity boundaries** (Figure 5.6): the four boundaries are realized
  in this stack as: (1) the gateway's `partition_traces_by_id`, (2) the
  Flink Kafka source offset commit at checkpoint, (3) the Flink keyed-state
  eviction policy (drop-whole-trace, never drop-random-spans), and (4) the
  collector OTLP exporter's `tls.insecure` ack-on-success semantics.
- **Hot-partition watch**: if you load-test with a small pool of trace IDs,
  one Kafka partition will dominate ingestion and one Flink keyed-state
  shard will dominate state size. This is the hot-partition failure mode
  from section 5.2.3. Watch `kafka_log_log_size` per partition in
  Prometheus to see it.
- **Late-span audit**: under a clean stack with synchronized clocks the
  `spans.late` topic stays empty. Under heavy producer load or a
  deliberately skewed clock on one container, late spans appear, and the
  Flink `numLateRecordsDropped` counter increments. That divergence is the
  alert section 5.3.4 names as the one that matters.

## Tear down

```bash
docker compose down -v
```

The `-v` flag drops the named volumes. Drop it to keep ClickHouse, Kafka,
and Flink state across restarts.
