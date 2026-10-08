# Chapter 7: Trace Storage Patterns

Runnable companion to chapter 7 of *Tracing Infrastructure in Action*.

It stands up the span-per-row store from section 7.2 (ClickHouse) and makes the
chapter's DDL listings real: the listing 7.1 schema, the listing 7.2 hot-to-cold
tiering policy, and the listing 7.4 per-tenant row policy.

```
checkout -> otel-collector (partition_traces_by_id) -> kafka(otlp_spans)
         -> consumer-clickhouse -> ClickHouse otel_traces  (listing 7.1)
         -> Grafana Tempo                                   (section 7.3)
```

The Collector sends every span to two stores. ClickHouse keeps it as a row you
scan with SQL. Grafana Tempo, the block archetype from section 7.3, keeps it in
an immutable block you look up by trace id. Both write to the same object
store: a SeaweedFS service stands in for AWS S3, GCS or Azure Blob.

The stack is continuous with `chapter-05/`. The checkout producer and the
Collector's `partition_traces_by_id` export are the same. Chapter 7 is about the
store, so there is one Kafka broker instead of three, no Flink and no Jaeger.

Steps 1 to 7 walk through the data. The three exercises after them each take
one listing apart. The why behind each design choice is in
[NOTES.md](NOTES.md). You don't need it to follow along.

## Listings

| Listing | File | Pattern |
|---------|------|---------|
| 7.1 | `clickhouse/init.sql` | ClickHouse trace table sized for compression and retention |
| 7.2 | `clickhouse/tiering.sql` | Hot-to-cold tiering policy |
| 7.3 | `clickhouse/compression.sql` | Per-column compression verification query |
| 7.4 | `clickhouse/tenancy.sql` | Row-level tenant isolation |

## Before you start

- Docker and Docker Compose v2, with about 3 GB of memory given to Docker
  (Docker Desktop: Settings, Resources) and about 4 GB of free disk.
- Python 3 and `curl` on your machine. On Windows, use WSL2.
- Stop any other chapter's stack first (`docker compose ls`). This one uses
  ports 3200, 4317, 4318, 4417, 8080, 8123, 8333, 8888, 9000, 9090 and 9363.

Run every command from the `chapter-07/` directory. Each step runs a small
script from `scripts/` and prints what it found. The query inside is shown
under the step, so you can run it yourself.

## 1. Start the stack

```bash
docker compose up -d --build --wait
docker compose ps
```

The first run pulls six images and builds the app image, so give it a few
minutes. `--wait` returns once every service is up. You should see eight
services running. `kafka-init` is a one-shot job that creates the Kafka topic
and exits, so it is not in the list.

## 2. Send some traffic

300 checkouts, one after another. Then wait for them to land in both stores:

```bash
./scripts/send-traffic.sh
./scripts/wait-until-ready.sh
```

```
sending 300 checkouts, one at a time (about a minute)...
sent 300 checkouts, 0 did not answer
waiting for all 2100 spans to reach ClickHouse... ok
waiting for the last checkout trace to reach Tempo... ok
ready
```

Each checkout makes a seven-span trace. A span takes the app's five-second
export timer, the Collector, Kafka and the consumer's batch on its way to
ClickHouse, so the second script counts the spans that arrived instead of
sleeping for a guessed time. Then it asks Tempo for the last trace by id.

What runs: `curl -s http://localhost:8080/checkout`, 300 times.

## 3. Look at the table

```bash
./scripts/show-schema.sh
```

You see the exact columns, the codecs (`Delta(8), ZSTD(1)`, `T64, ZSTD(1)`,
`ZSTD(3)`), the `bloom_filter` index on `trace_id`, `PARTITION BY
toYYYYMMDD(timestamp)` and `ORDER BY (service_name, span_name,
toStartOfHour(timestamp), trace_id)`. That is listing 7.1, plus the
`adjusted_count` column chapter 8 reads. What runs:

```sql
SHOW CREATE TABLE tracing.otel_traces FORMAT TSVRaw
```

## 4. Look at three traces

A nine-column table does not look much like a trace. This pulls the three most
recent checkout traces and marks each one's first span with `>`:

```bash
./scripts/show-recent-traces.sh
```

```
t  trace        time             span               took_ms  weight  status_code
>  2be478a0...  03:44:40.927588  GET /checkout      177.792       1  STATUS_CODE_UNSET
   2be478a0...  03:44:40.928170  validate_cart           21       1  STATUS_CODE_UNSET
   2be478a0...  03:44:40.949242  inventory.reserve   31.789       1  STATUS_CODE_UNSET
   2be478a0...  03:44:40.981199  payment.charge      91.909       1  STATUS_CODE_UNSET
   2be478a0...  03:44:41.032069  fraud.score         40.994       1  STATUS_CODE_UNSET
   2be478a0...  03:44:41.073205  order.create        20.749       1  STATUS_CODE_UNSET
   2be478a0...  03:44:41.094068  notification.send   11.014       1  STATUS_CODE_UNSET
>  40b7f935...  03:44:41.301452  GET /checkout       180.66       1  STATUS_CODE_UNSET
   ...
```

Your ids and times will differ. Seven rows per `>`, and the root span's 178ms
covers the six children that follow it. Those rows arrived independently, at
different times, possibly out of order, and nothing put them together until
this query sorted by `trace_id` and `timestamp`. That is the store-then-stitch
contract: spans land as rows, and a trace is something you rebuild when you
read.

`weight` is `adjusted_count`, the sample-rate reciprocal from section 7.4.3.
Nothing here is sampled, so it reads 1. What runs:

```sql
SELECT
  if(trace_id != lagInFrame(trace_id) OVER (ORDER BY trace_id, timestamp), '>', '') AS t,
  concat(substring(trace_id, 1, 8), '...') AS trace,
  formatDateTime(timestamp, '%H:%i:%S.%f') AS time,
  span_name AS span,
  round(duration_ns / 1000000.0, 3) AS took_ms,
  adjusted_count AS weight,
  status_code
FROM tracing.otel_traces
WHERE trace_id IN (
  SELECT trace_id FROM tracing.otel_traces
  WHERE span_name = 'GET /checkout' ORDER BY timestamp DESC LIMIT 3)
ORDER BY trace_id, timestamp
```

The app fails about one checkout in twenty at `fraud.score`, so three traces
usually come back clean. To find the ones that did not:

```bash
./scripts/show-error-spans.sh
```

```
trace_id                          span_name    status_code
ebaca499b32272912dbc0b332fd2e278  fraud.score  STATUS_CODE_ERROR
030739c8cb340f0378ae4fe8de87668d  fraud.score  STATUS_CODE_ERROR
1798a78f546e384e08978b0ed1618bac  fraud.score  STATUS_CODE_ERROR
fe362e1dfb8211b36f54ae9625ec8517  fraud.score  STATUS_CODE_ERROR
cbdc9bcfa0a06b52f001fc0d2f3d4d11  fraud.score  STATUS_CODE_ERROR
```

What runs:

```sql
SELECT trace_id, span_name, status_code FROM tracing.otel_traces
WHERE status_code = 'STATUS_CODE_ERROR' ORDER BY timestamp DESC LIMIT 5
```

## 5. See where the bytes are

```bash
./scripts/show-partitions.sh
```

```
partition  on_disk    rows  parts  disk
 20261008  51.88 KiB  2108      3  default
```

One partition per day, which is what `PARTITION BY toYYYYMMDD(timestamp)` asks
for, all on the local `default` disk. The tiering exercise moves one to S3.
What runs:

```sql
SELECT partition, formatReadableSize(sum(bytes_on_disk)) AS on_disk,
       sum(rows) AS rows, count() AS parts, any(disk_name) AS disk
FROM system.parts
WHERE database = 'tracing' AND table = 'otel_traces' AND active
GROUP BY partition ORDER BY partition
```

## 6. Look up one trace by id

```bash
./scripts/look-up-one-trace.sh
```

```
trace_id = 40b7f935f3629dc430b48fbab3393fe9

span_name          took_ms
GET /checkout        180.7
validate_cart         21.8
inventory.reserve     32.3
payment.charge        93.8
fraud.score             42
order.create          20.7
notification.send       11
```

The trace is the last checkout step 2 sent. `trace_id` is random, so the sort
key cannot narrow the search. The bloom filter index on it does: it rules out
most of the table without reading the column. What runs:

```sql
SELECT span_name, round(duration_ns / 1000000.0, 1) AS took_ms
FROM tracing.otel_traces WHERE trace_id = '$TID' ORDER BY timestamp
```

## 7. The same trace in the block store

Ask both stores for that trace:

```bash
./scripts/compare-stores.sh
```

```
trace_id = 40b7f935f3629dc430b48fbab3393fe9

ClickHouse (rows):
span_name          took_ms
GET /checkout        180.7
validate_cart         21.8
...

Tempo (block):
span_name         took_ms
GET /checkout       180.7
validate_cart        21.8
...
```

Seven spans either way, same names, same durations. One workload, one trace,
two layouts. ClickHouse scans rows for the id. Tempo finds the block that holds
it and reads the trace out whole. The Tempo half is
`curl -s http://localhost:3200/api/traces/<trace_id>`.

`collector/tempo.yaml` needs Tempo 2.9 or newer. The stack pins 3.0.2. On 2.8
or older Tempo refuses to start, because its `live_store` section did not exist
yet. The fix is to upgrade, not to edit the config back.

Both stores write to the same SeaweedFS. Look at the buckets:

```bash
./scripts/show-buckets.sh
```

```
  tempo-blocks	size:425536	logical:425536	chunk:10
  traces-cold	size:200	logical:130	chunk:0

tempo-blocks
├──single-tenant
│   ├──0db9aff4-6e89-4b66-9a9b-fc9b8af3507f
│   │   ├──bloom-0
│   │   ├──data.parquet
│   │   ├──index
│   │   └──meta.json
...
```

`tempo-blocks/` holds Tempo's blocks, each one a `data.parquet` plus its index
and `meta.json` under a block uuid. `traces-cold/` holds ClickHouse's aged
parts, and stays empty until the tiering exercise moves one. Same store, same
API, a completely different unit of storage. What runs:

```bash
echo "s3.bucket.list" | docker compose exec -T seaweedfs weed shell
echo "fs.tree /buckets/tempo-blocks" | docker compose exec -T seaweedfs weed shell
```

The retention boundary differs in the same way:

```bash
grep -B3 -A1 'block_retention' collector/tempo.yaml
grep -B2 -A6 'TTL' clickhouse/tiering.sql
```

Tempo expires whole blocks. Listing 7.2 moves and then deletes rows by a TTL
evaluated per part. Same two days, different unit of work, and that difference
is the contrast section 7.3 draws.

There is no Grafana in this stack, so Tempo has no UI. Its HTTP API is how you
query it.

**Try this.** Search Tempo by service and span name instead of by id. That is
the query a block store is worst at:

```bash
./scripts/search-tempo-by-service.sh
```

```
Tempo:
2b496ce2f33cbe6194b0ab3cccb528c GET /checkout
2b0a5bb61e81c4829f053aa4cf0820b GET /checkout
...
Tempo read 90913 bytes of blocks to answer

ClickHouse:
trace_id                          span_name
40b7f935f3629dc430b48fbab3393fe9  GET /checkout
9a1d0d5a7dadcc2b559722ad96d06627  GET /checkout
...
```

Tempo has to open blocks and read them to find matches. ClickHouse reads the
`service_name` and `span_name` columns, which lead its sort key, so it jumps
straight to the rows. That asymmetry, not the storage medium, is why section 7.3
says the two archetypes suit different questions.

The two lists name different traces: Tempo returns the first five it finds, not
the newest. It also prints an id without its leading zero, so some of its ids
are 31 characters long. What runs:

```bash
curl -s -G http://localhost:3200/api/search --data-urlencode 'q={ resource.service.name = "checkout-service" && name = "GET /checkout" }' --data-urlencode 'limit=5' | python3 -m json.tool | head -40
```

```sql
SELECT trace_id, span_name FROM tracing.otel_traces
WHERE service_name = 'checkout-service' AND span_name = 'GET /checkout'
ORDER BY timestamp DESC LIMIT 5
```

## Exercises

Each exercise starts from a running stack and cleans up after itself, so do
them in any order. None needs the traffic from step 2.

| Exercise | Listing | The question |
|---|---|---|
| [exercises/compression.md](exercises/compression.md) | 7.1, 7.3 | `LowCardinality` and a codec per column change no answers. What do they buy? |
| [exercises/tiering.md](exercises/tiering.md) | 7.2 | Aged parts move `TO VOLUME 'cold'`. Where do they actually go, and is the data still there? |
| [exercises/tenancy.md](exercises/tenancy.md) | 7.4 | A row policy stops one tenant reading another's spans. What does it not stop? |

If you only do one, do tenancy. There is a trap in it.

## Run the tests

Offline, no Docker needed. [`setup/README.md`](../setup/README.md) has the
virtualenv step for Debian, Ubuntu and Windows, which differ a little:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r tests/requirements.txt
python3 tests/test_static.py
```

Against the running stack. Either order works:

```bash
bash tests/test_stack.sh
bash tests/test_tenancy.sh
```

`test_stack.sh` checks the table exists, a trace round-trips, the row policy
blocks cross-tenant reads, and a live span reaches Tempo. `test_tenancy.sh`
proves the gap from section 7.5.2: the row policy gates reads, not writes.

## Storage benchmarks

Four measured runs against the live stack: per-column compression, bloom index
granule pruning, the cost of reading from the S3 cold tier, and the noisy-tenant
cardinality blowup from section 7.5.2. Details are in
[benchmarks/README.md](benchmarks/README.md), and the last recorded run is in
[RESULTS.md](RESULTS.md).

```bash
cd benchmarks
NUM_SPANS=200000 python3 compression_ratio.py
python3 bloom_index_pruning.py
python3 tiering_automation.py
python3 tenant_cardinality_blowup.py
cd ..
```

Each one builds its own scratch data and removes it, so they give the same
answer whatever ran before them.

## Tear down

The `-v` drops the named volumes, including Tempo's blocks and the SeaweedFS
buckets.

```bash
docker compose down -v
deactivate 2>/dev/null
rm -rf .venv
```

## Reference

### Running the book's listings verbatim

The book's listings are kept short. A few need a server-side prerequisite or a
run order that the code supplies. If you run one verbatim and it behaves
unexpectedly, see "Running the book's listings verbatim" in
[NOTES.md](NOTES.md).

### Ports

| Port | What |
|---|---|
| 8080 | `checkout-service` |
| 4317, 4318 | Collector OTLP gRPC and HTTP |
| 8888 | Collector internal telemetry |
| 8123, 9000 | ClickHouse HTTP and native |
| 9363 | ClickHouse Prometheus endpoint |
| 9090 | Prometheus |
| 3200 | Tempo HTTP API |
| 4417 | Tempo OTLP gRPC (moved off 4317, which the Collector holds) |
| 8333 | SeaweedFS S3 API |

All ports bind to `127.0.0.1` only. ClickHouse runs a password-less user and the
object store's S3 key pair is in the compose file, so neither belongs on a
shared network. To reach the stack from another machine, put an SSH tunnel in
front rather than widening the binding.

### Versions

| Component | Version | Role |
|---|---|---|
| ClickHouse | `clickhouse/clickhouse-server:25.8` (LTS) | primary trace store (listing 7.1) |
| OTel Collector contrib | `otel/opentelemetry-collector-contrib:0.154.0` | OTLP in, partition-by-trace-id, Kafka out |
| Apache Kafka | `apache/kafka:4.3.0` (KRaft) | replayable span buffer feeding the store |
| Prometheus | `prom/prometheus:v3.12.0` | collector + ClickHouse metrics |
| Grafana Tempo | `grafana/tempo:3.0.2` | block archetype for the section 7.3 contrast, fed by the Collector, blocks in SeaweedFS |
| SeaweedFS | `chrislusf/seaweedfs:4.47` (`weed mini`, Apache 2.0) | S3-compatible object store behind the cold tier and Tempo's blocks; creates both buckets on startup |
| Python | `python:3.12-slim` + OTel SDK 1.42.1 | checkout producer + consumer |

These match `chapter-05/` (Collector >= 0.151.0).

### Files

```
chapter-07/
├── docker-compose.yml          # ClickHouse + Collector + Kafka + consumer + Tempo
├── prometheus.yml
├── README.md
├── NOTES.md                    # why everything works the way it does
├── RESULTS.md                  # measured numbers, rendered from benchmarks/results/
├── scripts/                    # one script per step, sharing lib.sh
├── exercises/
│   ├── compression.md          # what listing 7.1's column types are worth
│   ├── tiering.md              # where a part goes when it gets old (listing 7.2)
│   └── tenancy.md              # what a row policy stops, and what it does not (listing 7.4)
├── app/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── checkout.py             # same producer as chapter-05/
│   └── consumer_clickhouse.py  # OTLP -> listing 7.1 columns -> ClickHouse
├── collector/
│   ├── gateway-config.yaml     # OTLP in, partition_traces_by_id, Kafka and Tempo out
│   └── tempo.yaml              # block-archetype backend, blocks in SeaweedFS
├── clickhouse/
│   ├── init.sql                # listing 7.1 + adjusted_count column (applied on first boot)
│   ├── tiering.sql             # listing 7.2 (applied by exercises/tiering.md)
│   ├── compression.sql         # listing 7.3 (per-column compression query)
│   ├── tenancy.sql             # listing 7.4 (applied by exercises/tenancy.md)
│   ├── config.d/
│   │   ├── storage.xml         # 'tiered' policy + S3-backed 'cold' volume (SeaweedFS)
│   │   ├── network.xml
│   │   └── prometheus.xml
│   └── users.d/
│       └── z-allow-network.xml
├── benchmarks/
│   ├── README.md               # how to run the four storage benchmarks
│   ├── chclient.py             # shared ClickHouse client (native driver or HTTP)
│   ├── compression_ratio.py    # per-column compression for listing 7.1
│   ├── bloom_index_pruning.py  # EXPLAIN-measured granule pruning
│   ├── tiering_automation.py   # TTL move to the S3 cold tier and its read cost
│   ├── tenant_cardinality_blowup.py  # noisy-tenant attribute cardinality
│   └── results/                # timestamped JSON
└── tests/
    ├── requirements.txt        # PyYAML, for test_static.py
    ├── test_static.py          # offline checks, including the reader path
    ├── test_stack.sh           # live: round-trip, row policy, Tempo fan-out
    └── test_tenancy.sh         # live: the ingest gap
```
