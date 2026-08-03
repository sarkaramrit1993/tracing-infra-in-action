# Chapter 7: Trace Storage Patterns

Runnable companion for Chapter 7 of *Tracing Infrastructure in Action*. It stands up the
span-per-row OLAP store from section 7.2 (ClickHouse) and makes the chapter's
three DDL listings real: the listing 7.1 schema, the listing 7.2 hot-to-cold
tiering policy, and the listing 7.4 per-tenant row policy.

This stack is continuous with `chapter-05/`: the checkout producer and the
Collector's `partition_traces_by_id` export contract are the same. Chapter 7 is
about the *store*, so the topology is deliberately lighter than chapter 5's:
one Kafka broker instead of three, no Flink, no Jaeger.

```
checkout -> otel-collector (partition_traces_by_id) -> kafka(otlp_spans)
         -> consumer-clickhouse -> ClickHouse otel_traces  (listing 7.1)
```

ClickHouse is the primary backend. Grafana Tempo is included behind a `block`
profile as the section 7.3 block archetype to read against it. It stays off by
default, it receives no traces in this stack, and its own storage here is a
container filesystem rather than MinIO. The section near the end says what it is
for and what it is not.

The cold tier is real object storage. A MinIO service stands in for AWS S3, GCS,
or Azure Blob, and ClickHouse's `s3_cold` disk (in `clickhouse/config.d/storage.xml`)
writes aged parts to it as S3 objects. Listing 7.2's `TO VOLUME 'cold'` moves data
onto that disk, which you can watch and verify against the bucket.

Bring the stack up, look at some traces, then pick whichever of the three
exercises in `exercises/` you feel like. They are independent on purpose.

[NOTES.md](NOTES.md) holds the why behind all of it: why there are two shell
helpers and how they handle stdin, why the compression exercise builds its own
tables, how the cold volume resolves, and what the book's listings assume. Read
it when something surprises you.

## Listings

| Listing | File | Pattern |
|---------|------|---------|
| 7.1 | `clickhouse/init.sql` | ClickHouse trace table sized for compression and retention |
| 7.2 | `clickhouse/tiering.sql` | Hot-to-cold tiering policy |
| 7.3 | `clickhouse/compression.sql` | Per-column compression verification query |
| 7.4 | `clickhouse/tenancy.sql` | Row-level tenant isolation |

## Prerequisites

- Docker and Docker Compose v2
- About 3 GB of memory given to Docker, and about 4 GB of free disk for images
  and volumes. On macOS and Windows that is Docker Desktop's setting under
  Settings, Resources, not free host RAM
- A POSIX shell with `curl`. On Windows, run the exercises inside WSL2
- Python 3 on the host, for `tests/test_static.py` and the four
  `benchmarks/*.py` scripts. CI verifies on 3.12

The stack binds host ports 8080, 4317, 4318, 8123, 8888, 9000, 9001, 9002,
9090 and 9363, plus 3200 and 4417 if you start Tempo. Tear down any
other chapter's stack first. [`setup/README.md`](../setup/README.md) has the
virtualenv step, including the extra package Debian and Ubuntu need and the
different activate path on Windows.

## Version manifest (one tag per image, N1)

| Component | Version | Role |
|---|---|---|
| ClickHouse | `clickhouse/clickhouse-server:25.8` (LTS) | primary trace store (listing 7.1) |
| OTel Collector contrib | `otel/opentelemetry-collector-contrib:0.154.0` | OTLP in, partition-by-trace-id, Kafka out |
| Apache Kafka | `apache/kafka:4.3.0` (KRaft) | replayable span buffer feeding the store |
| Prometheus | `prom/prometheus:v3.12.0` | collector + ClickHouse metrics |
| Grafana Tempo | `grafana/tempo:2.9.0` (`block` profile) | block archetype for the section 7.3 contrast, config only, receives no traces |
| MinIO | `minio/minio:RELEASE.2025-09-07T16-13-09Z` | S3-compatible object store behind the cold tier |
| MinIO client | `minio/mc:RELEASE.2025-08-13T08-35-41Z` | one-shot bucket bootstrap (`traces-cold`) |
| Python | `python:3.12-slim` + OTel SDK 1.42.1 | checkout producer + consumer |

These match `chapter-05/` (Collector >= 0.151.0).

## File tree

```
chapter-07/
├── docker-compose.yml          # ClickHouse + Collector + Kafka + consumer (+Tempo profile)
├── prometheus.yml
├── README.md
├── NOTES.md                    # why everything here works the way it does
├── RESULTS.md                  # the measured numbers, rendered from benchmarks/results/
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
│   ├── gateway-config.yaml     # OTLP in, partition_traces_by_id, Kafka out
│   └── tempo.yaml              # block-archetype backend (block profile)
├── clickhouse/
│   ├── init.sql                # listing 7.1 + adjusted_count column (auto-applied on first boot)
│   ├── tiering.sql             # listing 7.2 (applied by exercises/tiering.md)
│   ├── compression.sql         # listing 7.3 (per-column compression query)
│   ├── tenancy.sql             # listing 7.4 (applied by exercises/tenancy.md)
│   ├── config.d/
│   │   ├── storage.xml         # 'tiered' policy + S3-backed 'cold' volume (MinIO) for listing 7.2
│   │   ├── network.xml
│   │   └── prometheus.xml
│   └── users.d/
│       └── z-allow-network.xml
├── benchmarks/
│   ├── README.md               # how to run the four storage benchmarks
│   ├── chclient.py             # shared ClickHouse client (native driver or HTTP)
│   ├── compression_ratio.py    # per-column compression for the listing 7.1 schema
│   ├── bloom_index_pruning.py  # EXPLAIN granule pruning by the bloom skip index
│   ├── tiering_automation.py   # TTL move to the S3 cold tier, hot vs cold read cost
│   ├── tenant_cardinality_blowup.py  # noisy-tenant attribute cardinality (section 7.5.2)
│   └── results/                # measured JSON from real runs
└── tests/
    ├── requirements.txt        # PyYAML, the only install test_static.py needs
    ├── test_static.py          # offline: YAML/SQL/XML well-formedness, version pins
    ├── test_stack.sh           # live: table exists, round-trip, row-policy isolation
    └── test_tenancy.sh         # live: the section 7.5.2 ingest-gap trap (adversarial)
```

## Bring it up

```bash
docker compose up -d
docker compose ps
```

Give it about 45 seconds: Kafka elects its controller, the `otlp_spans` topic
lands, ClickHouse applies `init.sql`, and the consumer connects. The first run
also pulls seven images and builds the app image, so it takes longer before that
clock even starts. Then generate traffic:

```bash
for i in $(seq 1 300); do curl -s http://localhost:8080/checkout > /dev/null; done &
echo "traffic loop PID: $!"
sleep 10
```

Each `/checkout` call produces a seven-span trace, all under the
`checkout-service` resource. The exporter batches on a five-second timer, so the
first traces take a few seconds to reach ClickHouse, which is what the `sleep` is
for. The loop keeps running in the background while you work.

---

## Look at your trace data

Two helpers for everything below. `ch` runs a query, `ch_file` applies a `.sql`
file:

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
```

`ch` hands ClickHouse an empty stdin, which is what stops a query from sitting
there waiting for input. `ch_file` is the only one that feeds stdin, and it
feeds it a file. See [NOTES.md](NOTES.md).

If you have already worked through `exercises/tenancy.md`, drop its row policy
before reading anything here. That policy is `TO ALL`, so it applies to the
`default` admin too, and every query below would come back empty with nothing to
say why. The exercise drops it when it finishes, but not if you stopped part way:

```bash
ch --query "DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces"
```

First, the table the whole chapter is about:

```bash
ch --query "SHOW CREATE TABLE tracing.otel_traces FORMAT TSVRaw"
```

You should see the exact columns, codecs (`Delta(8), ZSTD(1)`, `T64, ZSTD(1)`,
`ZSTD(3)`), the `bloom_filter` index on `trace_id`, `PARTITION BY
toYYYYMMDD(timestamp)`, and `ORDER BY (service_name, span_name,
toStartOfHour(timestamp), trace_id)`.

Now the traces themselves. Nine columns of a wide table do not look much like a
trace, so this pulls three recent checkout traces and prints a `>` where each new
one begins:

```bash
ch --query "
SELECT
  if(trace_id != lag(trace_id) OVER (ORDER BY trace_id, timestamp), '>', ' ') AS t,
  trace_id,
  toString(timestamp) AS ts,
  rpad(service_name, 17) AS svc,
  rpad(span_name, 18) AS span,
  concat(toString(round(duration_ns / 1000000.0, 3)), 'ms') AS took,
  adjusted_count AS weight,
  status_code
FROM tracing.otel_traces
WHERE trace_id IN (
  SELECT trace_id FROM tracing.otel_traces
  WHERE span_name = 'GET /checkout' ORDER BY timestamp DESC LIMIT 3)
ORDER BY trace_id, timestamp"
```

The `trace_id` and the date half of the timestamp are trimmed below to fit the
page. Yours print in full.

```
>  0e833918...  18:39:37.168943260  checkout-service  GET /checkout      179.073ms  1  STATUS_CODE_UNSET
   0e833918...  18:39:37.169339802  checkout-service  validate_cart       21.138ms  1  STATUS_CODE_UNSET
   0e833918...  18:39:37.190615135  checkout-service  inventory.reserve   30.966ms  1  STATUS_CODE_UNSET
   0e833918...  18:39:37.221734468  checkout-service  payment.charge      92.398ms  1  STATUS_CODE_UNSET
   0e833918...  18:39:37.273568593  checkout-service  fraud.score          40.49ms  1  STATUS_CODE_UNSET
   0e833918...  18:39:37.314260093  checkout-service  order.create        22.409ms  1  STATUS_CODE_UNSET
   0e833918...  18:39:37.336787635  checkout-service  notification.send   10.802ms  1  STATUS_CODE_UNSET
>  657e8154...  18:39:36.972527468  checkout-service  GET /checkout      176.629ms  1  STATUS_CODE_UNSET
   657e8154...  18:39:36.972937718  checkout-service  validate_cart       20.232ms  1  STATUS_CODE_UNSET
   ...
>  9ffffaae...  18:39:37.368596885  checkout-service  GET /checkout      181.856ms  1  STATUS_CODE_UNSET
   ...
   9ffffaae...  18:39:37.475302760  checkout-service  fraud.score          41.92ms  1  STATUS_CODE_ERROR
   ...
```

Seven rows per `>`, and the root span's 179ms covers the six children that follow
it. `fraud.score` in the third trace came back `STATUS_CODE_ERROR`, which is the
kind of thing you came here to find. The producer fails about one checkout in
twenty, so three traces usually come back clean. To go and find one:

```bash
ch --query "
SELECT trace_id, span_name, status_code FROM tracing.otel_traces
WHERE status_code = 'STATUS_CODE_ERROR' ORDER BY timestamp DESC LIMIT 5"
```

Those rows arrived independently, at different times, possibly out of order, and
nothing assembled them until this query sorted by `trace_id` and `timestamp`.
That is the store-then-stitch contract: spans land as rows, a trace is something
you reconstruct on read.

`weight` is `adjusted_count`, the sample-rate reciprocal from section 7.4.4.
Everything here is unsampled, so it reads 1. Drop the `LIMIT 3` subquery and you
get the whole table, healthcheck traces included, one span each.

Two more worth having. Where the bytes are:

```bash
ch --query "
SELECT partition, formatReadableSize(sum(bytes_on_disk)) AS on_disk,
       sum(rows) AS rows, count() AS parts, any(disk_name) AS disk
FROM system.parts WHERE database = 'tracing' AND active GROUP BY partition"
```

And a point lookup, which is the bloom skip index answering a membership question
without scanning the random `trace_id` column:

```bash
TID=$(ch --query "
SELECT trace_id FROM tracing.otel_traces
WHERE span_name = 'GET /checkout' ORDER BY timestamp DESC LIMIT 1")
echo "trace_id=${TID:-(none found: either the first traces have not landed yet, or a row policy is still filtering you, see above)}"
ch --query "
SELECT service_name, span_name, duration_ns
FROM tracing.otel_traces WHERE trace_id = '$TID'"
```

---

## The three exercises

Three separate things live in this chapter, and none of them needs the other two.
So they are three separate files. Open whichever one you want, in any order, on
its own. Each puts the table into the state it needs, makes its own data, and
clears up after itself, so none of them assumes you ran another first and none of
them leaves a mess for the next.

| Exercise | Listing | The question |
|---|---|---|
| [exercises/compression.md](exercises/compression.md) | 7.1, 7.3 | `LowCardinality` and a codec per column change no answers. What do they buy? |
| [exercises/tiering.md](exercises/tiering.md) | 7.2 | Aged parts move `TO VOLUME 'cold'`. Where do they actually go, and is the data still there? |
| [exercises/tenancy.md](exercises/tenancy.md) | 7.4 | A row policy stops one tenant reading another's spans. What does it not stop? |

Each ends with a **Try this** section: a few one-line edits with a visible
consequence. Those are where the chapter's claims stop being claims. Working
through an exercise takes a couple of minutes; poking at it is the part worth
your time.

If you only do one, do tenancy. It is the one with a trap in it.

## Optional: the block archetype (Grafana Tempo)

```bash
docker compose --profile block up -d tempo
```

**It will hold no traces, and that is expected.** The Collector in this stack has
one exporter, Kafka, so nothing is routed to Tempo. `/api/search` returns an
empty list and `/var/tempo/blocks` stays empty. Tempo is here to be read, not
queried.

What it is for is the section 7.3 contrast, and that lives in the config rather
than in any query. Tempo writes immutable Parquet-shaped blocks and expresses its
cold boundary as a retention period on whole blocks:

```bash
grep -A2 'compactor:' collector/tempo.yaml
grep -B2 -A6 'TTL' clickhouse/tiering.sql
```

One deletes blocks after a period. The other moves and then deletes rows by a TTL
expression evaluated per part. That difference, not a screenshot of a UI, is the
archetype contrast the section draws.

Two things this stack does not do, in case the names mislead you. Tempo's backend
here is `backend: local` on a container filesystem, not the MinIO object store;
MinIO backs ClickHouse's `s3_cold` disk instead. And there is no Grafana, so
Tempo has no UI. Pointing the Collector at Tempo as a second exporter, and
putting a Grafana in front of it, is a reasonable thing to go and do, but it is
not wired up here.

---

## Run the tests

Offline (no Docker needed): well-formedness, version pins, and that the SQL
files carry the exact listing 7.1/7.2/7.4 statements. It reads YAML, so it
needs PyYAML:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r tests/requirements.txt
python3 tests/test_static.py
```

Live (stack must be up): table exists, a trace round-trips, and the row policy
blocks cross-tenant reads.

```bash
bash tests/test_stack.sh
```

Adversarial tenancy (stack must be up): proves the section 7.5.2 trap that the
row policy gates reads but not writes, so an ingest path must validate tenant_id.

```bash
bash tests/test_tenancy.sh
```

Both live scripts drop the row policy on the way in and on the way out, so they
run in either order and leave the admin login unfiltered.

## Storage benchmarks

Four measured exercises against the live stack: per-column compression, bloom
skip-index granule pruning, a TTL move to the S3 cold tier and what the same
query costs once its data is out there, and a noisy-tenant attribute-cardinality
blowup (section 7.5.2). See `benchmarks/README.md`. Measured results land in
`benchmarks/results/`.

```bash
cd benchmarks
NUM_SPANS=200000 python3 compression_ratio.py
python3 bloom_index_pruning.py
python3 tiering_automation.py
python3 tenant_cardinality_blowup.py
```

Three of them build and drop their own scratch copy of the listing 7.1 schema,
and `tiering_automation.py` owns its rows in `otel_traces` by service name and
restores the TTL in a `finally`, so all four give the same answer whatever ran
before them. If a tenancy run was interrupted and left the row policy behind, the
benchmarks will read an empty table. Drop it, in full here since you may not have
the `ch` helper defined in this shell:

```bash
docker compose exec -T clickhouse clickhouse-client \
  --query "DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces" < /dev/null
```

## Tear down

The `-v` flag drops the named volumes. Run the second line only if you started
Tempo.

```bash
docker compose down -v
docker compose --profile block down -v
```

With Tempo running, the first line says `Network chapter-07_ch7 Resource is
still in use` and exits 0. Tempo is still attached to it. The second line
removes Tempo and the network together.

## Notes on running the book's listings

The book's listings are kept terse. A few need a server-side prerequisite or a
specific run order that this code supplies. If you run one verbatim and it
behaves unexpectedly, see "Running the book's listings verbatim" in
[NOTES.md](NOTES.md).
