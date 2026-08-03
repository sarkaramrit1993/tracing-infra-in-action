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
profile to demonstrate the object-storage block archetype from section 7.3; it
stays off by default.

The cold tier is real object storage. A MinIO service stands in for AWS S3, GCS,
or Azure Blob, and ClickHouse's `s3_cold` disk (in `clickhouse/config.d/storage.xml`)
writes aged parts to it as S3 objects. Listing 7.2's `TO VOLUME 'cold'` moves data
onto that disk, which you can watch and verify against the bucket.

[NOTES.md](NOTES.md) holds the why behind the steps below: how the `ch()` helper
handles stdin, why a fresh demo reports no compression numbers, how the cold
volume resolves, and what the book's listings assume. Read it when a step
surprises you.

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
- A POSIX shell with `curl`. On Windows, run the walkthrough inside WSL2
- Python 3 on the host, for `tests/test_static.py` and the four
  `benchmarks/*.py` scripts. CI verifies on 3.12

The stack binds host ports 8080, 4317, 4318, 8123, 8888, 9000, 9001, 9002,
9090 and 9363, plus 3200 and 4417 if you start Tempo in step 7. Tear down any
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
| Grafana Tempo | `grafana/tempo:2.9.0` (`block` profile) | object-storage block archetype (section 7.3) |
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
├── NOTES.md                    # why the walkthrough works the way it does
├── RESULTS.md                  # the measured numbers, rendered from benchmarks/results/
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
│   ├── tiering.sql             # listing 7.2 (apply by hand in the walkthrough)
│   ├── compression.sql         # listing 7.3 (per-column compression query)
│   ├── tenancy.sql             # listing 7.4 (apply by hand in the walkthrough)
│   ├── config.d/
│   │   ├── storage.xml         # 'tiered' policy + S3-backed 'cold' volume (MinIO) for listing 7.2
│   │   ├── network.xml
│   │   └── prometheus.xml
│   └── users.d/
│       └── z-allow-network.xml
├── benchmarks/
│   ├── README.md               # how to run the four storage benchmarks
│   ├── chclient.py             # shared ClickHouse client (native driver or HTTP)
│   ├── compression_ratio.py    # per-column compression on the listing 7.1 table
│   ├── bloom_index_pruning.py  # EXPLAIN granule pruning by the bloom skip index
│   ├── tiering_automation.py   # TTL move to the S3 cold tier, timed
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
```

Each `/checkout` call produces a seven-span trace, all under the `checkout-service` resource.

---

## Verify it works

Run the admin steps (1-5) **before** you apply the row policy in step 6. That
policy filters the `default` admin login too, so once it is in place the earlier
queries return zero rows.

Two convenient aliases for the steps below. `ch` runs a query, `ch_file` applies
a `.sql` file:

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
```

`ch` hands ClickHouse an empty stdin, which is what stops a query from sitting
there waiting for input. `ch_file` is the only one that feeds stdin, and it
feeds it a file. See [NOTES.md](NOTES.md).

### 1. The listing 7.1 table exists

```bash
ch --query "SHOW CREATE TABLE tracing.otel_traces FORMAT TSVRaw"
```

You should see the exact columns, codecs (`Delta, ZSTD(1)`, `T64, ZSTD(1)`,
`ZSTD(3)`), the `bloom_filter` index on `trace_id`, `PARTITION BY
toYYYYMMDD(timestamp)`, and `ORDER BY (service_name, span_name,
toStartOfHour(timestamp), trace_id)`.

### 2. A trace round-trips (point lookup by trace_id)

Pick a trace_id, then look it up. The filter on `GET /checkout` is there so you
get a real checkout trace and not a healthcheck one.

```bash
TID=$(ch --query "SELECT trace_id FROM tracing.otel_traces \
        WHERE span_name = 'GET /checkout' ORDER BY timestamp DESC LIMIT 1")
echo "trace_id=$TID"
ch --query "SELECT service_name, span_name, duration_ns \
            FROM tracing.otel_traces WHERE trace_id = '$TID'"
```

You get back the spans of that one trace, assembled at query time. That is the
store-then-stitch contract: spans land independently, assembly is deferred.

### 3. Compression: compressed vs uncompressed bytes

```bash
ch --query "SELECT name, \
       formatReadableSize(sum(data_compressed_bytes))   AS compressed, \
       formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed, \
       round(sum(data_uncompressed_bytes)/sum(data_compressed_bytes),1) AS ratio \
   FROM system.columns \
   WHERE database='tracing' AND table='otel_traces' \
   GROUP BY name ORDER BY sum(data_uncompressed_bytes) DESC"
```

On a fresh demo this prints `0.00 B` and `nan` for every column. That is
expected, and [NOTES.md](NOTES.md) says why. The real per-column numbers come
from `benchmarks/compression_ratio.py`.

In those numbers the low-cardinality columns (`service_name`, `span_name`,
`status_code`) compress by one to three orders of magnitude, columns holding a
single repeated value compress further still, and `trace_id` shows the ~2x floor
that Table 7.2 calls out. For whole-part totals use `system.parts`:

```bash
ch --query "SELECT partition, \
       formatReadableSize(sum(bytes_on_disk)) AS on_disk, \
       sum(rows) AS rows, count() AS parts, any(disk_name) AS disk \
   FROM system.parts WHERE database='tracing' AND active GROUP BY partition"
```

### 4. Tiering: hot-to-cold (listing 7.2)

Apply the listing 7.2 policy. `TO VOLUME 'cold'` sends aged parts to the
S3-backed `cold` volume, which is MinIO in this stack:

```bash
ch_file clickhouse/tiering.sql
ch --query "SELECT move_ttl_info.expression, move_ttl_info.max \
            FROM system.parts WHERE database='tracing' AND active LIMIT 1 \
            FORMAT Vertical"
```

The rule fires on parts older than two days, and nothing on a fresh demo is that
old, so parts stay on `default`. To force a move now, lower the boundary to a few
seconds and move a partition by hand:

```bash
ch --query "ALTER TABLE tracing.otel_traces MODIFY TTL \
              toDateTime(timestamp) + INTERVAL 5 SECOND TO VOLUME 'cold', \
              toDateTime(timestamp) + INTERVAL 15 DAY DELETE"
PART=$(ch --query "SELECT partition FROM system.parts \
        WHERE database='tracing' AND active ORDER BY partition LIMIT 1")
ch --query "ALTER TABLE tracing.otel_traces MOVE PARTITION '$PART' TO VOLUME 'cold'"
ch --query "SELECT partition, disk_name FROM system.parts \
            WHERE database='tracing' AND active"
```

Watch `disk_name` flip from `default` to `s3_cold`. The moved part is now stored
as S3 objects in MinIO. Browse the bucket at the MinIO console on
http://localhost:9001 (user `traceadmin`, password `traceadmin-secret`, bucket
`traces-cold`) to see the objects ClickHouse wrote.

Restore the real boundary before you go on. The 5-second rule you just set stays
on the table until you do, and it pushes every new part to S3 seconds after the
part lands:

```bash
ch_file clickhouse/tiering.sql
```

### 5. DROP PARTITION is instant (metadata-time retention)

Each day is its own partition (`toYYYYMMDD`). On a demo you brought up today
there is only one, so this drops every row and leaves the table empty. Nothing
after this step needs the traffic you generated:

```bash
PART=$(ch --query "SELECT partition FROM system.parts \
        WHERE database='tracing' AND active ORDER BY partition LIMIT 1")
echo "dropping partition $PART"
time ch --query "ALTER TABLE tracing.otel_traces DROP PARTITION '$PART'"
ch --query "SELECT count() FROM tracing.otel_traces"
```

The drop returns in milliseconds regardless of how many rows the day held.

### 6. Row policy blocks cross-tenant reads (listing 7.4)

Apply listing 7.4. It adds a `tenant_id` column, creates the `tenant_filter` row
policy and the two tenant users, and seeds one row each for `tenant_a` and
`tenant_b`:

```bash
ch_file clickhouse/tenancy.sql
```

Now connect as each tenant:

```bash
docker compose exec -T clickhouse clickhouse-client --user tenant_a \
  --query "SELECT tenant_id, count() FROM tracing.otel_traces GROUP BY tenant_id"
docker compose exec -T clickhouse clickhouse-client --user tenant_a \
  --query "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'"
```

The first query returns only `tenant_a`; the second returns `0`. A tenant cannot
read another tenant's spans even by asking.

> After step 6 the `default` admin login is also filtered by the `TO ALL`
> policy and will see zero rows. To drop the policy and return to admin-wide
> visibility: `ch --query "DROP ROW POLICY tenant_filter ON tracing.otel_traces"`.

---

### 7. (Optional) The block archetype: Grafana Tempo

```bash
docker compose --profile block up -d tempo
```

Tempo writes immutable Parquet-shaped blocks to an object store rather than
rows. It is the section 7.3 contrast to the ClickHouse row store.

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

## Storage benchmarks

Four measured exercises against the live stack: per-column compression, bloom
skip-index granule pruning, a timed TTL move to the S3 cold tier, and a
noisy-tenant attribute-cardinality blowup (section 7.5.2). See
`benchmarks/README.md`. Measured results land in `benchmarks/results/`.

Two things first. If you worked through step 6, or a test run stopped part way,
the listing 7.4 row policy is still in place and every benchmark below will
report an empty table. Drop it:

```bash
ch --query "DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces"
```

And `compression_ratio.py` runs `TRUNCATE TABLE tracing.otel_traces` before it
loads its own rows, so it wipes whatever the walkthrough left behind. Set
`KEEP_EXISTING=1` to measure the table as it stands instead.

```bash
cd benchmarks
NUM_SPANS=200000 python3 compression_ratio.py
python3 bloom_index_pruning.py
python3 tiering_automation.py
python3 tenant_cardinality_blowup.py
```

## Tear down

The `-v` flag drops the named volumes. Run the second line only if you started
Tempo.

```bash
docker compose down -v
docker compose --profile block down -v
```

## Notes on running the book's listings

The book's listings are kept terse. A few need a server-side prerequisite or a
specific run order that this code supplies. If you run one verbatim and it
behaves unexpectedly, see "Running the book's listings verbatim" in
[NOTES.md](NOTES.md).
