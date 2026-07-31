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

## Listings

| Listing | File | Pattern |
|---------|------|---------|
| 7.1 | `clickhouse/init.sql` | ClickHouse trace table sized for compression and retention |
| 7.2 | `clickhouse/tiering.sql` | Hot-to-cold tiering policy |
| 7.3 | `clickhouse/compression.sql` | Per-column compression verification query |
| 7.4 | `clickhouse/tenancy.sql` | Row-level tenant isolation |

## Prerequisites

- Docker and Docker Compose
- Around 3 GB of free RAM for the default stack
- Python 3.10+ only if you want to run `tests/test_static.py` offline

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

These match `tasks/stack-baseline.md` and `chapter-05/` (Collector >= 0.151.0).

## File tree

```
chapter-07/
├── docker-compose.yml          # ClickHouse + Collector + Kafka + consumer (+Tempo profile)
├── prometheus.yml
├── README.md
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
│   ├── tenancy.sql             # listing 7.4 (apply by hand in the walkthrough)
│   ├── config.d/
│   │   ├── storage.xml         # 'tiered' policy + S3-backed 'cold' volume (MinIO) for listing 7.2
│   │   ├── network.xml
│   │   └── prometheus.xml
│   └── users.d/
│       └── z-allow-network.xml
├── benchmarks/
│   ├── README.md               # how to run the three storage benchmarks
│   ├── chclient.py             # shared ClickHouse client (native driver or HTTP)
│   ├── compression_ratio.py    # per-column compression on the listing 7.1 table
│   ├── bloom_index_pruning.py  # EXPLAIN granule pruning by the bloom skip index
│   ├── tiering_automation.py   # TTL move to the S3 cold tier, timed
│   ├── tenant_cardinality_blowup.py  # noisy-tenant attribute cardinality (section 7.5.2)
│   └── results/                # measured JSON from real runs
└── tests/
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
lands, ClickHouse applies `init.sql`, and the consumer connects. Then generate
traffic:

```bash
for i in $(seq 1 300); do curl -s http://localhost:8080/checkout > /dev/null; done &
echo "traffic loop PID: $!"
```

Each `/checkout` call produces a six-span trace, all under the `checkout-service` resource.

---

## Verify it works

Run the admin steps (1-5) **before** applying the row policy in step 6, because
the listing 7.4 policy is `TO ALL` and will also filter the `default` admin
login (its username is not a tenant_id, so it would see zero rows afterward).
That ordering is the point of section 7.5.2's trap: the policy gates every read.

A convenient alias for the steps below:

```bash
ch() { docker compose exec -T clickhouse clickhouse-client "$@"; }
```

### 1. The listing 7.1 table exists

```bash
ch --query "SHOW CREATE TABLE tracing.otel_traces FORMAT TSVRaw"
```

You should see the exact columns, codecs (`Delta, ZSTD(1)`, `T64, ZSTD(1)`,
`ZSTD(3)`), the `bloom_filter` index on `trace_id`, `PARTITION BY
toYYYYMMDD(timestamp)`, and `ORDER BY (service_name, span_name,
toStartOfHour(timestamp), trace_id)`.

### 2. A trace round-trips (point lookup by trace_id)

Pick a trace_id, then look it up. The bloom-filter skip index answers the
membership question without scanning the random column:

```bash
TID=$(ch --query "SELECT trace_id FROM tracing.otel_traces ORDER BY timestamp DESC LIMIT 1")
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

The low-cardinality columns (`service_name`, `span_name`, `status_code`) show
the largest ratios; `trace_id` shows the ~2x floor that Table 7.2 calls out.
For whole-part totals use `system.parts`:

```bash
ch --query "SELECT partition, \
       formatReadableSize(sum(bytes_on_disk)) AS on_disk, \
       sum(rows) AS rows, count() AS parts, any(disk_name) AS disk \
   FROM system.parts WHERE database='tracing' AND active GROUP BY partition"
```

### 4. DROP PARTITION is instant (metadata-time retention)

Each day is its own partition (`toYYYYMMDD`). Dropping one is a metadata
operation, not a row-by-row tombstone delete:

```bash
PART=$(ch --query "SELECT partition FROM system.parts \
        WHERE database='tracing' AND active ORDER BY partition LIMIT 1")
echo "dropping partition $PART"
time ch --query "ALTER TABLE tracing.otel_traces DROP PARTITION '$PART'"
ch --query "SELECT count() FROM tracing.otel_traces"   # rows for that day are gone
```

The drop returns in milliseconds regardless of how many rows the day held. This
is the Cassandra-tombstone contrast from the chapter opener.

### 5. Tiering: hot-to-cold (listing 7.2)

Apply the listing 7.2 policy. `TO VOLUME 'cold'` resolves against the `cold`
volume defined in `config.d/storage.xml`, whose disk is the S3-backed `s3_cold`
disk pointing at the MinIO object store (the same API AWS S3, GCS, and Azure Blob
expose; only the endpoint and credentials change between them):

```bash
ch --multiquery < clickhouse/tiering.sql
ch --query "SELECT move_ttl_info.expression, move_ttl_info.max \
            FROM system.parts WHERE database='tracing' AND active LIMIT 1 \
            FORMAT Vertical"
```

The `TO VOLUME 'cold'` rule fires on parts older than two days; on a fresh demo
nothing is two days old yet, so parts stay on `default`. To force a move for the
demo, lower the boundary to a few seconds and trigger the merge:

```bash
ch --query "ALTER TABLE tracing.otel_traces MODIFY TTL \
              toDateTime(timestamp) + INTERVAL 5 SECOND TO VOLUME 'cold', \
              toDateTime(timestamp) + INTERVAL 15 DAY DELETE"
# pick a real partition id, then move it explicitly (MOVE PARTITION takes a
# literal id, not a subquery)
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

(Restore the real boundary with `clickhouse/tiering.sql` afterward.)

### 6. Row policy blocks cross-tenant reads (listing 7.4)

Apply listing 7.4. It adds `tenant_id`, leads the sort key with it, creates the
`tenant_filter` row policy, and seeds one row each for `tenant_a` and `tenant_b`:

```bash
ch --multiquery < clickhouse/tenancy.sql
```

Now connect as each tenant. The policy rewrites `tenant_id = currentUser()` onto
every SELECT, so a tenant cannot read another tenant's spans even by asking:

```bash
# tenant_a sees only tenant_a rows
docker compose exec -T clickhouse clickhouse-client --user tenant_a \
  --query "SELECT tenant_id, count() FROM tracing.otel_traces GROUP BY tenant_id"
# explicit attempt to read tenant_b returns zero rows
docker compose exec -T clickhouse clickhouse-client --user tenant_a \
  --query "SELECT count() FROM tracing.otel_traces WHERE tenant_id = 'tenant_b'"
```

The first query returns only `tenant_a`; the second returns `0`. The row policy
gates reads transparently. Remember the trap from section 7.5.2: the policy does
**not** gate `INSERT` or `DROP PARTITION`, so a real ingest path must validate
`tenant_id` against the authenticated principal.

> After step 6 the `default` admin login is also filtered by the `TO ALL`
> policy and will see zero rows. To drop the policy and return to admin-wide
> visibility: `ch --query "DROP ROW POLICY tenant_filter ON tracing.otel_traces"`.

---

### 7. (Optional) The block archetype: Grafana Tempo

```bash
docker compose --profile block up -d tempo
```

Tempo writes immutable Parquet-shaped blocks to an object store (local
filesystem here, S3/GCS in production) rather than rows, and expresses the cold
boundary as `compactor.block_retention` rather than `TTL ... TO VOLUME`. It is
the section 7.3 contrast to the ClickHouse row store.

---

## Run the tests

Offline (no Docker needed): well-formedness, version pins, and that every SQL
file carries the exact listing 7.1/7.2/7.3 statements.

```bash
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

```bash
cd benchmarks
python compression_ratio.py
python bloom_index_pruning.py
python tiering_automation.py
python tenant_cardinality_blowup.py
```

## Tear down

```bash
docker compose down -v          # -v drops the named volumes
docker compose --profile block down -v   # if you started Tempo
```

## Notes on running the book's listings

The book's listings are kept terse. A few need a server-side prerequisite or a
specific run order that the code supplies. If you run a listing verbatim and it
behaves unexpectedly, these are why:

1. **Listing 7.1 needs `SETTINGS storage_policy = 'tiered'`** for listing 7.2's
   `TO VOLUME 'cold'` to resolve. A `MergeTree` table on the default policy has
   no volume named `cold`, so the tiering ALTER fails without it. `init.sql`
   supplies the SETTINGS line for you.
2. **Listing 7.2's `DROP PARTITION '20260601'`** targets a literal past date. On
   a fresh demo no such partition exists, so it no-ops (a silent success that is
   itself the metadata-retention point). The walkthrough above shows how to drop
   a partition that actually holds data.
3. **Listing 7.4's `MODIFY ORDER BY (tenant_id, ...)` cannot run on an existing
   table.** ClickHouse requires the primary key to stay a prefix of the sorting
   key, and `MODIFY ORDER BY` only accepts columns introduced in the same
   statement, so a pre-existing `tenant_id` column cannot be moved into the sort
   key by ALTER (verified on 25.8, `BAD_ARGUMENTS`, both prepend and append). A
   tenant-leading layout is therefore a `CREATE TABLE` property: a fresh store
   creates the table with `ORDER BY (tenant_id, service_name, span_name,
   toStartOfHour(timestamp), trace_id)`. `tenancy.sql` adds the `tenant_id`
   column and applies the row policy (the isolation boundary) without running the
   ALTER ClickHouse rejects. The sort-key prefix is a read-locality optimization,
   not the security mechanism.
4. **Listing 7.4's `currentUser()`** returns the connected SQL username, which is
   why the demo names its two users `tenant_a` / `tenant_b`. A real deployment
   maps an authenticated principal to a tenant claim rather than naming the SQL
   user after the tenant; the row-policy mechanics are identical.
