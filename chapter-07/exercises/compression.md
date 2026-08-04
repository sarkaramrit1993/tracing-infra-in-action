# Compression: what listing 7.1's column types are worth

Run this from `chapter-07/`. It does not depend on the other two exercises and
does not leave anything behind for them.

## The question

Listing 7.1 does not just name nine columns. It puts `LowCardinality` on three of
them and hangs an explicit codec on every one. Neither choice changes a single
answer the table gives back. So what do they buy?

The only honest way to find out is to store the same spans twice, once through
listing 7.1's declarations and once through none of them, and read the bytes off
disk.

## The starting state

Two scratch tables in the `tracing` database:

- `compress_listing` is listing 7.1 column for column, `LowCardinality` and
  codecs included.
- `compress_plain` holds the same nine columns as plain types with no `CODEC`
  clause at all, so ClickHouse falls back to its default LZ4.

Scratch tables rather than `otel_traces`, for two reasons. A live demo table is
too small for listing 7.3's per-column accounting to report anything, and these
tables need a fixed past timestamp that listing 7.2's TTL will not let into
`otel_traces`. [NOTES.md](../NOTES.md), "Why the compression exercise builds its
own tables", has both in full.

Build them:

```bash
ch() { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
```

The `< /dev/null` is not decoration. `clickhouse-client` reads stdin for INSERT
data even when the rows are inline, and `docker compose exec -T` hands it your
terminal, where it will sit and wait. [NOTES.md](../NOTES.md) has the long
version.

```bash
ch --query "DROP TABLE IF EXISTS tracing.compress_listing"
ch --query "DROP TABLE IF EXISTS tracing.compress_plain"
```

```bash
ch --query "
CREATE TABLE tracing.compress_listing
(
    timestamp      DateTime64(9) CODEC(Delta, ZSTD(1)),
    trace_id       String CODEC(ZSTD(1)),
    span_id        String CODEC(ZSTD(1)),
    service_name   LowCardinality(String) CODEC(ZSTD(1)),
    span_name      LowCardinality(String) CODEC(ZSTD(1)),
    status_code    LowCardinality(String) CODEC(ZSTD(1)),
    duration_ns    UInt64 CODEC(T64, ZSTD(1)),
    adjusted_count Float64 DEFAULT 1.0 CODEC(ZSTD(1)),
    attributes     Map(LowCardinality(String), String) CODEC(ZSTD(3)),
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id)"
```

```bash
ch --query "
CREATE TABLE tracing.compress_plain
(
    timestamp      DateTime64(9),
    trace_id       String,
    span_id        String,
    service_name   String,
    span_name      String,
    status_code    String,
    duration_ns    UInt64,
    adjusted_count Float64 DEFAULT 1.0,
    attributes     Map(String, String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id)"
```

Same sort key, same partitioning. The only differences are the ones being
measured.

## Load both with the same spans

200,000 spans, six per trace, generated inside ClickHouse so nothing large
crosses the wire. That is the row count `RESULTS.md` was measured at. The
timestamp anchor is fixed, so your bytes will match these to the byte on the
ClickHouse this stack pins, 25.8. On another version they will not. Codec
implementations, part format and index granularity defaults all move between
releases, and any of them shifts the per-column figures. The ratios between
the columns are the point and they survive the move; the exact byte counts do
not.

```bash
ch --query "
INSERT INTO tracing.compress_listing
  (timestamp, trace_id, span_id, service_name, span_name,
   status_code, duration_ns, adjusted_count, attributes)
SELECT
  toDateTime64('2026-01-01 00:00:00', 9) + toIntervalMillisecond(number),
  lower(hex(MD5(toString(intDiv(number, 6))))),
  lower(hex(reinterpretAsFixedString(toUInt64(number)))),
  ['checkout-service', 'inventory-service', 'payment-service',
   'fraud-service', 'notification-service'][(number % 5) + 1],
  ['validate_cart', 'inventory.reserve', 'payment.charge', 'fraud.score',
   'order.create', 'notification.send', 'db.query', 'cache.get',
   'http.request', 'grpc.call'][(number % 10) + 1],
  if(number % 20 = 0, 'STATUS_CODE_ERROR', 'STATUS_CODE_OK'),
  toUInt64(1000000 + (number * 2654435761) % 200000000),
  multiIf(intDiv(number, 6) % 100 < 80, 1.0,
          intDiv(number, 6) % 100 < 98, 10.0,
          100.0),
  map('http.method', ['GET', 'POST', 'PUT'][(number % 3) + 1],
      'k8s.pod.name', concat('pod-', toString(number % 32)))
FROM numbers(200000)"
```

The second table is filled from the first, so the rows really are the same rows
and not two draws from the same generator:

```bash
ch --query "INSERT INTO tracing.compress_plain SELECT * FROM tracing.compress_listing"
ch --query "OPTIMIZE TABLE tracing.compress_listing FINAL"
ch --query "OPTIMIZE TABLE tracing.compress_plain FINAL"
```

Check that claim rather than trust it:

```bash
ch --query "
SELECT
  (SELECT count() FROM tracing.compress_listing) AS listing_rows,
  (SELECT count() FROM tracing.compress_plain)   AS plain_rows,
  (SELECT sum(cityHash64(timestamp, trace_id, span_id, service_name, span_name,
                         status_code, duration_ns, adjusted_count,
                         toString(attributes)))
     FROM tracing.compress_listing) AS listing_hash,
  (SELECT sum(cityHash64(timestamp, trace_id, span_id, service_name, span_name,
                         status_code, duration_ns, adjusted_count,
                         toString(attributes)))
     FROM tracing.compress_plain) AS plain_hash
FORMAT Vertical"
```

Both counts read 200000 and both hashes read the same number.

## Read the bytes

This is listing 7.3's `system.columns` query widened to cover both tables at
once. `clickhouse/compression.sql` has the single-table version.

```bash
ch --query "
SELECT
  name AS column,
  formatReadableSize(sumIf(data_compressed_bytes, table = 'compress_listing')) AS listing_7_1,
  formatReadableSize(sumIf(data_compressed_bytes, table = 'compress_plain'))   AS plain,
  round(sumIf(data_compressed_bytes, table = 'compress_plain') /
        sumIf(data_compressed_bytes, table = 'compress_listing'), 1) AS smaller_by
FROM system.columns
WHERE database = 'tracing' AND table IN ('compress_listing', 'compress_plain')
GROUP BY name
ORDER BY sumIf(data_compressed_bytes, table = 'compress_plain') DESC"
```

```
column           listing_7_1     plain        smaller_by
trace_id         3.22 MiB        6.28 MiB     2
timestamp        1.03 MiB        1.23 MiB     1.2
duration_ns      685.38 KiB      1.08 MiB     1.6
span_id          641.56 KiB      1.04 MiB     1.7
attributes       356.46 KiB      833.21 KiB   2.3
adjusted_count   65.70 KiB       209.94 KiB   3.2
status_code      4.21 KiB        45.74 KiB    10.9
service_name     420.00 B        14.50 KiB    35.4
span_name        525.00 B        11.42 KiB    22.3
```

One caveat before you read anything into a single row. The plain table drops
`LowCardinality` **and** the explicit codec together, so a per-column figure here
is the two of them combined, not the dictionary alone. The first "Try this" below
separates them by changing only the cardinality and leaving the codec in place,
which is where the attribution actually comes from.

Read that from the bottom up. `service_name` and `span_name` are the two columns
that lead the sort key, they hold five and ten distinct values, and
`LowCardinality` turns each of them into a dictionary plus a column of small
integers. 420 bytes for 200,000 service names. `status_code` gets the same
treatment and lands 10.9x smaller, less than the other two because it is not in
the sort key, so its values are not grouped into runs.

Now read the top. `trace_id` is the biggest column in both tables and it only
halves. It is 32 hex characters carrying 16 random bytes, so half the stored
width is the encoding, and squeezing the encoding back out is roughly all any
codec can do with it. That is the floor Table 7.2 calls out, and it is why
`trace_id` drives the storage bill: not because it compresses badly relative to
its size, but because there is so much of it.

The whole-table figure:

```bash
ch --query "
SELECT
  table,
  formatReadableSize(sum(data_compressed_bytes))   AS on_disk,
  formatReadableSize(sum(data_uncompressed_bytes)) AS raw,
  sum(rows) AS rows
FROM system.parts
WHERE database = 'tracing' AND active
  AND table IN ('compress_listing', 'compress_plain')
GROUP BY table ORDER BY table"
```

```
table              on_disk     raw         rows
compress_listing   5.96 MiB    18.70 MiB   200000
compress_plain     10.71 MiB   31.20 MiB   200000
```

5.96 MiB against 10.71 MiB. The same spans, the same answers, 44% less disk.

Careful with the `raw` column though. It differs between the two tables (18.70
against 31.20) because `LowCardinality` changes what "uncompressed" even means:
the raw form of a dictionary column is already a column of integers. A ratio
built from those two raw numbers would be comparing different things.
`on_disk` is the number that means something across both.

## Try this

Each of these is one edit to the CREATE or the generator above, then re-run the
section. Everything you need is in this file.

**Break the low cardinality on a column that is not in the sort key.** In the
generator, replace the `status_code` line

```
  if(number % 20 = 0, 'STATUS_CODE_ERROR', 'STATUS_CODE_OK'),
```

with

```
  concat('STATUS_', toString(number % 50000)),
```

`status_code` goes from 4.21 KiB to 592.61 KiB, 140 times bigger, while its
declared type never changed. `LowCardinality` is not a compression setting. It
is a bet that the column has few distinct values, and it pays exactly as well as
the bet is true. Pick `status_code` and not `service_name` for this: changing
`service_name` also reshuffles the sort key, and then you are measuring two
things at once.

**Move the clock to the front of the sort key.** Change `compress_listing` to

```
ORDER BY (timestamp, service_name, span_name, trace_id)
```

`timestamp` drops from 1.03 MiB to 1.95 KiB. Then take `Delta` off that column,
leaving `CODEC(ZSTD(1))`, and it jumps to 478.91 KiB. Under listing 7.1's own
sort key the same edit moves it from 1.03 MiB to 1.09 MiB, a 6% difference you
would struggle to notice. `Delta` stores the gap to the previous value, so it
only earns anything when consecutive rows on disk hold consecutive times. Listing
7.1 sorts by service and span first, which scatters the clock, so most of what
`Delta` could do is already given away by the sort key. A codec is worth
whatever the row order lets it be worth.

**Turn the ZSTD level up and down on `attributes`.** That column is declared
`CODEC(ZSTD(3))` while everything else is `ZSTD(1)`. Try `ZSTD(1)` and `ZSTD(9)`:

```
ZSTD(1)  447.06 KiB
ZSTD(3)  356.46 KiB
ZSTD(9)  287.61 KiB
```

Each step buys real space, and each step costs CPU on every insert and every
read. Level 3 on the one column that carries repeated key and value text, level 1
everywhere else, is the trade listing 7.1 picked. The numbers above are what it
turned down on either side.

## Clean up

```bash
ch --query "DROP TABLE IF EXISTS tracing.compress_listing"
ch --query "DROP TABLE IF EXISTS tracing.compress_plain"
ch --query "SHOW TABLES FROM tracing"
```

That should print `otel_traces`, plus `tenant_users` if the tenancy exercise or
either live test script is part way through. Neither of them was touched by this
exercise.

## Going deeper

[NOTES.md](../NOTES.md), "Why the compression exercise builds its own tables",
explains the Compact-part accounting and the fixed clock in more detail.

`benchmarks/compression_ratio.py` runs the listing 7.1 side of this on its own,
asserts the two claims the chapter makes about `trace_id` and `service_name`,
and writes a JSON record into `benchmarks/results/`.
`benchmarks/tenant_cardinality_blowup.py` takes the high-cardinality variation
above and follows it into section 7.5.2, where the noisy tenant is a real
neighbor and not a `%` operator.
