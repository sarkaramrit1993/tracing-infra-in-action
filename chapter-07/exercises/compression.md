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

Every step below runs a script from `scripts/`, and the SQL it runs is shown
under it. The stack has to be up:

```bash
docker compose up -d --build --wait
```

The exercise works on two scratch tables in the `tracing` database:

- `compress_listing` is listing 7.1 column for column, `LowCardinality` and
  codecs included.
- `compress_plain` holds the same nine columns as plain types with no `CODEC`
  clause at all, so ClickHouse falls back to its default LZ4.

Scratch tables rather than `otel_traces`, for two reasons. A live demo table is
too small for listing 7.3's per-column accounting to report anything, and these
tables need a fixed past timestamp that listing 7.2's TTL will not let into
`otel_traces`. [NOTES.md](../NOTES.md), "Why the compression exercise builds its
own tables", has both in full.

Build them. The script first drops any scratch table an earlier run left:

```bash
./scripts/build-compression-tables.sh
```

```
created tracing.compress_listing and tracing.compress_plain, both empty
```

What it runs:

```sql
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
ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id)
```

```sql
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
ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id)
```

Same sort key, same partitioning. The only differences are the ones being
measured.

## Load both with the same spans

200,000 spans, six per trace, generated inside ClickHouse so nothing large
crosses the wire. That is the row count `RESULTS.md` was measured at.

```bash
./scripts/load-compression-tables.sh
```

```
Row 1:
──────
listing_rows: 200000
plain_rows:   200000
listing_hash: 4194180933656532226
plain_hash:   4194180933656532226
```

The script fills `compress_listing` from a generator, then fills
`compress_plain` from `compress_listing`, so the rows really are the same rows
and not two draws from the same generator. The last query checks that claim
rather than trusting it: both counts read 200000 and both hashes match.

The timestamp anchor is fixed, so your bytes below will match these to the byte
on the ClickHouse this stack pins, 25.8. On another version they will not.
Codec implementations, part format and index granularity defaults all move
between releases. The ratios between the columns are the point, and they
survive the move. What it runs:

```sql
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
          intDiv(number, 6) % 100 < 98, 10.0, 100.0),
  map('http.method', ['GET', 'POST', 'PUT'][(number % 3) + 1],
      'k8s.pod.name', concat('pod-', toString(number % 32)))
FROM numbers(200000)
```

```sql
INSERT INTO tracing.compress_plain SELECT * FROM tracing.compress_listing
```

Then `OPTIMIZE TABLE ... FINAL` on each, which merges each table into one part
so the byte counts are not split across several.

## Read the bytes

This is listing 7.3's `system.columns` query, widened to cover both tables at
once. `clickhouse/compression.sql` has the single-table version.

```bash
./scripts/compare-column-sizes.sh
```

```
column          listing_7_1  plain       smaller_by
trace_id        3.22 MiB     6.28 MiB             2
timestamp       1.03 MiB     1.23 MiB           1.2
duration_ns     685.38 KiB   1.08 MiB           1.6
span_id         641.56 KiB   1.04 MiB           1.7
attributes      356.46 KiB   833.21 KiB         2.3
adjusted_count  65.70 KiB    209.94 KiB         3.2
status_code     4.21 KiB     45.74 KiB         10.9
service_name    420.00 B     14.50 KiB         35.4
span_name       525.00 B     11.42 KiB         22.3
```

One caveat before you read anything into a single row. The plain table drops
`LowCardinality` **and** the explicit codec together, so a per-column figure here
is the two of them combined, not the dictionary alone. The first "Try this" below
separates them by changing only the cardinality and leaving the codec in place.

Read it from the bottom up. `service_name` and `span_name` are the two columns
that lead the sort key. They hold five and ten distinct values, and
`LowCardinality` turns each of them into a dictionary plus a column of small
integers. 420 bytes for 200,000 service names. `status_code` gets the same
treatment and lands 10.9x smaller, less than the other two because it is not in
the sort key, so its values are not grouped into runs.

Now read the top. `trace_id` is the biggest column in both tables and it only
halves. It is 32 hex characters carrying 16 random bytes, so half the stored
width is the encoding, and squeezing the encoding back out is roughly all any
codec can do with it. That is the floor Table 7.2 calls out. It is why
`trace_id` drives the storage bill: not because it compresses badly for its
size, but because there is so much of it.

What it runs:

```sql
SELECT
  name AS column,
  formatReadableSize(sumIf(data_compressed_bytes, table = 'compress_listing')) AS listing_7_1,
  formatReadableSize(sumIf(data_compressed_bytes, table = 'compress_plain'))   AS plain,
  round(sumIf(data_compressed_bytes, table = 'compress_plain')
      / sumIf(data_compressed_bytes, table = 'compress_listing'), 1)          AS smaller_by
FROM system.columns
WHERE database = 'tracing' AND table IN ('compress_listing', 'compress_plain')
GROUP BY name
ORDER BY sumIf(data_compressed_bytes, table = 'compress_plain') DESC
```

The whole-table figure:

```bash
./scripts/compare-table-sizes.sh
```

```
table             on_disk    raw          rows
compress_listing  5.96 MiB   18.70 MiB  200000
compress_plain    10.71 MiB  31.20 MiB  200000
```

5.96 MiB against 10.71 MiB. The same spans, the same answers, 44% less disk.

Careful with the `raw` column though. It differs between the two tables (18.70
against 31.20) because `LowCardinality` changes what "uncompressed" even means:
the raw form of a dictionary column is already a column of integers. A ratio
built from those two raw numbers would be comparing different things.
`on_disk` is the number that means something across both. What it runs:

```sql
SELECT table,
       formatReadableSize(sum(data_compressed_bytes))   AS on_disk,
       formatReadableSize(sum(data_uncompressed_bytes)) AS raw,
       sum(rows) AS rows
FROM system.parts
WHERE database = 'tracing' AND active
  AND table IN ('compress_listing', 'compress_plain')
GROUP BY table ORDER BY table
```

## Try this

Each of these builds one more scratch table that changes exactly one thing, and
compares it with `compress_listing`. They need the two tables loaded above.

**Break the low cardinality on a column that is not in the sort key.**

```bash
./scripts/try-many-status-codes.sh
```

```
table                 distinct_values  status_code_on_disk
compress_listing                    2  4.21 KiB
compress_many_status            50000  592.61 KiB
```

The script loads the same 200,000 spans into a copy of `compress_listing`, with
one line of the generator changed. Instead of
`if(number % 20 = 0, 'STATUS_CODE_ERROR', 'STATUS_CODE_OK')` the status is
`concat('STATUS_', toString(number % 50000))`.

`status_code` goes from 4.21 KiB to 592.61 KiB, 140 times bigger, while its
declared type never changed. `LowCardinality` is not a compression setting. It
is a bet that the column has few distinct values, and it pays exactly as well as
the bet is true. The script changes `status_code` and not `service_name`
because changing `service_name` also reshuffles the sort key, and then you are
measuring two things at once.

**Move the clock to the front of the sort key.**

```bash
./scripts/try-clock-first-sort-key.sh
```

```
sort_key         timestamp_codec  timestamp_on_disk
timestamp first  Delta, ZSTD(1)   1.95 KiB
timestamp first  ZSTD(1)          478.91 KiB
listing 7.1      Delta, ZSTD(1)   1.03 MiB
listing 7.1      ZSTD(1)          1.09 MiB
```

Ordered by time first, `timestamp` drops from 1.03 MiB to 1.95 KiB. Take `Delta`
off it there and it jumps to 478.91 KiB. Under listing 7.1's own sort key the
same edit moves it from 1.03 MiB to 1.09 MiB, a 6% difference you would struggle
to notice.

`Delta` stores the gap to the previous value, so it only earns anything when
consecutive rows on disk hold consecutive times. Listing 7.1 sorts by service
and span first, which scatters the clock, so most of what `Delta` could do is
already given away by the sort key. A codec is worth whatever the row order lets
it be worth. What it runs, before copying the rows in:

```sql
CREATE TABLE tracing.compress_clock_first AS tracing.compress_listing
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, service_name, span_name, trace_id)
```

```sql
ALTER TABLE tracing.compress_clock_first_no_delta
MODIFY COLUMN timestamp DateTime64(9) CODEC(ZSTD(1))
```

**Turn the ZSTD level up and down on `attributes`.** That column is declared
`CODEC(ZSTD(3))` while everything else is `ZSTD(1)`:

```bash
./scripts/try-zstd-levels.sh
```

```
attributes_codec  attributes_on_disk
CODEC(ZSTD(1))    447.06 KiB
CODEC(ZSTD(3))    356.46 KiB
CODEC(ZSTD(9))    287.61 KiB
```

Each step buys real space, and each step costs CPU on every insert and every
read. Level 3 on the one column that carries repeated key and value text, level
1 everywhere else, is the trade listing 7.1 picked. The other two rows are what
it turned down on either side. What it runs for level 9, before copying the
rows in:

```sql
ALTER TABLE tracing.compress_attr_zstd9
MODIFY COLUMN attributes Map(LowCardinality(String), String) CODEC(ZSTD(9))
```

## Clean up

```bash
./scripts/drop-compression-tables.sh
```

```
otel_traces
```

That drops every scratch table this exercise made and lists what is left. You
may also see `tenant_users` if the tenancy exercise or a live test script is
part way through. This exercise did not touch it.

## Going deeper

[NOTES.md](../NOTES.md), "Why the compression exercise builds its own tables",
explains the Compact-part accounting and the fixed clock in more detail.

`benchmarks/compression_ratio.py` runs the listing 7.1 side of this on its own,
asserts the two claims the chapter makes about `trace_id` and `service_name`,
and writes a JSON record into `benchmarks/results/`.
`benchmarks/tenant_cardinality_blowup.py` takes the high-cardinality variation
above and follows it into section 7.5.2, where the noisy tenant is a real
neighbor and not a `%` operator.
