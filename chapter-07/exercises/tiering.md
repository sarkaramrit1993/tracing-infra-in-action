# Tiering: where a part goes when it gets old

Run this from `chapter-07/`. It does not depend on the other two exercises and
does not leave anything behind for them.

## The question

Listing 7.2 says aged parts move `TO VOLUME 'cold'`. That sentence hides two
claims worth checking with your own eyes. Does the data physically leave the
local disk and turn into objects in a bucket? And once it has, is it still
queryable, or have you quietly archived it?

Then there is the other half of retention. Deleting a day of traces from a store
that holds billions of spans should not mean touching billions of rows. Listing
7.2 ends with a `DROP PARTITION` for a reason.

## The starting state

Three things to set up.

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
```

First, no row policy. The tenancy exercise builds a `TO ALL` policy that filters
the `default` admin to zero rows, and it drops it again when it finishes. If that
run was interrupted the policy is still there, and everything below would read an
empty table and move a partition it cannot name.

```bash
ch --query "DROP ROW POLICY IF EXISTS tenant_filter ON tracing.otel_traces"
ch --query "DROP ROW POLICY IF EXISTS audit_read ON tracing.otel_traces"
```

Second, listing 7.2's rule has to be on the table. `init.sql` only ships the
fifteen-day delete, so applying `tiering.sql` is what adds the two-day move.

```bash
ch_file clickhouse/tiering.sql
ch --query "
SELECT name, engine_full
FROM system.tables WHERE database = 'tracing' AND name = 'otel_traces'
FORMAT Vertical" | grep -i ttl
```

You should see `toIntervalDay(2) TO VOLUME 'cold'` and `toIntervalDay(15)`.

Third, this exercise needs rows of its own. It owns everything under
`service_name = 'tiering-demo'`, which is how it finds and removes its own data
without going near the spans the collector wrote. Clear anything a previous run
left:

```bash
ch --query "
ALTER TABLE tracing.otel_traces DELETE WHERE service_name = 'tiering-demo'
SETTINGS mutations_sync = 2"
```

## Stage a partition

50,000 spans dated yesterday at midday.

Yesterday is a deliberate choice. `toYYYYMMDD(timestamp)` gives them a partition
of their own, separate from whatever the collector is writing today, so the move
and the drop below touch nothing but this exercise's rows. And one day old is
inside listing 7.2's two-day boundary, so ClickHouse writes them to the hot
volume, which is the whole point: a part has to start hot before you can watch it
go cold.

```bash
ch --query "
INSERT INTO tracing.otel_traces
  (timestamp, trace_id, span_id, service_name, span_name,
   status_code, duration_ns, attributes)
SELECT
  toDateTime64(toStartOfDay(now()), 9) - toIntervalDay(1) + toIntervalHour(12)
    + toIntervalMillisecond(number),
  lower(hex(MD5(toString(intDiv(number, 6))))),
  lower(hex(reinterpretAsFixedString(toUInt64(number)))),
  'tiering-demo',
  ['validate_cart', 'payment.charge', 'order.create'][(number % 3) + 1],
  'STATUS_CODE_OK',
  toUInt64(1000000 + (number * 2654435761) % 200000000),
  map('tier', 'demo')
FROM numbers(50000)"
```

```bash
ch --query "
SELECT partition, disk_name, sum(rows) AS rows, count() AS parts,
       formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
WHERE database = 'tracing' AND table = 'otel_traces' AND active
GROUP BY partition, disk_name ORDER BY partition"
```

```
20260802   default   50000   1   820.61 KiB
20260803   default   488     4   18.77 KiB
```

Yesterday's partition, on `default`, holding only this exercise's rows. Today's
partition is the live traffic and stays where it is throughout.

Hold on to the partition id. Every command below derives it from the rows
themselves, so it is always your partition and never a guess:

```bash
PART=$(ch --query "
SELECT DISTINCT toYYYYMMDD(timestamp) FROM tracing.otel_traces
WHERE service_name = 'tiering-demo'")
echo "partition $PART"
ch --query "
SELECT service_name, count() FROM tracing.otel_traces
WHERE toYYYYMMDD(timestamp) = $PART GROUP BY service_name"
```

## Move it to the cold volume

The rule fires on parts older than two days and yesterday is not two days ago, so
nothing is going to move on its own. Move it by hand.

`MOVE PARTITION` is an explicit instruction. It does not need the part to be
TTL-eligible and it does not touch listing 7.2's rule, so the two-day boundary
stays exactly where the listing put it and there is nothing to restore
afterwards.

```bash
ch --query "ALTER TABLE tracing.otel_traces MOVE PARTITION '$PART' TO VOLUME 'cold'"
ch --query "
SELECT partition, disk_name, sum(rows) AS rows,
       formatReadableSize(sum(bytes_on_disk)) AS size
FROM system.parts
WHERE database = 'tracing' AND table = 'otel_traces' AND active
GROUP BY partition, disk_name ORDER BY partition"
```

```
20260802   s3_cold   50000   820.61 KiB
20260803   default   489     20.38 KiB
```

`disk_name` flipped to `s3_cold`. That disk is defined in
`clickhouse/config.d/storage.xml` and points at the MinIO service, which speaks
the same S3 API as AWS S3, GCS and Azure Blob. Swapping MinIO for one of those is
an endpoint and a credential, not a schema change.

## Check that the objects are really there

ClickHouse's own view first. This counts the blobs behind the parts that are
live right now, rather than the whole bucket, because ClickHouse removes the
blobs of a replaced part lazily and a bucket-wide count would include garbage
from earlier work:

```bash
ch --query "
SELECT count() AS s3_objects, formatReadableSize(sum(size)) AS bytes
FROM system.remote_data_paths
WHERE disk_name = 's3_cold'
  AND splitByChar('/', local_path)[-2] IN (
        SELECT name FROM system.parts
        WHERE database = 'tracing' AND table = 'otel_traces' AND active
          AND partition = '$PART' AND disk_name = 's3_cold')"
```

```
15   822.08 KiB
```

Now ask the object store, which has no idea ClickHouse exists:

```bash
docker compose exec -T minio mc alias set demo http://localhost:9000 traceadmin traceadmin-secret
docker compose exec -T minio mc ls --recursive --summarize demo/traces-cold
```

```
[2026-08-03 18:53:35 UTC]     1B STANDARD dhh/brfkimfxcpktnzhiomqkprxapkfse
[2026-08-03 18:53:35 UTC]   721B STANDARD ahl/dlqfrnjqutahzqxpsdwmyfuzwzsts
[2026-08-03 18:53:35 UTC] 790KiB STANDARD lyd/haeezylhkxkpvmsnakpllggmgeoza
[2026-08-03 18:53:35 UTC]  29KiB STANDARD qbz/mqospftppugsxzgawijatgwfpdyje
...

Total Size: 822 KiB
Total Objects: 15
```

Same object count, same bytes, from two sides that do not share a source. The
column files became opaque blobs with generated names, which is why you cannot
read a part out of a bucket without the server that wrote it.

If you have run this before, `mc` may report more than ClickHouse does. It is
listing the whole bucket, and ClickHouse holds on to a replaced part for
`old_parts_lifetime`, eight minutes by default, before deleting its blobs. That
is the reason the query above scopes itself to parts that are live right now
rather than counting the bucket: a bucket-wide count is not a fact about this
move.

Or open the MinIO console at http://localhost:9001, user `traceadmin`, password
`traceadmin-secret`, and click into the `traces-cold` bucket.

## The data is still data

```bash
ch --time --query "
SELECT count(), uniqExact(trace_id), round(avg(duration_ns) / 1000000.0, 2) AS avg_ms
FROM tracing.otel_traces WHERE service_name = 'tiering-demo'"
```

```
50000   8334   101
0.012
```

Same query, same three answers, and the rows are now sitting in a bucket. The
`--time` line underneath is what it cost, in seconds. Write yours down for the
first variation below; it moves around a little between runs, so take a few.

Nothing in the query mentions a disk. Tiering is invisible to the reader of the
data and visible only in the bill and the latency, which is the property the
whole pattern is built on.

## DROP PARTITION does not read the rows

```bash
time ch --query "ALTER TABLE tracing.otel_traces DROP PARTITION '$PART'"
ch --query "SELECT count() FROM tracing.otel_traces WHERE service_name = 'tiering-demo'"
```

```
real  0m0.132s
0
```

50,000 rows gone, and most of that tenth of a second was `docker exec` starting
a process. Dropping a partition unlinks a directory and updates metadata. It
never visits a row, so the cost does not depend on how many rows the day held,
and it does not care that the rows were on S3 rather than local disk.

That is the contrast the chapter opens with. A tombstone-based store has to write
a marker per row, keep serving reads around those markers, and pay again at
compaction. Here retention is a rename.

## Try this

The drop above took your partition with it, so re-run "Stage a partition" first.
These all work from that point.

**Move it back and time the same query again.** With the partition on `s3_cold`,
put it back on the hot volume and re-run the aggregate:

```bash
ch --query "ALTER TABLE tracing.otel_traces MOVE PARTITION '$PART' TO DISK 'default'"
ch --time --query "
SELECT count(), uniqExact(trace_id), round(avg(duration_ns) / 1000000.0, 2) AS avg_ms
FROM tracing.otel_traces WHERE service_name = 'tiering-demo'"
```

Same answers, and on this laptop roughly half the time, 0.006s against 0.012s.
One pair of readings is not a measurement, so take several of each before you
believe the size of the gap. Read it as a floor and not a forecast either. This
cold tier is MinIO on the same Docker network, the friendliest object store one
will ever have. A real S3 endpoint across a real network is slower, and the gap
grows with the size of the read. `benchmarks/tiering_automation.py` does this
properly, with two matched batches and interleaved repeats.

**Insert rows that are already too old.** With listing 7.2's rule on the table,
stage a batch dated a week back by changing `toIntervalDay(1)` to
`toIntervalDay(7)` in the INSERT, then look at `disk_name` straight away:

```
20260727   s3_cold   50000
20260803   default   629
```

The part never touched the hot volume, and nobody asked for a move. ClickHouse
picks an insert's destination from the move TTL at write time rather than
relocating it later. Backfilling a month of history into a tiered table therefore
writes a month of history to object storage, at object-storage write rates,
whether or not you expected that. It is also why the compression exercise keeps
its fixed past timestamps out of `otel_traces`.

**Let the rule do the moving.** Instead of `MOVE PARTITION`, lower the boundary
so the staged rows cross it, then make the existing part re-evaluate it:

```bash
ch --query "
ALTER TABLE tracing.otel_traces MODIFY TTL
  toDateTime(timestamp) + INTERVAL 1 HOUR TO VOLUME 'cold',
  toDateTime(timestamp) + INTERVAL 15 DAY DELETE"
ch --query "ALTER TABLE tracing.otel_traces MATERIALIZE TTL"
```

Then watch `disk_name` change with nobody asking it to:

```bash
for i in $(seq 1 60); do
  ch --query "
  SELECT now(), partition, disk_name FROM system.parts
  WHERE database = 'tracing' AND table = 'otel_traces' AND active
    AND partition = '$PART'"
  sleep 5
done
```

Give it a minute. It took about 55 seconds here, and a single check five seconds
after the ALTER will make you think nothing happened. The move-selecting task
sleeps five seconds when it is busy and backs off toward sixty when the server
has been quiet, so that delay is a fact about the scheduler and not about
storage. `tiering_automation.py` waits up to three minutes for the same reason,
and publishes no number measured from it.

Two warnings. Do not mix this with a manual `MOVE PARTITION`, because the
background mover will race you and the ALTER fails with
`PART_IS_TEMPORARILY_LOCKED`. And this leaves a one-hour boundary on the table,
which the cleanup below puts back.

## Clean up

```bash
ch --query "
ALTER TABLE tracing.otel_traces DELETE WHERE service_name = 'tiering-demo'
SETTINGS mutations_sync = 2"
ch_file clickhouse/tiering.sql
```

Re-applying the file is the restore: it sets listing 7.2's own boundary back,
whatever the variations above left behind. Confirm:

```bash
ch --query "SELECT count() FROM tracing.otel_traces WHERE service_name = 'tiering-demo'"
ch --query "
SELECT partition, disk_name, sum(rows) AS rows
FROM system.parts WHERE database = 'tracing' AND table = 'otel_traces' AND active
GROUP BY partition, disk_name ORDER BY partition"
```

Zero demo rows, and only today's live partition on `default`. The table keeps
listing 7.2's two-day rule, which is the state the chapter describes.

## Going deeper

[NOTES.md](../NOTES.md) covers how `TO VOLUME 'cold'` resolves against
`storage.xml`, why `DROP PARTITION` is a metadata operation, and why this
exercise moves the partition by hand instead of lowering the boundary first.

`benchmarks/tiering_automation.py` stages two matched batches, lets listing 7.2's
own boundary move the older one, measures what the cold tier costs to read, and
asserts that the moved batch answers identically before and after.
