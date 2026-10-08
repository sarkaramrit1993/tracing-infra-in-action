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

Every step below runs a script from `scripts/`, and the SQL it runs is shown
under it. The stack has to be up:

```bash
docker compose up -d --build --wait
```

Listing 7.2's rule has to be on the table. `init.sql` only ships the fifteen-day
delete, so applying `tiering.sql` is what adds the two-day move:

```bash
./scripts/apply-tiering-rule.sh
```

```
TTL toDateTime(timestamp) + toIntervalDay(2) TO VOLUME 'cold', toDateTime(timestamp) + toIntervalDay(15)
```

Two rules: move to `cold` after two days, delete after fifteen. ClickHouse
leaves the word `DELETE` off the second because it is the default. What it
runs is `clickhouse/tiering.sql`:

```sql
ALTER TABLE tracing.otel_traces MODIFY TTL
  toDateTime(timestamp) + INTERVAL 2 DAY TO VOLUME 'cold',
  toDateTime(timestamp) + INTERVAL 15 DAY DELETE;
ALTER TABLE tracing.otel_traces DROP PARTITION '20260601';
```

The `DROP PARTITION` targets a date that does not exist on a fresh stack, so it
does nothing. [NOTES.md](../NOTES.md) has why the listing still carries it.

## Stage a partition

This exercise works on a table of its own, `tracing.tiering_demo`, made with
`CREATE TABLE tracing.tiering_demo AS tracing.otel_traces`. That copies listing
7.1's columns, the `tiered` storage policy and the listing 7.2 rule you just
applied, and none of the rows. Every move, drop and boundary change below then
touches only this exercise's data, never your traffic from the README or a
benchmark's rows that happen to share a date. The script drops the table if an
earlier run left it, makes it again, and writes 50,000 spans dated yesterday at
midday. Running it again starts the exercise over:

```bash
./scripts/stage-tiering-partition.sh
```

```
partition  disk_name   rows  parts  size
 20261007  default    50000      1  820.62 KiB
```

One partition, on `default`. Your date will be yesterday in UTC, which is the
server's clock. One day old is inside listing 7.2's two-day boundary, so
ClickHouse writes the rows to the hot volume. A part has to start hot before you
can watch it go cold. What it runs:

```sql
CREATE TABLE tracing.tiering_demo AS tracing.otel_traces
```

```sql
INSERT INTO tracing.tiering_demo
  (timestamp, trace_id, span_id, service_name, span_name,
   status_code, duration_ns, attributes)
SELECT
  toDateTime64(toStartOfDay(now()), 9) - toIntervalDay($1) + toIntervalHour(12)
    + toIntervalMillisecond(number),
  lower(hex(MD5(toString(intDiv(number, 6))))),
  lower(hex(reinterpretAsFixedString(toUInt64(number)))),
  'tiering-demo',
  ['validate_cart', 'payment.charge', 'order.create'][(number % 3) + 1],
  'STATUS_CODE_OK',
  toUInt64(1000000 + (number * 2654435761) % 200000000),
  map('tier', 'demo')
FROM numbers(50000)
```

`$1` is the number of days back, 1 here.

## Move it to the cold volume

The rule fires on parts older than two days, and yesterday is not two days ago,
so nothing is going to move on its own. Move it by hand:

```bash
./scripts/move-partition-to-cold.sh
```

```
moving partition 20261007
partition  disk_name   rows  parts  size
 20261007  s3_cold    50000      1  820.62 KiB
```

`disk_name` flipped to `s3_cold`. That disk is defined in
`clickhouse/config.d/storage.xml` and points at the SeaweedFS service, which
speaks the same S3 API as AWS S3, GCS and Azure Blob. Swapping SeaweedFS for one
of those is an endpoint and a credential, not a schema change.

`MOVE PARTITION` is an explicit instruction. It does not need the part to be
past the TTL boundary, and it does not touch listing 7.2's rule. The script
reads the partition id off the demo table, never by guessing. What it runs:

```sql
ALTER TABLE tracing.tiering_demo MOVE PARTITION '$PART' TO VOLUME 'cold'
```

## Check that the objects are really there

```bash
./scripts/count-cold-objects.sh
```

```
ClickHouse:
s3_objects  bytes
        15  822.09 KiB

SeaweedFS:
block:  15	logical size:    841822	/buckets/traces-cold
18 directories, 15 files
```

The first count is ClickHouse's own record of the blobs behind the part, scoped
to this table by its uuid, because a demo table dropped by an earlier run keeps
its blobs for a few minutes and its part names are the same. The
second asks the object store, which has no idea ClickHouse exists. Same object
count, same bytes, from two sides that do not share a source. The column files
became opaque blobs with generated names, which is why you cannot read a part
out of a bucket without the server that wrote it.

If you have run this before, SeaweedFS may report more than ClickHouse does. It
counts the whole bucket, blobs from earlier work included, so the ClickHouse
count is the one that is a fact about this move. [NOTES.md](../NOTES.md) has how
long a replaced part's blobs stick around. What it runs:

```sql
SELECT count() AS s3_objects, formatReadableSize(sum(size)) AS bytes
FROM system.remote_data_paths
WHERE disk_name = 's3_cold'
  AND splitByChar('/', local_path)[3] = (
        SELECT toString(uuid) FROM system.tables
        WHERE database = 'tracing' AND name = 'tiering_demo')
  AND splitByChar('/', local_path)[-2] IN (
        SELECT name FROM system.parts
        WHERE database = 'tracing' AND table = 'tiering_demo' AND active
          AND partition = '$PART' AND disk_name = 's3_cold')
```

```bash
echo "fs.du /buckets/traces-cold" | docker compose exec -T seaweedfs weed shell
echo "fs.tree /buckets/traces-cold" | docker compose exec -T seaweedfs weed shell
```

## The data is still data

```bash
./scripts/time-demo-query.sh
```

```
partition 20261007 is on s3_cold
spans  traces  avg_ms
50000    8334     101
took 0.005s
```

Same query, same three answers, and the rows are sitting in a bucket. `took` is
the query's own time, without the second or so `docker compose exec` spends
starting the client. Write yours down for the first variation below. It moves a
little between runs, so take a few.

Nothing in the query mentions a disk. Tiering is invisible to whoever reads the
data, and shows up only in the bill and the latency. What it runs:

```sql
SELECT count() AS spans, uniqExact(trace_id) AS traces,
       round(avg(duration_ns) / 1000000.0, 2) AS avg_ms
FROM tracing.tiering_demo
```

## DROP PARTITION does not read the rows

```bash
./scripts/drop-tiering-partition.sh
```

```
dropped 50000 rows in 0.149s
rows left in tiering_demo: 0
```

50,000 rows gone, and most of that time was `docker compose exec` starting a
process. Dropping a partition unlinks a directory and updates metadata. It never
visits a row, so the cost does not depend on how many rows the day held, and it
does not care that the rows were on S3 rather than local disk.

That is the contrast the chapter opens with. A tombstone-based store has to write
a marker per row, keep serving reads around those markers, and pay again at
compaction. Here retention is a rename. What it runs:

```sql
ALTER TABLE tracing.tiering_demo DROP PARTITION '$PART'
```

## Try this

The drop took your partition with it, so stage it and move it again first.
Staging again starts from a fresh demo table:

```bash
./scripts/stage-tiering-partition.sh
./scripts/move-partition-to-cold.sh
./scripts/time-demo-query.sh
```

**Move it back and time the same query again.**

```bash
./scripts/move-partition-to-hot.sh
```

```
moving partition 20261007
partition  disk_name   rows  parts  size
 20261007  default    50000      1  820.62 KiB

spans  traces  avg_ms
50000    8334     101
took 0.005s
```

Same answers, and faster. Over eight interleaved rounds here the hot side ran
0.006s to 0.022s and the cold side 0.010s to 0.020s, with medians of 0.0085s
and 0.0135s, so about 1.6x. The two ranges overlap, so a single pair either way
can look like 2x or like nothing. Run `./scripts/time-demo-query.sh` a few
times on each side before you believe the size of the gap.

Read it as a floor and not a forecast. This cold tier is SeaweedFS on the same
Docker network, the friendliest object store you will ever have. A real S3
endpoint across a real network is slower, and the gap grows with the size of
the read. `benchmarks/tiering_automation.py` does this properly, with two
matched batches and interleaved repeats. What it runs:

```sql
ALTER TABLE tracing.tiering_demo MOVE PARTITION '$PART' TO DISK 'default'
```

**Insert rows that are already too old.** With listing 7.2's rule on the demo
table, stage a second batch dated a week back:

```bash
./scripts/stage-week-old-partition.sh
```

```
partition  disk_name   rows  parts  size
 20261001  s3_cold    50000      1  820.62 KiB
 20261007  default    50000      1  820.62 KiB
```

The week-old batch is the `s3_cold` line. The other is the partition you staged
earlier, still hot. It is the same INSERT as before, with `$1` set to 7.

That part never touched the hot volume, and nobody asked for a move. ClickHouse
picks an insert's destination from the move TTL at write time rather than
relocating it later. [NOTES.md](../NOTES.md) has what that costs you the day you
backfill history into a tiered table.

**Let the rule do the moving.** Instead of `MOVE PARTITION`, lower the demo
table's boundary to one hour so yesterday's rows cross it, tell the table to
re-evaluate its TTL, and watch `disk_name` change with nobody asking it to. It
needs yesterday's partition on `default`, which is where the first variation
left it:

```bash
./scripts/let-the-rule-move-it.sh
```

```
waiting for the background mover to take partition 20261007 to s3_cold... ok
moved after 3s, with nobody asking
partition  disk_name   rows  parts  size
 20261001  s3_cold    50000      1  820.62 KiB
 20261007  s3_cold    50000      1  820.62 KiB
```

Runs here have taken from under a second to about a minute, and the script
waits up to three. That delay is the scheduler and not the storage;
[NOTES.md](../NOTES.md) has why it swings so far.

The one-hour boundary stays on the demo table, and only there: a boundary that
low on `otel_traces` would send every part older than an hour to S3, your live
traffic included. While it is on, `move-partition-to-hot.sh` refuses, because
the mover would take the partition straight back. Run
`./scripts/stage-tiering-partition.sh` to start over with listing 7.2's rule.
What it runs:

```sql
ALTER TABLE tracing.tiering_demo MODIFY TTL
  toDateTime(timestamp) + INTERVAL 1 HOUR TO VOLUME 'cold',
  toDateTime(timestamp) + INTERVAL 15 DAY DELETE
```

```sql
ALTER TABLE tracing.tiering_demo MATERIALIZE TTL
```

## Clean up

```bash
./scripts/clean-up-tiering.sh
```

```
otel_traces
TTL toDateTime(timestamp) + toIntervalDay(2) TO VOLUME 'cold', toDateTime(timestamp) + toIntervalDay(15)
```

The script drops `tracing.tiering_demo`, which takes every part this exercise
moved with it, S3 objects included. `otel_traces` was never touched apart from
getting listing 7.2's rule, which it keeps: that is the state the chapter
describes. You may also see `tenant_users` in the table list if the tenancy
exercise is part way through. What it runs:

```sql
DROP TABLE IF EXISTS tracing.tiering_demo
```

## Going deeper

[NOTES.md](../NOTES.md) covers how `TO VOLUME 'cold'` resolves against
`storage.xml`, why `DROP PARTITION` is a metadata operation, and why this
exercise moves the partition by hand instead of lowering the boundary first.

`benchmarks/tiering_automation.py` stages two matched batches, lets listing 7.2's
own boundary move the older one, measures what the cold tier costs to read, and
asserts that the moved batch answers identically before and after.
