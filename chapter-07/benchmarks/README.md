# Chapter 7 benchmarks

Four exercises that probe the storage claims Chapter 7 makes, run against the
live companion stack. Each one measures the real thing: column bytes ClickHouse
wrote to disk, granules the query planner actually skipped, what the same query
costs once its data sits on the S3 cold tier, and how far a noisy tenant's
unique-per-span attribute collapses the shared column's compression. None of them
reproduces production scale on a laptop, and none of them prints a number it did
not measure. All four assert the claim they publish and exit non-zero when it
stops holding, so a regression fails rather than quietly printing a different
number. What they assert differs. The bloom, tiering and compression scripts hold
a measured figure to a stated bound. The cardinality script asserts only the
direction of its effect, because the size of that effect is a property of the
seed and asserting it would be asserting the fixture.

## Setup

Bring the stack up first. Wait for ClickHouse to report healthy in
`docker compose ps`, then run the scripts from this directory:

```bash
cd ..
docker compose up -d
docker compose ps
cd benchmarks
NUM_SPANS=200000 python3 compression_ratio.py
python3 bloom_index_pruning.py
python3 tiering_automation.py
python3 tenant_cardinality_blowup.py
```

The `NUM_SPANS` on the first line is the size the compression table in
`RESULTS.md` was measured at, so that run reproduces it. The other three use
their own defaults, which is a million rows each for the bloom and cardinality
scripts and two batches of fifty thousand for tiering. Rows are generated inside
ClickHouse, so the large ones still finish in seconds.

The order above is the order `RESULTS.md` was measured in, and nothing depends on
it. The compression, bloom and cardinality scripts each build and drop their own
scratch table, and the tiering script owns its rows in `otel_traces` by service
name, so all four give the same answer whatever ran before them.

The scripts talk to ClickHouse over the native protocol when `clickhouse-driver`
is importable, and otherwise fall back to the HTTP interface using only the
standard library, so no extra install is required. They find the server by asking
compose which address this project published for it, which is how they reach this
chapter's ClickHouse and not some other stack that happens to hold port 8123. If
that lookup fails, the stack being down being the usual reason, they print what to
do and stop rather than guess at localhost. Setting `CLICKHOUSE_HOST`,
`CLICKHOUSE_PORT` or `CLICKHOUSE_HTTP_PORT` skips the lookup and sends them
wherever you say; `CLICKHOUSE_DB`, `CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD`
work as before.

Results land in `results/` as timestamped JSON, one file per run named down to
the second, so two runs on the same day both survive. Those files are the
measured record; regenerate them on your own hardware with the commands above.
`scripts/render_results.py` renders the most recent run of each benchmark into
`RESULTS.md`.

## compression_ratio.py

Loads N synthetic spans (default one million, `NUM_SPANS` tunable) into a scratch
table that copies listing 7.1 column for column, then runs the listing 7.3
`system.columns` query and reports each column's stored bytes, raw bytes, and
ratio. Rows are generated server side with `FROM numbers(N)`, so nothing large
crosses the wire, but the byte counts are the actual on-disk column sizes after
Delta, T64, LowCardinality dictionary, and ZSTD encoding.

A fixed clock is what makes the byte counts repeat, and the scratch copy is what
lets the clock be fixed. Generating timestamps from `now64(9)` shifted the Delta
codec's base value between runs, a run that crossed an hour boundary reordered
rows inside every sort-key group, and a run that crossed UTC midnight split the
load into two partitions `OPTIMIZE FINAL` cannot merge. A fixed anchor pins all
three, but a fixed past anchor cannot live in `otel_traces`: ClickHouse reserves
space by the TTL rules at insert time, so listing 7.2 writes already-aged rows
straight onto the S3 cold disk, and its fifteen-day rule drops them on the first
merge. The fixture therefore carries listing 7.1's columns, codecs, sort key,
partitioning and skip index with no TTL and no storage policy, and it is dropped
on exit. To run listing 7.3 against the live table instead, apply
`clickhouse/compression.sql` by hand.

Every column in the fixture is written by the generator, `adjusted_count`
included. That one used to be left at its `DEFAULT 1.0`, and a column holding one
value for every row compressed 1185x and led the published table: the
compressibility of a constant, in a table meant to teach column economics. The
generator now writes the sample-rate reciprocals from section 7.4.4, one weight
per trace rather than per span, mostly 1.0 with a slice at 10.0 and a thin tail
at 100.0.

It fails if `trace_id` compresses outside a band around the ~2x floor the chapter
claims for a random identifier, which would mean the generator stopped producing
random ids or the codec path changed, and it fails if `service_name` drops under
a 50x floor, which is where losing the LowCardinality dictionary would put it.
Both bands are wide: `trace_id` measured 1.96x and `service_name` 477x at 200,000
rows. It also fails if `NUM_SPANS` is small enough that ClickHouse keeps the
fixture in a Compact part, roughly under a hundred thousand rows, because
`system.columns` reports no per-column bytes for those and every ratio comes back
`nan`.

What to look for: the LowCardinality columns (`service_name`, `span_name`,
`status_code`) compress hardest, collapsing to a tiny dictionary, and the two of
them that lead the sort key compress hardest of all, since sorting groups their
runs; `status_code` is not in the sort key. `timestamp` rides mid-key in this
schema (the sort key leads
with service and span, not the clock), so its Delta encoding gives only a modest
win rather than the heavy compression a clock-led sort key would earn; and
`trace_id` measured 1.96x, because 32 lowercase hex characters carry 16 random
bytes, so two characters per byte caps the ratio at 2x and the codec squeezes
that encoding back out. It is not the floor here,
`timestamp` compresses worse. What makes `trace_id` the cost driver is the size
it still occupies after that, the largest of any column in the table. See
`results/` for the figures this run measured on synthetic data; regenerate them
on your own hardware with the commands above.

## bloom_index_pruning.py

Builds a scratch table that copies listing 7.1's schema, codecs, sort key and
bloom index exactly, loads it from a fixed row generator, then runs
`EXPLAIN indexes = 1` for five fixed `trace_id` probes twice each: once with
`use_skip_indexes = 0` for what the query scans without the index, and once with
the bloom filter on. It parses the `Granules: selected/total` line from each and
reports the median, min and max across the probes. It fails if any probe gets no
pruning, if a probe trace did not load as generated, or if the median probe still
reads more than 20% of the table's granules.

Everything about the fixture is pinned: the row generator, the timestamp anchor
and the five probe ordinals. That is deliberate. `EXPLAIN indexes = 1` is
analysis time, so with fixed data and a fixed probe the granule counts are exact
and two runs return the same integers. An earlier version probed
`otel_traces` for its newest `trace_id` and reported a single `granules_pruned`
count. That number did not reproduce, because the probe followed whatever the
collector had just written and the granule layout moved with whichever other
benchmark had loaded the table first. Three runs gave 15, 32 and 16. Only the 15
is in `results/`. The other two were not thrown out: the old filenames carried a
date and nothing finer, so a repeat run on the same day overwrote the one before
it. Losing the evidence for a number that would not hold still is why result
files are now stamped to the second.

The unaided baseline reading 123 of 123 is a property of the fixture, not a law.
A primary index can exclude a trailing sort-key column when the leading columns
are constant across a mark range, and the earlier fixture packed its rows into a
narrow window, so the primary key was quietly cutting 147 granules to 19 before
the bloom filter ever saw them. This fixture spreads traces across a full day, so
every granule straddles several `(service_name, span_name, hour)` groups and none
can be excluded on its key range alone. That is the condition the chapter's claim
assumes, and it is what leaves all of the pruning to the bloom filter.

Expect to see: the same integers on every run of the same ClickHouse version.
The spread across the five probes is real and stays, because how many granules
survive depends on where a given trace's spans land. Changing `NUM_SPANS`,
changing the index granularity, or moving to a ClickHouse version that plans
differently will move the counts, and should.

The scratch table is dropped on exit, so `otel_traces` is never touched and the
run order of the other benchmarks does not matter.

## tiering_automation.py

Measures what listing 7.2's cold tier costs to read. It stages two identical
batches on the hot volume, one dated three days back and one dated one day back,
then restores listing 7.2's own boundary (two days to cold, fifteen days delete)
and materializes it, so the older partition qualifies for the move and the newer
one does not. Once the older part reaches `s3_cold` (the MinIO-backed S3 disk) it
runs the same aggregate against both partitions, interleaved, discards the first
round as a warm-up and reports the median, min and max for each plus the ratio.
Both batches come from the same generator, so the only difference between them is
which disk holds the part.

It also records what does not depend on timing: parts moved, rows moved, bytes
moved, how many S3 objects those parts became, and that the cold batch answers
the query identically before and after the move. It fails if the move does not
land inside `POLL_TIMEOUT_S`, if the moved part reports zero bytes, if the
answer changes, or if the cold tier reads less than 1.3x the hot tier.

Before it measures anything it puts the table into a known state: it restores the
listing 7.2 boundary, moves any part already on the cold disk back to the hot
volume, and deletes the rows a previous run left behind (it owns everything under
`service_name = 'tiering-bench'`). So it works on a fresh table and on one you
have already walked through. The restore also runs in a `finally` block, so an
error or a Ctrl-C cannot strand the staging boundary on the table.

The two batches are left in place when the run finishes, one partition on
`default` and one on `s3_cold`, so you can look at them and at the MinIO bucket.
The next run clears them.

What is no longer reported: `move_latency_seconds`, the wall-clock time from the
ALTER to the part appearing on S3. Three runs of the old script gave 1.01s, 9.09s
and 13.11s, and the published number was the 1.01. The 9.09 and the 13.11 are not
in `results/`, and they were not thrown out either: the old filenames carried a
date and nothing finer, so a repeat run on the same day overwrote the one before
it. The two `tiering-move` files that do survive read 1.02s and 1.01s, the low
end of that spread and not the truth of it.

It was never a storage measurement. ClickHouse's move-selecting task sleeps
`merge_selecting_sleep_ms` (5000) when idle and multiplies that by
`merge_selecting_sleep_slowdown_factor` (1.2) on each idle cycle up to
`max_merge_selecting_sleep_ms` (60000), so the three measurements are just points
on 5.0, 6.0, 7.2, 8.6, 10.4s. How long a busy server takes to notice a part is
due to move says nothing about the tier. How much the tier costs to read does,
and that is what replaced it.

Expect to see: parts moved, rows moved and the object count repeat exactly, and
the cold batch always answers the query identically before and after the move.
Bytes moved shifts by a fraction of a percent depending on whether ClickHouse
rewrote the part or relocated it as it stood. The absolute milliseconds do not
repeat at all, since they include the client round trip and whatever else the
machine is doing; the ratio is the number to read. It is a floor, not a
forecast. The cold tier here is MinIO on the same Docker network, which is the
friendliest object store a cold tier will ever have. A real S3 endpoint across a
network is slower, and the gap widens with the size of the read.

## tenant_cardinality_blowup.py

Proves the section 7.5.2 noisy-neighbor mechanism: the shared `attributes` column
compresses well while every tenant writes the same stable keys, but collapses
toward the incompressible floor once one tenant injects a unique-per-span id. It
copies the listing 7.1 schema and codecs into a scratch table, loads two equal
populations (a baseline of stable keys, then a blowup where one tenant of four
adds a `request.uid`), and measures the attributes column's compressed bytes,
ratio, and part count after each load. It reports the delta: how far the ratio
falls and how much the shared column grows. The size of the swing is a property
of the seed, not a universal number, so the script asserts only the direction,
that the ratio falls and the shared column grows, and prints the magnitude rather
than asserting it. Both loads run from a fixed timestamp anchor, for the reason
`compression_ratio.py` does, so two runs report the same bytes. The scratch table
is dropped on exit, so `otel_traces` is never touched.
