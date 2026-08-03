# Chapter 7 benchmarks

Four exercises that probe the storage claims Chapter 7 makes, run against the
live companion stack. Each one measures the real thing: column bytes ClickHouse
wrote to disk, granules the query planner actually skipped, the wall-clock time a
part took to move to the S3 cold tier, and how far a noisy tenant's unique-per-span
attribute collapses the shared column's compression. None of them reproduces
production scale on a laptop, and none of them prints a number it did not measure.

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
scripts and fifty thousand for tiering. Rows are generated inside ClickHouse,
so the large ones still finish in seconds.

The scripts talk to ClickHouse over the native protocol when `clickhouse-driver`
is importable, and otherwise fall back to the HTTP interface on port 8123 using
only the standard library, so no extra install is required. Connection settings
come from the environment (`CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`,
`CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_DB`); the defaults match the published ports
in `docker-compose.yml`.

Results land in `results/` as timestamped JSON. Those files are the measured
record; regenerate them on your own hardware with the commands above.

## compression_ratio.py

Loads N synthetic spans (default one million, `NUM_SPANS` tunable) into
`tracing.otel_traces`, then runs the listing 7.3 `system.columns` query and
reports each column's stored bytes, raw bytes, and ratio. Rows are generated
server side with `FROM numbers(N)`, so nothing large crosses the wire, but the
byte counts are the actual on-disk column sizes after Delta, T64, LowCardinality
dictionary, and ZSTD encoding.

The table is truncated first by default so the ratios describe exactly the rows
this script loaded. Set `KEEP_EXISTING=1` to measure the table as it stands.

What to look for: the low-cardinality columns that lead the sort key
(`service_name`, `span_name`, `status_code`) compress hardest, collapsing to a
tiny dictionary; `timestamp` rides mid-key in this schema (the sort key leads
with service and span, not the clock), so its Delta encoding gives only a modest
win rather than the heavy compression a clock-led sort key would earn; and
`trace_id` sits near the incompressible floor, because a random identifier
carries almost no redundancy, so it dominates the on-disk footprint. See
`results/` for the figures this run measured on synthetic data; regenerate them
on your own hardware with the commands above.

## bloom_index_pruning.py

Loads enough rows to span many granules, then runs `EXPLAIN indexes = 1` for a
real point lookup by `trace_id` twice: once with `use_skip_indexes = 0` (the
full-scan granule count, since `trace_id` is last in the sort key and the primary
index cannot prune it) and once with the bloom filter on. It parses the
`Granules: selected/total` line from each and asserts the bloom index selects
strictly fewer granules than the full scan. The script fails loudly rather than
reporting a non-result if the table is too small to span multiple granules.

## tiering_automation.py

Inserts a small recent batch on the hot volume, ages it a few seconds, sets a
short `TO VOLUME 'cold'` boundary and materializes it, then polls `system.parts`
until an active part's `disk_name` flips to `s3_cold` (the MinIO-backed S3 disk).
The reported latency is the wall-clock time from the ALTER to the part landing on
S3, so it includes the object upload, not only the metadata change.

Before it measures anything it puts the table into a known state: it restores the
listing 7.2 boundary (two days to cold, fifteen days delete) and moves any part
that is already on the cold disk back to the hot volume. So it works on a fresh
table and on one you have already walked through, where every part sits on S3
and there would otherwise be nothing left to move. The restore also runs in a
`finally` block, so an error or a Ctrl-C cannot strand the short boundary on the
table.

## tenant_cardinality_blowup.py

Proves the section 7.5.2 noisy-neighbor mechanism: the shared `attributes` column
compresses well while every tenant writes the same stable keys, but collapses
toward the incompressible floor once one tenant injects a unique-per-span id. It
copies the listing 7.1 schema and codecs into a scratch table, loads two equal
populations (a baseline of stable keys, then a blowup where one tenant of four
adds a `request.uid`), and measures the attributes column's compressed bytes,
ratio, and part count after each load. It reports the delta: how far the ratio
falls and how much the shared column grows. The size of the swing is a property
of the seed, not a universal number, so the script prints both populations rather
than asserting a fixed ratio. The scratch table is dropped on exit, so
`otel_traces` is never touched.
