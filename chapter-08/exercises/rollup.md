# Rollup: the view that answers before the scan starts

Run this from `chapter-08/`. It does not depend on the unbiased exercise, and the
cleanup at the end drops every view it created and removes its own rows.

## The question

Section 8.3.3 opens with a claim that sounds like a slogan until you price it.
The cheapest scan is the one that never happens. Listing 8.3 makes it concrete: a
materialized view fires on every insert and folds new spans into per-minute
rollups, so a rate-and-error dashboard reads a few dozen rows instead of scanning
the span table.

The speed is not the interesting part. The interesting part is what the view has
to get right for its fast answer to still be the same answer. Three things, and
every one of them fails quietly: the view keeps serving, at full speed, with a
number that no longer matches the raw scan.

## The starting state

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
```

The `< /dev/null` is not decoration. Without it the client waits on a stdin that
never reaches EOF. NOTES has the detail.

```bash
docker compose up -d
docker compose ps
```

Wait for `healthy`, then generate the population. Order matters here: generate
first, create the view second.

```bash
ch --query "DROP VIEW IF EXISTS tracing.red_by_service"
python3 generate/generate.py
```

```
[generate] population 10,000,000 requests, keeping 154,200 (1.54%)
[generate] true p99 over the full population: 180.0 ms
[generate] writing 1,079,400 spans (154,200 traces x 7)
[generate] 1,079,400 spans, 154,200 roots, sum(adjusted_count) over roots = 10,000,000
[generate] done. The weighted total reproduces the population exactly.
```

That `DROP` is the one thing to remember about this file. The generator truncates
`tracing.otel_traces` and writes the population again, and a truncate is not an
insert, so a materialized view never hears about it and keeps everything it had.
Run the generator twice with the view in place and the rollup holds two copies of
a population that exists once: 20,000,000 requests read back from a table that
holds ten million. Nothing errors, and the raw table is not wrong. The fix is
always the same: drop the view, run `clickhouse/rollup.sql` again. Going deeper,
at the end of this file, walks through causing it on purpose so you recognize the
shape in six months.

## The rollup

```bash
ch_file clickhouse/rollup.sql
```

```
checkout-service   10000000
```

Ten million requests, from a table the dashboard could print in full. Now grade
it against the query it replaced, which is listing 8.1's `sum(adjusted_count)`:

```bash
ch --query "
SELECT
  (SELECT sum(requests) FROM tracing.red_by_service
    WHERE minute >= toStartOfMinute(now() - INTERVAL 1 HOUR)) AS from_rollup,
  (SELECT sum(adjusted_count) FROM tracing.otel_traces
    WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)
      AND parent_span_id = '') AS from_raw,
  from_rollup - from_raw AS delta"
```

```
10000000   10000000   0
```

Delta zero. Not close, equal. That is the bar a pre-aggregation has to clear, and
the reason both sides snap their window to `toStartOfMinute`: the rollup is
bucketed by minute, so a bare `now() - INTERVAL 1 HOUR` on either side would cut
the boundary bucket differently and the two would disagree by whatever traffic
fell in that minute.

The error half of the dashboard:

```bash
ch --query "
SELECT status_code, sum(requests) AS requests
FROM tracing.red_by_service
WHERE minute >= toStartOfMinute(now() - INTERVAL 1 HOUR)
GROUP BY status_code ORDER BY requests DESC"
```

```
STATUS_CODE_UNSET    9970000
STATUS_CODE_ERROR      30000
```

30,000 errors, which is `tracing.ground_truth.errors` exactly. The rollup reads
root spans only, and the generator writes the request's outcome onto the root the
way an HTTP server records a response status. A producer that marks only the
failing child instead leaves every root looking healthy and this line reads near
zero.

Now the size of the thing:

```bash
ch --query "
SELECT (SELECT count() FROM tracing.red_by_service)  AS rollup_rows,
       (SELECT count() FROM tracing.otel_traces)     AS spans,
       (SELECT countDistinct(minute) FROM tracing.red_by_service) AS minutes"
```

```
42   1079400   21
```

42 rows against 1,079,400 spans. One row per service, per status, per minute, and
the generator lays its traces across the twenty minutes before it runs, so 21
minutes times 2 status codes is the whole dashboard. Expect 40 if the generator's
clock happened to fall on a minute boundary.

What the raw query costs instead:

```bash
ch --query "
EXPLAIN indexes = 1
SELECT sum(adjusted_count) FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''" | grep Granules
```

```
          Granules: 132/132
          Granules: 132/132
          Granules: 132/132
```

Every granule in the table, three times over: the min-max index keeps all 132,
the partition keeps all 132, the primary key keeps all 132. Nothing prunes,
because the sort key leads with `service_name` and `span_name` and there is one
service here, and because an hour of data is the whole table. The scan is the
plan. A million spans is nothing, so it returns fast anyway, and that is exactly
how this ships: at a hundred billion spans the same query is the 20-to-60-second
aggregation section 8.4 calls the inflection point. The rollup is how you never
find out.

That 132 is stable because the generator ends with `OPTIMIZE TABLE
tracing.otel_traces FINAL`, so the insert lands in one part rather than however
many the server felt like. One exception, in NOTES: a run between 00:00 and 00:20
straddles midnight, takes two daily partitions, and can report 133.

## Try this

Two edits, both one word or one clause, both silent in production. Each builds
its own view, tags its rows with its own service name, and drops what it made.
One helper first: it writes 500 root spans into a single minute, each carrying a
weight of 100 and a duration of 180 ms, so a batch is worth exactly 50,000
requests:

```bash
demo_batch() {
  ch --query "
  INSERT INTO tracing.otel_traces
    (timestamp, trace_id, span_id, parent_span_id, service_name, span_name,
     status_code, duration_ns, adjusted_count, attributes)
  SELECT toDateTime64('$2', 9),
         lower(hex(MD5(concat('$1', '$3', toString(number))))),
         lower(hex(reinterpretAsFixedString(toUInt64(number)))), '',
         '$1', 'GET /checkout', 'STATUS_CODE_UNSET',
         180000000, 100, map('http.method', 'POST')
  FROM numbers(500)"
}
```

**Build the view without POPULATE on a store that already has data.** One word
shorter than listing 8.3:

```bash
ch --query "DROP VIEW IF EXISTS tracing.red_no_populate"
ch --query "
CREATE MATERIALIZED VIEW tracing.red_no_populate
ENGINE = SummingMergeTree
ORDER BY (service_name, status_code, minute)
AS SELECT service_name, status_code, toStartOfMinute(timestamp) AS minute,
          sum(adjusted_count) AS requests
FROM tracing.otel_traces WHERE parent_span_id = ''
GROUP BY service_name, status_code, minute"
ch --query "
SELECT (SELECT count() FROM tracing.red_no_populate)      AS rollup_rows,
       (SELECT sum(requests) FROM tracing.red_no_populate) AS dashboard_requests,
       (SELECT count() FROM tracing.otel_traces)           AS spans_in_table"
```

```
0   0   1079400
```

A million spans in the store and the panel reads nothing. The view is a trigger
on insert, so it only ever sees rows that arrive after it exists, and none have.
A zero is at least loud. Now give it one batch of live traffic, the way a real
deployment would within seconds of the view being created:

```bash
MIN=$(ch --query "SELECT toString(toStartOfMinute(now()))")
demo_batch rollup-demo-a "$MIN" 1
ch --query "
SELECT service_name, sum(requests) AS requests
FROM tracing.red_no_populate GROUP BY service_name"
```

```
rollup-demo-a   50000
```

There it is, and this is the dangerous state, not the zero. The panel has a line
on it. The line has a plausible number. The axis is not flat, nothing is red, and
ten million requests are missing. A dashboard built this way looks like a service
that just launched. Compare against the view that was built correctly, which is
still watching the same table:

```bash
ch --query "
SELECT service_name, sum(requests) AS requests
FROM tracing.red_by_service GROUP BY service_name ORDER BY service_name"
```

```
checkout-service   10000000
rollup-demo-a         50000
```

Same source table, same engine, same SELECT, one keyword apart.

```bash
ch --query "DROP VIEW IF EXISTS tracing.red_no_populate"
```

**Leave the root-span filter out of the view.** Same mistake as the unbiased
exercise, one layer further from where anyone will look for it:

```bash
ch --query "DROP VIEW IF EXISTS tracing.red_no_root"
ch --query "
CREATE MATERIALIZED VIEW tracing.red_no_root
ENGINE = SummingMergeTree
ORDER BY (service_name, status_code, minute)
POPULATE
AS SELECT service_name, status_code, toStartOfMinute(timestamp) AS minute,
          sum(adjusted_count) AS requests
FROM tracing.otel_traces
GROUP BY service_name, status_code, minute"
ch --query "
SELECT
  (SELECT sum(requests) FROM tracing.red_no_root   WHERE service_name = 'checkout-service') AS no_root_filter,
  (SELECT sum(requests) FROM tracing.red_by_service WHERE service_name = 'checkout-service') AS listing_8_3,
  round(no_root_filter / listing_8_3, 2) AS ratio"
```

```
70000000   10000000   7
```

Seven times, exactly, because the table holds one row per span and a request is
seven of them. Then look at what it did to the other half of the dashboard:

```bash
ch --query "
SELECT status_code, sum(requests) AS requests
FROM tracing.red_no_root WHERE service_name = 'checkout-service'
GROUP BY status_code ORDER BY requests DESC"
```

```
STATUS_CODE_UNSET   69790000
STATUS_CODE_OK        150000
STATUS_CODE_ERROR      60000
```

The error rate was 30,000 in 10,000,000, which is 0.30 percent. It now reads
60,000 in 70,000,000, which is 0.086 percent. The request count went up seven
times and the error rate went down three and a half times, from the same edit, in
opposite directions, because each failing request contributes two failing spans
out of seven. A `STATUS_CODE_OK` row appeared out of nothing, too: those are the
healthy children of failing requests, which the root-span filter had never let
into the count.

Nothing here throws. Traffic looks up and errors look down, which is the one
combination nobody investigates.

```bash
ch --query "DROP VIEW IF EXISTS tracing.red_no_root"
```

## Clean up

Drop the views and take this exercise's rows back out of the span table:

```bash
ch --query "DROP VIEW IF EXISTS tracing.red_by_service"
ch --query "DROP VIEW IF EXISTS tracing.red_no_populate"
ch --query "DROP VIEW IF EXISTS tracing.red_no_root"
ch --query "
ALTER TABLE tracing.otel_traces DELETE WHERE service_name LIKE 'rollup-demo%'
SETTINGS mutations_sync = 2"
```

Confirm:

```bash
ch --query "SELECT name, engine FROM system.tables WHERE database = 'tracing' ORDER BY name"
ch --query "
SELECT count() AS spans, sum(adjusted_count) AS weighted
FROM tracing.otel_traces WHERE parent_span_id = ''"
```

```
ground_truth      MergeTree
otel_traces       MergeTree
sampling_policy   MergeTree
```

```
154200   10000000
```

Three tables, no views, and the span table back to the population it started
with. Drop the views before the rows, in that order: while a view exists it is
watching inserts, and a delete is not an insert, so rows removed from the source
stay in a rollup forever. That asymmetry is the same one that made the generator
double the rollup at the top of this file, seen from the other side.

## Going deeper

`clickhouse/rollup.sql` is listing 8.3 with its annotations, including why both
sides of the comparison snap to `toStartOfMinute`.

Cause the doubling on purpose once. Run `ch_file clickhouse/rollup.sql`, check
that it reads 10,000,000, run `python3 generate/generate.py` again, and read it
once more. 20,000,000, from a table that holds ten million requests, with no
error anywhere. Then fix it the only way it can be fixed, by dropping the view
and rebuilding it, and you will recognize the shape the next time a rollup and a
raw scan disagree.

`exercises/unbiased.md` is where the weight in `sum(adjusted_count)` comes from
and what it is worth, including what listing 8.1 returns when the weight goes
missing. The rollup inherits all of it: a materialized view over a biased query
is a fast biased query.

Two more if the engine itself interests you, both about SummingMergeTree rather
than about chapter 8's argument.

Send two `demo_batch` batches into one minute with `SYSTEM STOP MERGES` set, and
the view returns one row per insert rather than one per minute. Reading it the
way listing 8.3 does, `GROUP BY` and re-sum, is right either way; reading raw
rows undercounts until a merge that may never come. `SYSTEM START MERGES` and
`OPTIMIZE TABLE ... FINAL` collapse them.

And put `quantileExactWeighted` in a SummingMergeTree instead of a sum. It builds
without complaint, reads 180 ms per batch, then reads 360 after a merge, because
the engine adds a column it was never told was a percentile. `AggregatingMergeTree`
with `quantileExactWeightedState` and a `...Merge` at read time is the shape that
works, and the listing 8.1 weight rides through it unchanged.
