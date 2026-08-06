# Unbiased: the same bytes, a precise lie or a correct estimate

Run this from `chapter-08/`. It does not depend on the rollup exercise, it
creates no tables and no views, and the cleanup at the end puts the span table
back exactly as it found it.

## The question

Section 8.2 says every aggregate over sampled data is wrong until the query
weights it back. Listing 8.1 is that sentence written as four queries: a count
and a percentile, each asked twice, once ignoring the sampling weight and once
respecting it.

The interesting part is not that the two answers differ. Any two queries can
differ. The interesting part is that one of them is right, and that in production
you could never find out which, because the population an estimate estimates was
thrown away at the sampler. Here it was not. `tracing.ground_truth` holds what
the generator produced before it sampled anything, so all four answers can be
graded rather than merely compared.

## The starting state

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
```

The `< /dev/null` is not decoration. Without it `clickhouse-client` waits on a
stdin that never reaches EOF and the command hangs with no output. NOTES has the
detail.

```bash
docker compose up -d
docker compose ps
```

Wait for `healthy`. Then clear any materialized view the rollup exercise may have
left behind, and generate:

```bash
ch --query "DROP VIEW IF EXISTS tracing.red_by_service"
python3 generate/generate.py
```

The `DROP` matters. A truncate never reaches a materialized view's own storage,
so a view left over from `rollup.md` would quietly fold a second copy of the
population into its rollup while the span table stayed correct.

```
[generate] population 10,000,000 requests, keeping 154,200 (1.54%)
[generate] true p99 over the full population: 180.0 ms
[generate] writing 1,079,400 spans (154,200 traces x 7)
[generate] 1,079,400 spans, 154,200 roots, sum(adjusted_count) over roots = 10,000,000
[generate] done. The weighted total reproduces the population exactly.
```

Ten million requests happened. 154,200 of them survived the sampler, 1.54
percent, and each survivor was written as seven spans. That is the only reason
the numbers below are checkable: everything the sampler dropped is still known.

## The policy, on paper

`tracing.sampling_policy` holds the keep rate per class: normal kept one in a
hundred, slow one in two, errors whole. `adjusted_count` is the reciprocal of
that rate, which is the entire trick. A survivor kept at one in a hundred stands
for the hundred requests that looked like it.

Count the survivors by weight and multiply back:

```bash
ch --query "
SELECT p.class                   AS class,
       t.adjusted_count          AS weight,
       t.kept                    AS kept,
       t.kept * t.adjusted_count AS represents
FROM (
  SELECT adjusted_count, count() AS kept
  FROM tracing.otel_traces
  WHERE parent_span_id = '' GROUP BY adjusted_count) AS t
INNER JOIN tracing.sampling_policy AS p ON p.adjusted_count = t.adjusted_count
ORDER BY weight DESC"
```

```
normal   100   99200   9920000
slow     2     25000   50000
error    1     30000   30000
```

99,200 x 100 + 25,000 x 2 + 30,000 x 1 = 10,000,000. Do that on paper. Every
number below is that sum wearing different clothes.

And the row production does not get to have:

```bash
ch --query "SELECT requests, p99_ms, errors FROM tracing.ground_truth"
```

```
10000000   180   30000
```

Ten million requests, a true p99 of 180 ms, 30,000 errors. Computed over the
whole population before a single trace was dropped.

## Four answers and one truth

```bash
ch_file clickhouse/unbiased.sql
```

```
checkout-service   154200
checkout-service   10000000
checkout-service   1445
checkout-service   180
10000000   180
```

Five lines, and the last one is the answer key.

`count()` says 154,200 requests. Ten million happened. It is about 65 times low,
and it is low by exactly the factor the sampler dropped, which is the part worth
sitting with: the bias is not noise, it is the sampling policy showing through.

`sum(adjusted_count)` says 10,000,000. Not close to ten million. Ten million.
Same rows, same scan, same bytes off disk. The only difference is that the second
query multiplies each survivor by what it stands for.

Then the percentiles, and this is the pair the chapter's opening dashboard is
built on. The plain `quantile()` over the survivors says 1445 ms. The weighted
`quantileExactWeighted()` says 180 ms, which is the true p99 to the decimal.

1445 ms is not a rounding error and it is not a noisy tail. Ask the survivor set
what it is made of:

```bash
ch --query "
SELECT round(100 * countIf(adjusted_count < 100) / count(), 1) AS pct_of_kept,
       round(100 * sumIf(adjusted_count, adjusted_count < 100)
             / sum(adjusted_count), 1)                        AS pct_of_traffic
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''"
```

```
35.7   0.8
```

Slow and failing requests are 0.8 percent of the traffic and 35.7 percent of the
rows you kept. That is the whole of it: the sampler worked hardest to keep
exactly the traces that sit in the tail, so the survivor set answers honestly
about itself and tells you nothing true about production. An on-call engineer
reads 1445 ms, starts an investigation, and the system is fine.

One caveat on the window. Timestamps hang off `now()` and span the twenty minutes
before the run, and listing 8.1 asks for the last hour, so you have about forty
minutes. After that every total falls short of the population, which is the clock
and not the math. Re-run `generate/generate.py`.

## Try this

Three edits. Each is one line, each changes a number you can see, and the first
two are the mistakes people actually ship.

**Drop the root-span filter.** This is the single most likely mistake a reader
will make, so make it deliberately and look at what it costs:

```bash
ch --query "
SELECT count() AS spans, sum(adjusted_count) AS weighted
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)"
```

```
1079400   70000000
```

Seventy million. The unbiased query, the correct weight, the right column, and
the answer is seven times the truth. The table stores one row per span and a
request is seven of them, so without `parent_span_id = ''` the query counts spans
and calls them requests. Adding a child span to one service, which is a normal
week's work, moves every request count in the dashboard. And nothing about
70,000,000 looks broken; it is round, plausible and confident. The biased
`count()` over the same rows reads 1,079,400, seven times its own already-wrong
154,200. Both errors compose, and the pair is wrong twice while looking fine
once.

**Strip the weight and watch the correction quietly stop correcting.** This is
section 8.2.1's silent failure mode, made runnable. First at read time, without
touching a byte on disk, by asking listing 8.1's unbiased questions as if the
column held 1:

```bash
ch --query "
SELECT service_name,
       sum(1) AS requests,
       round(quantileExactWeighted(0.99)(duration_ns, toUInt64(1)) / 1e6, 1) AS p99_ms
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''
GROUP BY service_name"
```

```
checkout-service   154200   1444
```

The unbiased query just became the biased one. No error, no warning, no NULL.

Now do it the way it happens in production, where the column is not missing from
the query but empty in the store, because a processor stripped `tracestate` or an
enrichment job did not carry the column forward:

```bash
ch --query "
ALTER TABLE tracing.otel_traces UPDATE adjusted_count = 1
WHERE 1 SETTINGS mutations_sync = 2"
ch_file clickhouse/unbiased.sql
```

```
checkout-service   154200
checkout-service   154200
checkout-service   1445
checkout-service   1444
10000000   180
```

Read those five lines slowly. Listing 8.1 has not changed by one character. The
biased query and the unbiased query now return the same number. The broken
percentile and the corrected one now land a millisecond apart, 1445 against 1444,
which on a dashboard is the same reading. The two pairs the chapter built to
disagree have stopped disagreeing, and every query succeeded.

The only line that still knows anything is the ground-truth row at the bottom,
and production does not have it. That is the whole of section 8.2.1: the number
stays precise, stays confident, and turns wrong, and no part of the system is in
a position to notice. The dashboard stays green because green is a color, not a
claim.

That one-millisecond gap is the entire contribution of the estimator: #C is
`quantile()`, approximate, and #D is `quantileExactWeighted()`, exact. With the
weight gone, approximate against exact is all that is left between them. The
estimator never separated 1445 from 180. The weight did, and it was worth 1265 ms.

Put it back before moving on:

```bash
python3 generate/generate.py
```

**Count distinct instead of counting, and watch the rule run out.** Section 8.2.2
says the adjusted-count rule covers counts, sums and percentiles and stops at
distinct cardinality. Start with what the store can see:

```bash
ch --query "
SELECT uniqExact(trace_id) AS distinct_traces,
       sum(adjusted_count) AS weighted_requests
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''"
```

```
154200   10000000
```

The count on the right is exact. The distinct count on the left is 154,200 and
the truth is ten million distinct traces, so it is off by the same 65x. The
tempting move is obvious: scale it by the weight, the way the count rule does.

It works here, and it works for a bad reason. A trace ID is unique per request,
so distinct traces and requests are the same question, and the correction that
fixed one happens to fix the other. Ask about distinct users and the floor gives
way, because two survivors may be the same user and 9.85 million dropped requests
may be 9.85 million users or one.

Here is why no scale factor can exist. Two populations, sampled identically, that
the query engine cannot tell apart:

```bash
ch --query "
SELECT
  count()                           AS true_users_world_a,
  uniqExact(if(kept, n, 999999999)) AS true_users_world_b,
  uniqExactIf(n, kept)              AS sampled_users_both,
  sum(if(kept, w, 0))               AS weighted_requests
FROM (
  SELECT
    number AS n,
    multiIf(n < 9920000, 100, n < 9970000, 2, 1) AS w,
    multiIf(n < 9920000, n % 100 = 0,
            n < 9970000, (n - 9920000) % 2 = 0,
            1) AS kept
  FROM numbers(10000000))"
```

```
10000000   154201   154200   10000000
```

In world A every request came from its own user, so ten million users. In world B
every dropped request came from a user who also shows up in the kept set, so
154,201 users. Same sampler, same survivors, and the survivors carry the same
154,200 distinct user IDs in both.

The sample is identical. The truth differs by a factor of 65. The correct
multiplier is 64.85 in one world and 1.000006 in the other, and nothing in the
sampled data points at either. The count column on the right stays exactly right
in both worlds, which is the contrast: `sum(adjusted_count)` recovers a total
because a total is additive and the weight says what each survivor stands for. A
distinct count is not additive. There is no weight to apply, so a HyperLogLog
sketch over unsampled data, or accepting that the number is a floor, are the two
honest options.

## Clean up

If you ran the mutation in the second variation, the weights on disk are all 1.
Put the table back:

```bash
python3 generate/generate.py
ch_file clickhouse/unbiased.sql
```

```
checkout-service   154200
checkout-service   10000000
checkout-service   1445
checkout-service   180
10000000   180
```

Those five lines are the state this file started in. Nothing else to undo: this
exercise built no tables, created no views and added no indexes, and the last
three variations ran against `numbers()` and never touched storage. Confirm:

```bash
ch --query "SELECT name FROM system.tables WHERE database = 'tracing' AND engine NOT LIKE '%View%' ORDER BY name"
ch --query "SELECT count() AS skip_indexes FROM system.data_skipping_indices WHERE database = 'tracing' AND table = 'otel_traces'"
```

Three tables, `ground_truth`, `otel_traces` and `sampling_policy`, and zero skip
indexes. The table ships without the trace-ID bloom on purpose so that listing
8.2 has one to add. If you see an index here, `clickhouse/skipindex.sql` put it
there and `ALTER TABLE tracing.otel_traces DROP INDEX idx_trace_id` takes it off.

## Going deeper

`generate/generate.py` documents the population's shape at the top and it is
worth reading before changing anything. In particular, slow and error traffic sum
to under one percent of the population on purpose. That is what puts the true p99
inside the normal band and leaves the survivors, packed with the classes the
sampler favored, with a very different one. Push those two classes past one
percent and all three numbers collapse together and the exercise stops showing
anything. Try it, then read the docstring.

`clickhouse/unbiased.sql` is listing 8.1 with its annotations, including why the
weight argument to `quantileExactWeighted()` is rounded before the cast rather
than after. `clickhouse/init.sql` explains why this table carries
`parent_span_id` when listing 7.1 does not.

`exercises/rollup.md` takes the same weight into a materialized view, where
getting it wrong is quieter, because the rollup keeps answering at full speed
with a number that no longer matches the raw scan.

One more if you want it. The 10,000,000 above is exact because the generator
keeps every hundredth trace rather than flipping a coin per trace. Swap in a real
probabilistic sampler and the weighted total becomes an estimate with a spread:

```bash
ch --query "
SELECT seed, count() AS kept, sum(w) AS weighted,
       round(100 * (sum(w) - 10000000) / 1e7, 2) AS pct_off
FROM (
  SELECT number AS n, arrayJoin(range(1, 11)) AS seed,
         multiIf(n < 9920000, 100, n < 9970000, 2, 1) AS w
  FROM numbers(10000000))
WHERE cityHash64(n, seed) % w = 0
GROUP BY seed ORDER BY seed"
```

Ten seeds, ten answers, spread from about three quarters of a percent low to
half a percent high, and none of them exactly ten million. That spread is the
confidence interval the chapter says is usually missing from a sampled dashboard.
