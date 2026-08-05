# Notes

Why [README.md](README.md) and the two files in `exercises/` are built the way
they are. None of it is needed to run anything. Read a section when something
surprises you, or when you want to know what the demo is doing underneath.

The rule that decides what lands here: a finding that changes what a reader
**does** stays in the walkthrough, next to the command it governs. A finding that
explains what a reader **saw** comes here. Any one exercise can be finished start
to finish without opening this file. If a section here is load-bearing for a
step, it is in the wrong document.

## Why there is one service and no ingest path

Chapter 7's stack runs a producer, a Collector, Kafka, a consumer and two
storage backends, because chapter 7 is about the path spans take into the store
and every hop on that path is part of the subject. Chapter 8 asks a different
question: given bytes already at rest, is the answer true? That is a property of
the query and the schema. Nothing upstream can change it, and nothing upstream
can be blamed for it.

So the ingest path would have been scenery. Worse, it would have been slow
scenery: getting ten million requests through a real producer and a real
Collector takes a long time and a lot of disk, and the number you would get at
the end would be whatever the run happened to produce rather than a number you
can check against the page. If you want the ingest path, it is in `chapter-07/`
and it is the same table on the other end of it.

There is a second reason, and it is the one that actually decided it. The
population has to be known. A real pipeline throws the unsampled traffic away,
which is exactly the situation the chapter is about, and it is also the situation
in which no claim about correctness can be tested. The generator gets to write
down what it produced, because it produced it.

## Why there are two helpers, `ch` and `ch_file`

Both are defined at the top of README's "Look at your trace data", and again at
the top of each exercise, so each one stands alone.

`clickhouse-client` reads stdin for INSERT data even when the rows are already
inline in `VALUES` or come from a `SELECT`, and `docker compose exec -T` hands it
whatever stdin the caller had. If that stdin stays open and never reaches EOF,
the statement sits there forever with nothing printed. A terminal never sends
EOF. That is why `ch` always redirects `/dev/null`: a query it runs can never end
up waiting on your keyboard.

Applying `skipindex.sql` and `rollup.sql` needs the opposite. They feed a whole
`.sql` file to `--multiquery`, which means stdin has to carry the file. One
helper cannot do both, and guessing from the shape of stdin gets it wrong
somewhere. Guessing with `[ -t 0 ]` works at a terminal and hangs anywhere stdin
is an open pipe, which includes CI. So the file case gets its own name,
`ch_file`, and it is the only thing here that puts anything on stdin.

`tests/test_stack.sh` carries the same split as `CH` and `CH_FILE`, and
`generate/generate.py` passes `stdin=subprocess.DEVNULL` for the same reason.

## Why the table ships without the bloom index

Chapter 7's listing 7.1 creates a `bloom_filter` skip index on `trace_id`.
`clickhouse/init.sql` here does not, and that is the single most deliberate
omission in the directory.

Listing 8.2 is a before-and-after. It adds an index and shows two `EXPLAIN`
readings that differ. Add a second identical bloom next to one that is already
pruning and the two readings are the same, because the first one already did the
work, and the listing then demonstrates nothing while appearing to run fine. The
only way to see an index earn its keep is to watch a query that was not using one
start using one.

So the table starts bare, listing 8.2 adds the index, and README tells you to
drop it again afterwards. `tests/test_stack.sh` drops it before step 4 and again
at the end, so the test can run in any order and leaves the table the way boot
left it.

## Why `parent_span_id` exists, and what `''` means

Listing 7.1 does not carry `parent_span_id`. Chapter 7 does not need it: the
questions there are about bytes, compression, tiering and access, and all of
those are questions about spans. Chapter 8's questions are about requests, and
without the parent column there is no way to ask one.

The table stores one row per span and the producer writes seven spans per
request. A bare `count()` over this table returns a span count while the panel
above it says "requests", which is a number seven times too big and plausible
enough that nobody checks it. Every count and rate query in the chapter filters
`parent_span_id = ''` first.

An empty string is the root marker, not a null. A root span has no parent, and
OTLP represents that as a zero-length parent span ID rather than an absent field,
so the column is `String` and the root's value is `''`. Filtering on it picks out
exactly one row per trace. That is the row that stands for the request, which is
why the generator also puts the request's outcome on it, the way an HTTP server
records a response status on the response rather than on some internal call. A
producer that marks only the failing child leaves the root looking healthy, and
the error half of a RED dashboard built on roots then reads near zero while the
service is failing.

The generator does not model a real call tree. All seven spans of a trace carry
the trace's timestamp, and the six children carry the same duration as each
other, one eighth of the root's. A waterfall this is not. It is a population with
a known shape, which is what the chapter's questions need; if you want spans with
real parent-child timing, `chapter-07/` produces them from a real service.

## Why the generator counts instead of flipping coins

Sampling here is deterministic. Every hundredth ordinary request, every second
slow one, every error. A real tail sampler flips a weighted coin per trace, and
this one does not.

That is a real difference and worth naming rather than hiding, because it is the
reason the printed numbers reproduce. With a Bernoulli sampler,
`sum(adjusted_count)` lands *near* the population rather than *on* it, and the
spread is wide at low keep rates. A one-percent keep on 9.92 million requests
keeps about 99,200 traces, and each one carries a weight of 100, so the estimate
moves in steps of 100 and its sampling error is a real interval rather than a
rounding artifact. The README could then only say "close to ten million", and a
reader whose run came back further off than the page would have no way to tell a
bad build from an unlucky draw.

Determinism makes the whole thing gradable. `sum(adjusted_count)` returns
10,000,000 exactly, `quantileExactWeighted(0.99)` returns 180 ms exactly, and the
test script can assert equality instead of a tolerance. When a number is off,
something is wrong, and that is worth more here than realism.

The cost is that the demo hides the confidence interval, which section 8.2.2 says
is the gap that matters most in practice and is almost always missing from
production dashboards. `exercises/unbiased.md` has a variation that swaps the
counter for a real coin and shows the scatter, which is where the interval
becomes visible.

Rows are built server-side with `INSERT ... SELECT FROM numbers()`. A million
spans take seconds, nothing large crosses the wire, and the duration function and
the weight function are each written once and used both for the rows and for the
true-p99 computation, so the two cannot drift apart.

## Why slow and error stay under one percent

The three classes are 9,920,000 normal, 50,000 slow and 30,000 error. Slow and
error together are 0.8 percent of the population, and that is a tuned number, not
a rounded guess.

The p99 is the 99th percentile, so it sits one percent from the top. Keep slow
and error under one percent of the population and the true p99 lands inside the
normal band, at 180 ms. Meanwhile the survivors are 154,200 traces of which
55,000 are slow or error, 36 percent of the kept set against 0.8 percent of the
population, so the unweighted p99 over the survivors lands out at 1445 ms. Three
numbers, well apart: 180 true, 180 weighted, 1445 unweighted.

Push slow and error past one percent and the true p99 moves into the slow band.
All three numbers collapse toward each other, the weighted query no longer has a
low number to find, and the exercise shows nothing while still running clean.
This is the knob to turn first if you are adapting the generator, and the one
most likely to quietly destroy the demonstration.

The 1445 ms figure is also the one in the chapter opener, where a platform team
spent an afternoon rolling back a healthy deploy. That is not a coincidence, it
is the same arithmetic.

## Why timestamps are relative to `now()`

Every row is written at `now64(9)` minus some number of seconds up to 3000, so
the data spans the 50 minutes before the generator ran.

The listings filter on `timestamp >= toStartOfMinute(now() - INTERVAL 1 HOUR)`.
A fixed past anchor, which is what `chapter-07/`'s compression exercise uses for
reproducible byte counts, would put every row outside that window and every
listing would return zero rows on a fresh stack. A query that returns nothing is
the worst possible first experience of a demo, because it looks identical to a
broken build.

Moving the clock costs nothing here. The counts, the weights, the durations and
the true p99 are all functions of the row index and none of them touch the
timestamp, so every printed number is still fully deterministic. Only the clock
moves.

It does leave one edge, and README names it: the data covers 50 minutes and the
window covers 60, so there is about ten minutes of slack before the window starts
sliding off the oldest rows and the totals drift below 10,000,000. Rerun
`generate/generate.py` and they come back exact.

The clock does leave one visible fingerprint. `toStartOfHour(timestamp)` is the
third component of the sort key, so where the 50-minute span falls across the
hour boundary changes how the rows group, which changes what listing 8.2's
primary-key line reports. That is the one number in this directory that does not
reproduce, and "Why the primary key's own number moves between runs" below is
about it.

Partitioning is `toYYYYMMDD(timestamp)`, so a run that straddles UTC midnight
splits across two partitions. Nothing here measures per-part bytes, so that costs
nothing but a second entry in `system.parts`.

## What `EXPLAIN`'s chain means

`EXPLAIN indexes = 1` prints one section per pruning step, in the order the
planner applies them, and each section's denominator is what the section above it
left behind. It is a chain, not a set of independent ratios, and reading it as
independent ratios is how an index gets credit for work the primary key did.

For listing 8.2's query the chain is: MinMax and Partition see no time predicate
and pass all 132 granules through. The primary key is
`(service_name, span_name, toStartOfHour(timestamp), trace_id)` and the query
filters only on `trace_id`, the last component, so ClickHouse falls back to a
generic exclusion search and gets 132 down to some smaller number. Then the bloom
filter runs against those survivors and reports `Granules: 0/N`.

So the bloom's honest score is N to 0. Not 132 to 0. Credit an index with the
fall between its own two numbers and nothing more, because the reduction on the
line above happened whether or not the index existed. The corollary is the test
in section 8.3.4: an index whose own two numbers are the same is pure write
amplification, and the `EXPLAIN` says so directly.

### Why the primary key's own number moves between runs

README prints 27 and says yours will probably differ. Runs of the same generator
have given 20, 24 and 27. Two of the three numbers on that line are stable and
the third is not, which is worth understanding before you read anything into it.

132 is stable: 1,079,400 rows at 8,192 rows per granule is 132 granules, and the
row count is fixed. 0 is stable: the looked-up ID is absent, so no bloom can
report anything else.

The survivor count is the one that moves, and the sort key is why.
`service_name` is one value here and `span_name` is seven, but the third
component is `toStartOfHour(timestamp)` and the generator's timestamps hang off
`now()`. A run that starts at 16:20 spreads its 50 minutes across two hour
buckets, so the table sorts into fourteen groups of rows, each holding its own
sorted run of trace IDs. A run that starts at 16:55 lands inside one bucket and
sorts into seven. The generic exclusion search has to keep, per group, the
granule whose trace-ID range brackets the target, so twice the groups means
roughly twice the survivors. The split between the buckets moves too, which moves
the boundaries again.

None of that changes the reading. The bloom still takes whatever the sort key
left down to zero, which is the whole point of listing 8.2, and it is why the
chain matters more than any single ratio in it. It is also a small live
demonstration of section 8.3.4's real lesson: whether an index prunes is a
question about how the data clusters, not about how selective the predicate
looks.

If you want the same reading twice, generate once and read twice. Regenerating
between the two moves the clock, and moving the clock is the whole mechanism
above.

Zero is the strongest reading available, and it is why the listing looks up
`4bf92f3577b34da6a3ce929d0e0e4736`, the example trace ID from the W3C
trace-context spec. It is not in the data. The bloom answers "no block can
contain this" outright and the query reads nothing at all. Substitute an ID that
is in your data and the bloom cannot prune the granule that really holds it,
which is a correct reading rather than a failure, just a less dramatic one.

## Running the book's listings verbatim

The book's listings are kept terse. A few need a column, an index state or a run
order that this code supplies. If you run a listing verbatim and it behaves
unexpectedly, these are why:

1. **Listing 8.1 needs `parent_span_id`, which listing 7.1 does not create.**
   Run listing 8.1 against a chapter 7 table and it fails on an unknown column.
   `clickhouse/init.sql` here adds it. Drop the filter instead of adding the
   column and the queries do run, but every count is seven times too big, which
   is the more expensive of the two mistakes because nothing complains.
2. **Listing 8.2 needs a table with no trace-ID bloom on it.** Listing 7.1
   creates one. Run listing 8.2 against a chapter 7 table and both `EXPLAIN`
   readings are identical, because the existing index already pruned. `init.sql`
   here leaves the index off for exactly this reason.
3. **Listing 8.2's `MATERIALIZE INDEX` is not optional.** `ADD INDEX` only
   applies to parts written after it, so on a table that already holds rows the
   new index prunes nothing until it is built over what is there.
   `mutations_sync = 2` makes the statement wait for that build instead of
   returning while it runs, and without it the second `EXPLAIN` can read either
   way depending on timing.
4. **Listing 8.3 is not re-runnable on its own.** `CREATE MATERIALIZED VIEW`
   fails if the view already exists, so a second verbatim run errors out.
   `clickhouse/rollup.sql` leads with `DROP VIEW IF EXISTS`, which is why you can
   run it as many times as you like.
5. **Listing 8.3's `POPULATE` matters more than it looks.** Without it the view
   only ever sees inserts that arrive after it was created. Build the rollup on a
   store that is already full and the dashboard reads zero while looking
   perfectly healthy. With it, the view backfills from the existing rows and the
   rollup agrees with listing 8.1 to the unit.
6. **Both listings snap their window with `toStartOfMinute`.** The rollup is
   bucketed by minute, so a bare `now() - INTERVAL 1 HOUR` boundary would cut a
   bucket in half and the two queries would count that minute differently. Snap
   both sides or they disagree about traffic that never moved.
