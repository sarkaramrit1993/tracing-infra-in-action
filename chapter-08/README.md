# Chapter 8: Query Patterns and Performance

Runnable companion to chapter 8 of *Tracing Infrastructure in Action*. Chapter 7
built the store. This makes it answerable, and it makes the chapter's claim
checkable: the same bytes return a precise lie or a correct estimate depending on
how the query weights what it reads.

One service, ClickHouse. Chapter 7's stack carried a producer, a Collector, Kafka
and a consumer because chapter 7 is about the path into the store. Chapter 8 asks
whether the answer is true, which is a question you put to the store and not to
the path, so there is no ingest path here. The data comes from
`generate/generate.py`, which builds a ten-million-request population
server-side, keeps it at a rate that differs by class the way a tail sampler
does, writes the survivors, and records what it produced before it sampled
anything.

That last part is the point of the whole directory. In production the population
is gone, thrown away at the sampler, so an unbiased estimate can be compared with
other estimates but never graded. Here it sits in `tracing.ground_truth`, one
row, so you can put a biased `count()`, an unbiased `sum(adjusted_count)` and the
number of requests that actually happened side by side and see which one is
telling the truth.

[NOTES.md](NOTES.md) holds the why behind all of it: why there is no ingest path,
why there are two shell helpers and what breaks if you merge them, why this table
ships without the trace-ID index chapter 7 gives it, and why the generator counts
instead of flipping coins. Read it when something surprises you.

## Listings

| Listing | File | What it shows |
|---------|------|---------------|
| 8.1 | `clickhouse/unbiased.sql` | Biased and unbiased aggregates over sampled data |
| 8.2 | `clickhouse/skipindex.sql` | A bloom-filter skip index, and the `EXPLAIN` that proves it prunes |
| 8.3 | `clickhouse/rollup.sql` | A materialized view that pre-aggregates request and error rates |

The table those three run against is `clickhouse/init.sql`, applied on first
boot. It is chapter 7's listing 7.1 schema with two deliberate changes: it adds
`parent_span_id`, and it leaves off listing 7.1's trace-ID bloom index. Both
changes are load-bearing and both are explained in NOTES.

## Prerequisites

- Docker and Docker Compose v2
- About 2 GB of memory given to Docker. On macOS and Windows that is Docker
  Desktop's own setting under Settings, Resources, not free host RAM. The single
  container settles under 1 GB once the data is loaded
- About 1 GB of free disk for the ClickHouse image, plus 35 MB for the data the
  generator writes
- Python 3 on the host, for `generate/generate.py`. Nothing to install: it shells
  out to `clickhouse-client` inside the container
- A POSIX shell. On Windows, run this inside WSL2

Tear down any other chapter's stack first. This one binds host ports 8123 and
9000, and chapter 7 binds both of those.

## Bring it up

```bash
docker compose up -d
docker compose ps
```

About 20 seconds to healthy, plus whatever the first image pull costs. ClickHouse
applies `init.sql` on the way up, which creates three tables: `tracing.otel_traces`
for the spans, `tracing.ground_truth` for what the generator produced before it
sampled, and `tracing.sampling_policy` for the keep rate per class.

## Generate the data

```bash
python3 generate/generate.py
```

A few seconds. It builds the rows server-side with `INSERT ... SELECT FROM
numbers()`, so nothing large crosses the wire:

```
[generate] population 10,000,000 requests, keeping 154,200 (1.54%)
[generate] true p99 over the full population: 180.0 ms
[generate] writing 1,079,400 spans (154,200 traces x 7)
[generate] 1,079,400 spans, 154,200 roots, sum(adjusted_count) over roots = 10,000,000
[generate] done. The weighted total reproduces the population exactly.
```

Those numbers reproduce exactly on your machine. The sampling is deterministic,
every hundredth and every second rather than a coin flip per trace, which is what
lets this file print a number you can check yours against. NOTES says what that
costs and what a real Bernoulli sampler would do instead.

The generator writes timestamps spread over the 50 minutes before it ran, and
listings 8.1 and 8.3 filter on the last hour. So there is about ten minutes of
slack. If you leave the stack up over lunch and come back, run
`generate/generate.py` again before you compare numbers, or the window will have
slid off the oldest rows.

---

## Look at your trace data

Two helpers for everything below. `ch` runs a query, `ch_file` applies a `.sql`
file:

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
```

`ch` hands ClickHouse an empty stdin, which is what stops a query from sitting
there waiting on your keyboard. `ch_file` is the only thing here that puts
anything on stdin, and what it puts there is a file. See [NOTES.md](NOTES.md) for
why that split is not optional.

Start with the shape of one trace, because a wide table of spans does not look
much like a trace until you group it. This pulls one whole trace from each
sampling class, root first and then its children in call order, and marks each
root with a `>`:

```bash
ch --query "
SELECT
  if(parent_span_id = '', '>', ' ') AS root,
  substring(trace_id, 1, 8) AS trace,
  rpad(span_name, 18) AS span,
  concat(toString(round(duration_ns / 1e6, 1)), 'ms') AS took,
  adjusted_count AS weight,
  status_code
FROM tracing.otel_traces
WHERE trace_id IN (
  SELECT min(trace_id) FROM tracing.otel_traces
  WHERE parent_span_id = '' GROUP BY adjusted_count)
ORDER BY
  weight DESC,
  trace_id,
  indexOf(['GET /checkout', 'validate_cart', 'inventory.reserve',
           'payment.charge', 'fraud.score', 'order.create',
           'notification.send'], span_name)"
```

```
>   00003e3b   GET /checkout       101ms     100   STATUS_CODE_UNSET
    00003e3b   validate_cart       12.6ms    100   STATUS_CODE_UNSET
    00003e3b   inventory.reserve   12.6ms    100   STATUS_CODE_UNSET
    00003e3b   payment.charge      12.6ms    100   STATUS_CODE_UNSET
    00003e3b   fraud.score         12.6ms    100   STATUS_CODE_UNSET
    00003e3b   order.create        12.6ms    100   STATUS_CODE_UNSET
    00003e3b   notification.send   12.6ms    100   STATUS_CODE_UNSET
>   0001261e   GET /checkout       796ms       2   STATUS_CODE_UNSET
    0001261e   validate_cart       99.5ms      2   STATUS_CODE_UNSET
    0001261e   inventory.reserve   99.5ms      2   STATUS_CODE_UNSET
    0001261e   payment.charge      99.5ms      2   STATUS_CODE_UNSET
    0001261e   fraud.score         99.5ms      2   STATUS_CODE_UNSET
    0001261e   order.create        99.5ms      2   STATUS_CODE_UNSET
    0001261e   notification.send   99.5ms      2   STATUS_CODE_UNSET
>   00031f14   GET /checkout       1444ms      1   STATUS_CODE_ERROR
    00031f14   validate_cart       180.5ms     1   STATUS_CODE_OK
    00031f14   inventory.reserve   180.5ms     1   STATUS_CODE_OK
    00031f14   payment.charge      180.5ms     1   STATUS_CODE_OK
    00031f14   fraud.score         180.5ms     1   STATUS_CODE_ERROR
    00031f14   order.create        180.5ms     1   STATUS_CODE_OK
    00031f14   notification.send   180.5ms     1   STATUS_CODE_OK
```

Seven rows per `>`. The root's duration covers the six children under it, and the
root is the only row with an empty `parent_span_id`, which is what makes it
countable as one request. Every count and rate query in this chapter filters on
that, because the table holds seven rows per request and a bare `count()` here
answers a question nobody asked.

The `weight` column is the interesting one, and it is why these three traces are
not interchangeable. The first is ordinary traffic kept at one in a hundred, so
it stands for 100 requests. The second is slow, kept at one in two, standing for
2. The third failed and was kept whole, standing for 1. The store gives all three
one row each. Only the weight remembers that the first one had 99 peers thrown
away and the third had none.

The third trace also shows where the failure sits. `fraud.score` returned
`STATUS_CODE_ERROR` and the root carries the error too, the way an HTTP server
records a response status. That is what lets a rollup count errors by reading
roots alone. A producer that marks only the failing child leaves the root looking
healthy, and the error half of a RED dashboard then reads near zero.

The weights are not invented per row. They are the reciprocal of the keep rate,
and the policy is a table you can read:

```bash
ch --query "
SELECT class, keep_rate, adjusted_count, round(1 / keep_rate, 0) AS reciprocal
FROM tracing.sampling_policy ORDER BY adjusted_count DESC
FORMAT PrettyCompactMonoBlock"
```

```
   ┌─class──┬─keep_rate─┬─adjusted_count─┬─reciprocal─┐
1. │ normal │      0.01 │            100 │        100 │
2. │ slow   │       0.5 │              2 │          2 │
3. │ error  │         1 │              1 │          1 │
   └────────┴───────────┴────────────────┴────────────┘
```

Which shakes out as 99,200 normal traces at weight 100, 25,000 slow at 2, and
30,000 errors at 1. Multiply and add: 9,920,000 + 50,000 + 30,000 = 10,000,000.
So this is what is on disk, and what it stands for:

```bash
ch --query "
SELECT count() AS spans, countIf(parent_span_id = '') AS traces,
       toUInt64(sumIf(adjusted_count, parent_span_id = '')) AS requests_represented
FROM tracing.otel_traces FORMAT PrettyCompactMonoBlock"
```

```
   ┌───spans─┬─traces─┬─requests_represented─┐
1. │ 1079400 │ 154200 │             10000000 │
   └─────────┴────────┴──────────────────────┘
```

And this is the answer sheet, written by the generator before it sampled a thing:

```bash
ch --query "
SELECT requests AS true_requests, p99_ms AS true_p99_ms, errors AS true_errors
FROM tracing.ground_truth FORMAT PrettyCompactMonoBlock"
```

```
   ┌─true_requests─┬─true_p99_ms─┬─true_errors─┐
1. │      10000000 │         180 │       30000 │
   └───────────────┴─────────────┴─────────────┘
```

154,200 traces on disk. Ten million requests behind them. A true p99 of 180 ms
that no unweighted query over those 154,200 rows will ever find, because the rows
that survived are packed with the slow and failing traffic the sampler kept on
purpose. That gap is the rest of this directory.

---

## The two exercises

Two separate things live in this chapter and neither needs the other. So they are
two separate files. Open either one in any order. Each puts the table into the
state it needs and clears up after itself, so neither assumes you ran the other
and neither leaves a mess behind.

| Exercise | Listing | The question |
|---|---|---|
| [exercises/unbiased.md](exercises/unbiased.md) | 8.1 | Four queries over one table, two of them wrong. The population is on disk, so which two is not a matter of opinion. |
| [exercises/rollup.md](exercises/rollup.md) | 8.3 | A materialized view turns the RED dashboard into a lookup. Three ways to build it wrong, all three silent. |

Each ends with a **Try this** section: a few one-line edits with a visible
consequence. Those are where the chapter's claims stop being claims.

If you only do one, do unbiased. It is the chapter's thesis, and it is the only
place in the book where you get to grade an estimate against the population it
estimates.

## The skip index (listing 8.2)

Short enough to run here. The table ships with no index on `trace_id`, so the
first reading is a real before and not a formality:

```bash
ch_file clickhouse/skipindex.sql
```

Two `EXPLAIN` blocks come back around the `ALTER`. The first, from one real run:

```
      PrimaryKey
        Keys:
          trace_id
        Condition: (trace_id in [\'4bf92f3577b34da6a3ce929d0e0e4736\', \'4bf92f3577b34da6a3ce929d0e0e4736\'])
        Parts: 1/1
        Granules: 27/132
        Search Algorithm: generic exclusion search
        Ranges: 25
```

The second, after `ADD INDEX` and `MATERIALIZE INDEX`:

```
      PrimaryKey
        ...
        Granules: 27/132
        Search Algorithm: generic exclusion search
      Skip
        Name: idx_trace_id
        Description: bloom_filter GRANULARITY 1
        Parts: 0/1
        Granules: 0/27
        Ranges: 0
```

Read that as a chain, not as one ratio. `EXPLAIN` prints a section per step. The
primary key goes first and reports how many of the table's 132 granules survived
the sort-key comparison. `trace_id` is last in the sort key and the IDs are
random, so all the sort key can do is a generic exclusion search, and this is as
far as it gets. Then each skip index prints below it, and its denominator is
whatever the step above already left. The bloom's line reads 0 out of 27, not 0
out of 132. Credit an index with the fall between its own two numbers and nothing
else. The reduction on the line above was free.

Your primary-key number will probably not be 27, and that is not a problem. It is
the one number here that moves between runs: 13, 20 and 24 are also real readings
from the same generator, and which one you get depends on where the clock is. NOTES explains why, and the short version is that the
sort key groups by hour and the generator's clock is `now()`. The 132 does not
move, and neither does the bloom's 0.

Zero granules is the strongest reading you can get. `4bf92f3577b34da6a3ce929d0e0e4736`
is the example trace ID from the W3C trace-context spec and it is not in your
data, so the bloom answers the membership question outright and the query reads
nothing at all. Proving absence without touching a granule is exactly what a
point lookup needs from an index.

Put the table back when you are done:

```bash
ch --query "ALTER TABLE tracing.otel_traces DROP INDEX IF EXISTS idx_trace_id"
```

## Run the tests

Offline, no Docker needed: that the schema carries `parent_span_id` and ships
without the trace-ID bloom, that the three `.sql` files hold listings 8.1, 8.2
and 8.3 exactly as the book prints them, and that the generator's arithmetic
closes on ten million. It reads YAML, so it needs PyYAML:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r tests/requirements.txt
python3 tests/test_static.py
```

Live, so the stack must be up and `generate/generate.py` must have run:

```bash
bash tests/test_stack.sh
```

It does not check that the biased and unbiased answers differ, which would pass
on any pair of wrong numbers. It checks that the weighted answer equals the
recorded truth and the biased one does not: `sum(adjusted_count)` against the
population, the weighted p99 against the true p99, the rollup against the raw
scan, and the rollup's error count against the errors the generator produced. It
drops the index and the view it created on the way out, so you can run it before
or after anything else here.

## Tear down

The `-v` flag drops the named volume holding the generated spans.

```bash
docker compose down -v
```

## Notes on running the book's listings

The book's listings are kept terse. A few assume a column, an index state or a
run order that this code supplies. If you run one verbatim and it behaves
unexpectedly, see "Running the book's listings verbatim" in [NOTES.md](NOTES.md).

## Reference

Nothing below is needed to run anything above it.

### Ports

The stack binds host ports 8123 (HTTP) and 9000 (native protocol).

### Version manifest (one tag per image)

| Component | Version | Role |
|---|---|---|
| ClickHouse | `clickhouse/clickhouse-server:25.8` (LTS) | the whole stack: query tier, storage, and the tables all three listings run against |
| Python | 3 on the host, standard library only | `generate/generate.py`, which shells out to `clickhouse-client` in the container |

The tag matches `chapter-07/`, so the differences between the two chapters are in
the schema and not in the server.

### File tree

```
chapter-08/
├── docker-compose.yml          # one service, no ingest path
├── README.md
├── NOTES.md                    # why everything here works the way it does
├── generate/
│   └── generate.py             # builds the population, samples it, records the truth
├── exercises/
│   ├── unbiased.md             # listing 8.1: grade four answers against the population
│   └── rollup.md               # listing 8.3: pre-aggregation, and the silent ways to get it wrong
├── clickhouse/
│   ├── init.sql                # the query-tier table (auto-applied on first boot)
│   ├── unbiased.sql            # listing 8.1
│   ├── skipindex.sql           # listing 8.2
│   ├── rollup.sql              # listing 8.3
│   ├── config.d/
│   │   └── network.xml         # listen on the container network, not just localhost
│   └── users.d/
│       └── z-allow-network.xml # let the default user connect from the network
└── tests/
    ├── requirements.txt        # PyYAML, the only install test_static.py needs
    ├── test_static.py          # offline: schema shape, listings match the book, generator arithmetic
    └── test_stack.sh           # live: the weighted answer equals the truth, the biased one does not
```
