# Chapter 8: Query Patterns and Performance

Runnable companion to chapter 8 of *Tracing Infrastructure in Action*.

Chapter 7 built the store. This one asks whether its answers are true. You will
see the chapter's main claim with your own eyes: the same bytes on disk give a
precise lie or a correct estimate, depending on whether the query weights what
it reads by the sampling rate.

There is one service, ClickHouse, and no ingest path. `generate/generate.py`
builds a population of ten million requests inside ClickHouse, keeps them at a
rate that differs by class the way a tail sampler does, and records what it
produced before it sampled anything. In production that population is gone,
thrown away by the sampler. Here it sits in `tracing.ground_truth`, one row, so
you can put the biased `count()`, the unbiased `sum(adjusted_count)` and the
number of requests that actually happened side by side and see which one is
telling the truth.

Steps 1 to 3 get you to that comparison. Everything after them is optional. The
why behind each design choice is in [NOTES.md](NOTES.md). You don't need it to
follow along, but it's worth opening when something surprises you.

## Listings

| Listing | File | What it shows |
|---------|------|---------------|
| 8.1 | `clickhouse/unbiased.sql` | Biased and unbiased aggregates over sampled data |
| 8.2 | `clickhouse/skipindex.sql` | A bloom-filter skip index, and the `EXPLAIN` that proves it prunes |
| 8.3 | `clickhouse/rollup.sql` | A materialized view that pre-aggregates request and error rates |

All three run against `clickhouse/init.sql`, which ClickHouse applies on first
boot. It is chapter 7's listing 7.1 schema with two deliberate changes: it adds
`parent_span_id`, and it leaves off 7.1's trace-ID bloom filter. NOTES says why.

## Before you start

- Docker with Docker Compose v2, with **about 2 GB of memory** for Docker
  (Docker Desktop: Settings, Resources). The container settles under 1 GB once
  the data is loaded.
- **About 1 GB of free disk** for the ClickHouse image and the 38 MB of data the
  generator writes.
- Python 3 on your machine. Nothing to install: the generator runs
  `clickhouse-client` inside the container. On Windows, use WSL2.
- Stop any other chapter's stack first (`docker compose ls`). This one uses
  ports 8123 and 9000, and chapter 7 uses both.

Run every command from this `chapter-08/` directory. Each step runs a small
script from `scripts/` that prints what it found, and the query inside it is
shown under the step, so you can run it yourself.

## 1. Start the stack

```bash
docker compose up -d --wait
```

It returns once ClickHouse is healthy, about 20 seconds after the first image
pull. On the way up ClickHouse creates three tables: `tracing.otel_traces` for
the spans, `tracing.ground_truth` for what the generator produced before it
sampled, and `tracing.sampling_policy` for the keep rate per class.

## 2. Generate the data

```bash
python3 generate/generate.py
```

```
[generate] population 10,000,000 requests, keeping 154,200 (1.54%)
[generate] true p99 over the full population: 180.0 ms
[generate] writing 1,079,400 spans (154,200 traces x 7)
[generate] 1,079,400 spans, 154,200 roots, sum(adjusted_count) over roots = 10,000,000
[generate] done. The weighted total reproduces the population exactly.
```

A few seconds. Those numbers come out the same on your machine, because the
sampling is deterministic: every hundredth normal request, every second slow
one, every error.

The data covers the twenty minutes before you ran the generator, and every
query here reads the last hour. So you have about forty minutes. After that the
scripts stop and tell you to run `python3 generate/generate.py` again.

## 3. Ask listing 8.1's questions

This is the main result of the chapter. Listing 8.1 asks how many requests there
were and what the p99 latency was, twice: once ignoring the sampling weight and
once using it. The script lines the answers up against the truth:

```bash
./scripts/compare-to-the-truth.sh
```

```
                       requests   p99_ms
ignoring the weight      154200     1445
using the weight       10000000      180
what really happened   10000000      180
```

Ignoring the weight, the survivors say 154,200 requests and a p99 of about
1445 ms. Using it, they say 10,000,000 and 180 ms, which is what happened. Same
rows, same store. The only thing separating the precise lie from the correct
estimate is whether the query weighted what it read.

The unweighted p99 is the one number here that is not fixed: yours may read a
little lower, down to about 1442. The weighted 180 and both request counts are exact every time. The
last row is the generator's own record, written before it sampled anything. It
is the only way to tell which answer is right, and it is the one row production
does not get to have. `exercises/unbiased.md` takes it apart.

What it runs: listing 8.1, `clickhouse/unbiased.sql`, plus one query for the
truth. You can run the file directly with
`docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/unbiased.sql`.

```sql
SELECT service_name, count() AS requests
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(
        now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''
GROUP BY service_name;

SELECT service_name,
       sum(adjusted_count) AS requests
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(
        now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''
GROUP BY service_name;

SELECT service_name,
       round(quantile(0.99)(duration_ns)
             / 1e6, 1) AS p99_ms
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(
        now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''
GROUP BY service_name;

SELECT service_name,
       round(quantileExactWeighted(0.99)(
             duration_ns,
             toUInt64(round(adjusted_count)))
             / 1e6, 1) AS p99_ms
FROM tracing.otel_traces
WHERE timestamp >= toStartOfMinute(
        now() - INTERVAL 1 HOUR)
  AND parent_span_id = ''
GROUP BY service_name;
```

```sql
SELECT requests AS true_requests, p99_ms AS true_p99_ms
FROM tracing.ground_truth;
```

## 4. Look at one trace from each class

The span table doesn't look much like traces until you group it. This pulls one
whole trace from each sampling class, root span first, marked `>`:

```bash
./scripts/show-one-trace-per-class.sh
```

```
root   trace      span                took      weight   status_code
>      00003e3b   GET /checkout       101ms        100   STATUS_CODE_UNSET
       00003e3b   validate_cart       12.6ms       100   STATUS_CODE_UNSET
       00003e3b   inventory.reserve   12.6ms       100   STATUS_CODE_UNSET
       00003e3b   payment.charge      12.6ms       100   STATUS_CODE_UNSET
       00003e3b   fraud.score         12.6ms       100   STATUS_CODE_UNSET
       00003e3b   order.create        12.6ms       100   STATUS_CODE_UNSET
       00003e3b   notification.send   12.6ms       100   STATUS_CODE_UNSET
>      0001261e   GET /checkout       796ms          2   STATUS_CODE_UNSET
       0001261e   validate_cart       99.5ms         2   STATUS_CODE_UNSET
       0001261e   inventory.reserve   99.5ms         2   STATUS_CODE_UNSET
       0001261e   payment.charge      99.5ms         2   STATUS_CODE_UNSET
       0001261e   fraud.score         99.5ms         2   STATUS_CODE_UNSET
       0001261e   order.create        99.5ms         2   STATUS_CODE_UNSET
       0001261e   notification.send   99.5ms         2   STATUS_CODE_UNSET
>      00031f14   GET /checkout       1444ms         1   STATUS_CODE_ERROR
       00031f14   validate_cart       180.5ms        1   STATUS_CODE_OK
       00031f14   inventory.reserve   180.5ms        1   STATUS_CODE_OK
       00031f14   payment.charge      180.5ms        1   STATUS_CODE_OK
       00031f14   fraud.score         180.5ms        1   STATUS_CODE_ERROR
       00031f14   order.create        180.5ms        1   STATUS_CODE_OK
       00031f14   notification.send   180.5ms        1   STATUS_CODE_OK
```

These come out the same on every run. Seven rows per `>`. The root's duration
covers the six children under it, and the root is the only row with an empty
`parent_span_id`, which is what makes it countable as one request. Every count
and rate query in this chapter filters on that, because the table holds seven
rows per request and a bare `count()` here answers a question nobody asked.

The `weight` column is why these three traces are not interchangeable. The first
is ordinary traffic kept at one in a hundred, so it stands for 100 requests. The
second is slow, kept at one in two, standing for 2. The third failed and was kept
whole, standing for 1. The store gives all three one row each. Only the weight
remembers that the first one had 99 peers thrown away and the third had none.

The third trace also shows where a failure sits. `fraud.score` returned
`STATUS_CODE_ERROR` and the root carries the error too, the way an HTTP server
records a response status. That is what lets a rollup count errors by reading
root spans alone. A producer that marks only the failing child leaves the root
looking healthy, and the error half of a RED dashboard then reads near zero.

The weights are not invented per row. They are the reciprocal of the keep rate,
and `tracing.sampling_policy` holds that rate per class so you can read the
policy rather than trust it. It works out as 99,200 normal traces at weight 100,
25,000 slow at 2 and 30,000 errors at 1: 9,920,000 + 50,000 + 30,000 =
10,000,000, from 1,079,400 spans on disk. `exercises/unbiased.md` does that
arithmetic against the table.

What it runs:

```sql
SELECT
  if(parent_span_id = '', '>', '') AS root,
  substring(trace_id, 1, 8) AS trace,
  span_name AS span,
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
           'notification.send'], span_name)
```

## 5. Read the answer sheet

The generator wrote this before it sampled a single trace:

```bash
./scripts/show-ground-truth.sh
```

```
true_requests   true_p99_ms   true_errors
     10000000           180         30000
```

154,200 traces on disk. Ten million requests behind them. A true p99 of 180 ms
that no unweighted query over those 154,200 rows will ever find, because the
rows that survived are packed with the slow and failing traffic the sampler kept
on purpose. That gap is what the rest of this directory is about.

What it runs:

```sql
SELECT requests AS true_requests, p99_ms AS true_p99_ms, errors AS true_errors
FROM tracing.ground_truth
```

## 6. Add a skip index (listing 8.2)

The table ships with no index on `trace_id`, so the first reading is a real
"before". Listing 8.2 looks up one trace ID, adds a bloom filter index, and
looks it up again:

```bash
./scripts/add-trace-id-index.sh
```

```
1. before the index
  MinMax
    Condition: true
    Parts: 1/1
    Granules: 132/132
  Partition
    Condition: true
    Parts: 1/1
    Granules: 132/132
  PrimaryKey
    Keys:
      trace_id
    Condition: (trace_id in [\'4bf92f3577b34da6a3ce929d0e0e4736\', \'4bf92f3577b34da6a3ce929d0e0e4736\'])
    Parts: 1/1
    Granules: 27/132
    Search Algorithm: generic exclusion search
  Ranges: 25

2. after ADD INDEX and MATERIALIZE INDEX
  ...
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

3. the same lookup, bounded to the last hour
  ...
  PrimaryKey
    Keys:
      toStartOfHour(timestamp)
      trace_id
    Condition: and((toStartOfHour(timestamp) in [1788321600, +Inf)), (trace_id in [...]))
    Parts: 1/1
    Granules: 27/132
    Search Algorithm: generic exclusion search
  Skip
    Name: idx_trace_id
    ...
    Granules: 0/27

the bloom filter took the 27 granules the primary key left down to 0
```

Read it as a chain, not one ratio. The primary key goes first. `trace_id` is the
last column of the sort key and trace IDs are random, so all it can do is a
generic exclusion search, which keeps 27 of 132 granules. The bloom filter runs
below it, so its denominator is what the primary key left: 27 down to 0. Credit
the index with that and nothing more.

The primary key's 27, and the `Ranges` under it, will probably be different for you: 13, 20, 24 and 27 are
all real readings of the same data, and the number moves with the clock. The 132
and the bloom's 0 don't move. NOTES explains both, and the one time a day 132
reads 133.

The third reading adds `AND timestamp >= now() - INTERVAL 1 HOUR`, and on this
data it matches the second. The generator's rows all fall inside the last hour,
so there is nothing outside the window to prune. NOTES has the longer reading.

Put the table back when you are done:

```bash
./scripts/drop-trace-id-index.sh
```

```
dropped idx_trace_id; tracing.otel_traces has no skip index
```

What it runs: listing 8.2, `clickhouse/skipindex.sql`, after dropping any index
a previous run left. To see the full plans, run
`docker compose exec -T clickhouse clickhouse-client --multiquery < clickhouse/skipindex.sql`.

```sql
EXPLAIN indexes = 1
SELECT * FROM tracing.otel_traces
WHERE trace_id = '4bf92f3577b34da6a3ce929d0e0e4736'
SETTINGS use_query_condition_cache = 0,
         use_skip_indexes_on_data_read = 0;

ALTER TABLE tracing.otel_traces
  ADD INDEX idx_trace_id trace_id
  TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE tracing.otel_traces
  MATERIALIZE INDEX idx_trace_id
  SETTINGS mutations_sync = 2;
```

The file then runs the same `EXPLAIN` twice more: once as it is, and once with
`AND timestamp >= now() - INTERVAL 1 HOUR` added. The drop is:

```sql
ALTER TABLE tracing.otel_traces DROP INDEX IF EXISTS idx_trace_id
```

## Exercises

Each exercise starts from a running stack, makes its own data and cleans up
after itself, so do them in any order.

| Exercise | Listing | What you'll learn |
|---|---|---|
| [exercises/unbiased.md](exercises/unbiased.md) | 8.1 | Four queries over one table, two of them wrong, graded against the real population |
| [exercises/rollup.md](exercises/rollup.md) | 8.3 | How a materialized view turns a RED dashboard into a lookup, and three ways to build it wrong without any error |

If you only do one, do unbiased. It is the chapter's main claim, and the only
place in the book where you can grade an estimate against the population it
estimates. Each ends with a **Try this** section of small changes with visible
results.

## Run the tests

Offline, no Docker needed:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r tests/requirements.txt
python3 tests/test_static.py
```

If you have the book's manuscript checked out, the same run also compares
listings 8.1, 8.2 and 8.3 against the chapter line by line. Without it, those
three checks skip. NOTES says how to point the tests at a checkout.

Against the running stack, after `python3 generate/generate.py`:

```bash
bash tests/test_stack.sh
```

It checks each answer against the recorded truth, not just that the biased and
weighted answers differ. It removes the index and view it creates, so you can
run it any time.

## Tear down

`-v` also removes the data volume. The last two lines remove the test step's
virtual environment, and are harmless if you never made one.

```bash
docker compose down -v
deactivate 2>/dev/null
rm -rf .venv
```

## Reference

### Running the book's listings verbatim

If you run a listing exactly as printed and it behaves unexpectedly, see
"Running the book's listings verbatim" in [NOTES.md](NOTES.md).

### Ports

| Port | What |
|---|---|
| 8123 | ClickHouse HTTP |
| 9000 | ClickHouse native protocol |

Both bind to `127.0.0.1` only.

### Versions

| Component | Version | Role |
|---|---|---|
| ClickHouse | `clickhouse/clickhouse-server:26.1` | The whole stack: query tier, storage, and the tables all three listings run against |
| Python | 3 on the host, standard library only | `generate/generate.py`, which runs `clickhouse-client` in the container |

`chapter-07/` runs 25.8. This one can't, because of listing 8.2; NOTES says why.

### Files

```
chapter-08/
├── docker-compose.yml        # one service, no ingest path
├── README.md
├── NOTES.md                  # why everything works the way it does
├── generate/
│   └── generate.py           # builds the population, samples it, records the truth
├── scripts/                  # one script per step; lib.sh holds what they share
├── exercises/
│   ├── unbiased.md           # listing 8.1: grade four answers against the population
│   └── rollup.md             # listing 8.3: pre-aggregation, and how to get it wrong
├── clickhouse/
│   ├── init.sql              # the query-tier table (auto-applied on first boot)
│   ├── unbiased.sql          # listing 8.1
│   ├── skipindex.sql         # listing 8.2
│   ├── rollup.sql            # listing 8.3
│   ├── config.d/
│   │   └── network.xml       # listen on the container network, not just localhost
│   └── users.d/
│       └── z-allow-network.xml  # let the default user connect over that network
└── tests/
    ├── requirements.txt      # PyYAML, the only install test_static.py needs
    ├── test_static.py        # offline: schema shape, listings match the book, generator arithmetic
    └── test_stack.sh         # live: the weighted answer equals the truth, the biased one does not
```
