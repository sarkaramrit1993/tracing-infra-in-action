# Chapter 10: Extracting Metrics from Traces

Runnable companion to chapter 10 of *Tracing Infrastructure in Action*. You run one
small checkout service, send it a known number of requests, and see two things the
chapter teaches:

1. **Where you count decides whether the count is true.** The same 20,000 requests
   are counted in four places and behind three samplers. You know how many were sent
   and how many failed, so you can see which counts are right.
2. **Every label costs series.** One label set is 16 series. Add a label with 6,000
   values and one Collector exposes 160,000.

Every output block below is one measured run. Exact counts move a little from run to
run; the ratios hold. [RESULTS.md](RESULTS.md) has the spread over several runs, and
[NOTES.md](NOTES.md) explains how the stack is wired. You don't need either to follow
along.

## Listings in this directory

| Listing | File | What it shows |
|---------|------|---------------|
| 10.1 | `collector/edge-config.yaml` | Span-metrics and service-graph connectors placed before the sampler |
| 10.2 | `flink/red.sql` | A Flink tumbling-window RED query |
| 10.3 | `clickhouse/red_views.sql` | A ClickHouse insert-time materialized view for RED |
| 10.4 | `clickhouse/red_query.sql` | The same RED query at query time, no view |

Each listing sits between a `---- Listing 10.N` line and a `---- end listing 10.N`
line, exactly as printed, except that Listing 10.1's bucket list is on one line.
Everything outside the markers is what the book leaves out: receivers, processors,
exporters and the tables the listings read from and write to.

## Before you start

- Docker with Docker Compose v2, with **about 8 GB of memory** for Docker (Docker
  Desktop: Settings, Resources). The stack peaks near 5 GB, and step 7 adds about
  1 GB while it runs.
- Python 3 and `curl`. On Windows, use WSL2.
- Stop any other chapter's stack first (`docker compose ls`). This one uses ports
  8123, 8888, 8889, 8890, 8891, 8892, 9000, 9090, 9363 and 18081, all on 127.0.0.1.

Run every command from the `chapter-10/` directory. Each step runs one script and
prints what it found. The whole pass takes about 20 minutes.

| Component | Version |
|-----------|---------|
| OpenTelemetry Collector (contrib) | `otel/opentelemetry-collector-contrib:0.154.0` |
| Apache Kafka | `apache/kafka:4.3.1` |
| ClickHouse | `clickhouse/clickhouse-server:26.1` |
| Prometheus | `prom/prometheus:v3.14.0` |
| Apache Flink | `flink:2.2.1-scala_2.12-java17` with `flink-sql-connector-kafka-5.0.0-2.2` |

A "Try this" that changes a config file edits it, restarts the Collector and runs the
step again. Its `reset` puts the shipped file back exactly and restarts. If you press
Ctrl-C partway, run the `reset` and you are back where you started.

## 1. Start the stack (3 to 6 minutes)

```bash
docker compose up -d --build
./scripts/wait-until-ready.sh
```

```text
waiting for checkout-service... ok
waiting for edge-collector... ok
waiting for sampler-collector... ok
waiting for prometheus... ok
waiting for clickhouse... ok
waiting for flink... ok
waiting for the Flink job to run... ok
waiting for the ClickHouse tables... ok
waiting for the edge Collector's first span metrics (it flushes every 60 s)... ok
waiting for the sampler tier's first span metrics... ok
waiting for Flink to close its first window... ok
ready
```

The script returns once every place that counts has produced its first numbers.

## 2. Count in four places (4 to 7 minutes)

Section 10.2.1 and Figure 10.2. The script sends 20,000 checkout requests, 580 of
which fail, and reads what each placement counted. A sampler is the stage that
decides which traces to keep; this one keeps 1 in 100 at random.

```bash
./scripts/compare-placements.sh
```

```text
placement                             calls  errors  error rate  calls vs truth
ground truth (load generator)         20000  580     2.90%       1.00x
1 Collector edge, before the sampler  20000  580     2.90%       1.00x
2 Flink 60 s windows (Listing 10.2)   20000  580     2.90%       1.00x
3 ClickHouse view (Listing 10.3)      205    7       3.41%       0.01x
4 ClickHouse query (Listing 10.4)     205    7       3.41%       0.01x
```

The edge and Flink sit before the sampler, see every span, and are exact. The two
ClickHouse placements read only the rows the sampler kept, about a hundredth. The
first three rows will match yours exactly; the last two land near 200, most runs
between about 170 and 230.

## 3. See what sampling does to a count (about 1 minute)

Sections 10.1.2 and 10.3.2. The same workload, read behind three samplers. A tail
sampler decides after it has seen a whole trace and counts each kept span once. A
probabilistic sampler records on each kept span how many requests it stands for, its
weight, and the counter multiplies by it. The third keeps every error trace plus 1 in
100 of the rest.

```bash
./scripts/show-sampling-bias.sh
```

```text
counted                                             calls  errors  error rate  calls vs truth
ground truth (load generator)                       20000  580     2.90%       1.00x
edge, before the sampler                            20000  580     2.90%       1.00x
after a 1% tail sampler, each survivor once         205    7       3.41%       0.01x
after a 1% probabilistic sampler, at its weight     19599  500     2.55%       0.98x
after keep-every-error plus 1%, each survivor once  778    580     74.55%      0.04x

each span the probabilistic sampler kept was counted at a weight of 100 (calls counted divided by spans kept)
```

The tail sampler is low by exactly its sample rate. The weighted count is close to
the truth but not on it: right on average, within about 20 percent in any one run,
and its error count, built from a handful of kept errors, moves more. The last row
keeps all 580 errors but a hundredth of the successes, so its error rate reads near
75 percent against a true 2.9. The chapter's opener quotes 64 percent from chapter
9's smaller run, 9 errors in 14 kept calls; with 20,000 requests it settles near 75.

An exemplar is a trace ID attached to a metric, so a slow bucket leads to a trace. It
only helps if that trace is still stored. This takes the exemplars from before the
sampler and from after it and looks each trace ID up in ClickHouse:

```bash
./scripts/check-exemplars.sh
```

```text
exemplar taken                    trace IDs  found in ClickHouse  share
before the sampler (edge, :8889)  6          0                    0.00%
after the sampler (:8890)         6          6                    100.00%
```

Before the sampler, almost every exemplar points at a trace that was dropped. After
it, every one resolves. The counts are small, so read the share.

## 4. Send a late span into a closed window (about 1 minute)

Section 10.2.3 and Figure 10.3. Flink closes a one-minute window once its watermark,
its estimate of how far event time has got, passes the window's end. This sends a
span timestamped inside a window that has already closed.

```bash
./scripts/send-late-span.sh
```

```text
window already closed: GET /cart  2026-11-03 00:53:00  calls 118  errors 0  p99 9.6ms
late spans Flink has dropped so far: 0
sent one GET /cart span with an event time 30 s into that window
late spans Flink has dropped now: 1  (seen after 11 s)
window after:  GET /cart  2026-11-03 00:53:00  calls 118  errors 0  p99 9.6ms
```

The window row doesn't change: Flink dropped the span and counted the drop. Your
window times will differ.

## 5. Compare the view and the query (under 1 minute)

Section 10.2.3, Listings 10.3 and 10.4. Both read the same stored rows. The view keeps
a small running table up to date as rows arrive; the query scans the span table every
time.

```bash
./scripts/compare-mv-and-query.sh
```

```text
read from             calls  errors  rows read  server time
view (Listing 10.3)   205    7       23         2ms
query (Listing 10.4)  205    7       618        3ms
the load generator sent 20000 requests; both read the 205 stored rows the sampler kept

Listing 10.4 as printed gives a row per operation per minute; GET /checkout, newest minutes:
GET /checkout  2026-11-03 00:51:00  60   1  51.1ms
GET /checkout  2026-11-03 00:50:00  119  3  54ms
GET /checkout  2026-11-03 00:49:00  26   3  53.4ms

weight check: 50 stored rows, each standing for 100 requests (adjusted_count = 100)
read as                             rows  calls  errors  p99
each row once                       50    50     2       500ms
view at its weight (Listing 10.3)   50    5000   200     500ms
query at its weight (Listing 10.4)  50    5000   200     500ms
the column holds real weights, so the view and the query count the population: 50 rows are 5000 calls
```

The answers agree, and the view reads a small fraction of the rows. Both count kept
rows, not traffic, because the tail sampler leaves each row's `adjusted_count` at 1.

The weight check loads 50 rows that each carry a weight of 100, reads them both ways,
and deletes them again. Counted once they are 50 calls; at their weight they are
5,000 calls, 200 errors and a p99 of 500ms, from both. That is callout 5 in Listing
10.3: once the column holds real weights, the count is right for the whole traffic.

## 6. Count the series one label set costs (about 1 minute, then 2 for the Try this)

Sections 10.3.5 and 10.4.1. A label set is one combination of labels, here service,
operation, span kind and status. Prometheus stores each histogram bucket, the sum,
the count and the calls counter as its own series.

```bash
./scripts/count-series.sh
```

```text
what                                                        series
one label set (checkout-service, GET /cart, SERVER, UNSET)  16
  of which histogram buckets                                13
label sets the edge Collector holds                         7
span-metrics series the edge Collector holds                112

the 16 series of that one label set:
traces_span_metrics_calls_total
traces_span_metrics_duration_milliseconds_bucket{le="2.0"}
traces_span_metrics_duration_milliseconds_bucket{le="5.0"}
traces_span_metrics_duration_milliseconds_bucket{le="10.0"}
traces_span_metrics_duration_milliseconds_bucket{le="20.0"}
traces_span_metrics_duration_milliseconds_bucket{le="50.0"}
traces_span_metrics_duration_milliseconds_bucket{le="100.0"}
traces_span_metrics_duration_milliseconds_bucket{le="200.0"}
traces_span_metrics_duration_milliseconds_bucket{le="500.0"}
traces_span_metrics_duration_milliseconds_bucket{le="1000.0"}
traces_span_metrics_duration_milliseconds_bucket{le="2000.0"}
traces_span_metrics_duration_milliseconds_bucket{le="5000.0"}
traces_span_metrics_duration_milliseconds_bucket{le="10000.0"}
traces_span_metrics_duration_milliseconds_bucket{le="+Inf"}
traces_span_metrics_duration_milliseconds_count
traces_span_metrics_duration_milliseconds_sum
```

Twelve boundaries make 13 buckets (the last is `+Inf`), plus the sum, the count and
the calls counter: 16 per label set. The 112 is 7 label sets times 16.

**Try this** - Section 10.3.3 says a 300 ms alert needs a 300 ms boundary. This adds
one, restarts the edge Collector and counts again: 17 series per label set. The
restart empties the Collector's memory, so "label sets the edge Collector holds"
reads lower than 7 until traffic touches each operation again. Then put the shipped
config back:

```bash
./scripts/try-edge.sh bucket
```

```bash
./scripts/try-edge.sh reset
```

## 7. Add a label with 6,000 values (about 2 minutes, then 3 for the Try this)

Section 10.3.5. A second edge Collector adds `user.id` as a label. The workload
carries 6,000 distinct user IDs. The script starts that Collector, sends 7,000
requests and shows where the series go.

```bash
./scripts/cardinality-blast.sh
```

```text
series on the edge Collector without user.id:   270
series on the edge Collector with user.id:      160190

calls series per service on the user.id Collector, and the overflow series among them:
checkout-service 5001
payment-service 5001
overflow series (users past the limit, folded together) 2

the scrape job Prometheus refused, and why:
edge-userid-spanmetrics down sample limit exceeded
the other 5 scrape jobs are up
```

Two limits show up. The Collector's `aggregation_cardinality_limit` of 5000 folds each
service's other 1,000 users into one overflow series. That still leaves 160,000
series, more than Prometheus's `sample_limit` of 50,000, so Prometheus refuses that
whole scrape and the other jobs stay up. Expect 5,001 and `down`; the 160,190 shifts
a little.

Remove the extra Collector. Nothing else is touched:

```bash
./scripts/cardinality-down.sh
```

```text
clickhouse up
collectors up
collectors up
edge-spanmetrics up
sampler-spanmetrics up
```

**Try this** - Lower the limit from `5000` to `100` and run the step again. Now the
Collector folds first: about 100 label sets per service, a few thousand series, and
Prometheus accepts the scrape. The metrics degrade instead of disappearing. Then put
the limit back, which also removes the extra Collector:

```bash
./scripts/try-edge.sh limit
```

```bash
./scripts/try-edge.sh reset
```

## Clean up

```bash
docker compose -f docker-compose.yml -f docker-compose.cardinality.yml down -v
```

This removes the containers, volumes and network, including step 7's extra Collector.
If you ran a "Try this" and skipped its `reset`, run the `reset` before you start the
stack again.
