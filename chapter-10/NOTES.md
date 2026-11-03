# Notes

How this stack is wired, and why. You don't need any of it to run the README.

If something surprised you:

- A late span that Flink drops but never counts: [ONE_PHASE](#why-the-flink-job-runs-one_phase).
- A sampled count that is a hundredth of the truth: [the tail sampler writes no weight](#three-samplers-one-topic).
- A metrics endpoint that is `down` in Prometheus: [sample_limit](#the-userid-collector).
- An exemplar that points at nothing: [exemplars](#exemplars-before-and-after-the-sampler).

## The topology

```
checkout-service -> payment-service
        |  OTLP
        v
  edge-collector --- span metrics, service graph (Listing 10.1) ---> :8889 --> Prometheus
        |  every span, unsampled
        v
  kafka  otlp_spans
        |-- sampler-collector --- uniform  (1% of traces) --> otlp_sampled --> span-consumer --> ClickHouse
        |                    \-- policy   (every error + 1%)  --> span metrics only
        |                    \-- weighted (1%, threshold set) --> span metrics only
        |                                                         all three on :8890 --> Prometheus
        '-- span-consumer --> spans.flat --> Flink (Listing 10.2) --> red_metrics
```

The edge Collector counts everything before anything is dropped; nothing in
`traces/in` samples. The sampler is a second Collector that reads the same spans
back off Kafka, as the chapter describes, so one workload can be read through three
sampling decisions at once. Each sampler has its own consumer group and reads every
span.

`checkout-service` calls `payment-service`, so a request makes three spans: a SERVER
span at checkout, a CLIENT span for the call, and a SERVER span at payment. Every
comparison reads the SERVER span `GET /checkout`, so a number counts requests, not
spans.

## Ground truth

The load generator fails request `i` when `i * 29 % 1000 < 29`, so any multiple of
1,000 requests has exactly 2.9 percent errors, and 20,000 requests have 580. Each
request carries a user ID from a pool of 6,000. Nothing is random, so every count can
be checked against what was sent. Background traffic (two `GET /cart` requests a
second) keeps the dashboards from being empty; the comparisons filter it out by span
name.

## Three samplers, one topic

- **uniform**: `tail_sampling` with a 1 percent probabilistic policy. The pinned
  Collector (0.154.0) writes no sampling threshold on the spans it keeps, so the
  connector behind it counts each survivor once, a hundredth of the truth. This is
  the chapter's opening panel. Its survivors also go to `otlp_sampled`, which fills
  ClickHouse.
- **policy**: keeps every error trace plus 1 percent of the rest. The error count is
  exact and the call count isn't, so the error rate comes out near 75 percent against
  a true 2.9: `580 / (580 + 1% of 19,420)`.
- **weighted**: `probabilistic_sampler` in `proportional` mode, 1 percent. It writes
  the threshold onto each kept span (`tracestate` `ot=th:...`), and the connector
  counts each survivor at its weight. The count is right on average and noisy in one
  run: about 7 percent at 200 survivors, more for the handful of errors.

ClickHouse's `adjusted_count` stays at its default of 1.0 for the same reason as the
first bullet, so the view and the query count survivors. Nothing in the stack writes
a real weight, so step 5 loads 50 rows with `adjusted_count = 100` under the service
name `weight-check`, reads them both ways, and deletes them. That exercises the
weighting in Listings 10.3 and 10.4 instead of only printing it.

## Why the Flink job runs ONE_PHASE

Listing 10.2 runs verbatim. The `SET` line before it is the only addition, and it is
there so the late span gets counted.

By default Flink splits a window aggregate into a local half and a global half. A
late span is dropped in the local half, which has no late-record counter, so the
window row doesn't change but `numLateRecordsDropped` stays at zero.
`table.optimizer.agg-phase-strategy = ONE_PHASE` keeps the aggregate on one operator,
and that operator counts what it drops. The REST value can lag by 30 seconds or more,
so `send-late-span.sh` polls.

`spans.flat` has one partition on purpose. A source's watermark is the minimum over
its partitions, and an idle partition holds it back so no window ever closes. A
window closes when the watermark (the largest event time seen, minus five seconds)
passes its end; the background traffic keeps it moving.

Don't resubmit the job without `docker compose down -v`: it replays `spans.flat` and
writes every window a second time.

## What differs from the printed listings

- **Listing 10.1** is in `collector/edge-config.yaml`, byte for byte, with the bucket
  list on one line. Everything it leaves out (receiver, exporters, the Collector's own
  telemetry, `memory_limiter` and `batch`) is in `collector/edge-base.yaml`, merged
  with a second `--config` flag. The limiter runs first, so a batch it refuses and the
  SDK retries is counted once.
- **Listing 10.2** runs verbatim. The source and sink tables and the `SET` sit above
  it in `flink/red.sql`.
- **Listing 10.3** needs its target table first; `clickhouse/red_views.sql` creates
  `red_by_service`, then the view.
- **Listing 10.4** is `clickhouse/red_query.sql`, unchanged. The scripts wrap it in a
  sub-select to filter to one workload.

The trace table stores no span kind, so Listings 10.3 and 10.4 also count the CLIENT
span `payment.charge`, in rows of its own, as the chapter says. The scripts read the
`GET /checkout` rows.

## The user.id Collector

`docker-compose.cardinality.yml` adds `edge-collector-userid`: the edge config plus a
`user.id` dimension. It reads `otlp_spans` from Kafka in its own consumer group,
starting at `latest`, and exports no spans, so the trace path is unchanged. It only
counts spans sent after it starts, which is why `cardinality-blast.sh` waits for it
before sending. `cardinality-down.sh` removes that one service.

The connector's `aggregation_cardinality_limit` of 5000 is per service, so each of
the two services keeps 5,000 label sets and folds the rest into one
`otel_metric_overflow="true"` series. (The Collector docs spell it
`otel.metric.overflow`; Prometheus turns dots into underscores.) Prometheus's
`sample_limit` of 50,000 is the cap that fires: 160,000 series is over it, so the
whole scrape is refused and only that job goes `down`.

## Exemplars, before and after the sampler

Before the sampler, the connector sees every span, so an exemplar can be any trace,
and ClickHouse keeps one in a hundred. After the sampler, the connector only sees
kept traces, which are exactly the ones stored, so every exemplar resolves.

Both connectors set `exemplars.enabled`, both Prometheus exporters set
`enable_open_metrics: true` (the plain text format has no place for an exemplar), and
Prometheus runs with `--enable-feature=exemplar-storage`. The Collector mints one
exemplar per series per flush, not one per span, so the number of exemplars is not a
sample size. Chapter 9's exemplar benchmark
(`chapter-09/benchmarks/exemplar_resolution.py`) measures the resolution rate over a
larger population.

## Why the scripts wait

Every link between a span and a number has a timer: the connector flushes every 60
seconds, the tail sampler waits 5 to decide, Prometheus scrapes every 15, Flink
closes a window 5 seconds after its minute ends. A number read early is short and
doesn't look wrong. So the scripts record the counters before a workload, send it,
and wait until every placement has counted it, instead of sleeping for a guess. That
is why step 3 returns at once after step 2: it reuses the workload step 2 counted.

## Ports and memory

Every port is bound to 127.0.0.1. OTLP (4317, 4318) and the checkout service (8080)
aren't published; the apps reach the Collector on the compose network. Flink's REST
port is 18081 because 8081 is often taken.

The stack peaks near 4.8 GB, 5.6 GB with step 7 running (see `RESULTS.md`).
ClickHouse, Kafka and Flink are most of it. Docker Desktop's default allocation is
smaller; raise it, or Flink is the first to be killed.

## Where to look yourself

The edge Collector (`:8889`; add `-H 'Accept: application/openmetrics-text'` to see
exemplars) serves `traces_span_metrics_calls_total`,
`traces_span_metrics_duration_milliseconds_bucket|_sum|_count` and
`traces_service_graph_request_total` with its `_failed_total`, `_client_seconds_*`
and `_server_seconds_*` relatives. The sampler tier (`:8890`) serves
`uniform_calls_total`, `policy_calls_total` and `weighted_calls_total`, each with the
same duration family. Labels are `service_name`, `span_name`, `span_kind` and
`status_code`, plus `le` on buckets.

`spans.flat` holds one JSON object per span and `red_metrics` one per closed window.
Read either with
`docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9093 --topic red_metrics --from-beginning --timeout-ms 8000 < /dev/null`.

ClickHouse holds `tracing.otel_traces` (chapter 9's schema), `default.red_by_service`
and `default.red_by_service_mv` (Listing 10.3); open a shell with
`docker compose exec clickhouse clickhouse-client`. Flink's REST interface is at
`http://localhost:18081`.
