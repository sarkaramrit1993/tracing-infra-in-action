# Chapter 9: Trace-Driven Insights

Runnable companion to chapter 9 of *Tracing Infrastructure in Action*.

You will run one small checkout service and watch two things the chapter teaches:

1. **A sampled error rate is wrong.** The Collector counts span metrics twice:
   `spanmetrics/pre` before the tail sampler and `spanmetrics/post` after it. The
   sampler keeps every error trace but only one in a hundred of the rest, so the
   two counts give very different error rates for the same traffic.
2. **Traces, logs and metrics join on the trace ID.** Logs carry the trace ID of
   the span they were written in, so you can go from a log line to its trace and
   from a metric to an example trace.

The why behind each design choice is in [NOTES.md](NOTES.md). You don't need it
to follow along.

## Listings

| Listing | File | What it shows |
|---------|------|---------------|
| 9.1 | `collector/gateway-config.yaml` | Span metrics and the service graph derived before the sampler runs |
| 9.2 | `clickhouse/error_index.sql` | An error-issue index as a materialized view, fingerprinting on a normalized message |
| 9.3 | `collector/gateway-config.yaml` | The three bridges between signals, declared in one config |

Listings 9.1 and 9.3 are two parts of the same Collector config file.
The book prints a readable excerpt of each, so the files here differ from it in
small ways. If you plan to paste a listing into your own stack, read
[NOTES.md](NOTES.md) under "Running the book's listings verbatim" first.

## Before you start

- Docker with Docker Compose v2, with **about 5 GB of memory** for Docker (Docker
  Desktop: Settings, Resources). The fingerprint benchmark needs that much at its
  peak.
- **About 4 GB of free disk** inside Docker, and the disk under 90 percent full.
  Check with `docker run --rm alpine df -h /`. Past 90 percent, Loki silently
  stops accepting logs.
- Python 3 and `curl` on your machine. On Windows, use WSL2.
- Stop any other chapter's stack first (`docker compose ls`). This one uses ports
  8080, 3100, 4317, 4318, 8123, 8888, 8889, 9000, 9090 and 9363.

Run every command from this `chapter-09/` directory. Each step runs a small
script from `scripts/` that prints what it found, and the query inside it is
shown under the step, so you can run it yourself.

## 1. Start the stack

```bash
docker compose up -d --build
docker compose ps
```

You should see seven services up. A one-shot job, `kafka-init`, creates the
Kafka topic and exits. Some take a minute to become ready; the next script waits
for them, so you can go straight on.

## 2. Send some traffic

300 normal checkouts, plus 6 that are forced to fail so there are always errors
to look at. Then wait for that traffic to arrive everywhere the next steps read
it:

```bash
./scripts/send-traffic.sh
./scripts/wait-until-ready.sh
```

```
sending 300 ordinary checkouts and 6 forced failures...
sent 306 checkouts, 9 of them failed
waiting for the Collector to receive all 2142 spans the app sent... ok
waiting for the tail sampler to decide all 306 traces... ok
waiting for span metrics to reach Prometheus... ok
waiting for the 98 kept spans to reach ClickHouse... ok
ready
```

Nine failed: the six forced ones, plus every hundredth ordinary checkout, which
the app fails on its own. A span goes through the Collector, the tail sampler,
a 15-second metrics flush, a 15-second Prometheus scrape, and Kafka on its way
to ClickHouse, so the second script checks each of those has counted everything
instead of sleeping for a guessed time. The number of kept spans will differ for
you, because the sampler keeps a random one in a hundred of the successes.

What it runs:

```text
curl -s http://localhost:8080/checkout             300 times
curl -s "http://localhost:8080/checkout?fail=1"    6 times
```

## 3. Look at one failing trace

```bash
./scripts/show-failing-trace.sh
```

```
>  d3035323  GET /checkout      177.9ms  STATUS_CODE_ERROR
   d3035323  validate_cart      20.8ms   STATUS_CODE_UNSET
   d3035323  inventory.reserve  30.7ms   STATUS_CODE_UNSET
   d3035323  payment.charge     92.5ms   STATUS_CODE_UNSET
   d3035323  fraud.score        41.5ms   STATUS_CODE_ERROR
   d3035323  order.create       21ms     STATUS_CODE_UNSET
   d3035323  notification.send  10.9ms   STATUS_CODE_UNSET
```

Your IDs and timings will differ. Two spans are in error: `fraud.score` is where
the error happened (the deepest error span, which is what section 9.2.2 calls the
origin), and the root span `GET /checkout` failed because of it.

What it runs:

```sql
SELECT
  if(parent_span_id = '', '>', ' ') AS root,
  substring(trace_id, 1, 8) AS trace,
  span_name AS span,
  concat(toString(round(duration_ns / 1e6, 1)), 'ms') AS took,
  status_code
FROM tracing.otel_traces
WHERE trace_id = (
  SELECT trace_id FROM tracing.otel_traces
  WHERE status_code = 'STATUS_CODE_ERROR' ORDER BY timestamp DESC LIMIT 1)
ORDER BY parent_span_id = '' DESC, timestamp
```

## 4. Compare the error rate before and after the sampler

This is the main result of the chapter. Count `fraud.score` calls and errors in
both series:

```bash
./scripts/compare-error-rates.sh
```

```
                  calls  errors  error rate
before sampler      306       9        2.9%
after sampler        14       9       64.3%
```

Before the sampler: 306 calls, 9 errors. After it: 14 calls, still 9 errors. The
sampler kept every error but dropped most of the successes. Your first three
numbers will match these. The after-sampler total won't: the sampler keeps a
random one in a hundred of the successes, so that total usually lands between 9
and 16, and the after-sampler error rate anywhere from about 56 percent to 100.
It changes from run to run.

The real error rate is 2.9 percent. In the block above, which is the run
recorded in [RESULTS.md](RESULTS.md), the sampled data says 64 percent. Any
dashboard or alert built on the post-sampler series would show the same wrong
number. `exercises/divergence.md` walks through it.

What it runs, in PromQL. The error rate is errors divided by calls:

```promql
sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})
sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})
sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})
sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})
```

## 5. Look at the service graph

The service graph is built from the same pre-sampler stream:

```bash
./scripts/show-service-graph.sh
```

```
waiting for the service graph to reach Prometheus... ok
checkout-service -> fraud-service 297
checkout-service -> inventory-service 306
checkout-service -> notification-service 306
checkout-service -> payment-service 306
user -> checkout-service 297
checkout-service -> fraud-service 9 (failed)
user -> checkout-service 9 (failed)
```

Two edges appear twice: once for successful calls (297) and once for failed ones
(9). Together they add up to all 306 calls. The service graph writes its counts
out less often than the span metrics, which is why the script waits for it.

What it runs, in PromQL:

```promql
traces_service_graph_request_total
```

## Exercises

Each exercise starts from a running stack and cleans up after itself, so do them
in any order.

| Exercise | Listing | What you'll learn |
|---|---|---|
| [exercises/divergence.md](exercises/divergence.md) | 9.1 | Why the post-sampler error rate is wrong, and how it changes with the sample rate |
| [exercises/fingerprints.md](exercises/fingerprints.md) | 9.2 | How fingerprinting turns two million error spans into a short list of issues |
| [exercises/correlation.md](exercises/correlation.md) | 9.3 | How traces, logs and metrics join on the trace ID, and how each join breaks |

If you only do one, do divergence.

## Alerting rules (not in the book)

`rules/burn_rate.yml` alerts on the error budget using the **pre**-sampler
series, because the post-sampler one is wrong. `rules/span_ingest_gap.yml`
alerts when spans go missing between the app and the Collector. Both load
automatically, so the eight recording rules and three alerts are live as soon as
the stack is up. Check them:

```bash
./scripts/check-rules.sh
```

```
checkout_slo_burn_rate recording slo:checkout_errors:ratio_rate5m ok
checkout_slo_burn_rate recording slo:checkout_errors:ratio_rate30m ok
checkout_slo_burn_rate recording slo:checkout_errors:ratio_rate1h ok
checkout_slo_burn_rate recording slo:checkout_errors:ratio_rate6h ok
checkout_slo_burn_rate alerting CheckoutErrorBudgetBurnFast ok
checkout_slo_burn_rate alerting CheckoutErrorBudgetBurnSlow ok
span_ingest_gap recording spans:received:rate5m ok
span_ingest_gap recording spans:expected:rate5m ok
span_ingest_gap recording spans:ingest_gap:ratio5m ok
span_ingest_gap recording spans:ingest_gap:measurable ok
span_ingest_gap alerting SpanIngestGap ok
```

Eleven rules, all `ok`. The script reads them from
`http://localhost:9090/api/v1/rules`. NOTES.md explains how each rule works.

## Run the tests

Offline, no Docker needed:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r tests/requirements.txt
python3 tests/test_static.py
```

Against the running stack (after you have sent traffic):

```bash
bash tests/test_stack.sh
bash tests/test_correlation.sh
```

Benchmarks are in [benchmarks/README.md](benchmarks/README.md). The last
recorded run is in [RESULTS.md](RESULTS.md).

## Tear down

```bash
docker compose down -v
deactivate 2>/dev/null
rm -rf .venv
```

## Reference

### Ports

| Port | What |
|---|---|
| 8080 | `checkout-service`, and its own `/metrics` |
| 4317, 4318 | Collector OTLP gRPC and HTTP |
| 8888 | Collector internal telemetry |
| 8889 | Collector span-metrics and service-graph scrape endpoint |
| 8123, 9000 | ClickHouse HTTP and native |
| 9363 | ClickHouse Prometheus endpoint |
| 9090 | Prometheus |
| 3100 | Loki HTTP |

All ports bind to `127.0.0.1` only.

### Versions

| Component | Image |
|---|---|
| OpenTelemetry Collector (contrib) | `otel/opentelemetry-collector-contrib:0.154.0` |
| ClickHouse | `clickhouse/clickhouse-server:26.1` |
| Apache Kafka | `apache/kafka:4.3.1` |
| Prometheus | `prom/prometheus:v3.14.0` |
| Loki | `grafana/loki:3.7.8` |
| Python | 3.12 in the app image |
