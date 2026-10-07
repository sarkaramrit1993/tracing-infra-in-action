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

## Before you start

- Docker with Docker Compose v2, with **about 5 GB of memory** for Docker (Docker
  Desktop: Settings, Resources). The fingerprint benchmark needs that much at its
  peak.
- **About 4 GB of free disk** inside Docker. Check with
  `docker run --rm alpine df -h /`. If the disk fills, Loki silently stops
  accepting logs.
- Python 3 and `curl` on your machine. On Windows, use WSL2.
- Stop any other chapter's stack first (`docker compose ls`). This one uses ports
  8080, 3100, 4317, 4318, 8123, 8888, 8889, 9000, 9090 and 9363.

## 1. Start the stack

```bash
docker compose up -d --build
docker compose ps
```

Give it about 90 seconds. You should see seven services up. A one-shot job,
`kafka-init`, creates the Kafka topic and exits.

## 2. Send some traffic

300 normal checkouts, plus 6 that are forced to fail so there are always errors
to look at:

```bash
for _ in $(seq 1 300); do curl -s -o /dev/null http://localhost:8080/checkout; done
for _ in $(seq 1 6); do curl -s -o /dev/null "http://localhost:8080/checkout?fail=1"; done
```

Paste these helpers into your shell. `ch` runs a ClickHouse query, `promq` reads
one number from Prometheus, and `await` / `await_rows` wait until data has
arrived instead of sleeping for a guessed time:

```bash
ch()      { docker compose exec -T clickhouse clickhouse-client "$@" < /dev/null; }
ch_file() { docker compose exec -T clickhouse clickhouse-client --multiquery < "$1"; }
promq()   { curl -s -G http://localhost:9090/api/v1/query --data-urlencode "query=$1" \
              | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(r[0]['value'][1] if r else 'no data')"; }
await()      { for _ in $(seq 1 180); do
                 awk -v v="$(promq "$1")" -v t="$2" 'BEGIN { exit !(v + 0 >= t) }' && return 0
                 sleep 2
               done
               echo "timed out: $1 never reached $2" >&2; return 1; }
await_rows() { for _ in $(seq 1 180); do
                 [ "$(ch --query "$1" 2>/dev/null)" -ge "$2" ] 2>/dev/null && return 0
                 sleep 2
               done
               echo "timed out: $1 never reached $2" >&2; return 1; }
```

## 3. Look at one failing trace

```bash
await_rows "SELECT count() FROM tracing.otel_traces WHERE status_code = 'STATUS_CODE_ERROR'" 9
ch --query "
SELECT
  if(parent_span_id = '', '>', ' ') AS root,
  substring(trace_id, 1, 8) AS trace,
  rpad(span_name, 18) AS span,
  concat(toString(round(duration_ns / 1e6, 1)), 'ms') AS took,
  status_code
FROM tracing.otel_traces
WHERE trace_id = (
  SELECT trace_id FROM tracing.otel_traces
  WHERE status_code = 'STATUS_CODE_ERROR' ORDER BY timestamp DESC LIMIT 1)
ORDER BY parent_span_id = '' DESC, timestamp"
```

```
>  d3035323  GET /checkout       177.9ms  STATUS_CODE_ERROR
   d3035323  validate_cart       20.8ms   STATUS_CODE_UNSET
   d3035323  inventory.reserve   30.7ms   STATUS_CODE_UNSET
   d3035323  payment.charge      92.5ms   STATUS_CODE_UNSET
   d3035323  fraud.score         41.5ms   STATUS_CODE_ERROR
   d3035323  order.create        21ms     STATUS_CODE_UNSET
   d3035323  notification.send   10.9ms   STATUS_CODE_UNSET
```

Your IDs and timings will differ. Two spans are in error: `fraud.score` is where
the error happened (the deepest error span, which is what section 9.2.2 calls the
origin), and the root span `GET /checkout` failed because of it.

## 4. Compare the error rate before and after the sampler

This is the main result of the chapter. Count `fraud.score` calls and errors in
both series:

```bash
await 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})' 9
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"})'
```

```
306
14
9
9
```

Before the sampler: 306 calls, 9 errors. After it: 14 calls, still 9 errors. The
sampler kept every error but dropped most of the successes. Your post-sampler
total will vary a little from run to run; the other three numbers won't.

Now the two error rates:

```bash
promq 'sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"}) / sum(pre_calls_total{service_name="checkout-service",span_name="fraud.score"})'
promq 'sum(post_calls_total{service_name="checkout-service",span_name="fraud.score",status_code="STATUS_CODE_ERROR"}) / sum(post_calls_total{service_name="checkout-service",span_name="fraud.score"})'
```

```
0.029411764705882353
0.6428571428571429
```

The real error rate is 2.9 percent. The sampled data says 64 percent. Any
dashboard or alert built on the post-sampler series would show the same wrong
number. `exercises/divergence.md` walks through it.

## 5. Look at the service graph

The service graph is built from the same pre-sampler stream:

```bash
await 'sum(traces_service_graph_request_total{client="user",server="checkout-service"})' 306
curl -s -G http://localhost:9090/api/v1/query \
  --data-urlencode 'query=traces_service_graph_request_total' \
  | python3 -c "
import sys,json
for r in json.load(sys.stdin)['data']['result']:
    m = r['metric']
    print(m.get('client','?'), '->', m.get('server','?'), r['value'][1])"
```

```
checkout-service -> fraud-service 297
checkout-service -> inventory-service 306
checkout-service -> notification-service 306
checkout-service -> payment-service 306
user -> checkout-service 297
checkout-service -> fraud-service 9
user -> checkout-service 9
```

Two edges appear twice: once for successful calls (297) and once for failed ones
(9). Together they add up to all 306 calls.

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
curl -s http://localhost:9090/api/v1/rules \
  | python3 -c "
import sys,json
for g in json.load(sys.stdin)['data']['groups']:
    for r in g['rules']:
        print(g['name'], r['type'], r.get('name'), r['health'])"
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

Eleven rules, all `ok`. NOTES.md explains how each rule works.

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
