# Chapter 9: Trace-Driven Insights

Runnable companion to chapter 9 of *Tracing Infrastructure in Action*.

You will run one small checkout service and watch two things the chapter teaches:

1. **A sampled error rate is wrong.** The Collector counts span metrics twice:
   `span_metrics/pre` before the tail sampler and `span_metrics/post` after it. The
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

## Agent workloads

An AI agent makes the same kinds of calls a checkout does: it calls a model, it calls tools, and a tool calls a service. These three scripts send five agent runs through the stack above and show which of those calls become edges in the service graph, which call holds the time, and where a failure lands. The agent and its stand-in model come from [`../chapter-02/agent/`](../chapter-02/agent/), so there is no model to sign up for and every run takes the same steps. The stand-in also plays a payment service, `payment-stub`, so the agent's refund tool has something to call.

Run these after steps 1 to 5. The first run builds the agent image, which takes about a minute.

### A1. Send five agent runs

```bash
./scripts/send-agent-traffic.sh
```

```
sending 4 ordinary agent runs and 1 run whose refund fails every time...
waiting for the runs to reach ClickHouse... ok
sent 5 agent runs, 35 calls from the agent, 7 of them failed
Next: ./scripts/show-agent-dependencies.sh
```

In an ordinary run the refund fails once, with a 503, and the agent retries it. In the fifth run the refund fails on every attempt, so the agent gives up and the run ends in error. A call from the agent is any model call or HTTP request it makes. The failed ones are the 503s: one per ordinary run, three in the failing run.

What it runs, once per run:

```text
docker compose --profile agent run --rm -T agent
AGENT_FAIL_LAST_TOOL=1 docker compose --profile agent run --rm -T agent
```

### A2. Read the service graph and the error spans

```bash
./scripts/show-agent-dependencies.sh
```

```
waiting for the service graph to reach Prometheus... ok
support-agent -> checkout-service 5
support-agent -> payment-stub 4
support-agent -> unknown 19
support-agent -> payment-stub 7 (failed)

Spans with an error status in the run whose refund failed every time:
service        span                        parent
support-agent  invoke_agent support-agent
support-agent  execute_tool issue_refund   invoke_agent support-agent
support-agent  POST                        execute_tool issue_refund
payment-stub   POST /payments/refund       POST
support-agent  POST                        execute_tool issue_refund
payment-stub   POST /payments/refund       POST
support-agent  POST                        execute_tool issue_refund
payment-stub   POST /payments/refund       POST
```

Three edges leave the agent. `support-agent -> checkout-service` is the order lookup, five of them. `support-agent -> payment-stub` is the refund call: 4 that succeeded and 7 that failed, 11 in all, which is two attempts in each ordinary run and three in the failing one. `support-agent -> unknown` is the 19 model calls. They are client calls with no server span on the other side, because the stand-in model does not trace, so the graph has nobody to name.

No edge carries a tool name. A tool call, `execute_tool issue_refund`, is an internal span, so it is neither half of a call and the graph draws nothing for it. The same goes for the retrieval: the agent reads the policy text in its own process, so `retrieval refund-policy` is internal too and the five retrievals make no edge. The tool's name sits one level up: the HTTP call under it is the client half, and `payment-stub` is the server half.

The error table is the failing run. The 503 marks eight spans: the three HTTP calls, the three `payment-stub` spans that answered them, the tool span, and the root. The tool span is there because the agent raised the failure inside it. In an ordinary run the retry succeeds, so only the first HTTP call and its `payment-stub` span are in error, and the tool span ends unset. The parent column is how you walk from the failing service to the tool: `payment-stub` is under `POST`, which is under `execute_tool issue_refund`.

The service graph writes its counts out once a minute, which is why the script waits.

What it runs, in PromQL and SQL:

```promql
traces_service_graph_request_total{client="support-agent"}
```

```sql
SELECT s.service_name AS service, s.span_name AS span, p.span_name AS parent
FROM tracing.otel_traces AS s
LEFT JOIN tracing.otel_traces AS p ON p.trace_id = s.trace_id AND p.span_id = s.parent_span_id
WHERE s.trace_id = '<the failing run>' AND s.status_code = 'STATUS_CODE_ERROR'
ORDER BY s.timestamp, s.span_id
```

### A3. Find the call that holds the time

```bash
./scripts/show-agent-critical-path.sh
```

```
1  chat stand-in-1             18.4%
2  execute_tool lookup_order    0.2%
3  chat stand-in-1             15.5%
4  retrieval refund-policy      0.0%
5  chat stand-in-1             15.5%
6  execute_tool issue_refund   23.0%
7  chat stand-in-1             27.3%
   between steps                0.0%
The call holding the most time: step 7, chat stand-in-1, 27.3% of the run
```

This is the last ordinary run, walked backwards from the end of the root span: take the child that finished last, move to its start, repeat. The agent's steps run one after another, so every step is on the path and the shares add up to 100. Step 7 is the last model call, the one that writes the answer. The seconds belong to the stand-in, which sleeps a set time per call, so read the shares. Yours will differ in the last digit.

What it runs:

```sql
SELECT span_id, parent_span_id, span_name, toUnixTimestamp64Nano(timestamp) AS start_ns, duration_ns
FROM tracing.otel_traces
WHERE trace_id = '<the last ordinary run>'
```

The tail sampler keeps every trace with an error span, so all five runs are stored. A trace with no error would be kept one time in a hundred.

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
docker compose --profile agent down -v
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

Every image and package version is pinned once, in [`versions.yaml`](../versions.yaml) at the repository root. `scripts/check_versions.py` fails if a file in this chapter disagrees with it.
