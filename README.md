# Tracing Infrastructure in Action: Companion Code

Runnable code for the book *Tracing Infrastructure in Action* (Manning). Each directory is a
self-contained stack for one chapter, with its own `README.md` and a
"verify it works" walkthrough.

| Directory | Chapter | Stack |
|-----------|---------|-------|
| `chapter-02/` | Instrumenting a service | Flask + OpenTelemetry SDK + Collector + Jaeger (docker-compose) |
| `chapter-03/` | Collection tiers | Three-tier collection, agent/gateway/consumer Collector configs, Kubernetes manifests, benchmarks |
| `chapter-04/` | Ingestion and buffering | OpenTelemetry gateway + 3-broker Kafka (KRaft) + Jaeger, partitioning benchmarks |
| `chapter-05/` | Trace assembly | Kafka + Flink assembly job + ClickHouse + Jaeger, query-time vs stream-time benchmarks |
| `chapter-07/` | Trace storage | ClickHouse wide-table store + Kafka + Grafana Tempo, both archetypes fed by one Collector and both backed by SeaweedFS object storage; compression, tiering and tenant-isolation exercises and benchmarks |
| `chapter-08/` | Query patterns | ClickHouse alone, with a generated sampled population and the ground truth behind it, so an unbiased query can be graded and not just compared; unbiased-aggregate and pre-aggregation exercises |
| `chapter-09/` | Trace-driven insights | Collector running span metrics twice, once before the tail sampler and once after, feeding Prometheus, ClickHouse and Loki off one pipeline; divergence, error-fingerprint and three-signal-correlation exercises |
| `chapter-10/` | Extracting metrics from traces | Edge Collector deriving span metrics before a sampler tier, three sampling policies read side by side, a Flink window job, a ClickHouse view and query, and a cardinality override; counted against a workload whose size and error count are known |

## Running a chapter

Each chapter is independent:

```bash
cd chapter-04
docker compose up -d
```

Follow that chapter's README for the verification walkthrough, then tear the
stack down when you're finished:

```bash
docker compose down -v
```

## Requirements

- Docker + Docker Compose
- Python 3.12 (for the app and benchmark scripts)
- Chapter 3 additionally uses [kind](https://kind.sigs.k8s.io/) for the Kubernetes path

## Verifying

Want to check that a stack works? Start it, then run its test:

```bash
cd chapter-04
docker compose up -d
bash tests/test_stack.sh
```

Each check prints `PASS` when it holds, and the script stops at the first
one that doesn't. Each chapter checks the thing that chapter teaches:

| Chapter | What the test checks |
|---------|----------------------|
| 2 | The services are up, and a trace reaches Jaeger |
| 3 | The collection tier works, read from its Prometheus metrics |
| 4 | A trace reaches Jaeger, even with one Kafka broker down |
| 5 | Spans come back together as whole traces in ClickHouse |
| 7 | A trace round-trips by ID, and one tenant can't read another's spans |
| 8 | The weighted query gets the right answer and the unweighted one doesn't |
| 9 | Span metrics before and after the sampler disagree, and one trace ID links a trace, a log line and a metric |
| 10 | Each place that counts calls is checked against a workload of known size |

Chapter 9 has a second script, `tests/test_correlation.sh`, that follows one
request across the trace, the log and the metric.

If a stack won't start, see [`setup/README.md`](setup/README.md) for memory
and ports, and [`troubleshooting.md`](troubleshooting.md) for common fixes.
