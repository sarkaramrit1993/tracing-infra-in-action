# Tracing Infrastructure in Action: Companion Code

Runnable code for the book *Tracing Infrastructure in Action* (Manning). Each directory is a
self-contained stack for one chapter, with its own `README.md` and a
"verify it works" walkthrough.

| Directory | Chapter | Stack |
|-----------|---------|-------|
| `chapter-02/` | Instrumenting a service | Flask + OpenTelemetry SDK + Collector + Jaeger (docker-compose) |
| `chapter-03/` | Collection tiers | Three-tier collection, agent/gateway/consumer Collector configs, Kubernetes manifests, benchmarks |
| `chapter-04/` | Ingestion and buffering | OpenTelemetry gateway + 3-broker Kafka (KRaft) + Jaeger, partitioning benchmarks |
| `chapter-05/` | Trace assembly | Kafka + Flink assembly job + ClickHouse + Jaeger, store-then-stitch vs stream-time benchmarks |
| `chapter-07/` | Trace storage | ClickHouse wide-table store + Kafka + Grafana Tempo, both archetypes fed by one Collector and both backed by MinIO object storage; compression, tiering and tenant-isolation exercises and benchmarks |
| `chapter-08/` | Query patterns | ClickHouse alone, with a generated sampled population and the ground truth behind it, so an unbiased query can be graded and not just compared; unbiased-aggregate and pre-aggregation exercises |
| `chapter-09/` | Trace-driven insights | Collector running span metrics twice, once before the tail sampler and once after, feeding Prometheus, ClickHouse and Loki off one pipeline; divergence, error-fingerprint and three-signal-correlation exercises |

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

Each chapter has a `tests/test_stack.sh` that checks that chapter's stack
against a live run, but what it checks differs by chapter. Chapters 2 and 4
confirm services are up, a trace is produced, and the trace reaches Jaeger
(chapter 4 also checks that the trace still lands with one Kafka broker
down, and that two brokers down leaves some partitions without a leader
while checkout keeps returning 200). Chapter 3 confirms the collection tier
through Prometheus metrics and does not query Jaeger. Chapter 5 confirms
trace assembly through ClickHouse and Kafka and has no services-up step.
Chapter 7 confirms the store rather than the ingest path: that the listing 7.1
schema exists, that a trace round-trips by ID, and that the row policy keeps
one tenant from reading another's spans. Chapter 8 has no ingest path at all
and confirms the answers instead: it generates a sampled population, records
what it generated, and then checks that the weighted query reproduces that
number while the unweighted one does not. Chapter 9 confirms what can be
derived rather than what is stored: that the span-metric series taken before
the sampler and the one taken after disagree in the direction the chapter
predicts, that the error-issue index folds many raw error spans into one
fingerprint, and that one trace id reaches all three of a trace, a log line
and a metric exemplar. It has a second live script, `tests/test_correlation.sh`,
which walks those three crossings for a single request.
See [`setup/README.md`](setup/README.md) for memory, ports, and per-chapter
requirements, and [`troubleshooting.md`](troubleshooting.md) for what to do
when a stack doesn't come up cleanly.
