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

## Running a chapter

Each chapter is independent:

```bash
cd chapter-04
docker compose up -d
# follow that chapter's README for the verification walkthrough
docker compose down -v   # tear down when finished
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
See [`setup/README.md`](setup/README.md) for memory, ports, and per-chapter
requirements, and [`troubleshooting.md`](troubleshooting.md) for what to do
when a stack doesn't come up cleanly.
