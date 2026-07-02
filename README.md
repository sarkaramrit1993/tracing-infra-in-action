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
