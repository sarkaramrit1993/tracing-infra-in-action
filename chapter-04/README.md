# Chapter 4: Ingestion and Buffering at Scale

Code and exploration exercises for Chapter 4 of *Tracing Infrastructure in Action*.

## Prerequisites

- Docker and Docker Compose
- Python 3.10+

## Architecture

```
App (OTel SDK) → Agent Collector → Gateway Collector → Kafka (3-broker KRaft) → Consumer Collector → Jaeger
                                                                                                    └─ Prometheus (metrics)
```

### Components

- **app/**: Flask application instrumented with OpenTelemetry
- **collector/**: OTel Collector configs (agent, gateway, consumer)
- **benchmarks/**: Serialization format comparison exercise

## Running

```bash
docker compose up -d
```

This starts:
- 3-broker KRaft Kafka cluster (kafka-1, kafka-2, kafka-3 on port 9093)
- `kafka-init` one-shot job that creates `otlp_spans` and `otlp_spans.DLT` topics
- OTel Collector agent, gateway, and consumer
- Jaeger v2 (trace backend)
- Prometheus (metrics)

## Verify it works

```bash
# 1. Generate a trace
curl http://localhost:8080/checkout

# 2. View it in Jaeger UI
open http://localhost:16686     # macOS; on Linux use xdg-open

# 3. Watch metrics in Prometheus
open http://localhost:9090

# 4. Check consumer lag (should be near-zero under light load)
docker compose exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka-1:9093 --describe --group trace-storage
```

### Failure test

Replication factor 2 tolerates one broker down, and `curl` returns 200 either way, so the real signal is whether the trace still reaches Jaeger, not whether checkout succeeds:

```bash
docker compose stop kafka-2
curl http://localhost:8080/checkout    # 200, and the trace still lands in Jaeger (one broker down)
docker compose stop kafka-1
curl http://localhost:8080/checkout    # still 200, but the trace does NOT reach Jaeger (two brokers down)
docker compose start kafka-1 kafka-2
```

Watch Jaeger, not the curl exit code: that gap between "request succeeded" and "trace captured" is the operational intuition the chapter prose cannot match.

### Dead-letter topic

The `kafka-init` job also creates `otlp_spans.DLT`, mirroring the dead-letter discussion in the chapter. The happy-path demo does not route to it, so it stays empty until you add a DLQ consumer that writes failed messages (schema mismatches, invalid trace IDs, corrupted payloads) with error metadata in their headers. A non-zero DLQ rate is a pipeline-health signal.

## Serialization Exercise

Compares OTLP Protobuf vs JSON encoding for 10,000 spans across 100 iterations. No infrastructure required. Runs purely on CPU.

These exercises run on a local machine. They demonstrate concepts from the chapter but cannot reproduce production-scale behavior. For industry benchmarks, see chapter footnotes [^2] (LinkedIn), [^8] (Bindplane), [^9] (Confluent).

### Run with Docker

```bash
cd benchmarks
docker build -t ch4-bench .
docker run --rm ch4-bench
```

### Run locally

```bash
cd benchmarks
pip install -r requirements.txt
python3 bench_serialization.py
```

Results are written to `benchmarks/results/` (gitignored).

## Collector Configs

| Config | Role |
|--------|------|
| `collector/agent-config.yaml` | Sidecar agent, forwards to gateway |
| `collector/gateway-config.yaml` | Gateway, exports to Kafka with trace-ID partitioning |
| `collector/consumer-config.yaml` | Kafka receiver, writes spans to Jaeger |

## Listings

| Listing | File | Pattern |
|---------|------|---------|
| 4.2 | `collector/gateway-config.yaml` | Kafka exporter with trace-ID partitioning |
| 4.3 | `docker-compose.yml` | Trace topic creation (dev-scale partition count; the chapter shows the production partition count) |
| 4.7 | `collector/gateway-config.yaml` | Gateway collector, full pipeline (memory limiter, batch, Kafka exporter) |
| 4.8 | `collector/consumer-config.yaml` | Consumer collector with Kafka receiver |

Not mapped: 4.1 (tenant-based routing connector; this stack doesn't route by tenant), 4.4 and 4.5 (raw Kafka producer/consumer client properties; the OTel Collector's Kafka exporter and receiver expose a smaller settings surface than a raw Kafka client, so these don't have a direct file equivalent here), 4.6 (Kafka tiered storage, not configured in this stack; see chapter 7 for cold-tier storage).
