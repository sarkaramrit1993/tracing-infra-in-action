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

First, generate a trace:

```bash
curl http://localhost:8080/checkout
```

Then view it in the Jaeger UI. `open` is macOS; on Linux use `xdg-open`.

```bash
open http://localhost:16686
```

Then watch metrics in Prometheus:

```bash
open http://localhost:9090
```

Finally, check consumer lag. It should be near-zero under light load.

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka-1:9093 --describe --group trace-storage
```

### Failure test

Replication factor 2 tolerates one broker down, and `curl` returns 200 either way, so the real signal is whether the trace still reaches Jaeger, not whether checkout succeeds:

```bash
bash tests/test_stack.sh
```

Step 3 stops `kafka-2` and confirms one broker down is tolerated: every
partition still has a live replica, so the trace lands. The script restores
the broker on exit.

### Losing two brokers, by hand

The two-broker case is worth seeing, but it does not make a good automated
check, so run it yourself. Watch Jaeger between the stop and the start: some
checkouts produce a trace, some do not.

```bash
docker compose stop kafka-2 kafka-1
curl -s http://localhost:8080/checkout
docker compose start kafka-1 kafka-2
```

What you should see is a partial outage rather than a total one. `otlp_spans`
has 6 partitions at RF=2 across 3 brokers, so the surviving broker already
leads a share of them before anything fails. Stopping two leaves only the
partitions whose replicas both sat on the stopped brokers without a leader.
The rest keep a leader on the survivor and keep accepting spans. Checkout
answers 200 throughout, so a fraction of new traces goes silently missing
while nothing looks wrong at the edge. That is the lesson, and it is a
harder failure to notice than a clean outage would be.

On one run against this topology, stopping `kafka-2` then `kafka-1` left 2 of
6 partitions leaderless and 12 of 20 checkouts still produced a trace. Those
numbers follow from which brokers you stop and how the replicas happen to be
assigned, so treat them as an illustration rather than a fixed rate.

Two things make this a poor automated assertion, which is why the script
stops at step 3. Asserting that a single trace fails is a coin flip, since
most partitions survive. Reading partition metadata instead is worse: with
one broker left there is no controller quorum, so `kafka-topics.sh --describe`
can only answer from that broker's cached metadata, and once the cache is
gone it stalls for about a minute and returns nothing.

Note also that when Kafka comes back, the gateway flushes everything it
queued while the brokers were down, so the trace count jumps in one step.
That backlog is why a naive before-and-after count is misleading here.

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
python3 -m venv .venv
source .venv/bin/activate
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
| 4.1 | `collector/tenant-routing-config.yaml` | Tenant context propagation and routing to a dedicated or shared topic |
| 4.2 | `collector/gateway-config.yaml` | Kafka exporter with trace-ID partitioning |
| 4.3 | `docker-compose.yml` | Trace topic creation (dev-scale partition count; the chapter shows the production partition count) |
| 4.7 | `collector/gateway-config.yaml` | Gateway collector, full pipeline (memory limiter, batch, Kafka exporter) |
| 4.8 | `collector/consumer-config.yaml` | Consumer collector with Kafka receiver |

Not mapped: 4.4 and 4.5 (raw Kafka producer/consumer client properties; the OTel Collector's Kafka exporter and receiver expose a smaller settings surface than a raw Kafka client, so these don't have a direct file equivalent here), 4.6 (Kafka tiered storage, not configured in this stack; see chapter 7 for cold-tier storage).
