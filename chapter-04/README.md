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
- **scripts/**: the AI agent workload exercise (see Agent workloads)

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

## Agent workloads

An AI agent that keeps prompt text on its spans sends Kafka far bigger records than a checkout service does. These two scripts show the exporter's `max_message_bytes` cap (1,000,000 by default) dropping one of those records, who sees it, and what moves the cap. The agent and its stand-in model come from `../chapter-02/agent` (one copy, no API keys, the same result every run).

The scripts switch the gateway (`otel-gateway`) to a demo config, `collector/gateway-agent-config.yaml`. It is Listing 4.6 with two changes: a 30-second batch timer, so one run reaches the Kafka exporter as one batch, and a filter for the `GET /health` spans that `checkout-service` sends every 10 seconds, so the counters count only the run. `partition_traces_by_id: true` and `max_message_bytes: 1000000` are unchanged.

Run every command from this folder (`chapter-04/`). The first run builds the agent image, which takes about a minute.

### 1. Start the stack

```bash
docker compose up --build -d
```

Wait about a minute for Kafka and the Collectors to come up.

### 2. Send a run that is too big for one record

```bash
./scripts/send-agent-to-kafka.sh
```

You will see:

```
Running one AI agent run with 24 model calls. This takes about 40 seconds.
run 1/1: trace_id=77655efef8a2ccaae234f4076b4d7112 size=32k capture=span_only seconds=1.4
Gateway: took in 53 spans, receiver refused 0 spans
Kafka exporter: sent 2 spans, failed 51 spans
Gateway log:
record size 1020473 exceeds max 1000000: MESSAGE_TOO_LARGE
Spans of that trace in Jaeger: 27
Next: ./scripts/raise-kafka-limits.sh
```

Your trace ID and timing differ. Prompt capture is on at the 32 KB prompt size, so every model call carries its prompt on the span, and the run is 53 spans.

- The gateway took every span in and refused none, so the sender sees a success. The failure shows only in the gateway's log and in `otelcol_exporter_send_failed_spans`.
- The error is permanent, so nothing retries. The exporter dropped that record for good.
- The exporter splits a batch by resource, scope and trace ID, so a trace becomes more than one record. Only the oversized record is lost. Jaeger has 27 of the trace's 53 spans, the small ones that travelled in records of their own: the 21 retrieval spans, the agent's own span, its 3 HTTP calls and the checkout service's 2. The other 26, the 24 model calls that carry the prompts and the 2 tool calls, were in the oversized record and are gone. The failed count of 51 is the whole export call, including those small records that Kafka accepted.
- The 2 spans counted as sent are the checkout service's, which the batch processor exports on their own because it batches per `x-tenant-id` value.
- The record is 1,020,473 bytes, and the cap counts it before compression. It is a little smaller than the whole trace's request size because it leaves out those small records. With 23 model calls the same run stays under the cap and all 51 spans arrive.

### 3. Raise the limits

```bash
./scripts/raise-kafka-limits.sh
```

You will see:

```
Completed updating config for topic otlp_spans.
Running the same AI agent run, 24 model calls. This takes about 40 seconds.
run 1/1: trace_id=2a0cc3d4adf091b44442d91cf46af232 size=32k capture=span_only seconds=1.3
Gateway: took in 53 spans, receiver refused 0 spans
Kafka exporter: sent 53 spans, failed 0 spans
MESSAGE_TOO_LARGE lines in the gateway log: 0
Spans of that trace in Jaeger: 53
```

The script raises the topic's `max.message.bytes` to 2097152 first, then switches the gateway to `collector/gateway-agent-raised-config.yaml`, which differs from the demo config in one line, `max_message_bytes: 2000000`. The topic goes first because a record the exporter can now send has to be one the broker accepts. The same run now lands: all 53 spans reach Jaeger through Kafka and the consumer Collector.

In this stack the topic limit is not the one that stops the run. The broker and topic default is 1,048,588 bytes and it counts the compressed batch, while the exporter's 1,000,000 counts the record before compression, so the exporter's cap is reached first. The stand-in prompts are repeated text, which `lz4` shrinks a lot, so the record fit under the topic's default limit too. Real prompts compress less, so raise both.

### 4. Go back to the shipped config

```bash
docker compose up -d
```

This puts the shipped gateway config back and leaves the topic limit as you set it. The other exercises in this chapter run on the shipped config.

### 5. Remove everything

```bash
docker compose --profile agent down -v
```

This removes the containers and volumes, including the agent and the stand-in model.

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
| `collector/gateway-agent-config.yaml` | Gateway demo for the agent workload: Listing 4.6 with a 30-second batch timer |
| `collector/gateway-agent-raised-config.yaml` | The same demo with the exporter's `max_message_bytes` at 2000000 |

## Listings

| Listing | File | Pattern |
|---------|------|---------|
| 4.1 | `collector/tenant-routing-config.yaml` | Tenant context propagation and routing to a dedicated or shared topic |
| 4.2 | `collector/gateway-config.yaml` | Kafka exporter with trace-ID partitioning |
| 4.3 | `docker-compose.yml` | Trace topic creation (dev-scale partition count; the chapter shows the production partition count) |
| 4.6 | `collector/gateway-config.yaml` | Gateway collector, full pipeline (memory limiter, batch, Kafka exporter) |
| 4.7 | `collector/consumer-config.yaml` | Consumer collector with Kafka receiver |

Not mapped: 4.4 (raw Kafka consumer client properties; the OTel Collector's Kafka receiver exposes a smaller settings surface than a raw Kafka client, so it doesn't have a direct file equivalent here), 4.5 (Kafka tiered storage, not configured in this stack; see chapter 7 for cold-tier storage).

## Versions

Every image and package version is pinned once, in [`versions.yaml`](../versions.yaml) at the repository root. `scripts/check_versions.py` fails if a file in this chapter disagrees with it.
