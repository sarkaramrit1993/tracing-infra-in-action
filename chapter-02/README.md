# Chapter 2: OpenTelemetry and Trace Fundamentals

Companion code for Chapter 2 of *Tracing Infrastructure in Action*.

This environment demonstrates:
- OpenTelemetry's three-layer architecture (SDK → Collector → Backend)
- Context propagation across service boundaries
- Proper cardinality handling in span names
- Error recording with status and exceptions
- Span links for batch/queue patterns

---

## Quick Start

Start all services:

```bash
docker compose up --build
```

In another terminal, generate some traces:

```bash
curl http://localhost:8080/checkout
```

View the traces:

```bash
open http://localhost:16686
```

---

## Verify it works

```bash
bash tests/test_stack.sh
```

Asserts that all three services are running, that `/checkout` answers, and that the resulting trace is queryable in Jaeger. Exits non-zero on any failure. Safe to re-run.

---

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
│  checkout-service   │────▶│   otel-collector    │────▶│       jaeger        │
│    (Python/Flask)   │     │                     │     │                     │
│      Port 8080      │     │  gRPC: 4317         │     │    UI: 16686        │
│                     │     │  HTTP: 4318         │     │                     │
└─────────────────────┘     └─────────────────────┘     └─────────────────────┘
       Layer 1                    Layer 2                     Layer 3
    Application+SDK             Collector                   Backend
```

---

## Endpoints

### `GET /checkout`
**Demonstrates:** Nested spans, context propagation, business attributes

```bash
curl http://localhost:8080/checkout
```

**Trace tree produced:**
```
GET /checkout (root, automatic from Flask)
├─ validate_cart
│    attributes: cart.id, cart.items
├─ check_inventory
│    attributes: inventory.warehouse
├─ process_payment
│    attributes: payment.method, payment.amount
│    └─ fraud_check
│         attributes: fraud.score
└─ send_confirmation
     attributes: notification.channel
```

---

### `GET /users/<user_id>`
**Demonstrates:** Low-cardinality span naming

```bash
curl http://localhost:8080/users/12345
curl http://localhost:8080/users/67890
```

Both requests create spans with the **same name** (`fetch_user_data`), with the user ID in an **attribute**:

```
GET /users/<user_id>  ← template, not actual ID
└─ fetch_user_data
     attribute: user.id = 12345
```

This is **correct**. Putting user IDs in span names would create millions of unique names and explode your index.

---

### `GET /error`
**Demonstrates:** Error recording with status, attributes, and exception events

```bash
curl http://localhost:8080/error
```

**Trace shows:**
- Span status: `ERROR`
- Attribute: `error.type = ValueError`
- Event: Exception with full stack trace

In Jaeger, expand the span to see the exception event with stack trace.

---

### `GET /batch`
**Demonstrates:** Span links for message queue patterns

```bash
curl http://localhost:8080/batch
```

When processing batches from a queue, you can't make any single message the "parent". Instead, use **span links** to connect the batch processor to all originating traces. The endpoint creates 5 producer spans (simulating messages from separate traces) and links them to the batch consumer span. Open the trace in Jaeger and look for the "Links" section on the `process_batch` span.

---

### `GET /slow`
**Demonstrates:** Identifying slow operations

```bash
curl http://localhost:8080/slow
```

Creates a span with random 0.5-2s delay. Useful for demonstrating latency analysis in Jaeger.

---

### `GET /orders/<order_id>`
**Demonstrates:** Another cardinality example

```bash
curl http://localhost:8080/orders/ORD-12345
```

---

## Load Generator

Generate realistic traffic to populate Jaeger with traces.

If you're running outside Docker, install the dependencies into a virtual
environment first:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install requests
```

Run with defaults (60 seconds, medium load):

```bash
python3 scripts/load-generator.py
```

Custom duration and rate:

```bash
python3 scripts/load-generator.py --duration 120 --rate high
```

Gentle load for demos:

```bash
python3 scripts/load-generator.py --duration 30 --rate low
```

**Rate options:**
| Rate | Requests/sec | Workers |
|------|-------------|---------|
| low | 1 | 2 |
| medium | 5 | 5 |
| high | 20 | 10 |

---

## Configuration Reference

### SDK Environment Variables

Set in `docker-compose.yml`:

```yaml
OTEL_SERVICE_NAME: checkout-service
OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-collector:4317
OTEL_BSP_MAX_EXPORT_BATCH_SIZE: 512    # Spans per batch
OTEL_BSP_SCHEDULE_DELAY: 5000          # Max ms before export
OTEL_BSP_MAX_QUEUE_SIZE: 2048          # Queue before dropping
OTEL_BSP_EXPORT_TIMEOUT: 30000         # Export timeout ms
```

### Collector Configuration

See `collector/config.yaml`:

```yaml
receivers:
  otlp:
    protocols:
      grpc: {endpoint: 0.0.0.0:4317}
      http: {endpoint: 0.0.0.0:4318}

processors:
  memory_limiter:    # MUST be first
    check_interval: 1s
    limit_mib: 512
  batch:             # SHOULD be last
    timeout: 1s
    send_batch_size: 1024

exporters:
  otlp_grpc/jaeger:
    endpoint: jaeger:4317
```

**Why this order?**
- `memory_limiter` first: Applies backpressure before spans consume memory
- `batch` last: Improves compression by grouping spans

---

## Viewing Traces in Jaeger

1. Open http://localhost:16686
2. Select **Service**: `checkout-service`
3. Click **Find Traces**
4. Click any trace to see the waterfall view

**Things to look for:**
- Trace tree structure (parent-child relationships)
- Span durations (bar lengths)
- Attributes on each span
- Error spans (red)
- Exception events (expand error spans)

---

## Agent workloads

A small support agent runs against a stand-in model, so there are no API keys, no model bills and the same result every run. It makes four model calls, two tool calls and one retrieval, then answers. The stand-in's token counts are the byte length divided by 4, a fixed rule, not a tokenizer. The agent's spans leave through the same Collector as `checkout-service`.

Run every command from this folder (`chapter-02/`). Each script starts what it needs, so the Quick Start stack does not have to be running.

### 1. Run the agent once

```bash
./scripts/run-agent.sh
```

You will see:

```
Building the agent image if needed and running one agent run. This takes under a minute the first time.
run 1/1: trace_id=38626ccf5982bc5f875b8bf777d4f87c size=8k capture=no_content seconds=3.6
Waiting for the trace to reach Jaeger.
Trace 38626ccf5982bc5f875b8bf777d4f87c has 13 spans in Jaeger.
Open: http://localhost:16686/trace/38626ccf5982bc5f875b8bf777d4f87c
```

Your trace ID and timing differ. Capture is off, which is the default.

### 2. Look at the trace in Jaeger

```bash
open http://localhost:16686
```

Pick the service `support-agent`, click **Find Traces**, and open the newest trace. It has 13 spans. One root span, `invoke_agent support-agent`, sits at the top. Under it are four `chat stand-in-1` spans (the model calls), two `execute_tool` spans (`lookup_order` and `issue_refund`), one `retrieval refund-policy` span, and three HTTP client spans. Two more spans, `GET /orders/<order_id>` and `fetch_order`, belong to `checkout-service`, which the agent calls over HTTP.

Click a `chat stand-in-1` span and look at its attributes. They carry the model name and `gen_ai.usage.input_tokens` and `gen_ai.usage.output_tokens`. There is no `gen_ai.input.messages`: no prompt text is stored.

### 3. Measure the trace with capture off

```bash
./scripts/measure-agent-trace.sh
```

You will see:

```
Measuring three agent runs with capture off. This takes about 15 seconds.

              spans per run  capture off (bytes per span)  capture on (bytes per span)
------------  -------------  ----------------------------  ---------------------------
invoke_agent              1                           201             not measured yet
chat                      4                    441 to 447             not measured yet
execute_tool              2                           243             not measured yet
retrieval                 1                           171             not measured yet
http_client               3                    227 to 229             not measured yet
whole trace              11                          3797             not measured yet
largest span                                          447             not measured yet

Across the 3 runs the byte counts were the same every time.

Next: ./scripts/capture-on.sh (or capture-off.sh), then ./scripts/measure-agent-trace.sh again.
Files: chapter-02/measure-out/
```

The table counts the 11 spans the agent process emits. Jaeger shows 13 because it adds the two `checkout-service` spans.

### 4. Switch capture on and measure again

```bash
./scripts/capture-on.sh
./scripts/measure-agent-trace.sh
```

You will see:

```
Capture is on. The next run puts prompt, tool and retrieval text on the spans.
Next: ./scripts/run-agent.sh
Measuring three agent runs with capture on. This takes about 15 seconds.

              spans per run  capture off (bytes per span)  capture on (bytes per span)
------------  -------------  ----------------------------  ---------------------------
invoke_agent              1                           201                          201
chat                      4                    441 to 447                9064 to 10780
execute_tool              2                           243                   445 to 503
retrieval                 1                           171                          828
http_client               3                    227 to 229                   227 to 229
whole trace              11                          3797                        42830
largest span                                          447                        10780

Across the 3 runs the byte counts were the same every time.

Next: ./scripts/capture-on.sh (or capture-off.sh), then ./scripts/measure-agent-trace.sh again.
Files: chapter-02/measure-out/
```

The trace grows from 3,797 to 42,830 bytes. The four `chat` spans grow with every model call (9,064 bytes for the first, 10,780 for the last) because each call carries the conversation so far.

### 5. See what capture put on the spans

```bash
./scripts/run-agent.sh
```

You will see:

```
Building the agent image if needed and running one agent run. This takes under a minute the first time.
run 1/1: trace_id=67afcd98ec8a6dae356763c68c1fb715 size=8k capture=span_only seconds=2.4
Waiting for the trace to reach Jaeger.
Trace 67afcd98ec8a6dae356763c68c1fb715 has 13 spans in Jaeger.
Open: http://localhost:16686/trace/67afcd98ec8a6dae356763c68c1fb715
```

Refresh Jaeger, open the newest `support-agent` trace, and click a `chat stand-in-1` span. It now has `gen_ai.input.messages` and `gen_ai.output.messages`, the prompt and the reply as text.

### 6. Switch capture back off

```bash
./scripts/capture-off.sh
```

You will see:

```
Capture is off. The next run carries model, token and tool metadata and no text.
Next: ./scripts/run-agent.sh
```

The two scripts write one line to a `.env` file in this folder, which Docker Compose reads on every run, so you never edit a file. The measurement files are in `measure-out/`.

---

## Common Issues

### "Connection refused" errors
Make sure all services are running:

```bash
docker compose ps
```

Check the collector logs:

```bash
docker compose logs otel-collector
```

### No traces appearing in Jaeger
Check if the app is exporting:

```bash
docker compose logs checkout-service | grep -i otel
```

Verify the collector is receiving:

```bash
docker compose logs otel-collector | grep -i trace
```

### Port conflicts
Check what's using the ports:

```bash
lsof -i :8080
lsof -i :16686
```

---

## Cleanup

Switch capture back off, so the next run starts from the default:

```bash
./scripts/capture-off.sh
```

Stop all services:

```bash
docker compose --profile agent down
```

Remove the volumes too:

```bash
docker compose --profile agent down -v
```

---

## Chapter 2 Concepts Demonstrated

| Concept | Endpoint | What to observe |
|---------|----------|-----------------|
| Context propagation | `/checkout` | All spans share same trace_id |
| Nested spans | `/checkout` | fraud_check inside process_payment |
| Low cardinality | `/users/{id}` | Same span name for different IDs |
| Error recording | `/error` | Status=ERROR, exception event |
| Span links | `/batch` | 5 cross-trace links on process_batch span |
| Business attributes | `/checkout` | cart.id, payment.amount, etc. |

---

## Listings

Maps chapter listing numbers to the file implementing them. A listing without a row here has no standalone match in this environment (see notes):

| Listing | File                     | Pattern                                          |
|---------|--------------------------|---------------------------------------------------|
| 2.1     | `docker-compose.yml`     | SDK batch configuration environment variables      |
| 2.3     | `app/main.py`            | Context propagation in a checkout endpoint         |
| 2.6     | `app/main.py`            | Batch consumer with span links                     |
| 2.7     | `app/main.py`            | Recording errors with status, attributes, events   |
| 2.8     | `app/main.py`            | Attribute placement for high-cardinality values    |
| 2.9     | `app/main.py`            | Automatic instrumentation setup with Flask         |

Not mapped: 2.2 (production Kafka pipeline; `collector/config.yaml` is a simplified local-demo variant, see its header comment), 2.4-2.5 (SQL queries against a trace store with a SQL interface; this environment's backend is Jaeger's in-memory store, not applicable here), 2.10 (abridged excerpt of the same checkout code as 2.3, already covered above), 2.11 (a deliberate anti-pattern the book warns against, not something this demo implements).

## Next Steps

- **Chapter 3**: Collector deployment patterns (agent tier, gateway tier)
- **Chapter 4**: Protecting infrastructure from instrumentation mistakes
- **Chapter 5**: Trace assembly patterns
- **Chapter 6**: Sampling strategies

## Versions

Every image and package version is pinned once, in [`versions.yaml`](../versions.yaml) at the repository root. `scripts/check_versions.py` fails if a file in this chapter disagrees with it.
