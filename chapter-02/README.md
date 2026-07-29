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

```bash
# Start all services
docker-compose up --build

# In another terminal, generate some traces
curl http://localhost:8080/checkout

# View traces
open http://localhost:16686
```

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
GET /users/{user_id}  ← template, not actual ID
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

Generate realistic traffic to populate Jaeger with traces:

```bash
# Install dependencies (if running outside Docker)
pip install requests

# Run with defaults (60 seconds, medium load)
python scripts/load-generator.py

# Custom duration and rate
python scripts/load-generator.py --duration 120 --rate high

# Gentle load for demos
python scripts/load-generator.py --duration 30 --rate low
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
  otlp/jaeger:
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

## Common Issues

### "Connection refused" errors
```bash
# Make sure all services are running
docker-compose ps

# Check collector logs
docker-compose logs otel-collector
```

### No traces appearing in Jaeger
```bash
# Check if app is exporting
docker-compose logs checkout-service | grep -i otel

# Verify collector is receiving
docker-compose logs otel-collector | grep -i trace
```

### Port conflicts
```bash
# Check what's using the ports
lsof -i :8080
lsof -i :16686
```

---

## Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes too
docker-compose down -v
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
