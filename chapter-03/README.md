# Chapter 3: Trace Collection and Routing

Companion code for *Tracing in Action* (Manning, 2026) by Amrit Sarkar.

Multi-tier collector architectures, trace-aware load balancing, backpressure handling, and production Kubernetes deployments.

## Architecture

```
                         ┌─────────────────┐
                         │  otel-gateway-1  │──→ jaeger
checkout-service ──→ otel-agent ──┤                         │
                         │  otel-gateway-2  │──→ jaeger
                         └─────────────────┘
```

The agent routes spans by `traceID` so all spans from one trace land on the same gateway. This avoids cross-partition coordination in downstream stream processors.

## Quick Start

### Multi-tier (agent + gateway)

```bash
docker compose up --build
```

| Service           | URL                             |
|-------------------|---------------------------------|
| Checkout app      | http://localhost:8080            |
| Jaeger UI         | http://localhost:16686           |
| Agent metrics     | http://localhost:8888/metrics    |
| Gateway-1 metrics | http://localhost:8889/metrics    |
| Gateway-2 metrics | http://localhost:8890/metrics    |
| Prometheus        | http://localhost:9090            |

### Single-tier (baseline)

```bash
docker compose -f docker-compose.single-tier.yml up --build
```

### Generate traffic

```bash
python scripts/load-generator.py --scenario steady --duration 60 --rate 10
```

Scenarios: `steady`, `spike`, `backpressure`, `multi-tenant`, `hot-trace`, `failover`.

## Application Endpoints

| Endpoint          | Method | What it does                                      |
|-------------------|--------|---------------------------------------------------|
| `/health`         | GET    | Health probe                                      |
| `/checkout`       | GET    | Multi-span checkout flow                          |
| `/batch-job`      | POST   | Configurable span count (`{"span_count": 500}`)   |
| `/multi-tenant`   | GET    | Reads `X-Tenant-ID` / `X-Tenant-Tier` headers     |
| `/burst`          | POST   | Rapid-fire spans (`{"count": 100}`)               |
| `/orders/<id>`    | GET    | Low-priority spans for filtering demo              |

## Collector Configs

All configs live in `collector/`. Each maps to a chapter listing:

| File                            | Listing(s)   | Pattern                                    |
|---------------------------------|--------------|--------------------------------------------|
| `agent-config.yaml`             | 3.1, 3.4, 3.5 | Load balancing + memory limiter + retry  |
| `tenant-routing-config.yaml`    | 3.2          | Tenant-aware routing connector             |
| `spillover-config.yaml`         | 3.3          | Fallback exporter for hot spots            |
| `persistent-queue-config.yaml`  | 3.6          | Disk-backed queue                          |
| `gateway-config.yaml`           | 3.7          | Priority filtering + Jaeger export         |
| `resilient-agent-config.yaml`   | 3.8          | Complete production agent                  |
| `single-tier-config.yaml`       | --           | Baseline single collector                  |

To swap agent configs, edit the volume mount in `docker-compose.yml`:

```yaml
otel-agent:
  volumes:
    - ./collector/spillover-config.yaml:/etc/otel/config.yaml:ro
```

## Kubernetes

For the full cluster walkthrough, see [k8s/README.md](k8s/README.md).

## Tear down

```bash
./scripts/teardown.sh
```

Removes the docker-compose stacks (with their volumes) and deletes the `tracing-ch3` Kind cluster.

## Benchmarks

The `benchmarks/` directory contains five automated benchmarks that validate the chapter's claims. Run them via Docker Compose:

```bash
docker compose -f docker-compose.yml -f docker-compose.benchmark.yml up --build
```

Results land in `benchmarks/results/`. See [BENCHMARKS.md](BENCHMARKS.md) for methodology, interpretation, and consolidated findings.

## Testing Patterns

**Verify trace-aware routing:**
1. Start multi-tier compose
2. `python scripts/load-generator.py --scenario steady --duration 30`
3. Open Jaeger UI -- all spans from one trace should hit the same gateway

**Test backpressure:**
1. `python scripts/load-generator.py --scenario backpressure --duration 60 --rate 50`
2. Watch `otelcol_processor_dropped_spans` at http://localhost:8888/metrics

**Test hot spots:**
1. `python scripts/load-generator.py --scenario hot-trace --duration 60`
2. Compare gateway-1 and gateway-2 metrics for load skew

**Test multi-tenant routing:**
1. Swap agent config to `tenant-routing-config.yaml`
2. `python scripts/load-generator.py --scenario multi-tenant --duration 60`
3. Watch spans route to different gateways by tenant
