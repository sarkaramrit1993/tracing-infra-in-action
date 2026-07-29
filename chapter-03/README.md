# Chapter 3: Trace Collection and Routing

Companion code for *Tracing Infrastructure in Action* (Manning, 2026) by Amrit Sarkar.

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

## Verify it works

```bash
bash tests/test_stack.sh
```

Asserts that the agent and both gateways are running and scraped, that spans
accepted by the agent are exported onward, and that trace-aware routing puts
spans on both gateways. Exits non-zero on any failure. Safe to re-run.

The routing assertion only checks that each gateway received a non-zero
share, not a fixed split; for the full skew measurement (Gini coefficient,
skew ratio) see `BENCHMARKS.md` and `benchmarks/bench_routing.py`.

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

All configs live in `collector/`. `single-tier-config.yaml` is a baseline single collector with no book listing of its own; the rest map to chapter listings:

| Listing | File                                       | Pattern                                     |
|---------|---------------------------------------------|----------------------------------------------|
| 3.1     | `collector/agent-config.yaml`               | Load balancing exporter routing by trace ID |
| 3.2     | `collector/tenant-routing-config.yaml`      | Tenant-aware routing connector              |
| 3.3     | `collector/spillover-config.yaml`           | Fallback exporter for hot spot spillover    |
| 3.4     | `collector/gateway-config.yaml`             | Memory limiter for controlled degradation   |
| 3.5     | `collector/agent-config.yaml`               | Retry and sending queue configuration       |
| 3.7     | `collector/persistent-queue-config.yaml`    | Disk-backed queue with file storage         |
| 3.9     | `collector/resilient-agent-config.yaml`     | Complete resilient agent configuration      |

To swap agent configs, edit the volume mount in `docker-compose.yml`:

```yaml
otel-agent:
  volumes:
    - ./collector/spillover-config.yaml:/etc/otel/config.yaml:ro
```

## Kubernetes

For the full cluster walkthrough, see [k8s/README.md](k8s/README.md). Manifests map to chapter listings:

| Listing | File                          | Pattern                                                          |
|---------|-------------------------------|-------------------------------------------------------------------|
| 3.10    | `k8s/agent-daemonset.yaml`    | DaemonSet running one collector agent per node                    |
| 3.11    | `k8s/agent-configmap.yaml`    | Agent ConfigMap with node enrichment and DNS-based gateway routing |
| 3.12    | `k8s/gateway-deployment.yaml` | Gateway Deployment with anti-affinity and headless service        |
| 3.13    | `k8s/gateway-configmap.yaml`  | Gateway ConfigMap with health-check filtering and Kafka export    |
| 3.14    | `k8s/hpa.yaml`                | HPA with asymmetric scale-up/scale-down policies                  |
| 3.15    | `k8s/network-policies.yaml`   | Network policies restricting agent and gateway traffic            |
| 3.16    | `k8s/prometheus-rules.yaml`   | PrometheusRule alerts for collector health                        |

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
2. Watch `otelcol_receiver_refused_spans` at http://localhost:8888/metrics

**Test hot spots:**
1. `python scripts/load-generator.py --scenario hot-trace --duration 60`
2. Compare gateway-1 and gateway-2 metrics for load skew

**Test multi-tenant routing:**
1. Swap agent config to `tenant-routing-config.yaml`
2. `python scripts/load-generator.py --scenario multi-tenant --duration 60`
3. Watch spans route to different gateways by tenant
