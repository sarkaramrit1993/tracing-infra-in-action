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
python3 scripts/load-generator.py --scenario steady --duration 60 --rate 10
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
| none    | `collector/agent-genai-config.yaml`         | Agent tier for the agent workload demo      |
| none    | `collector/agent-genai-trim-config.yaml`    | Same, with the prompt trim                  |
| none    | `collector/gateway-default-limit-config.yaml` | Gateway receiver at the 4 MiB default     |

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

## Agent workloads

An AI agent that keeps prompt text on its spans sends the Collector far bigger requests than a checkout service does. These four scripts show where a request that is too large is refused, who sees the refusal, and two places to fix it so the same run passes: a cap on the batch, and a trim of the prompts. The agent and its stand-in model come from `../chapter-02/agent` (one copy, no API keys, the same result every run).

The shipped gateway raises its receiver to `max_recv_msg_size_mib: 16`. The scripts switch both gateways to `collector/gateway-default-limit-config.yaml`, the same file with that line removed, so the receiver is at its 4 MiB default. They also switch the agent tier (`otel-agent`) to a demo config with four changes: one plain `otlp_grpc` exporter to gateway 1 (the load-balancing exporter does not publish `otelcol_exporter_send_failed_spans`), `compression: none` so the size is the plain size of the request, a 30-second batch timer that the scripts restart so a whole run leaves as one request, and a filter for the `GET /health` spans that `checkout-service` sends every 10 seconds.

Run every command from this folder (`chapter-03/`). The first run builds the agent image, which takes about a minute.

### 1. Start the stack

```bash
docker compose up --build -d
```

Wait about 15 seconds for the Collectors to come up.

### 2. Send a burst that fits

```bash
./scripts/send-agent-burst.sh
```

You will see:

```
Switching the Collectors to the agent demo configs.
Running 5 AI agent runs with 4 model calls each. This takes about 40 seconds.
run 1/5: trace_id=1ddcce7c265832f4d06cc2a16bdae309 size=8k capture=span_only seconds=0.3
run 2/5: trace_id=b21cc9e01f6ce4c05183e7e9e0fe7f79 size=8k capture=span_only seconds=0.3
run 3/5: trace_id=1296d3f00ea1ece8b41eb94df3300483 size=8k capture=span_only seconds=0.3
run 4/5: trace_id=e7202152d296d297e16ed9af9eba75e8 size=8k capture=span_only seconds=0.2
run 5/5: trace_id=ca00a3912fe545d2e29384f63ca295f3 size=8k capture=span_only seconds=0.2
Agent tier batch: 65 spans in 1 request, 216371 bytes in total
Agent tier exporter: sent 65 spans, failed 0 spans
Gateway 1 receiver: accepted 65 spans, refused 0 spans
Next: ./scripts/send-oversized-agent-run.sh
```

Your trace IDs and timings differ. Prompt capture is on, so each model call carries its prompt on the span. Five short runs make one request of about 216 KB, far under 4 MiB, and every span arrives.

### 3. Send one run that does not fit

```bash
./scripts/send-oversized-agent-run.sh
```

You will see:

```
Running one AI agent run with 90 model calls. This takes about 40 seconds.
run 1/1: trace_id=0c693fed3f8704ef5fcb60b70e698762 size=32k capture=span_only seconds=5.3
Agent tier batch: 185 spans in 1 request, 6349437 bytes in total
Agent tier exporter: sent 0 spans, failed 185 spans
Gateway 1 receiver: accepted 0 spans, refused 0 spans
Agent tier log:
"error": "not retryable error: Permanent error: rpc error: code = ResourceExhausted desc = grpc: received message larger than max (6349437 vs. 4194304)", "dropped_items": 185
Agent tier metric:
otelcol_exporter_send_failed_spans{error_permanent="true",error_type="ResourceExhausted",exporter="otlp_grpc/gateway",server_address="otel-gateway-1",server_port="4317"} 185
Gateway 1 metric:
otelcol_receiver_refused_spans{receiver="otlp",transport="grpc"} 0
Spans of that trace in Jaeger: 0
Next: ./scripts/cap-batch-spans-at-agent-tier.sh
```

The request is 6,349,437 bytes. Your byte count can differ by a few bytes. The gateway refused it at 4,194,304 bytes before any processor ran. The refusal is permanent, so the agent tier does not retry: it drops all 185 spans. The only evidence is on the agent tier, in its log and in `otelcol_exporter_send_failed_spans`. The gateway logs nothing and its refused counter stays at 0, so an alert on `otelcol_receiver_refused_spans` never fires. The agent's trace is not in Jaeger.

### 4. Cap the spans per batch at the agent tier

```bash
./scripts/cap-batch-spans-at-agent-tier.sh
```

You will see:

```
Cap: send_batch_max_size: 100 spans per request, prompts left whole.
Running the same AI agent run, 90 model calls, with the cap in place. This takes about 40 seconds.
run 1/1: trace_id=9d21156c713e7c5e5c244c6aebed67c8 size=32k capture=span_only seconds=5.0
Agent tier batch: 185 spans in 2 requests, 6349437 bytes in total
Agent tier exporter: sent 185 spans, failed 0 spans
Gateway 1 receiver: accepted 185 spans, refused 0 spans
Waiting for the trace to reach Jaeger.
Spans of that trace in Jaeger: 185
Open: http://localhost:16686/trace/9d21156c713e7c5e5c244c6aebed67c8
Next: ./scripts/trim-prompts-at-agent-tier.sh
```

The prompts are untouched and the byte total is the same as in step 3. Your total can come out a few hundred bytes higher than step 3's. The count includes a short header for each batch the services sent, and how many batches a run splits into depends on timing. What changed is how the bytes leave: as two requests, one of 100 spans and one of 85, each about 3.4 MB or less, under the gateway's 4,194,304. The change is in the `batch` processor of `collector/agent-genai-batchcap-config.yaml`:

```yaml
batch:
  send_batch_size: 100
  send_batch_max_size: 100
  timeout: 30s
```

`send_batch_max_size` is the cap. The Collector refuses to start if it is smaller than `send_batch_size`, which is why `send_batch_size` drops from 1000 to 100 as well. The first 100 spans leave as soon as they arrive. The other 85 wait for the 30-second timer, which is why the script still takes about 40 seconds. The cap counts spans, not bytes, so it works here because the spans are all about the same size. A batch of 100 much larger spans would still be refused.

The cap does not depend on the exporter. The shipped load-balancing exporter in `collector/agent-config.yaml` sends the same two requests. With that exporter the gateway that receives them can be either one, so look at both gateways' `otelcol_receiver_accepted_spans`.

### 5. Trim the prompts at the agent tier

```bash
./scripts/trim-prompts-at-agent-tier.sh
```

You will see:

```
Trim: truncate_all(attributes, 2048), on spans that carry gen_ai.operation.name.
Running the same AI agent run, 90 model calls, with the trim in place. This takes about 40 seconds.
run 1/1: trace_id=580e50b52086da1874c00c8fefd99ebc size=32k capture=span_only seconds=5.4
Agent tier batch: 185 spans in 1 request, 324343 bytes in total
Agent tier exporter: sent 185 spans, failed 0 spans
Gateway 1 receiver: accepted 185 spans, refused 0 spans
Waiting for the trace to reach Jaeger.
Spans of that trace in Jaeger: 185
Open: http://localhost:16686/trace/580e50b52086da1874c00c8fefd99ebc
To go back to the shipped configs: docker compose up -d
```

The same run now leaves as a 324,343-byte request and all 185 spans arrive. The config is `collector/agent-genai-trim-config.yaml`: a `transform` processor between `filter/drop_health` and `batch`, so the cut happens before the request is built.

```yaml
transform/trim_prompts:
  trace_statements:
    - context: span
      statements:
        - truncate_all(attributes, 2048) where attributes["gen_ai.operation.name"] != nil
```

The 2048 is this run's choice. It has nothing to do with Tempo's attribute cap. The cut leaves each long attribute value as a plain string that is no longer valid JSON. A trim at the gateway cannot help, because the gateway's receiver refuses the request before its processors run. Raising `max_recv_msg_size_mib` on the receiver, as the shipped gateway does, is the other fix.

### 6. Go back to the shipped configs

```bash
docker compose up -d
```

This puts the shipped agent tier and gateway configs back. The other exercises in this chapter run on them.

### 7. Remove everything

```bash
docker compose --profile agent down -v
```

This removes every container and volume, including the agent and its stand-in model.

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
2. `python3 scripts/load-generator.py --scenario steady --duration 30`
3. Open Jaeger UI -- all spans from one trace should hit the same gateway

**Test backpressure:**
1. `python3 scripts/load-generator.py --scenario backpressure --duration 60 --rate 50`
2. Watch `otelcol_receiver_refused_spans` at http://localhost:8888/metrics

**Test hot spots:**
1. `python3 scripts/load-generator.py --scenario hot-trace --duration 60`
2. Compare gateway-1 and gateway-2 metrics for load skew

**Test multi-tenant routing:**
1. Swap agent config to `collector/tenant-routing-config.yaml`
2. `python3 scripts/load-generator.py --scenario multi-tenant --duration 60`
3. Watch spans route to different gateways by tenant

## Versions

Every image and package version is pinned once, in [`versions.yaml`](../versions.yaml) at the repository root. `scripts/check_versions.py` fails if a file in this chapter disagrees with it.
