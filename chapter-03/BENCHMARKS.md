# Benchmark Methodology and Results

How the chapter-03 benchmarks work, what they measure, and what we found.

## Overview

Five benchmarks validate the claims in Chapter 3. Each runs inside Docker Compose alongside the full collector stack. All load generation uses [telemetrygen](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/cmd/telemetrygen) for consistent, repeatable span generation.

| Benchmark           | What it measures                                    |
|---------------------|-----------------------------------------------------|
| `bench_memory`      | Collector RSS at idle and under increasing load      |
| `bench_routing`     | Span distribution skew: single-gateway vs trace-aware |
| `bench_throughput`  | Gateway export throughput ceiling                    |
| `bench_persistent_queue` | Throughput delta: memory queue vs disk queue    |
| `bench_backpressure` | Cascade timeline from backend stall to SDK drops    |

Run order matters: backpressure pauses Jaeger, so it runs last.

> **Backpressure note:** The backpressure benchmark fills collector queues permanently.
> For multi-run consistency, restart the blackhole collectors between runs:
> `docker compose -f docker-compose.yml -f docker-compose.benchmark.yml up -d --force-recreate otel-gateway-blackhole otel-agent-blackhole`

## Requirements

**Hardware:**
- 8+ GB RAM (the full stack runs ~10 containers)
- 4+ CPU cores (telemetrygen is CPU-hungry at high rates)
- SSD recommended for persistent queue benchmark

**Software:**
- Docker and Docker Compose v2
- No other heavy workloads running (benchmarks are sensitive to contention)

## Running

Full suite, 5 runs averaged:

```bash
docker compose -f docker-compose.yml -f docker-compose.benchmark.yml \
  run --build benchmark --all --runs 5
```

Single benchmark:

```bash
docker compose -f docker-compose.yml -f docker-compose.benchmark.yml \
  run --build benchmark --bench throughput --runs 3
```

Results write to `benchmarks/results/` as JSON (raw data) and Markdown (human-readable report).

## What Each Benchmark Does

### Memory (`bench_memory.py`)

Measures collector RSS at idle, 1K/sec, 10K/sec, and 50K/sec. Sends spans via the OTel Python SDK to the agent, then reads `otelcol_process_memory_rss` from both agent and gateway metrics endpoints. Each load level runs for 15 seconds with a cooldown between levels.

**Why:** The chapter claims agents run ~50 MB and gateways ~250 MB. This validates those numbers under real load.

### Routing (`bench_routing.py`)

Two phases:
- **Phase A:** telemetrygen sends 50K spans/sec through an agent with no routing key (all traffic hits gateway-1).
- **Phase B:** Same rate through an agent with `routing_key: traceID` (traffic distributes across both gateways).

Measures Gini coefficient and skew ratio for each phase.

**Why:** The chapter claims trace-aware routing gives near-equal distribution. Phase A (Gini ~0.5) vs Phase B (Gini ~0) shows the difference.

### Throughput (`bench_throughput.py`)

Sends spans directly to a gateway at increasing rates: 10K, 25K, 50K, 75K, 100K, 150K, 200K, 300K, 400K, 500K spans/sec. Each step runs for 30 seconds. After sending, waits for the gateway's export queue to drain, then measures actual throughput as `exporter_sent_spans / wall_time`.

**Why:** The chapter claims 50K--100K spans/sec per gateway. The plateau point reveals the real ceiling for this hardware.

### Persistent Queue (`bench_persistent_queue.py`)

Two phases at 50K spans/sec for 30 seconds each:
- **Phase A:** Agent with in-memory sending queue.
- **Phase B:** Agent with `file_storage`-backed queue (identical config otherwise).

Compares agent-side `exporter_sent_spans` throughput between phases.

**Why:** Disk-backed queues survive restarts but add I/O overhead. This measures how much.

### Backpressure (`bench_backpressure.py`)

Sends 50K spans/sec into a "blackhole" cascade: agent (queue=500) -> gateway (queue=100) -> nonexistent backend. The gateway queue fills first, then refuses spans, then the agent queue fills, then the agent refuses spans from the SDK.

Polls metrics every 0.5s to capture four timestamps (T1--T4).

**Why:** The chapter describes the cascade from backend stall to SDK-visible drops. This measures how fast each stage triggers.

## Interpreting Results

**Absolute numbers vary.** Docker Compose on a shared laptop gives different numbers than bare-metal production. What matters:

- **Relative comparisons within the same run are valid.** Memory queue vs disk queue, single-gateway vs trace-aware -- the delta is meaningful even if the absolute throughput differs from production.
- **Multi-run averaging smooths noise.** Use `--runs 3` minimum. The runner reports mean, stddev, and coefficient of variation (CV%) for key metrics.
- **CV% > 20% means noisy data.** Likely host contention. Close background apps and re-run.

**Key metrics to check:**
- `peak_throughput` in throughput benchmark -- where the gateway plateaus
- `traceaware_gini` in routing -- should be < 0.01
- `throughput_delta_pct` in PQ -- the overhead percentage
- `cascade_duration_sec` in backpressure -- T1 to T4 elapsed time

## Known Limitations

1. **Docker Compose overhead.** Container networking adds latency that doesn't exist in bare-metal or Kubernetes with host networking. Throughput numbers will be lower than production.

2. **Host contention.** All containers share the same CPU and memory. A background process spike affects all benchmarks. Close other apps during runs.

3. **Single-machine topology.** Real deployments spread agents and gateways across nodes. Network latency and bandwidth differences don't show up here.

4. **Jaeger as backend.** The throughput benchmark sends to a real Jaeger instance. Gateway export throughput is bounded by both the gateway and Jaeger's ingest rate. Production backends (Kafka, cloud storage) have different characteristics.

5. **telemetrygen generates synthetic traces.** Real application traces have variable span counts, attributes, and timing. Synthetic load is more uniform than production traffic.

## Consolidated Findings

From 3 runs / 15 iterations on an Apple M-series laptop (16 GB RAM, Docker Desktop):

### How much memory Docker gets changes these numbers

Give the Docker VM less than the 8 GB above and the results move, in both
directions, so record the allocation alongside any figure you quote.

Reproduced on an Apple M-series machine at two allocations:

| Metric | 3.8 GB | 7.8 GB | Table above (16 GB) |
|---|---|---|---|
| Throughput peak | 49,678/sec | 52,909/sec | 50K-100K/sec |
| PQ overhead | -0.1% | 1.1% | 0-8% |
| Cascade T1-T4 | 68.9 s | 45.6 s | ~28 s |
| Agent RSS idle-peak | 199-210 MB | 204-212 MB | 185-206 MB |
| Gateway RSS idle-peak | 225-229 MB | 245 MB | 230-238 MB |
| Routing Gini, trace-aware | 0.009 | 0.009 | < 0.01 |
| Routing Gini, single-gateway | 0.500 | 0.500 | ~0.5 |

Two things to take from this.

Throughput and persistent-queue overhead sit below their published range when
the VM is starved and land inside it once it is not, so a run under 8 GB is not
a measurement of this stack.

Resident memory moves the other way. More headroom means the Go runtime
collects less often, so RSS rises with the allocation rather than falling. The
gateway measured 225 MB at 3.8 GB and 245 MB at 7.8 GB. Treat the published
range as the shape of the number, not a bound.

The cascade timeline is the most allocation-sensitive figure here and trends
toward the published value as the VM grows.

Routing distribution does not depend on memory at all, and reproduces exactly at
every allocation.

| Metric                         | Range           | Notes                          |
|--------------------------------|-----------------|--------------------------------|
| Agent RSS (idle--50K/sec)      | 185--206 MB     | Grows ~20 MB under load        |
| Gateway RSS (idle--50K/sec)    | 230--238 MB     | Relatively stable              |
| Throughput peak                | 50K--100K/sec   | Per gateway container          |
| PQ overhead                    | 0--8%           | Median ~3%                     |
| Backpressure cascade (T1--T4)  | ~28 seconds     | Gateway fills first, then agent |
| Routing Gini (trace-aware)     | < 0.01          | Near-perfect distribution      |
| Routing Gini (single-gateway)  | ~0.5            | All load on one gateway        |
