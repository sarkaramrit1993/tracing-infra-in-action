# Setup

## What you need

- Docker and Docker Compose v2
- Python 3.10 or later, for the load-generator and benchmark scripts that run outside Docker
- kind, for chapter 3's Kubernetes path only

## Memory and CPU per chapter

Chapter 5 is the one to plan for. Raise Docker Desktop's memory limit under
Settings, Resources before starting it.

| Chapter | Memory | Notes |
|---|---|---|
| 2 | ~1 GB | three containers; the stack is up and answering requests in about 20 seconds |
| 3 | ~200 MB | six containers (agent, two gateways, Jaeger, Prometheus, checkout service). The 8+ GB and 4+ core figure in `BENCHMARKS.md` is for the benchmark suite (`docker-compose.benchmark.yml`), not for the plain `docker compose up` stack |
| 4 | ~1.9 GB | three Kafka brokers; allow 60 to 90 seconds for controller election before the stack settles |
| 5 | does not fit in 3.8 GB | the Flink taskmanager gets OOM-killed partway through a run on a small allocation. See "Chapter 5 seems to run, then behaves oddly" in `troubleshooting.md` before you start this one |

## Apple Silicon

Flink and ClickHouse images are multi-arch, but the PyFlink wheel is the usual
sticking point. If `docker compose build flink` in chapter 5 cannot find a
wheel for your Python version, downgrade the `apache-flink==2.2.1` pin in
`chapter-05/flink/Dockerfile` to the closest available `2.2.x`. The
`KeyedProcessFunction` API the assembly job uses is identical.

## Ports

Chapters reuse the same host ports, so run one chapter at a time. Tear down
with `docker compose down -v` before starting another.

| Port | Service |
|---|---|
| 8080 | checkout-service |
| 4317, 4318 | OTLP gRPC and HTTP |
| 16686 | Jaeger UI |
| 9090 | Prometheus (chapters 3, 4, 5) |
| 8123 | ClickHouse HTTP (chapter 5) |

## Verifying a chapter

```bash
cd chapter-04
docker compose up -d
bash tests/test_stack.sh
docker compose down -v
```

Each chapter has the same three steps: bring the stack up, run
`tests/test_stack.sh`, tear it down with `-v` so the next chapter starts
clean. See `troubleshooting.md` if a step doesn't behave as expected.
