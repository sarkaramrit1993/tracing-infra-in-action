# Setup

## What you need

- Docker and Docker Compose v2
- Python 3.12, for the load-generator and benchmark scripts that run outside Docker. This is the version CI installs, so it is the only one the chapters are verified against
- kind, for chapter 3's Kubernetes path only

## Python environment

Several chapters run scripts outside Docker. Install their dependencies into a
virtual environment rather than the system Python. Recent Debian, Ubuntu,
Fedora and Homebrew installs refuse a system-wide `pip install` outright with
`error: externally-managed-environment`, and a venv is what CI uses for every
chapter anyway.

From the chapter directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

On Debian and Ubuntu, `python3 -m venv` fails until you install `python3-venv`,
which does not come with `python3`. The error message says so. Install it with
`sudo apt install python3-venv` and run the command again. This applies to WSL2
as well, since WSL2 is Ubuntu.

On Windows, activate with `.venv\Scripts\activate` instead. Once the
environment is active, `python` and `pip` point into it, so the install
commands in each chapter work as written. Run `deactivate` when you're done.

Stock macOS has no `python` or `pip` on the PATH, only `python3`, which is why
the chapters create the environment with `python3 -m venv`.

## Memory and CPU per chapter

Chapter 5 is the one to plan for. Raise Docker Desktop's memory limit under
Settings, Resources before starting it.

| Chapter | Memory | Notes |
|---|---|---|
| 2 | ~160 MB | three containers (checkout service, Jaeger, OTel Collector); measured with `docker stats --no-stream` after the stack settled. The stack is up and answering requests in about 20 seconds |
| 3 | ~245 MB | six containers (agent, two gateways, Jaeger, Prometheus, checkout service); measured the same way as chapter 2. The 8+ GB and 4+ core figure in `BENCHMARKS.md` is for the benchmark suite (`docker-compose.benchmark.yml`), not for the plain `docker compose up` stack |
| 4 | ~1.9 GB (approximate) | three Kafka brokers; allow 60 to 90 seconds for controller election before the stack settles |
| 5 | ~6 GB at the published Flink sizing | the heaviest stack. On a 3.8 GB allocation the Flink taskmanager gets OOM-killed partway through a run, and the stack still looks healthy while it is gone. It does fit in about 2.8 GB if you shrink Flink; both the symptom and that workaround are in `troubleshooting.md` |
| 7 | ~1.7 GB settled, ~2.1 GB peak during the benchmarks | eight containers (ClickHouse, Kafka, Collector, MinIO, Tempo, Prometheus, checkout service, consumer); measured with `docker stats --no-stream` after the stack settled and again mid-benchmark. Tempo is about 85 MB of that. Allow about 45 seconds to settle, longer on a first run while the images pull and the app image builds |
| 8 | ~1 GB settled | one container, ClickHouse alone. The generated population is 38 MB on disk and the server settles under 1 GB once it is loaded. About 20 seconds to healthy |
| 9 | ~2.2 GB settled, ~3.9 GB peak during the fingerprint benchmark | seven running containers (ClickHouse, Kafka, Collector, Prometheus, Loki, checkout service, consumer) plus a one-shot topic creator; measured with `docker stats --no-stream` with traffic driven, and again while `benchmarks/fingerprint_compression.py` built its two million rows. ClickHouse is 1.2 GB of the settled figure and 2.9 GB of the peak, Kafka about 700 MB. Give Docker 5 GB: at the settled figure the benchmark OOM-kills ClickHouse partway through and reads as a query that hung. Allow about 90 seconds to settle, and another 30 for the first span-metric series to reach Prometheus |

## Apple Silicon

Flink and ClickHouse images are multi-arch, but the PyFlink wheel is the usual
sticking point. If `docker compose build flink-jobmanager flink-taskmanager
flink-job-submit` in chapter 5 cannot find a wheel for your Python version,
downgrade the `apache-flink==2.2.1` pin in `chapter-05/flink/Dockerfile` to
the closest available `2.2.x` and re-run the same build command. The
`KeyedProcessFunction` API the assembly job uses is identical. The
Dockerfile's first line also pins `FROM flink:2.2.1-scala_2.12-java17`;
downgrading only the pip pin without matching that base image tag can leave
the container's Flink runtime and its PyFlink wheel at different versions.

## Ports

Chapters reuse the same host ports, so run one chapter at a time. Tear down
with `docker compose down -v` before starting another.

| Port | Service |
|---|---|
| 8080 | checkout-service (chapters 2, 3, 4, 5, 7, 9) |
| 4317, 4318 | OTLP gRPC and HTTP |
| 16686 | Jaeger UI |
| 9090 | Prometheus (chapters 3, 4, 5, 7, 9) |
| 8123 | ClickHouse HTTP (chapters 5, 7, 8, 9) |
| 9000, 9363 | ClickHouse native and metrics (chapters 7, 8, 9) |
| 8888 | Collector metrics (chapters 7, 9) |
| 9001, 9002 | MinIO console and API (chapter 7) |
| 3200, 4417 | Tempo query and OTLP (chapter 7) |
| 8889 | Collector span-metrics and service-graph scrape endpoint (chapter 9) |
| 3100 | Loki HTTP (chapter 9) |

## Verifying a chapter

The `sleep` covers chapter 4's Kafka controller election, which takes 60 to 90
seconds:

```bash
cd chapter-04
docker compose up -d
sleep 90
bash tests/test_stack.sh
docker compose down -v
```

Each chapter has the same three steps: bring the stack up, give it time to
settle, run `tests/test_stack.sh`, tear it down with `-v` so the next
chapter starts clean. See `troubleshooting.md` if a step doesn't behave as
expected.
