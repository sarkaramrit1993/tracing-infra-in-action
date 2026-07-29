# Troubleshooting

## The stack looks broken 30 seconds after `docker compose up -d`

It usually is not. Kafka has to elect controllers (chapters 4 and 5),
ClickHouse has to apply its schema (chapter 5), and Prometheus needs a couple
of scrape cycles before it reports targets as up (chapters 3, 4, 5). Give it
60 to 90 seconds and watch progress rather than guessing:

```bash
docker compose ps
docker compose logs -f flink-job-submit   # chapter 5 only
```

## A verification script fails on its first assertion

The stack is not up yet, or a container is still starting. Run
`docker compose ps` and confirm every service shows `running` (and
`healthy` where the compose file defines a healthcheck). The scripts assert
liveness first precisely so this failure is unambiguous.

## Port already in use

Chapters share host ports (see the table in `setup/README.md`). Tear the
previous chapter down completely before starting the next one:

```bash
docker compose down -v
```

The `-v` matters: leftover volumes carry stale Kafka topic state and, for
chapter 5, stale ClickHouse data into the next run.

## No traces in Jaeger, but curl returns 200

Expected in some cases, and it is the point of chapter 4's failure test. The
SDK batches spans before export, so allow up to 60 seconds. If nothing
arrives after that, check the collector logs for export errors:

```bash
docker compose logs otel-collector | grep -i error   # chapter 2
docker compose logs otel-agent | grep -i error        # chapters 3, 4, 5
```

## Chapter 3 benchmarks give inconsistent numbers between runs

The backpressure benchmark fills collector queues permanently, so it runs
last. Restart the blackhole collectors between runs:

```bash
docker compose -f docker-compose.yml -f docker-compose.benchmark.yml \
  up -d --force-recreate otel-gateway-blackhole otel-agent-blackhole
```

Benchmarks are also contention-sensitive. Close other heavy workloads first,
and see the hardware note in `BENCHMARKS.md`: the benchmark suite needs more
memory and more CPU than the plain chapter 3 stack.

## Chapter 5's Flink image will not build

See the Apple Silicon note in `setup/README.md`. Downgrade the
`apache-flink==2.2.1` pin to the closest available `2.2.x` and rebuild.

## Chapter 5 seems to run, then behaves oddly

This is the one worth reading before you file a bug. Chapter 5 needs more
memory than the other chapters, and on a Docker allocation under about 4 GB,
the Flink taskmanager can get OOM-killed a few minutes into a run while every
other container keeps going. `docker compose ps` still shows everything
`running`, because Kafka, ClickHouse, and the collectors never crashed. Only
Flink did.

The confusing part is that `tests/test_stack.sh` can still report a pass.
Its assertions check whether ClickHouse and Kafka already hold data, not
whether Flink is currently alive, so if the taskmanager assembled and wrote a
few traces before it died, the script finds that pre-crash data and passes
anyway.

If chapter 5 looks healthy but the numbers don't add up, check for an OOM
kill directly:

```bash
docker compose ps -a
docker inspect $(docker compose ps -aq flink-taskmanager) --format '{{.State.OOMKilled}}'
```

`true` means the taskmanager was killed for memory. Raise Docker Desktop's
memory limit (Settings, Resources) before running chapter 5 again. The
chapter's own target is around 6 GB; give it that much or more.
