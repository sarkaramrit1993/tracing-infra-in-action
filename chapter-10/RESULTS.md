# Chapter 10 results

Every output block in the README is one measured run, written down below. Then the
same README run four times from `docker compose down -v`, to show which numbers are
exact and how far the sampled ones move.

Measured 2026-11-02 on Docker Desktop with 8 CPUs. Runs 1 to 3 gave Docker 15.6 GiB;
run 4, the README's reference, gave it 7.75 GiB, the README's stated minimum. Versions
pinned as in `docker-compose.yml`:

| Component | Version |
|-----------|---------|
| OpenTelemetry Collector (contrib) | `otel/opentelemetry-collector-contrib:0.154.0` |
| Apache Kafka | `apache/kafka:4.3.1` |
| ClickHouse | `clickhouse/clickhouse-server:26.1` |
| Prometheus | `prom/prometheus:v3.14.0` |
| Apache Flink | `flink:2.2.1-scala_2.12-java17` with `flink-sql-connector-kafka-5.0.0-2.2` |

## The README's run

`./scripts/compare-placements.sh` with its default of 20,000 requests at 200 a
second (199.8 achieved). Request `i` fails when `i * 29 % 1000 < 29`. Every
comparison reads the SERVER span `GET /checkout` at `checkout-service`.

| Quantity | Value |
|---|---|
| requests | 20000 |
| errors | 580 |
| edge.calls | 20000 |
| edge.errors | 580 |
| flink.calls | 20000 |
| flink.errors | 580 |
| uniform.calls | 205 |
| uniform.errors | 7 |
| policy.calls | 778 |
| policy.errors | 580 |
| weighted.calls | 19599 |
| weighted.errors | 500 |
| clickhouse.rows | 205 |

## Four runs

Exact means the same in every run. For the sampled numbers the range is min to max,
then the median.

| Claim (README step) | README says | Four runs |
|---|---|---|
| Truth, edge, Flink: calls / errors (2) | 20000 / 580, exact | 20000 / 580 in all four |
| 1% tail survivors (2, 3) | near 200, most runs 170 to 230 | 205 to 209, median 208.5 |
| 1% tail errors (3) | a handful | 4 to 7 |
| 1% probabilistic, weighted, vs truth (3) | within about 20 percent | 0.98x to 1.13x, median 1.02x |
| Keep-every-error plus 1%, error rate (3) | near 75 percent | 73.89 to 74.55 percent |
| Exemplars found in ClickHouse, before / after the sampler (3) | almost none / every one | 0 of 4-7 / every one of 5-6 |
| Late span dropped 0 -> 1, window unchanged (4) | yes | yes in all four, seen after 11 s |
| View = query calls (5) | equal | equal in all four |
| Rows read, view / query (5) | the view reads a small fraction | 15 to 23 / 618 to 633 |
| Weight check, 50 rows -> calls / errors / p99 (5) | 5000 / 200 / 500ms | exact in all four |
| Series per label set / buckets / label sets / total (6) | 16 / 13 / 7 / 112 | exact in all four |
| Try this, 300ms boundary (6) | 17 per label set | 17 in all four |
| Series without / with `user.id` (7) | 270 / 160,190 | 270 / 160,190 in all four |
| Calls series per service / overflow series (7) | 5001 / 2 | exact in all four |
| `user.id` job down, sample limit exceeded (7) | yes | yes in all four |
| Try this, limit 100: series / per service / accepted (7) | a few thousand / about 100 / yes | 3390 / 101 / yes in all four |

The weighted count's spread is the sampler's draw: about 200 survivors each standing
for 100 requests. The tail sampler's error count is built from a handful of kept
errors, so it moves the most.

## Time

Seconds per README step, whole run included, min to max then median. Step 3 and step
5 reuse the workload step 2 already sent and counted, which is why they take seconds.

| README step | README says | Four runs (s) |
|---|---|---|
| 1. Start the stack | 3 to 6 minutes | 145 to 324, median 181 |
| 2. Count in four places | 4 to 7 minutes | 202 to 267, median 261 |
| 3. Sampling and exemplars | about 1 minute | 1 to 2 |
| 4. Late span | about 1 minute | 48 to 56, median 52 |
| 5. View and query | under 1 minute | 1 to 2 |
| 6. Series, then its Try this and reset | about 1 minute, then 2 | 0 to 1, then 68 to 78 |
| 7. `user.id`, then its Try this and reset | about 2 minutes, then 3 | 133 to 136, then 137 to 155 |
| Whole pass, through `down -v` | about 20 minutes | 760 to 1001, median 867 |

Run 4, with 7.75 GiB, took 837 seconds and fell inside every range above.

## Peak memory

Every container's `docker stats` summed, sampled about every 20 seconds, so a spike
shorter than that can fall between samples.

| | Four runs |
|---|---|
| Peak outside step 7 | 4.42 to 5.50 GiB |
| Peak during step 7 | 5.12 to 6.05 GiB |

Per container in run 4: ClickHouse 2,095 MiB, Kafka 985, Flink jobmanager 827, Flink
taskmanager 757, sampler Collector 576, Prometheus 372, the `user.id` Collector 96,
the edge Collector 91, everything else under 90 each.
