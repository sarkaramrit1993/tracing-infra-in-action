#!/usr/bin/env python3
"""Generate a sampled trace population, and record what it generated.

The point of this script is the last part. In production you can never check an
unbiased estimate, because the population it estimates was thrown away at the
sampler. Here the population is known, so listing 8.1's queries can be graded
rather than merely compared.

The shape, all of it deliberate and all of it checkable by hand:

    class    requests    keep rate    kept     adjusted_count
    normal   9,920,000   1 in 100     99,200   100
    slow        50,000   1 in 2       25,000     2
    error       30,000   keep all     30,000     1
    total   10,000,000                154,200

    99,200 x 100  +  25,000 x 2  +  30,000 x 1  =  10,000,000

So sum(adjusted_count) over the root spans should return the population exactly,
while a bare count() returns 154,200 and reads 65x low.

Slow and error together are under one percent of the population on purpose. That
is what puts the true p99 inside the normal band, so the weighted query has a
low number to find and the survivors, packed with the classes the sampler
favored, have a high one. Push those two classes up and the true p99 climbs with
them. The weighted query still finds it, to within a millisecond at every split
measured between 0.8 and 10 percent, so the correction itself never fails. What
closes is the distance to the unweighted number: by about three percent the two
read 1366 against 1447 and there is nothing left to point at.

Sampling here is deterministic (every hundredth, every second) rather than a
coin flip per trace. That is a real difference and worth naming: with a real
Bernoulli sampler the weighted total lands NEAR the population, not on it, and
the spread is wide at low keep rates. Determinism is what lets this file print a
number that reproduces on your machine. exercises/unbiased.md has a variation
that switches the sampler to a real coin and shows the scatter, which is the
confidence interval the chapter says is usually missing.

Rows are built server-side with INSERT ... SELECT FROM numbers(), so a million
spans take seconds and nothing large crosses the wire.

Timestamps hang off now() rather than a fixed anchor, because every query in
listing 8.1 filters on the last hour and a fixed anchor would return nothing.
Everything else is deterministic: the counts, the weights and the durations are
functions of a row's index, so the numbers reproduce exactly. Only the clock
moves. The spread is twenty minutes, which leaves about forty before the oldest
rows age out of a one-hour window; past that the totals start falling short of
the population and the honest fix is to run this script again.

Usage:  python3 generate/generate.py
"""
import subprocess
import sys
from pathlib import Path

CHAPTER_DIR = Path(__file__).resolve().parent.parent

POPULATION = 10_000_000
NORMAL, SLOW, ERROR = 9_920_000, 50_000, 30_000
NORMAL_KEEP, SLOW_KEEP, ERROR_KEEP = 100, 2, 1

NORMAL_KEPT = NORMAL // NORMAL_KEEP          # 99,200
SLOW_KEPT = SLOW // SLOW_KEEP                # 25,000
ERROR_KEPT = ERROR // ERROR_KEEP             # 30,000
KEPT = NORMAL_KEPT + SLOW_KEPT + ERROR_KEPT  # 154,200
SPANS_PER_TRACE = 7

# Duration of a request, in milliseconds, as a function of its index in the
# population. Written once here and reused for both the true-p99 computation
# and the rows themselves, so the two cannot drift apart.
#
# The bands are chosen so the three numbers the exercise compares actually
# separate: normal tops out at 180ms, which is where the true p99 lands because
# slow and error together are only 1% of the population; the survivors are
# packed with slow and error, so their unweighted p99 lands out at 1.4s.
DURATION_MS = f"""
multiIf(
  n < {NORMAL},          40 + (n % 141),
  n < {NORMAL + SLOW},  700 + (n % 100),
                       1350 + (n % 100))
"""

# Index into the population, given an index into the kept set. Keeping this
# mapping explicit is what makes the weight verifiable: a kept normal trace is
# every hundredth of the population, so it stands for a hundred.
POPULATION_INDEX = f"""
multiIf(
  t < {NORMAL_KEPT},              t * {NORMAL_KEEP},
  t < {NORMAL_KEPT + SLOW_KEPT},  {NORMAL} + (t - {NORMAL_KEPT}) * {SLOW_KEEP},
                                  {NORMAL + SLOW} + (t - {NORMAL_KEPT + SLOW_KEPT}))
"""

WEIGHT = f"""
multiIf(
  t < {NORMAL_KEPT},              {NORMAL_KEEP},
  t < {NORMAL_KEPT + SLOW_KEPT},  {SLOW_KEEP},
                                  {ERROR_KEEP})
"""

CHILD_NAMES = ("'validate_cart', 'inventory.reserve', 'payment.charge', "
               "'fraud.score', 'order.create', 'notification.send'")


def ch(sql, *, quiet=False):
    """Run one statement. stdin is /dev/null, never the caller's terminal.

    clickhouse-client reads stdin for INSERT data even when the rows are inline
    or come from a SELECT, and `docker compose exec -T` hands it whatever stdin
    the caller had. From a terminal that never reaches EOF and the whole thing
    hangs with no output. See NOTES.md.
    """
    argv = ["docker", "compose", "exec", "-T", "clickhouse",
            "clickhouse-client", "--query", sql]
    proc = subprocess.run(argv, cwd=CHAPTER_DIR, stdin=subprocess.DEVNULL,
                          capture_output=True, text=True)
    if proc.returncode != 0:
        if not quiet:
            print(f"[generate] ClickHouse rejected a statement:\n{proc.stderr.strip()}",
                  file=sys.stderr)
        raise SystemExit(1)
    return proc.stdout.strip()


def main():
    try:
        ch("SELECT 1", quiet=True)
    except SystemExit:
        print("[generate] cannot reach ClickHouse. Is the stack up?\n"
              "           docker compose up -d && docker compose ps",
              file=sys.stderr)
        raise SystemExit(1)

    print(f"[generate] population {POPULATION:,} requests, "
          f"keeping {KEPT:,} ({KEPT / POPULATION:.2%})")

    # The true p99, computed over the whole population rather than asserted.
    # This is the number the weighted query has to reproduce.
    true_p99 = float(ch(f"""
        SELECT quantileExact(0.99)(dur) FROM (
          SELECT {DURATION_MS} AS dur
          FROM (SELECT number AS n FROM numbers({POPULATION})))
    """))
    print(f"[generate] true p99 over the full population: {true_p99:.1f} ms")

    for table in ("otel_traces", "ground_truth", "sampling_policy"):
        ch(f"TRUNCATE TABLE IF EXISTS tracing.{table}")

    ch(f"""
        INSERT INTO tracing.sampling_policy (class, keep_rate, adjusted_count)
        VALUES ('normal', {1 / NORMAL_KEEP}, {NORMAL_KEEP}),
               ('slow',   {1 / SLOW_KEEP},   {SLOW_KEEP}),
               ('error',  {1 / ERROR_KEEP},  {ERROR_KEEP})
    """)

    ch(f"""
        INSERT INTO tracing.ground_truth
          (run_id, generated_at, requests, p99_ms, errors)
        VALUES ('run', now(), {POPULATION}, {true_p99}, {ERROR})
    """)

    # One row per span. The root carries the request's outcome, the way an HTTP
    # server records a response status, which is what lets a rollup count errors
    # by reading roots alone.
    print(f"[generate] writing {KEPT * SPANS_PER_TRACE:,} spans "
          f"({KEPT:,} traces x {SPANS_PER_TRACE})")
    ch(f"""
        INSERT INTO tracing.otel_traces
          (timestamp, trace_id, span_id, parent_span_id, service_name,
           span_name, status_code, duration_ns, adjusted_count, attributes)
        SELECT
          now64(9) - toIntervalMillisecond((t % 1200) * 1000) AS timestamp,
          lower(hex(MD5(toString(t)))) AS trace_id,
          lower(hex(reinterpretAsFixedString(toUInt64(number)))) AS span_id,
          if(s = 0, '',
             lower(hex(reinterpretAsFixedString(toUInt64(t * {SPANS_PER_TRACE})))))
            AS parent_span_id,
          'checkout-service' AS service_name,
          if(s = 0, 'GET /checkout', [{CHILD_NAMES}][s]) AS span_name,
          multiIf(n < {NORMAL + SLOW}, 'STATUS_CODE_UNSET',
                  s = 0, 'STATUS_CODE_ERROR',
                  s = 4, 'STATUS_CODE_ERROR',
                  'STATUS_CODE_OK') AS status_code,
          if(s = 0, dur_ms * 1000000, intDiv(dur_ms * 1000000, 8)) AS duration_ns,
          w AS adjusted_count,
          map('http.method', 'POST') AS attributes
        FROM (
          SELECT number, t, s, n, w, {DURATION_MS} AS dur_ms
          FROM (
            SELECT
              number,
              toInt64(intDiv(number, {SPANS_PER_TRACE})) AS t,
              toInt64(number % {SPANS_PER_TRACE}) AS s,
              {POPULATION_INDEX} AS n,
              {WEIGHT} AS w
            FROM numbers({KEPT * SPANS_PER_TRACE})
          )
        )
        SETTINGS max_insert_threads = 4
    """)

    # Collapse into one part. Without this the insert lands in however many
    # parts the server felt like, and the granule totals listing 8.2 prints move
    # between runs for a reason that has nothing to do with indexes.
    ch("OPTIMIZE TABLE tracing.otel_traces FINAL")

    rows = int(ch("SELECT count() FROM tracing.otel_traces"))
    roots = int(ch("SELECT count() FROM tracing.otel_traces "
                   "WHERE parent_span_id = ''"))
    weighted = float(ch("SELECT sum(adjusted_count) FROM tracing.otel_traces "
                        "WHERE parent_span_id = ''"))
    print(f"[generate] {rows:,} spans, {roots:,} roots, "
          f"sum(adjusted_count) over roots = {weighted:,.0f}")
    if weighted != POPULATION:
        print(f"[generate] FAIL: weighted total {weighted:,.0f} is not the "
              f"population {POPULATION:,}", file=sys.stderr)
        raise SystemExit(1)
    print("[generate] done. The weighted total reproduces the population exactly.")


if __name__ == "__main__":
    main()
