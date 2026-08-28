"""Chapter 9 benchmark: what the listing 9.2 normalization is worth.

Section 9.2.3 says the compression is "dramatic and reliable": millions of raw
error spans collapse to tens or low thousands of distinct fingerprints, because
a handful of code paths throw the overwhelming majority of the errors. That is
an unsupported claim in the chapter. This script turns it into a measurement.

In production you can count issues but never grade the count, because nobody
knows how many distinct bugs the service really has. Here the generator knows:
it seeds exactly P code paths, each one a distinct (exception.type, top frame,
message template) triple, and then asks the listing 9.2 view how many issues it
found.

Be exact about what that grades. F is measured: it comes out of the view, over
rows the view folded itself. P is declared: the same constant that drives the
generator is written into a truth table and read back, so the round trip proves
the store kept the number, not that the generator produced that many paths. The
seeding loop below is what makes P true, and it is an assertion about this file
rather than a measurement. So this is not chapter 8's ground-truth discipline,
where the recorded population is the thing an estimator has to recover. It is
one measured number checked against one declared one, which is enough for the
claim being made and is not enough to call P evidence.

    N   raw error spans generated
    D   distinct fingerprints WITHOUT normalization
    F   distinct fingerprints WITH the listing 9.2 normalization
    P   code paths actually seeded, read back from the truth table

The assertions are mechanisms, not magic constants:

    F == P          normalization neither over-merges two bugs into one issue
                    nor under-merges one bug into many. This is the whole test.
                    A regex that misses a variable token pushes F up toward N;
                    one that strips too much pulls F below P.
    D > 0.9 * N     the raw messages really are near-unique, so the compression
                    below is normalization's doing and not the generator's.
    top-10 > 50%    the volume is Zipf across paths, so "a handful of code paths
                    throw most of the errors" is modelled rather than asserted.

The ratio N/F is reported and never asserted. It is a property of how many code
paths were seeded, so it says nothing about your service.

Rows are built server-side with INSERT ... SELECT FROM numbers(), so nothing
large crosses the wire. Everything is a function of the row index, so two runs
on the same P and N return the same numbers.

The measuring pass runs the real clickhouse/error_index.sql, with its three
table names redirected at a scratch database prefix. Nothing here reimplements
the normalization; if the listing changes, this benchmark changes with it, and
the live store is never touched.

Run (stack up):
    python3 benchmarks/fingerprint_compression.py
    PATHS=300 SPANS=200000 python3 benchmarks/fingerprint_compression.py
"""
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

CHAPTER = Path(__file__).resolve().parent.parent

PATHS = int(os.environ.get("PATHS", "1200"))
SPANS = int(os.environ.get("SPANS", "2000000"))
# The exercise sends the reader to read the truth table for themselves. Dropping
# it on the way out makes that read fail on a correct stack, so KEEP_SCRATCH
# leaves the three scratch tables in place for exactly that.
KEEP_SCRATCH = os.environ.get("KEEP_SCRATCH", "") not in ("", "0", "false")

# Zipf exponent s = 1 + ALPHA. At 0.5 the busiest path takes about 30% of the
# volume and the top ten about 70%, which is the shape an error tracker sees.
# It is a truncated Pareto rather than a clamped one: clamping every draw past
# the last rank onto that rank puts a spike at the tail, and the rarest bug
# comes out as the second most common.
ALPHA = 0.5

SCRATCH_SPANS = "tracing.fp_bench_spans"
SCRATCH_ISSUES = "tracing.fp_bench_issues"
SCRATCH_VIEW = "tracing.fp_bench_mv"
SCRATCH_TRUTH = "tracing.fp_bench_truth"

# Three word lists, read as a mixed-radix decomposition of the path index, so
# 12 x 10 x 10 gives 1200 distinct message templates. Not one of these words
# survives the listing 9.2 regex as a number: every token it strips is a digit
# run or eight-plus lowercase hex characters, so a template stays distinct
# after normalization while the ids inside it do not.
SUBSYSTEMS = ["fraud", "payment", "inventory", "ledger", "catalog", "shipping",
              "pricing", "identity", "session", "wallet", "refund", "tax"]
OPERATIONS = ["lookup", "score", "authorize", "reserve", "settle", "quote",
              "verify", "enqueue", "snapshot", "reconcile"]
CAUSES = ["deadline exceeded", "connection reset by peer", "no route to host",
          "pool exhausted", "lock wait timeout", "checksum mismatch",
          "schema version drift", "quota exceeded",
          "upstream returned a malformed body", "credential rotation in flight"]
TYPES = ["TimeoutError", "ConnectionResetError", "KeyError", "ValueError",
         "IntegrityError", "PermissionError", "JSONDecodeError", "OSError"]


def _array(values):
    return "[" + ", ".join("'" + v.replace("'", "\\'") + "'" for v in values) + "]"


def ch(sql, *, stdin_sql=None, quiet=False):
    """Run one statement inside the ClickHouse container.

    stdin is either the .sql file being applied or /dev/null. It is never left
    on the terminal, which is what stops the client hanging on an EOF that
    never arrives.
    """
    argv = ["docker", "compose", "exec", "-T", "clickhouse",
            "clickhouse-client", "--database", "tracing"]
    argv += ["--multiquery"] if stdin_sql is not None else ["--query", sql]
    proc = subprocess.run(argv, cwd=CHAPTER, text=True, capture_output=True,
                          input=stdin_sql if stdin_sql is not None else "")
    if proc.returncode != 0:
        if not quiet:
            print("[fingerprint] ClickHouse rejected a statement:\n"
                  + proc.stderr.strip(), file=sys.stderr)
        raise SystemExit(1)
    return proc.stdout.strip()


def scalar(sql):
    return ch(sql).strip()


def drop_scratch():
    for name in (SCRATCH_VIEW,):
        ch(f"DROP VIEW IF EXISTS {name}")
    for name in (SCRATCH_ISSUES, SCRATCH_SPANS, SCRATCH_TRUTH):
        ch(f"DROP TABLE IF EXISTS {name}")


def record_the_truth():
    """Write down what is about to be generated, before anything measures it.

    Reading P back out of this table rather than off the Python constant keeps
    the assertion honest about ORDER: the number F is compared against was
    committed to the store before the first measuring query existed, so no
    measurement can have influenced it. It does not make P evidence. PATHS
    generates the rows and PATHS is what lands in this table, so p_truth == PATHS
    by construction. F is the measurement; P is the declaration it is graded
    against.
    """
    ch(f"""CREATE TABLE {SCRATCH_TRUTH} (
             recorded_at  DateTime,
             code_paths   UInt32,
             error_spans  UInt64,
             zipf_alpha   Float64,
             template_space UInt32
           ) ENGINE = MergeTree ORDER BY recorded_at""")
    space = len(SUBSYSTEMS) * len(OPERATIONS) * len(CAUSES)
    ch(f"""INSERT INTO {SCRATCH_TRUTH} VALUES
           (now(), {PATHS}, {SPANS}, {ALPHA}, {space})""")


def generate():
    """Build N error spans over P code paths, server-side.

    Every raw message carries three variable tokens: a seven-digit cart id, a
    duration in milliseconds, and a sixteen-character lowercase hex request id.
    The hex id is what makes a raw message effectively unique, and lowercase is
    load-bearing: the listing 9.2 regex reads [0-9a-f], so an uppercase hex()
    would sail through normalization and D and F would come out equal.

    The first P rows are seeded one per path, so every path is present by
    construction and F == P is an identity about the normalization rather than
    a statement about how a random draw happened to land.
    """
    subs, ops, causes, types = (_array(SUBSYSTEMS), _array(OPERATIONS),
                                _array(CAUSES), _array(TYPES))
    ns = len(SUBSYSTEMS)
    no = len(OPERATIONS)

    # u is a hash of the row index, so the distribution is reproducible without
    # a per-row coin flip. The inverse CDF below is a Pareto truncated at P.
    sql = f"""
INSERT INTO {SCRATCH_SPANS}
    (timestamp, trace_id, span_id, parent_span_id, service_name, span_name,
     status_code, duration_ns, adjusted_count, attributes)
SELECT
    now64(9) - toIntervalMillisecond(toUInt32(number % 600000)) AS timestamp,
    lower(hex(MD5(concat('trace', toString(number)))))          AS trace_id,
    lower(hex(cityHash64(number, 'span')))                      AS span_id,
    lower(hex(cityHash64(number, 'parent')))                    AS parent_span_id,
    'checkout-service'                                          AS service_name,
    'fraud.score'                                               AS span_name,
    'STATUS_CODE_ERROR'                                         AS status_code,
    (30000 + ms) * 1000000                                      AS duration_ns,
    1.0                                                         AS adjusted_count,
    map('exception.type', etype,
        'exception.message', message,
        'exception.stacktrace', stacktrace)                     AS attributes
FROM (
    SELECT
        number,
        path,
        arrayElement({types}, toUInt32((path - 1) % {len(TYPES)}) + 1)   AS etype,
        arrayElement({subs}, toUInt32((path - 1) % {ns}) + 1)             AS subsystem,
        arrayElement({ops}, toUInt32(intDiv(path - 1, {ns}) % {no}) + 1)  AS operation,
        arrayElement({causes},
                     toUInt32(intDiv(path - 1, {ns * no}) % {len(CAUSES)}) + 1) AS cause,
        concat('svc_', leftPad(toString(path), 4, '0'))          AS module,
        concat('handle_', leftPad(toString(path), 4, '0'))       AS func,
        1000000 + toUInt32(cityHash64(number, 'cart') % 9000000) AS cart,
        toUInt32(cityHash64(number, 'ms') % 5000)                AS ms,
        lower(hex(cityHash64(number, 'req')))                    AS req,
        80 + toUInt32(cityHash64(number, 'deploy') % 3) * 7      AS raise_line,
        concat(subsystem, '.', operation, ' failed for cart ', toString(cart),
               ': ', cause, ' after ', toString(ms), 'ms (req ', req, ')') AS message,
        concat('Traceback (most recent call last):\\n',
               '  File "/app/checkout.py", line 197, in checkout\\n',
               '    result = dispatch(request)\\n',
               '  File "/app/services/', module, '.py", line ',
               toString(raise_line), ', in ', func, '\\n',
               '    raise ', etype, '(detail)\\n',
               etype, ': ', message)                            AS stacktrace
    FROM (
        SELECT
            number,
            if(number < {PATHS},
               toUInt32(number) + 1,
               least(toUInt32({PATHS}), greatest(toUInt32(1), toUInt32(ceil(
                   pow(1 - (toFloat64(cityHash64(number, 'zipf'))
                            / 18446744073709551616.0)
                           * (1 - pow({PATHS}, -{ALPHA})),
                       -1 / {ALPHA})))))) AS path
        FROM numbers({SPANS})
    )
)
SETTINGS max_insert_threads = 4
"""
    ch(sql)


def arm_the_listing():
    """Apply clickhouse/error_index.sql against the scratch tables.

    Three literal substitutions and nothing else, so the normalization the
    benchmark measures is character-for-character the one the book prints. The
    view has to exist before the insert, because a materialized view fires on
    insert and never backfills.
    """
    src = (CHAPTER / "clickhouse" / "error_index.sql").read_text()
    for real, scratch in (("tracing.exc_mv", SCRATCH_VIEW),
                          ("tracing.exceptions", SCRATCH_ISSUES),
                          ("tracing.otel_traces", SCRATCH_SPANS)):
        assert real in src, f"error_index.sql no longer names {real}"
        src = src.replace(real, scratch)
    ch(None, stdin_sql=src)


def run():
    print(f"[fingerprint] paths={PATHS:,} spans={SPANS:,} zipf_alpha={ALPHA}")
    try:
        ch("SELECT 1", quiet=True)
    except SystemExit:
        print("[fingerprint] cannot reach ClickHouse. Is the stack up?\n"
              "              docker compose up -d && docker compose ps",
              file=sys.stderr)
        raise SystemExit(1)

    drop_scratch()
    record_the_truth()
    print("[fingerprint] seeded path count recorded before any measuring query ran")

    ch(f"CREATE TABLE {SCRATCH_SPANS} AS tracing.otel_traces")
    arm_the_listing()
    print("[fingerprint] listing 9.2 view armed over an empty scratch table")

    print("[fingerprint] generating error spans server-side...")
    generate()

    # Every number below comes back out of the store, P included, so the
    # comparison is between two things the store holds rather than between a
    # query and a live Python constant. What that buys is ordering, not
    # independence: the same PATHS drove the generator.
    p_truth = int(scalar(f"SELECT code_paths FROM {SCRATCH_TRUTH}"))
    n = int(scalar(f"SELECT count() FROM {SCRATCH_SPANS}"))
    d = int(scalar(f"""
        SELECT uniqExact(cityHash64(attributes['exception.type'],
                                    attributes['exception.message'],
                                    attributes['exception.stacktrace']))
        FROM {SCRATCH_SPANS}"""))
    f = int(scalar(f"SELECT uniqExact(fingerprint) FROM {SCRATCH_ISSUES}"))
    folded = int(scalar(f"SELECT sum(error_count) FROM {SCRATCH_ISSUES}"))
    top10 = int(scalar(f"""
        SELECT sum(c) FROM (
            SELECT sum(error_count) AS c FROM {SCRATCH_ISSUES}
            GROUP BY fingerprint ORDER BY c DESC LIMIT 10)"""))
    busiest = int(scalar(f"""
        SELECT sum(error_count) AS c FROM {SCRATCH_ISSUES}
        GROUP BY fingerprint ORDER BY c DESC LIMIT 1"""))
    sample = ch(f"""
        SELECT concat(any(error_type), '  |  ', any(msg_template))
        FROM {SCRATCH_ISSUES} GROUP BY fingerprint
        ORDER BY sum(error_count) DESC LIMIT 1""")

    ratio = n / f if f else 0.0
    top10_share = top10 / folded if folded else 0.0
    busiest_share = busiest / folded if folded else 0.0

    print(f"[fingerprint] N raw error spans        : {n:,}")
    print(f"[fingerprint] D distinct un-normalized : {d:,}  ({d / n:.4%} of N)")
    print(f"[fingerprint] F distinct fingerprints  : {f:,}")
    print(f"[fingerprint] P code paths seeded      : {p_truth:,}")
    print(f"[fingerprint] compression N/F          : {ratio:,.0f}x")
    print(f"[fingerprint] top-10 share of volume   : {top10_share:.1%}"
          f"  (busiest alone {busiest_share:.1%})")
    print(f"[fingerprint] busiest issue            : {sample}")

    if folded != n:
        raise SystemExit(f"[fingerprint] the index folded {folded:,} spans but the "
                         f"table holds {n:,}; the view missed some of the insert")
    if f != p_truth:
        direction = "over-merging distinct bugs" if f < p_truth else \
                    "leaving a variable token in the template"
        raise SystemExit(f"[fingerprint] F={f:,} against a recorded P={p_truth:,}. "
                         f"The normalization is {direction}.")
    if not d > 0.9 * n:
        raise SystemExit(f"[fingerprint] D={d:,} is not above 90% of N={n:,}, so the "
                         "raw messages were already near-duplicates and the "
                         "compression below is the generator's, not the regex's.")
    if not top10_share > 0.5:
        raise SystemExit(f"[fingerprint] the top ten issues carry {top10_share:.1%} of "
                         "the volume; the population is too flat to model an "
                         "error tracker's workload")

    print(f"[fingerprint] PASS: F == P == {f:,}; D is {d / n:.1%} of N; "
          f"top ten carry {top10_share:.1%}")

    stamp = datetime.now(timezone.utc)
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"fingerprint-compression-{stamp.strftime('%Y-%m-%dT%H%M%S')}.json"
    out.write_text(json.dumps({
        "benchmark": "fingerprint_compression",
        "measured_at_utc": stamp.isoformat(),
        "generated": {
            "code_paths_recorded": p_truth,
            "raw_error_spans": n,
            "zipf_alpha": ALPHA,
            "template_space": len(SUBSYSTEMS) * len(OPERATIONS) * len(CAUSES),
        },
        "measured": {
            "distinct_unnormalized": d,
            "distinct_unnormalized_share_of_n": round(d / n, 6),
            "distinct_fingerprints": f,
            "compression_ratio": round(ratio, 1),
            "top10_share_of_error_volume": round(top10_share, 4),
            "busiest_issue_share": round(busiest_share, 4),
        },
        "note": "What generalizes is the mechanism and the result F == P: the "
                "listing 9.2 normalization recovers the seeded code-path count "
                "exactly, from raw messages that were "
                f"{d / n:.1%} distinct before it ran. What does not generalize is "
                f"the {ratio:,.0f}x ratio. That is N divided by the number of code "
                "paths seeded here, so it is a property of this generator and not "
                "a figure to quote for a service. Line numbers vary across three "
                "values per path to stand in for three deploys; dropping the "
                "line-number strip from the listing forks each issue into three.",
    }, indent=2) + "\n")
    print(f"[fingerprint] wrote {out}")

    if KEEP_SCRATCH:
        print(f"[fingerprint] KEEP_SCRATCH set; {SCRATCH_TRUTH} and the two scratch "
              "tables were left in place. Drop them with DROP TABLE when done.")
    else:
        drop_scratch()
        print("[fingerprint] scratch tables dropped; the live store was never touched")


if __name__ == "__main__":
    run()
