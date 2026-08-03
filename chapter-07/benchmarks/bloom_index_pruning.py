"""Chapter 7 benchmark: the bloom-filter skip index prunes granules.

The listing 7.1 table sorts by (service_name, span_name, hour, trace_id), so a
point lookup by trace_id gets no help from the primary key. trace_id rides last,
and once the table holds more than a handful of hours every granule straddles
several sort-key groups, so no granule can be excluded on its key range alone.
ClickHouse has to consider all of them. The bloom_filter index on trace_id
answers "this granule cannot hold trace X" and the planner reads only the
granules that might.

The measurement runs against a scratch table that copies listing 7.1's schema,
codecs, sort key and skip index exactly, the same approach
tenant_cardinality_blowup.py takes, and drops it on exit so otel_traces is never
touched. The scratch table is rebuilt on every run from a fixed row generator
and a fixed timestamp anchor, so the part layout, the granule boundaries and the
probe trace ids are identical on every run and on every machine.

That fixture is the point. An earlier version probed otel_traces for its newest
trace_id, which meant the probe followed whatever the collector had just
written, and the granule counts also moved with whichever other benchmark had
loaded the table first. `EXPLAIN indexes = 1` is analysis time, so with fixed
data and a fixed probe the answer is exact and repeats.

For each fixed probe it runs `EXPLAIN indexes = 1` twice:
  - with skip indexes off (SETTINGS use_skip_indexes = 0): the PrimaryKey stage
    reports the granules the query would scan without the index,
  - with skip indexes on: the Skip stage reports how many granules survive the
    bloom filter.

Run (stack must be up):
  python3 bloom_index_pruning.py
  NUM_SPANS=2000000 python3 bloom_index_pruning.py
"""
import os
import re
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path

from chclient import CH

NUM_SPANS = int(os.environ.get("NUM_SPANS", "1000000"))
SPANS_PER_TRACE = int(os.environ.get("SPANS_PER_TRACE", "6"))
PROBES = int(os.environ.get("PROBES", "5"))
SCRATCH = "tracing.bloom_probe_scratch"

# The claim under test: a point lookup reads a small slice of the table. Fail if
# the bloom index stops holding the median probe under this share of granules.
# Measured at 0.05 on the committed fixture, so a run has to lose most of the
# index's effect before this trips.
MAX_GRANULE_FRACTION = float(os.environ.get("MAX_GRANULE_FRACTION", "0.20"))

# Fixed clock for the fixture. The sort key hashes the hour, so generating rows
# from now() would reshuffle granule boundaries between runs and move the
# published counts. The rows spread across a full day at any NUM_SPANS, which is
# what leaves the primary key nothing to prune.
TS_ANCHOR = "2026-01-01 00:00:00"
SPREAD_MS = 86_400_000

SERVICES = ("checkout-service", "inventory-service", "payment-service",
            "fraud-service", "notification-service")
SPAN_NAMES = ("validate_cart", "inventory.reserve", "payment.charge",
              "fraud.score", "order.create", "notification.send",
              "db.query", "cache.get", "http.request", "grpc.call")

GRANULE_RE = re.compile(r"Granules:\s*(\d+)\s*/\s*(\d+)")


def _sql_array(values):
    return "[" + ", ".join(f"'{v}'" for v in values) + "]"


def _build_fixture(ch):
    # Same schema, codecs, sort key and skip index as listing 7.1. No TTL and no
    # storage policy: the scratch table is ephemeral and never tiers.
    ch.execute(f"DROP TABLE IF EXISTS {SCRATCH}")
    ch.execute(f"""
        CREATE TABLE {SCRATCH}
        (
          timestamp     DateTime64(9)          CODEC(Delta, ZSTD(1)),
          trace_id      String                 CODEC(ZSTD(1)),
          span_id       String                 CODEC(ZSTD(1)),
          service_name  LowCardinality(String) CODEC(ZSTD(1)),
          span_name     LowCardinality(String) CODEC(ZSTD(1)),
          status_code   LowCardinality(String) CODEC(ZSTD(1)),
          duration_ns   UInt64                 CODEC(T64, ZSTD(1)),
          attributes    Map(LowCardinality(String), String) CODEC(ZSTD(3)),
          INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(timestamp)
        ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id)
    """)
    services, spans = _sql_array(SERVICES), _sql_array(SPAN_NAMES)
    step_ms = SPREAD_MS // NUM_SPANS
    ch.execute(f"""
        INSERT INTO {SCRATCH}
          (timestamp, trace_id, span_id, service_name, span_name,
           status_code, duration_ns, attributes)
        SELECT
          toDateTime64('{TS_ANCHOR}', 9) + toIntervalMillisecond(number * {step_ms}),
          lower(hex(MD5(toString(intDiv(number, {SPANS_PER_TRACE}))))),
          lower(hex(reinterpretAsFixedString(toUInt64(number)))),
          {services}[(number % {len(SERVICES)}) + 1],
          {spans}[(number % {len(SPAN_NAMES)}) + 1],
          if(number % 20 = 0, 'STATUS_CODE_ERROR', 'STATUS_CODE_OK'),
          toUInt64(1000000 + (number * 2654435761) % 200000000),
          map('http.method', ['GET', 'POST', 'PUT'][(number % 3) + 1])
        FROM numbers({NUM_SPANS})
    """)
    ch.execute(f"OPTIMIZE TABLE {SCRATCH} FINAL")


def _probe_trace_ids(ch):
    """Trace ids at fixed, evenly spaced ordinals in the generated set.

    The loader hashes the trace ordinal, so asking ClickHouse for the same hash
    keeps the probe tied to the generator rather than to a second MD5
    implementation that could disagree about encoding.
    """
    total_traces = NUM_SPANS // SPANS_PER_TRACE
    ordinals = [(k * total_traces) // (PROBES + 1) for k in range(1, PROBES + 1)]
    return [(o, ch.scalar(f"SELECT lower(hex(MD5(toString({o}))))")) for o in ordinals]


def _explain_granules(ch, trace_id, use_skip):
    setting = 1 if use_skip else 0
    text = "\n".join(
        "\t".join(r) for r in ch.query(
            f"EXPLAIN indexes = 1 "
            f"SELECT count() FROM {SCRATCH} "
            f"WHERE trace_id = '{trace_id}' "
            f"SETTINGS use_skip_indexes = {setting}"
        )
    )
    pairs = [(int(m.group(1)), int(m.group(2))) for m in GRANULE_RE.finditer(text)]
    # The last Granules line is the granule count after the final stage
    # (the skip index when it is on, the primary key when it is off).
    return pairs[-1] if pairs else (None, None)


def _spread(samples):
    return {"median": int(statistics.median(samples)),
            "min": min(samples), "max": max(samples)}


def run():
    ch = CH()
    print(f"[bloom] transport={ch.transport} rows={NUM_SPANS:,} probes={PROBES}")
    _build_fixture(ch)

    try:
        granules_total = int(ch.scalar(
            f"SELECT sum(marks) - count() FROM system.parts "
            f"WHERE database = 'tracing' AND table = 'bloom_probe_scratch' AND active"))
        print(f"[bloom] fixture built: {granules_total} granules across the table")

        without, with_bloom = [], []
        for ordinal, trace_id in _probe_trace_ids(ch):
            spans = int(ch.scalar(f"SELECT count() FROM {SCRATCH} "
                                  f"WHERE trace_id = '{trace_id}'"))
            if spans != SPANS_PER_TRACE:
                raise SystemExit(
                    f"[bloom] probe trace {trace_id} holds {spans} spans, expected "
                    f"{SPANS_PER_TRACE}; the fixture did not load as generated")

            without_sel, _ = _explain_granules(ch, trace_id, use_skip=False)
            with_sel, _ = _explain_granules(ch, trace_id, use_skip=True)
            if without_sel is None or with_sel is None:
                raise SystemExit("[bloom] could not parse the Granules line from EXPLAIN; "
                                 "load more rows so the table spans multiple granules")
            if with_sel >= without_sel:
                raise SystemExit(
                    f"[bloom] no pruning for trace {trace_id}: the bloom index "
                    f"selected {with_sel} of {without_sel} scanned granules")

            without.append(without_sel)
            with_bloom.append(with_sel)
            print(f"[bloom] trace #{ordinal:<7} {trace_id}  "
                  f"without index {without_sel:>4} granules, with index {with_sel:>4}")

        without_s, with_s = _spread(without), _spread(with_bloom)
        fraction = round(with_s["median"] / granules_total, 4)
        print(f"[bloom] without skip index : median {without_s['median']} granules "
              f"(min {without_s['min']}, max {without_s['max']})")
        print(f"[bloom] with bloom index   : median {with_s['median']} granules "
              f"(min {with_s['min']}, max {with_s['max']})")

        if fraction > MAX_GRANULE_FRACTION:
            raise SystemExit(
                f"[bloom] pruning regressed: the median probe still reads "
                f"{with_s['median']} of {granules_total} granules ({fraction:.2%}), "
                f"over the {MAX_GRANULE_FRACTION:.0%} ceiling this benchmark asserts")

        print(f"[bloom] PASS: a point lookup reads {with_s['median']} of "
              f"{granules_total} granules ({fraction:.2%}), down from "
              f"{without_s['median']} without the index")
    finally:
        ch.execute(f"DROP TABLE IF EXISTS {SCRATCH}")

    stamp = datetime.now(timezone.utc)
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"bloom-index-{stamp.strftime('%Y-%m-%dT%H%M%S')}.json"
    out.write_text(json.dumps({
        "benchmark": "bloom_index_pruning",
        "measured_at_utc": stamp.isoformat(),
        "transport": ch.transport,
        "num_spans": NUM_SPANS,
        "spans_per_trace": SPANS_PER_TRACE,
        "probe_count": PROBES,
        "granules_total": granules_total,
        "granules_without_skip_index": without_s,
        "granules_with_bloom_index": with_s,
        "granules_pruned_median": without_s["median"] - with_s["median"],
        "granule_fraction_read_median": fraction,
        "note": (
            "Measured on a scratch copy of the listing 7.1 schema built from a "
            "fixed row generator and a fixed timestamp anchor, so two runs on "
            "the same ClickHouse version return the same counts. The spread "
            "across the probes is real: how many granules survive depends on "
            "where a given trace's spans land. What changes the numbers is the "
            "row count, the index granularity, or a ClickHouse version that "
            "plans differently."
        ),
    }, indent=2) + "\n")
    print(f"[bloom] wrote {out}")


if __name__ == "__main__":
    run()
