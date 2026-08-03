"""Chapter 7 benchmark: per-column compression for the listing 7.1 schema.

Loads N synthetic spans into a scratch table that copies listing 7.1 column for
column, then runs the listing 7.3 system.columns query to report each column's
compressed bytes, raw bytes, and ratio. This is a real measurement against a
running ClickHouse: the rows are generated server side with `FROM numbers(N)` so
no large payload crosses the wire, but the bytes reported are the actual on-disk
column sizes ClickHouse wrote after Delta, T64, LowCardinality dictionary, and
ZSTD encoding.

Why a scratch copy and not tracing.otel_traces itself. The byte counts only
repeat if the generated timestamps are fixed, and a fixed past anchor collides
with listing 7.2's TTL on the real table: ClickHouse reserves space by the TTL
rules at insert time, so rows already past the two-day boundary are written
straight onto the S3 cold disk and never land hot, and rows past the fifteen-day
boundary are dropped on the first merge. The measurement would then be of
S3-resident parts, or of nothing. The scratch table carries the same columns,
codecs, sort key, partitioning and skip index with no TTL and no storage policy,
which is the approach bloom_index_pruning.py and tenant_cardinality_blowup.py
take. It is dropped on exit, so otel_traces is never touched. To run listing 7.3
against the live table instead, apply clickhouse/compression.sql by hand.

What the chapter argues, and what this proves or refutes on your own hardware:
  - service_name / span_name / status_code : these are LowCardinality columns, so
    they collapse to a tiny dictionary and compress hardest of all. service_name
    and span_name also lead the sort key, which groups their runs. status_code is
    not in the sort key.
  - adjusted_count : the sample-rate reciprocal from section 7.4.4 holds one of a
    few values and repeats across every span of a trace, so it compresses far
    harder than the wide columns without being free.
  - timestamp   : it rides mid-key in this schema (the sort key leads with
    service and span, not the clock), so Delta encoding gives only a modest win,
    not the heavy compression a clock-led sort key would earn.
  - trace_id    : 32 lowercase hex characters encode 16 random bytes, so half the
    stored width is the encoding and not the id. Two characters per byte caps the
    ratio at 2x, and it measured 1.96x: the hex being squeezed back out, not any
    general incompressibility. timestamp
    compresses worse. What makes trace_id the cost driver the chapter calls out
    is its size on disk, which is the largest of any column here, not its ratio.

Run (stack must be up):
  python3 compression_ratio.py
  NUM_SPANS=250000 python3 compression_ratio.py
"""
import os
import json
from datetime import datetime, timezone
from pathlib import Path

from chclient import CH, StackNotRunning

NUM_SPANS = int(os.environ.get("NUM_SPANS", "1000000"))
SPANS_PER_TRACE = int(os.environ.get("SPANS_PER_TRACE", "6"))
SCRATCH = "tracing.compression_scratch"

# Fixed clock for the fixture. Generating from now64(9) moved the published byte
# counts three ways: the Delta codec's base value shifted between runs, a run
# that straddled an hour boundary reordered rows inside every sort-key group and
# changed how well trace_id packed, and a run that straddled UTC midnight split
# the load into two partitions that OPTIMIZE FINAL cannot merge. One anchor pins
# all three, so two runs at the same settings report the same bytes.
TS_ANCHOR = "2026-01-01 00:00:00"

# The two claims this benchmark asserts, both from the chapter's own argument.
# trace_id sits near 2x because hex costs two characters per byte: a ratio above
# the band means the generator stopped emitting random ids, so the entropy is no
# longer what is being measured, and a ratio below it means the codec path
# changed under us.
# service_name is the opposite end of the same argument, a low-cardinality column
# leading the sort key. Its ratio climbs with the row count (the dictionary is
# fixed, the raw column grows), so the floor sits well under the 477x measured at
# 200,000 rows and catches the collapse to single digits that losing the
# LowCardinality dictionary, or writing unique service names, would cause.
TRACE_ID_RATIO_MIN = float(os.environ.get("TRACE_ID_RATIO_MIN", "1.5"))
TRACE_ID_RATIO_MAX = float(os.environ.get("TRACE_ID_RATIO_MAX", "3.0"))
SERVICE_NAME_RATIO_MIN = float(os.environ.get("SERVICE_NAME_RATIO_MIN", "50"))

SERVICES = ("checkout-service", "inventory-service", "payment-service",
            "fraud-service", "notification-service")
SPAN_NAMES = ("validate_cart", "inventory.reserve", "payment.charge",
              "fraud.score", "order.create", "notification.send",
              "db.query", "cache.get", "http.request", "grpc.call")

# adjusted_count is the sample-rate reciprocal from section 7.4.4: a span kept at
# 100 percent carries 1.0, a span kept at 1 in 100 carries 100.0. The sampling
# decision is taken per trace, not per span, so the weight is derived from the
# trace ordinal and repeats across that trace's spans. The mix is most traffic
# kept whole, a slice at 1 in 10, a thin tail at 1 in 100.
_ADJUSTED_COUNT = f"""
          multiIf(intDiv(number, {SPANS_PER_TRACE}) % 100 < 80, 1.0,
                  intDiv(number, {SPANS_PER_TRACE}) % 100 < 98, 10.0,
                  100.0)
"""


def _sql_array(values):
    inner = ", ".join(f"'{v}'" for v in values)
    return f"[{inner}]"


def _create_scratch(ch):
    # Listing 7.1 column for column, including the adjusted_count column that
    # section 7.4.4 adds, with the same codecs, sort key, partitioning and skip
    # index. Only the TTL and the storage policy are left out, for the reason in
    # the module docstring.
    ch.execute(f"DROP TABLE IF EXISTS {SCRATCH}")
    ch.execute(f"""
        CREATE TABLE {SCRATCH}
        (
            timestamp      DateTime64(9) CODEC(Delta, ZSTD(1)),
            trace_id       String CODEC(ZSTD(1)),
            span_id        String CODEC(ZSTD(1)),
            service_name   LowCardinality(String) CODEC(ZSTD(1)),
            span_name      LowCardinality(String) CODEC(ZSTD(1)),
            status_code    LowCardinality(String) CODEC(ZSTD(1)),
            duration_ns    UInt64 CODEC(T64, ZSTD(1)),
            adjusted_count Float64 DEFAULT 1.0 CODEC(ZSTD(1)),
            attributes     Map(LowCardinality(String), String) CODEC(ZSTD(3)),
            INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(timestamp)
        ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id)
    """)


def _load(ch):
    services = _sql_array(SERVICES)
    spans = _sql_array(SPAN_NAMES)
    # trace_id repeats SPANS_PER_TRACE times (one trace, several spans); span_id
    # is unique per row; timestamps step a millisecond per row from the anchor so
    # the sorted prefix has small deltas within each granule.
    ch.execute(f"""
        INSERT INTO {SCRATCH}
          (timestamp, trace_id, span_id, service_name, span_name,
           status_code, duration_ns, adjusted_count, attributes)
        SELECT
          toDateTime64('{TS_ANCHOR}', 9) + toIntervalMillisecond(number),
          lower(hex(MD5(toString(intDiv(number, {SPANS_PER_TRACE}))))),
          lower(hex(reinterpretAsFixedString(toUInt64(number)))),
          {services}[(number % {len(SERVICES)}) + 1],
          {spans}[(number % {len(SPAN_NAMES)}) + 1],
          if(number % 20 = 0, 'STATUS_CODE_ERROR', 'STATUS_CODE_OK'),
          toUInt64(1000000 + (number * 2654435761) % 200000000),
          {_ADJUSTED_COUNT},
          map('http.method', ['GET', 'POST', 'PUT'][(number % 3) + 1],
              'k8s.pod.name', concat('pod-', toString(number % 32)))
        FROM numbers({NUM_SPANS})
    """)
    ch.execute(f"OPTIMIZE TABLE {SCRATCH} FINAL")


def _measure(ch):
    rows = ch.query(f"""
        SELECT name,
               sum(data_compressed_bytes),
               sum(data_uncompressed_bytes),
               round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2)
        FROM system.columns
        WHERE database = 'tracing' AND table = 'compression_scratch'
        GROUP BY name
        ORDER BY sum(data_uncompressed_bytes) DESC
    """)
    return [{"column": name, "stored_bytes": int(comp), "raw_bytes": int(raw),
             "ratio": float(ratio)} for name, comp, raw, ratio in rows]


def _assert_claims(columns):
    # Under roughly 100,000 rows ClickHouse keeps the whole part in Compact
    # format, one file for every column, and system.columns then reports no
    # per-column bytes at all. Every ratio comes back nan. Catch that here
    # rather than write a result file full of nan.
    if any(c["raw_bytes"] == 0 for c in columns):
        raise SystemExit(
            f"[compression] system.columns reports no per-column bytes for "
            f"{NUM_SPANS:,} rows: ClickHouse stored the fixture as a Compact "
            f"part, which accounts for all columns together. Raise NUM_SPANS.")

    by_column = {c["column"]: c["ratio"] for c in columns}
    trace_id = by_column["trace_id"]
    service_name = by_column["service_name"]

    if not TRACE_ID_RATIO_MIN <= trace_id <= TRACE_ID_RATIO_MAX:
        raise SystemExit(
            f"[compression] trace_id compressed {trace_id}x, outside the "
            f"{TRACE_ID_RATIO_MIN}x to {TRACE_ID_RATIO_MAX}x band this benchmark "
            f"asserts around the incompressible floor the chapter claims for a "
            f"random identifier")
    if service_name < SERVICE_NAME_RATIO_MIN:
        raise SystemExit(
            f"[compression] service_name compressed {service_name}x, under the "
            f"{SERVICE_NAME_RATIO_MIN}x floor this benchmark asserts for a "
            f"low-cardinality column leading the sort key")

    print(f"[compression] PASS: trace_id {trace_id}x is the hex coming back out, "
          f"service_name {service_name}x collapses to a dictionary")


def run():
    ch = CH()
    print(f"[compression] transport={ch.transport} num_spans={NUM_SPANS:,} "
          f"spans_per_trace={SPANS_PER_TRACE} anchor={TS_ANCHOR}Z")

    _create_scratch(ch)
    try:
        _load(ch)
        total_rows = int(ch.scalar(f"SELECT count() FROM {SCRATCH}"))
        columns = _measure(ch)

        print(f"[compression] measured over {total_rows:,} rows")
        print(f"{'column':<16}{'stored':>14}{'raw':>16}{'ratio':>9}")
        for c in columns:
            print(f"{c['column']:<16}{c['stored_bytes']:>14,}"
                  f"{c['raw_bytes']:>16,}{c['ratio']:>8.2f}x")

        _assert_claims(columns)
    finally:
        ch.execute(f"DROP TABLE IF EXISTS {SCRATCH}")

    stamp = datetime.now(timezone.utc)
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"compression-ratio-{stamp.strftime('%Y-%m-%dT%H%M%S')}.json"
    out.write_text(json.dumps({
        "benchmark": "compression_ratio",
        "measured_at_utc": stamp.isoformat(),
        "transport": ch.transport,
        "num_spans_loaded": NUM_SPANS,
        "spans_per_trace": SPANS_PER_TRACE,
        "total_rows_measured": total_rows,
        "timestamp_anchor_utc": TS_ANCHOR,
        "columns": columns,
        "note": (
            "Measured on a scratch copy of the listing 7.1 schema, same columns "
            "and codecs, loaded from a fixed row generator and a fixed timestamp "
            "anchor, so two runs at the same row count report the same bytes. "
            "The copy carries no TTL: on the real table listing 7.2 would write "
            "fixed-anchor rows straight to the cold volume, and the numbers "
            "would describe S3-resident parts. Every column here is written by "
            "the generator, so none of the ratios is the compressibility of a "
            "column left at its default."
        ),
    }, indent=2) + "\n")
    print(f"[compression] wrote {out}")


if __name__ == "__main__":
    try:
        run()
    except StackNotRunning as exc:
        # The guidance is the whole message; a traceback through chclient
        # only buries it.
        raise SystemExit(str(exc))
