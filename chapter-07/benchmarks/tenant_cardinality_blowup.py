"""Chapter 7 benchmark: a noisy tenant's high-cardinality attribute swells the
shared attributes column (section 7.5.2).

The listing 7.1 table stores span attributes in one shared column,
`attributes Map(LowCardinality(String), String) CODEC(ZSTD(3))`. When every
tenant writes the same handful of stable keys (http.method, a pod name, a tenant
id), the values repeat heavily and ZSTD folds them to a small dictionary, so the
column compresses well. The risk section 7.5.2 calls out is that the column is
shared: a single tenant that attaches a unique-per-span identifier (a request id,
a session id, a cursor) fills the Map values with strings that never repeat.
ZSTD has nothing to fold, the column's ratio falls toward the incompressible
floor, and the on-disk footprint of the shared column swells for everyone.

This script measures that swing directly on a scratch table that copies the
listing 7.1 schema and codecs exactly (same Map type, same CODEC(ZSTD(3)) on
attributes, same ORDER BY and partitioning). It loads two populations of the
same size into the scratch table and measures the attributes column each time:

  - baseline: every span carries only stable low-cardinality attribute keys,
  - blowup:  one tenant of TENANTS (default one in four spans) additionally
             attaches a unique-per-span request id into the same Map.

It reads the attributes column's compressed bytes, uncompressed bytes, ratio,
and active part count from system.columns and system.parts after each load, then
reports the delta: how far the ratio collapses and how much the shared column
grows once the noisy tenant arrives. The size of that delta is a property of this
seed (row count, tenant share, id width), not a universal number, so the script
asserts the direction of the mechanism, that the ratio falls and the column
grows, and prints the magnitude rather than asserting it.

Both loads run from a fixed timestamp anchor, so two runs at the same settings
report the same bytes. The scratch table is dropped on exit, so otel_traces is
never touched.

Run (stack must be up):
  python3 tenant_cardinality_blowup.py
  NUM_SPANS=500000 TENANTS=4 python3 tenant_cardinality_blowup.py
"""
import os
import json
from datetime import datetime, timezone
from pathlib import Path

from chclient import CH

NUM_SPANS = int(os.environ.get("NUM_SPANS", "1000000"))
SPANS_PER_TRACE = int(os.environ.get("SPANS_PER_TRACE", "6"))
TENANTS = int(os.environ.get("TENANTS", "4"))
SCRATCH = "tracing.attr_cardinality_scratch"

# Fixed clock for both loads. Generating from now64(9) moved the reported bytes
# three ways: the Delta codec's base value shifted between runs, a load that
# straddled an hour boundary reordered rows inside every sort-key group, and a
# load that straddled UTC midnight split into two partitions that OPTIMIZE FINAL
# cannot merge. One anchor pins all three, and it also keeps the two phases
# comparable, since only the attributes expression differs between them.
TS_ANCHOR = "2026-01-01 00:00:00"

SERVICES = ("checkout-service", "inventory-service", "payment-service",
            "fraud-service", "notification-service")
SPAN_NAMES = ("validate_cart", "inventory.reserve", "payment.charge",
              "fraud.score", "order.create", "notification.send",
              "db.query", "cache.get", "http.request", "grpc.call")


def _sql_array(values):
    inner = ", ".join(f"'{v}'" for v in values)
    return f"[{inner}]"


def _create_scratch(ch):
    # Same schema and codecs as listing 7.1's otel_traces: the attributes column
    # is Map(LowCardinality(String), String) CODEC(ZSTD(3)), the sort key leads
    # with the low-cardinality columns, trace_id rides last. No TTL or storage
    # policy: the scratch table is ephemeral and dropped on exit.
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
            attributes     Map(LowCardinality(String), String) CODEC(ZSTD(3)),
            INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(timestamp)
        ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id)
    """)


# The columns that never change between the two loads. Only the attributes Map
# differs, so the delta isolates the noisy-tenant effect on that one column.
_COMMON_SELECT = f"""
      toDateTime64('{TS_ANCHOR}', 9) + toIntervalMillisecond(number),
      lower(hex(MD5(toString(intDiv(number, {SPANS_PER_TRACE}))))),
      lower(hex(reinterpretAsFixedString(toUInt64(number)))),
      {{services}}[(number % {len(SERVICES)}) + 1],
      {{spans}}[(number % {len(SPAN_NAMES)}) + 1],
      if(number % 20 = 0, 'STATUS_CODE_ERROR', 'STATUS_CODE_OK'),
      toUInt64(1000000 + (number * 2654435761) % 200000000),
"""


def _load(ch, attributes_expr):
    services, spans = _sql_array(SERVICES), _sql_array(SPAN_NAMES)
    select = _COMMON_SELECT.format(services=services, spans=spans)
    ch.execute(f"""
        INSERT INTO {SCRATCH}
          (timestamp, trace_id, span_id, service_name, span_name,
           status_code, duration_ns, attributes)
        SELECT
          {select}
          {attributes_expr}
        FROM numbers({NUM_SPANS})
    """)
    ch.execute(f"OPTIMIZE TABLE {SCRATCH} FINAL")


# Baseline: only stable low-cardinality keys. http.method is one of three values,
# k8s.pod.name is one of 32, tenant.id is one of TENANTS. Every value repeats, so
# ZSTD compresses the Map values hard.
_BASELINE_ATTRS = f"""
      map('http.method', ['GET', 'POST', 'PUT'][(number % 3) + 1],
          'k8s.pod.name', concat('pod-', toString(number % 32)),
          'tenant.id', concat('tenant-', toString(number % {TENANTS})))
"""

# Blowup: one tenant of TENANTS (number % TENANTS = 0) attaches request.uid, a
# unique-per-span id, to the same shared Map. The other tenants are unchanged, so
# only a fraction of rows inject unique values, yet the shared column carries them
# for everyone.
_BLOWUP_ATTRS = f"""
      if(number % {TENANTS} = 0,
         map('http.method', ['GET', 'POST', 'PUT'][(number % 3) + 1],
             'k8s.pod.name', concat('pod-', toString(number % 32)),
             'tenant.id', concat('tenant-', toString(number % {TENANTS})),
             'request.uid', lower(hex(reinterpretAsFixedString(toUInt64(number))))),
         map('http.method', ['GET', 'POST', 'PUT'][(number % 3) + 1],
             'k8s.pod.name', concat('pod-', toString(number % 32)),
             'tenant.id', concat('tenant-', toString(number % {TENANTS}))))
"""


def _measure_attributes(ch):
    comp, raw, ratio = ch.query(f"""
        SELECT sum(data_compressed_bytes),
               sum(data_uncompressed_bytes),
               round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2)
        FROM system.columns
        WHERE database = 'tracing' AND table = 'attr_cardinality_scratch'
          AND name = 'attributes'
    """)[0]
    parts = ch.scalar(f"""
        SELECT count()
        FROM system.parts
        WHERE database = 'tracing' AND table = 'attr_cardinality_scratch' AND active
    """)
    rows = ch.scalar(f"SELECT count() FROM {SCRATCH}")
    return {
        "stored_bytes": int(comp),
        "raw_bytes": int(raw),
        "ratio": float(ratio),
        "active_parts": int(parts),
        "rows": int(rows),
    }


def _phase(ch, label, attributes_expr):
    ch.execute(f"TRUNCATE TABLE {SCRATCH}")
    _load(ch, attributes_expr)
    m = _measure_attributes(ch)
    print(f"{label:<10}{m['stored_bytes']:>16,}{m['raw_bytes']:>18,}"
          f"{m['ratio']:>9.2f}x{m['active_parts']:>8}")
    return m


def _assert_direction(baseline, blowup):
    """The mechanism, not its size.

    How far the ratio falls depends on the seed, so asserting a magnitude would
    be asserting the fixture. What section 7.5.2 claims, and what has to hold on
    any seed, is the direction: unique-per-span values in the shared Map cost
    compression and cost bytes.
    """
    if blowup["ratio"] >= baseline["ratio"]:
        raise SystemExit(
            f"[cardinality] the noisy tenant did not cost compression: "
            f"attributes went {baseline['ratio']:.2f}x -> {blowup['ratio']:.2f}x")
    if blowup["stored_bytes"] <= baseline["stored_bytes"]:
        raise SystemExit(
            f"[cardinality] the noisy tenant did not cost bytes: the shared "
            f"column went {baseline['stored_bytes']:,} -> "
            f"{blowup['stored_bytes']:,} bytes")


def run():
    ch = CH()
    print(f"[cardinality] transport={ch.transport} num_spans={NUM_SPANS:,} "
          f"tenants={TENANTS} (noisy tenant is 1 in {TENANTS} spans) "
          f"anchor={TS_ANCHOR}Z")
    _create_scratch(ch)

    try:
        print(f"[cardinality] attributes column, same {NUM_SPANS:,} rows each phase")
        print(f"{'phase':<10}{'stored':>16}{'raw':>18}{'ratio':>10}{'parts':>8}")
        baseline = _phase(ch, "baseline", _BASELINE_ATTRS)
        blowup = _phase(ch, "blowup", _BLOWUP_ATTRS)

        ratio_drop = round(baseline["ratio"] - blowup["ratio"], 2)
        growth_x = round(blowup["stored_bytes"] / baseline["stored_bytes"], 2) \
            if baseline["stored_bytes"] else None
        added = blowup["stored_bytes"] - baseline["stored_bytes"]
        print(f"[cardinality] attributes ratio fell {baseline['ratio']:.2f}x -> "
              f"{blowup['ratio']:.2f}x (drop {ratio_drop}), stored grew "
              f"{growth_x}x (+{added:,} bytes) once one tenant injected unique ids")
        print("[cardinality] the drop and growth are properties of this seed, not "
              "a universal ratio")

        _assert_direction(baseline, blowup)
        print("[cardinality] PASS: the shared column lost compression and gained "
              "bytes from one tenant's unique ids")
    finally:
        ch.execute(f"DROP TABLE IF EXISTS {SCRATCH}")

    stamp = datetime.now(timezone.utc)
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"tenant-cardinality-{stamp.strftime('%Y-%m-%dT%H%M%S')}.json"
    out.write_text(json.dumps({
        "benchmark": "tenant_cardinality_blowup",
        "measured_at_utc": stamp.isoformat(),
        "transport": ch.transport,
        "num_spans_per_phase": NUM_SPANS,
        "spans_per_trace": SPANS_PER_TRACE,
        "tenants": TENANTS,
        "column": "attributes",
        "baseline": baseline,
        "blowup": blowup,
        "delta": {
            "ratio_drop": ratio_drop,
            "stored_bytes_growth_x": growth_x,
            "stored_bytes_added": added,
        },
    }, indent=2) + "\n")
    print(f"[cardinality] wrote {out}")


if __name__ == "__main__":
    run()
