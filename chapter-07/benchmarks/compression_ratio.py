"""Chapter 7 benchmark: per-column compression on the listing 7.1 table.

Loads N synthetic spans into tracing.otel_traces, then runs the listing 7.3
system.columns query to report each column's compressed bytes, raw bytes, and
ratio. This is a real measurement against a running ClickHouse: the rows are
generated server side with `FROM numbers(N)` so no large payload crosses the
wire, but the bytes reported are the actual on-disk column sizes ClickHouse
wrote after Delta, T64, LowCardinality dictionary, and ZSTD encoding.

What the chapter argues, and what this proves or refutes on your own hardware:
  - service_name / span_name / status_code : these low-cardinality columns lead
    the sort key, so they collapse to a tiny LowCardinality dictionary and
    compress hardest of all.
  - timestamp   : it rides mid-key in this schema (the sort key leads with
    service and span, not the clock), so Delta encoding gives only a modest win,
    not the heavy compression a clock-led sort key would earn.
  - trace_id    : a random 32-char id carries almost no redundancy, so it sits
    near the incompressible floor and dominates the on-disk footprint. This is
    the cost driver the chapter calls out.

By default the table is TRUNCATEd first so the reported ratios describe exactly
the N rows this script loaded. Set KEEP_EXISTING=1 to measure the table as-is.

Run (stack must be up):
  python compression_ratio.py
  NUM_SPANS=250000 python compression_ratio.py
"""
import os
import json
from datetime import datetime, timezone
from pathlib import Path

from chclient import CH

NUM_SPANS = int(os.environ.get("NUM_SPANS", "1000000"))
SPANS_PER_TRACE = int(os.environ.get("SPANS_PER_TRACE", "6"))
KEEP_EXISTING = os.environ.get("KEEP_EXISTING", "0") == "1"

SERVICES = ("checkout-service", "inventory-service", "payment-service",
            "fraud-service", "notification-service")
SPAN_NAMES = ("validate_cart", "inventory.reserve", "payment.charge",
              "fraud.score", "order.create", "notification.send",
              "db.query", "cache.get", "http.request", "grpc.call")


def _sql_array(values):
    inner = ", ".join(f"'{v}'" for v in values)
    return f"[{inner}]"


def _load(ch):
    services = _sql_array(SERVICES)
    spans = _sql_array(SPAN_NAMES)
    # trace_id repeats SPANS_PER_TRACE times (one trace, several spans); span_id
    # is unique per row; timestamps spread across a day so the sorted prefix has
    # small deltas within each granule.
    ch.execute(f"""
        INSERT INTO tracing.otel_traces
          (timestamp, trace_id, span_id, service_name, span_name,
           status_code, duration_ns, attributes)
        SELECT
          now64(9) - toIntervalMillisecond({NUM_SPANS} - number),
          lower(hex(MD5(toString(intDiv(number, {SPANS_PER_TRACE}))))),
          lower(hex(reinterpretAsFixedString(toUInt64(number)))),
          {services}[(number % {len(SERVICES)}) + 1],
          {spans}[(number % {len(SPAN_NAMES)}) + 1],
          if(number % 20 = 0, 'STATUS_CODE_ERROR', 'STATUS_CODE_OK'),
          toUInt64(1000000 + (number * 2654435761) % 200000000),
          map('http.method', ['GET', 'POST', 'PUT'][(number % 3) + 1],
              'k8s.pod.name', concat('pod-', toString(number % 32)))
        FROM numbers({NUM_SPANS})
    """)
    ch.execute("OPTIMIZE TABLE tracing.otel_traces FINAL")


def run():
    ch = CH()
    print(f"[compression] transport={ch.transport} num_spans={NUM_SPANS:,} "
          f"spans_per_trace={SPANS_PER_TRACE} keep_existing={KEEP_EXISTING}")

    if not KEEP_EXISTING:
        ch.execute("TRUNCATE TABLE tracing.otel_traces")
    _load(ch)

    total_rows = int(ch.scalar("SELECT count() FROM tracing.otel_traces"))
    rows = ch.query("""
        SELECT name,
               sum(data_compressed_bytes),
               sum(data_uncompressed_bytes),
               round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2)
        FROM system.columns
        WHERE database = 'tracing' AND table = 'otel_traces'
        GROUP BY name
        ORDER BY sum(data_uncompressed_bytes) DESC
    """)

    columns = []
    print(f"[compression] measured over {total_rows:,} rows")
    print(f"{'column':<16}{'stored':>14}{'raw':>16}{'ratio':>9}")
    for name, comp, raw, ratio in rows:
        comp_i, raw_i = int(comp), int(raw)
        columns.append({
            "column": name,
            "stored_bytes": comp_i,
            "raw_bytes": raw_i,
            "ratio": float(ratio),
        })
        print(f"{name:<16}{comp_i:>14,}{raw_i:>16,}{float(ratio):>8.2f}x")

    stamp = datetime.now(timezone.utc)
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"compression-ratio-{stamp.strftime('%Y-%m-%d')}.json"
    out.write_text(json.dumps({
        "benchmark": "compression_ratio",
        "measured_at_utc": stamp.isoformat(),
        "transport": ch.transport,
        "num_spans_loaded": NUM_SPANS,
        "spans_per_trace": SPANS_PER_TRACE,
        "total_rows_measured": total_rows,
        "columns": columns,
    }, indent=2) + "\n")
    print(f"[compression] wrote {out}")


if __name__ == "__main__":
    run()
