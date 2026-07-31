"""Chapter 7 benchmark: the bloom-filter skip index prunes granules.

The listing 7.1 table sorts by (service_name, span_name, hour, trace_id), so a
point lookup by trace_id gets no help from the primary key: trace_id rides last.
Without a skip index, ClickHouse must consider every granule. The bloom_filter
index on trace_id answers "this granule cannot hold trace X" so the planner
reads only the granules that might.

This script measures that gap directly. It runs `EXPLAIN indexes = 1` for a real
trace_id twice:
  - with skip indexes off (SETTINGS use_skip_indexes = 0): the PrimaryKey stage
    reports the total granules the query would scan,
  - with skip indexes on: the Skip stage reports how many granules survive the
    bloom filter.
It parses the `Granules: selected/total` lines from each and asserts the bloom
index selects strictly fewer granules than the full scan. The point lookup only
demonstrates pruning when the table spans several granules, so load enough rows
first (default one million, about 120 granules at the 8192 default).

Run (stack must be up):
  python bloom_index_pruning.py
  NUM_SPANS=1000000 python bloom_index_pruning.py
"""
import os
import re
import json
from datetime import datetime, timezone
from pathlib import Path

from chclient import CH

NUM_SPANS = int(os.environ.get("NUM_SPANS", "1000000"))
SPANS_PER_TRACE = int(os.environ.get("SPANS_PER_TRACE", "6"))

SERVICES = ("checkout-service", "inventory-service", "payment-service",
            "fraud-service", "notification-service")
SPAN_NAMES = ("validate_cart", "inventory.reserve", "payment.charge",
              "fraud.score", "order.create", "notification.send",
              "db.query", "cache.get", "http.request", "grpc.call")

GRANULE_RE = re.compile(r"Granules:\s*(\d+)\s*/\s*(\d+)")


def _sql_array(values):
    return "[" + ", ".join(f"'{v}'" for v in values) + "]"


def _ensure_rows(ch):
    have = int(ch.scalar("SELECT count() FROM tracing.otel_traces"))
    if have >= NUM_SPANS:
        print(f"[bloom] table already holds {have:,} rows, reusing")
        return
    services, spans = _sql_array(SERVICES), _sql_array(SPAN_NAMES)
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
          map('http.method', ['GET', 'POST', 'PUT'][(number % 3) + 1])
        FROM numbers({NUM_SPANS})
    """)
    ch.execute("OPTIMIZE TABLE tracing.otel_traces FINAL")


def _explain_granules(ch, trace_id, use_skip):
    setting = 1 if use_skip else 0
    text = "\n".join(
        "\t".join(r) for r in ch.query(
            f"EXPLAIN indexes = 1 "
            f"SELECT count() FROM tracing.otel_traces "
            f"WHERE trace_id = '{trace_id}' "
            f"SETTINGS use_skip_indexes = {setting}"
        )
    )
    pairs = [(int(m.group(1)), int(m.group(2))) for m in GRANULE_RE.finditer(text)]
    # The last Granules line is the granule count after the final stage
    # (the skip index when it is on, the primary key when it is off).
    selected, total = pairs[-1] if pairs else (None, None)
    return selected, total, text


def run():
    ch = CH()
    print(f"[bloom] transport={ch.transport} target_rows={NUM_SPANS:,}")
    _ensure_rows(ch)

    trace_id = ch.scalar(
        "SELECT trace_id FROM tracing.otel_traces "
        "ORDER BY timestamp DESC LIMIT 1"
    )
    print(f"[bloom] probe trace_id={trace_id}")

    without_sel, without_tot, without_txt = _explain_granules(ch, trace_id, use_skip=False)
    with_sel, with_tot, with_txt = _explain_granules(ch, trace_id, use_skip=True)

    print(f"[bloom] without skip index : {without_sel}/{without_tot} granules scanned")
    print(f"[bloom] with bloom index   : {with_sel}/{with_tot} granules scanned")

    if with_sel is None or without_sel is None:
        raise SystemExit("[bloom] could not parse Granules line from EXPLAIN; "
                         "load more rows so the table spans multiple granules")
    pruned = with_sel < without_sel
    if not pruned:
        raise SystemExit(
            f"[bloom] no pruning: bloom index selected {with_sel} of "
            f"{without_sel} scanned granules. Load more rows (NUM_SPANS) so the "
            f"table spans several granules, or the probe trace sits in every one."
        )
    print(f"[bloom] PASS: bloom index pruned {without_sel - with_sel} of "
          f"{without_sel} granules ({with_sel} survived)")

    stamp = datetime.now(timezone.utc)
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"bloom-index-{stamp.strftime('%Y-%m-%d')}.json"
    out.write_text(json.dumps({
        "benchmark": "bloom_index_pruning",
        "measured_at_utc": stamp.isoformat(),
        "transport": ch.transport,
        "probe_trace_id": trace_id,
        "granules_without_skip_index": {"selected": without_sel, "total": without_tot},
        "granules_with_bloom_index": {"selected": with_sel, "total": with_tot},
        "granules_pruned": without_sel - with_sel,
    }, indent=2) + "\n")
    print(f"[bloom] wrote {out}")


if __name__ == "__main__":
    run()
