"""
Chapter 5: Figure 5.4 demonstration. Scatter-gather query against ClickHouse.

Given a trace_id, query each ClickHouse shard in parallel, gather the
matching spans, and assemble a parent-child waterfall in memory. In a
single-shard local dev stack the "scatter" collapses to one query; the code
still walks each step so the reader can see where fan-out, gather, and
in-memory assembly happen in a production multi-shard deployment.

Usage:
    python scatter_gather_query.py <trace_id_hex>

The trace_id can be copied from the Jaeger UI or pulled from ClickHouse:
    docker compose exec clickhouse clickhouse-client --query \
      "SELECT trace_id FROM tracing.otel_traces LIMIT 1"
"""

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
SHARDS = os.environ.get("CLICKHOUSE_SHARDS", CLICKHOUSE_HOST).split(",")
LOOKBACK = os.environ.get("LOOKBACK", "24 HOUR")

QUERY = """
    SELECT
        span_id,
        parent_span_id,
        service_name,
        span_name,
        toUnixTimestamp64Nano(timestamp) AS start_ns,
        duration,
        status_code
    FROM tracing.otel_traces
    WHERE trace_id = %(trace_id)s
      AND timestamp >= now() - INTERVAL {lookback}
    ORDER BY start_ns ASC
    SETTINGS optimize_read_in_order = 1
""".format(lookback=LOOKBACK)


def _query_shard(host: str, trace_id: str) -> list:
    from clickhouse_driver import Client
    client = Client(host=host, port=CLICKHOUSE_PORT, database="tracing")
    start = time.perf_counter()
    rows = client.execute(QUERY, {"trace_id": trace_id})
    elapsed_ms = (time.perf_counter() - start) * 1000
    print(f"  shard={host} returned={len(rows)} elapsed_ms={elapsed_ms:.1f}")
    return rows


def _assemble_waterfall(rows: list) -> dict:
    by_id = {r[0]: r for r in rows}
    children: dict = {}
    root = None
    for r in rows:
        span_id, parent_id = r[0], r[1]
        children.setdefault(parent_id, []).append(span_id)
        if not parent_id and root is None:
            root = span_id
    return {"root": root, "by_id": by_id, "children": children}


def _print_waterfall(node: str, tree: dict, indent: int = 0, root_start: int = 0):
    r = tree["by_id"].get(node)
    if not r:
        return
    span_id, _parent_id, service, name, start_ns, duration_ns, status = r
    offset_us = (start_ns - root_start) / 1e3 if root_start else 0
    dur_ms = duration_ns / 1e6
    print(f"  {'  ' * indent}{service:24} {name:28} +{offset_us:8.1f}us {dur_ms:6.2f}ms [{status}]")
    for child in tree["children"].get(span_id, []):
        _print_waterfall(child, tree, indent + 1, root_start)


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(1)
    trace_id = sys.argv[1].lower().strip()
    print(f"scatter-gather across {len(SHARDS)} shard(s) for trace_id={trace_id}")
    print(f"shards: {SHARDS}")

    rows: list = []
    overall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=len(SHARDS)) as pool:
        futures = [pool.submit(_query_shard, h, trace_id) for h in SHARDS]
        for f in as_completed(futures):
            rows.extend(f.result())
    overall_ms = (time.perf_counter() - overall_start) * 1000

    if not rows:
        print(f"no spans found for trace_id={trace_id}")
        sys.exit(2)

    tree = _assemble_waterfall(rows)
    print(f"\nassembled {len(rows)} spans in {overall_ms:.1f}ms (tail-shard bound)")
    print("\nwaterfall:")
    root_start = tree["by_id"][tree["root"]][4] if tree["root"] else 0
    if tree["root"]:
        _print_waterfall(tree["root"], tree, root_start=root_start)
    else:
        print("  no root span found; printing flat list")
        for r in rows:
            print(f"  {r[2]:24} {r[3]:28} {r[5] / 1e6:6.2f}ms")


if __name__ == "__main__":
    main()
