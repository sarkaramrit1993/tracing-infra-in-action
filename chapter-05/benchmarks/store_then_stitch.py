"""Chapter 5 benchmark: storage write cost on raw spans (Tempo / SigNoz style).

Measures the cost of the store-then-stitch ingestion path: take a batch of
synthetic OTLP spans, serialize them, batch them, and time the write to a
local backend. The default backend is an in-process Parquet file with zstd
compression (a Tempo block proxy). A ClickHouse backend can be selected by
setting BACKEND=clickhouse and CLICKHOUSE_HOST.

This is a local CPU and disk benchmark. It is not a production benchmark.
The chapter cares about the compression ratio of a columnar block, which sits
in the 6 to 10 to 1 range and matches Tempo's published numbers (footnote
[^10]). Two baselines are reported so the ratio is honest:

  raw_json:     len(json.dumps(span)) summed. Verbose and field-name-heavy, so
                the ratio against it is an upper bound, not a real-world figure.
  proto_proxy:  a compact length-delimited binary encoding of the same fields
                with no field names, a proxy for OTLP protobuf wire size. This
                is the tighter, more honest denominator; production ingest
                carries protobuf, not JSON.

The clickhouse backend inserts rows over the network and lets the server own
compression, so it reports compressed_size=0 and prints no client-side ratio.
Read the on-disk size from system.parts instead (see the top-level README
step 4).
"""

import io
import os
import time
import uuid
import json
import random
import statistics
from pathlib import Path
from datetime import datetime, timezone

random.seed(42)
NUM_SPANS = int(os.environ.get("NUM_SPANS", "10000"))
NUM_ITERATIONS = int(os.environ.get("NUM_ITERATIONS", "20"))
BACKEND = os.environ.get("BACKEND", "parquet")


def _gen_spans(n: int) -> list:
    services = ["checkout-service", "inventory-service", "payment-service",
                "fraud-service", "notification-service"]
    out = []
    for i in range(n):
        out.append({
            "trace_id": uuid.uuid4().hex,
            "span_id": random.randbytes(8).hex(),
            "parent_span_id": random.randbytes(8).hex() if i % 4 else "",
            "service_name": random.choice(services),
            "span_name": f"op_{i % 20}",
            "start_time_unix_nano": int(time.time() * 1e9) + i * 1_000_000,
            "duration_ns": random.randint(1_000_000, 200_000_000),
            "status_code": random.choice(["STATUS_CODE_OK"] * 9 + ["STATUS_CODE_ERROR"]),
            "attributes": {
                "http.method": random.choice(["GET", "POST", "PUT"]),
                "http.status_code": random.choice([200, 201, 400, 500]),
                "k8s.pod.name": f"pod-{i % 32}",
            },
        })
    return out


def _raw_size(spans: list) -> int:
    """Verbose JSON baseline. Field names repeat per span, so this is the
    most flattering (largest) denominator for the compression ratio."""
    return sum(len(json.dumps(s)) for s in spans)


def _proto_proxy_size(spans: list) -> int:
    """Compact length-delimited binary proxy for OTLP protobuf wire size: pack
    each field's bytes with a length prefix, no field names. Not a real OTLP
    encoder, but a far tighter and more honest denominator than verbose JSON."""
    import struct

    total = 0
    for s in spans:
        for v in (s["trace_id"], s["span_id"], s["parent_span_id"],
                  s["service_name"], s["span_name"], s["status_code"]):
            total += len(struct.pack(">H", 0)) + len(v.encode())
        total += 16  # two int64 fields: start_time, duration_ns
        for k, v in s["attributes"].items():
            total += len(struct.pack(">H", 0)) + len(k.encode()) + len(str(v).encode())
    return total


def _parquet_write(spans: list) -> tuple:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        print("[bench] pyarrow not installed; install with: pip install pyarrow")
        raise

    table = pa.Table.from_pylist([
        {
            "trace_id": s["trace_id"],
            "span_id": s["span_id"],
            "parent_span_id": s["parent_span_id"],
            "service_name": s["service_name"],
            "span_name": s["span_name"],
            "start_time": s["start_time_unix_nano"],
            "duration_ns": s["duration_ns"],
            "status_code": s["status_code"],
            "attributes_json": json.dumps(s["attributes"]),
        } for s in spans
    ])

    buf = io.BytesIO()
    start = time.perf_counter()
    pq.write_table(table, buf, compression="zstd", compression_level=3)
    elapsed = time.perf_counter() - start
    return elapsed, buf.tell()


def _clickhouse_write(spans: list) -> tuple:
    from clickhouse_driver import Client
    host = os.environ.get("CLICKHOUSE_HOST", "localhost")
    port = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
    client = Client(host=host, port=port, database="tracing")
    rows = [
        (
            s["start_time_unix_nano"] / 1e9,
            s["trace_id"], s["span_id"], s["parent_span_id"], "",
            s["span_name"], "SPAN_KIND_SERVER", s["service_name"],
            {}, "", "", {k: str(v) for k, v in s["attributes"].items()},
            s["duration_ns"], s["status_code"], "",
            [], [], [], [], [], [], [],
        ) for s in spans
    ]
    start = time.perf_counter()
    client.execute(
        "INSERT INTO tracing.otel_traces VALUES",
        rows,
        types_check=True,
    )
    elapsed = time.perf_counter() - start
    # ClickHouse size on disk is reported separately by the server
    return elapsed, 0


def run():
    print(f"[store-then-stitch] backend={BACKEND} num_spans={NUM_SPANS} iters={NUM_ITERATIONS}")
    spans = _gen_spans(NUM_SPANS)
    raw_size = _raw_size(spans)
    proto_size = _proto_proxy_size(spans)

    write_times = []
    compressed_size = 0

    for i in range(NUM_ITERATIONS):
        if BACKEND == "parquet":
            t, size = _parquet_write(spans)
        elif BACKEND == "clickhouse":
            t, size = _clickhouse_write(spans)
        else:
            raise SystemExit(f"unknown BACKEND: {BACKEND}")
        write_times.append(t)
        compressed_size = max(compressed_size, size)

    avg_ms = statistics.mean(write_times) * 1000
    p95_ms = (sorted(write_times)[int(len(write_times) * 0.95)]) * 1000
    throughput = NUM_SPANS / statistics.mean(write_times)

    print(f"[store-then-stitch] raw_json_bytes={raw_size:,} "
          f"proto_proxy_bytes={proto_size:,}")
    if compressed_size:
        ratio_json = raw_size / compressed_size
        ratio_proto = proto_size / compressed_size
        print(f"[store-then-stitch] compressed_size_bytes={compressed_size:,} "
              f"ratio_vs_json={ratio_json:.1f}x (upper bound) "
              f"ratio_vs_proto={ratio_proto:.1f}x (honest)")
    else:
        print(f"[store-then-stitch] compressed_size_bytes=0 "
              f"(backend={BACKEND}: server owns compression, see system.parts)")
    print(f"[store-then-stitch] avg_write_ms={avg_ms:.2f} p95={p95_ms:.2f} throughput={throughput:,.0f} spans/s")

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    (out_dir / f"store_then_stitch-{stamp}.json").write_text(json.dumps({
        "backend": BACKEND,
        "num_spans": NUM_SPANS,
        "num_iterations": NUM_ITERATIONS,
        "raw_json_bytes": raw_size,
        "proto_proxy_bytes": proto_size,
        "compressed_size_bytes": compressed_size,
        "avg_write_ms": avg_ms,
        "p95_write_ms": p95_ms,
        "throughput_spans_per_s": throughput,
    }, indent=2))


if __name__ == "__main__":
    run()
