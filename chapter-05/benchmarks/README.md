# Chapter 5 Benchmarks

Three local exercises that probe the trade-offs the chapter argues about.
Each one runs on a single laptop. None of them reproduces production scale.
They demonstrate the cost-curve shape from Figure 5.2 and the atomicity
imperative from section 5.3.2.

## Setup

```bash
pip install -r ../app/requirements.txt
pip install pyarrow  # only needed for store_then_stitch.py parquet backend
```

Results land in `results/` (gitignored).

Run the benchmark unit tests with `python3 -m pytest` (from the chapter root or
from here).

## Storage-time write cost

Measures Parquet block write throughput and compression ratio. The reported
ratio is compressed Parquet size against a **verbose JSON** baseline, so treat
it as an upper bound, not a production figure: real ingest paths carry OTLP
protobuf on the wire, which is already more compact than JSON. The script also
prints the protobuf-proxy baseline (a tighter denominator) so you can see both.
The shape of the curve, not the exact number, is the point of Figure 5.2.

```bash
python store_then_stitch.py
# or against the live ClickHouse:
BACKEND=clickhouse CLICKHOUSE_HOST=localhost python store_then_stitch.py
```

The `clickhouse` backend inserts rows over the network and lets the server own
compression, so it reports `compressed_size=0` (no client-side ratio); read the
on-disk size from `system.parts` as shown in the top-level README step 4.

Tunable knobs: `NUM_SPANS`, `NUM_ITERATIONS`.

## Stream-time assembly cost

Models the Flink keyed-state buffer cost without standing up Flink and without
wall-clock pacing. It advances a synthetic event-time clock deterministically,
fills the in-flight population to steady state (every trace accumulates its
spans inside the `decision_wait` window before its timer fires), and reports the
peak buffer. The default run reproduces the chapter's back-of-envelope from
section 5.1.2: at 5,000 traces/sec, 30s `decision_wait`, 8 spans/trace,
2 KB/span, the on-the-wire buffer reaches roughly 2.4 GB, and the collector's
in-memory representation runs about 4x that.

```bash
python stream_time.py
TRACES_PER_SEC=1000 DECISION_WAIT_S=10 python stream_time.py
```

Because the clock is synthetic, the run is instant and deterministic; there is
no `DURATION_S` knob. Tunable knobs: `TRACES_PER_SEC`, `SPANS_PER_TRACE`,
`SPAN_SIZE_BYTES`, `DECISION_WAIT_S`, `MEM_FACTOR` (in-memory expansion vs wire,
default 4).

## Atomicity audit

A self-contained model of the audit logic, not a probe of the running stack.
It generates synthetic traces in memory, injects one failure mode, and asserts
no partial traces emerge. The audit passes for `none`, `producer-crash`, and
`drop-whole-trace` failure modes (all compliant with the atomicity imperative)
and fails for `buffer-overflow` (which evicts random spans and violates the
imperative). It demonstrates the detection logic you would run against real
assembled traces.

```bash
python atomicity_audit.py                              # clean run, expect PASS
FAILURE_MODE=producer-crash python atomicity_audit.py  # expect PASS
FAILURE_MODE=buffer-overflow python atomicity_audit.py # expect FAIL
FAILURE_MODE=drop-whole-trace python atomicity_audit.py # expect PASS
```

The exit code is non-zero when partial traces are detected. To make it a real
guardrail you would feed it traces read from the live `traces.assembled` topic
(or ClickHouse) instead of the synthetic generator; that wiring is left as an
exercise.
