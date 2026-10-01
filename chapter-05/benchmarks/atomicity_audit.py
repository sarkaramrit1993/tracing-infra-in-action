"""Chapter 5: a self-contained model of the partial-trace audit logic.

This is NOT a probe of the running stack. It does not read Kafka
`traces.assembled` or ClickHouse. It generates synthetic traces in memory,
applies one failure mode, and demonstrates how you would detect the silent
data loss failure mode from section 5.3.2. The detection logic here is exactly
what you would run against real assembled traces; wiring it to the live
`traces.assembled` topic (or ClickHouse) is left as an exercise.

The model:

1. Generates N synthetic traces with M spans each.
2. Applies one failure mode: producer-crash (drop whole producer batches,
   each holding spans from many traces), buffer-overflow (evict random spans),
   or drop-whole-trace (evict whole traces).
3. Audits the surviving traces and asserts the only acceptable outcomes:
      A. Whole trace present (all M spans, root present).
      B. Whole trace absent (zero spans).
   Any partial trace (some spans present, some missing) is a violation of the
   atomicity imperative and fails the audit.

Set FAILURE_MODE to one of:
  none              clean run, expect 100% whole-trace outcomes
  producer-crash    drop whole batches at the producer (violation the
                    assembler cannot prevent: boundary 1)
  buffer-overflow   evict random spans inside the assembler (violation)
  drop-whole-trace  evict whole traces inside the assembler (compliant)
"""

import os
import sys
import time
import random
import json
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

random.seed(42)

NUM_TRACES = int(os.environ.get("NUM_TRACES", "1000"))
SPANS_PER_TRACE = int(os.environ.get("SPANS_PER_TRACE", "8"))
FAILURE_MODE = os.environ.get("FAILURE_MODE", "none")
FAILURE_RATE = float(os.environ.get("FAILURE_RATE", "0.05"))
BATCH_SPANS = int(os.environ.get("BATCH_SPANS", "50"))


def _gen_traces(num: int, spans_per: int) -> list:
    out = []
    for i in range(num):
        trace_id = f"trace-{i:06d}"
        spans = [(trace_id, f"span-{i:06d}-{j:03d}", j == 0) for j in range(spans_per)]
        out.append((trace_id, spans))
    return out


def _producer_crash_filter(traces: list, rate: float) -> list:
    """Drop random whole producer batches.

    A trace's spans leave different hosts through different gateway
    collectors, so no producer batch holds a whole trace. Model that by
    interleaving every span and cutting the stream into BATCH_SPANS-sized
    batches. A lost batch takes part of several traces with it, so this mode
    produces partial traces: boundary 1 is the one the assembler cannot
    protect."""
    all_spans = [span for _trace_id, spans in traces for span in spans]
    random.shuffle(all_spans)
    kept = []
    for start in range(0, len(all_spans), BATCH_SPANS):
        if random.random() >= rate:
            kept.extend(all_spans[start:start + BATCH_SPANS])
    by_trace = defaultdict(list)
    for span in kept:
        by_trace[span[0]].append(span)
    return list(by_trace.items())


def _buffer_overflow_filter(traces: list, rate: float) -> list:
    """Evict random spans inside the assembler. Violates the imperative."""
    out = []
    for trace_id, spans in traces:
        kept = [s for s in spans if random.random() >= rate]
        out.append((trace_id, kept))
    return out


def _drop_whole_trace_filter(traces: list, rate: float) -> list:
    """Evict the oldest N% of traces wholesale. Compliant with the imperative."""
    keep_count = int(len(traces) * (1 - rate))
    return random.sample(traces, keep_count)


FAILURE_FILTERS = {
    "none": lambda t, r: t,
    "producer-crash": _producer_crash_filter,
    "buffer-overflow": _buffer_overflow_filter,
    "drop-whole-trace": _drop_whole_trace_filter,
}


def _audit(emitted: list, expected_spans_per_trace: int) -> dict:
    by_trace = defaultdict(list)
    for trace_id, spans in emitted:
        by_trace[trace_id].extend(spans)

    whole = 0
    absent = 0
    partial = 0
    partial_examples = []

    seen_trace_ids = set()
    for trace_id, spans in by_trace.items():
        seen_trace_ids.add(trace_id)
        if len(spans) == expected_spans_per_trace and any(is_root for (_t, _s, is_root) in spans):
            whole += 1
        elif len(spans) == 0:
            absent += 1
        else:
            partial += 1
            if len(partial_examples) < 5:
                partial_examples.append({
                    "trace_id": trace_id,
                    "spans_present": len(spans),
                    "spans_expected": expected_spans_per_trace,
                })

    return {
        "whole": whole,
        "absent": absent,
        "partial": partial,
        "partial_examples": partial_examples,
    }


def run():
    print(f"[atomicity-audit] mode={FAILURE_MODE} rate={FAILURE_RATE} "
          f"traces={NUM_TRACES} spans_per_trace={SPANS_PER_TRACE}")
    if FAILURE_MODE not in FAILURE_FILTERS:
        print(f"[atomicity-audit] unknown FAILURE_MODE: {FAILURE_MODE}")
        sys.exit(2)

    traces = _gen_traces(NUM_TRACES, SPANS_PER_TRACE)
    emitted = FAILURE_FILTERS[FAILURE_MODE](traces, FAILURE_RATE)

    expected_absent = NUM_TRACES - len(emitted)
    audit = _audit(emitted, SPANS_PER_TRACE)
    audit["absent"] += expected_absent

    total = audit["whole"] + audit["absent"] + audit["partial"]
    print(f"[atomicity-audit] whole={audit['whole']:,} absent={audit['absent']:,} "
          f"partial={audit['partial']:,} total={total:,}")
    if audit["partial"] > 0:
        print(f"[atomicity-audit] partial-trace examples:")
        for ex in audit["partial_examples"]:
            print(f"  {ex}")

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    (out_dir / f"atomicity_audit-{FAILURE_MODE}-{stamp}.json").write_text(json.dumps({
        "mode": FAILURE_MODE,
        "rate": FAILURE_RATE,
        "result": audit,
    }, indent=2))

    if audit["partial"] > 0:
        print(f"[atomicity-audit] FAIL: {audit['partial']:,} partial traces violate atomicity")
        sys.exit(1)
    else:
        print(f"[atomicity-audit] PASS: no partial traces emitted")


if __name__ == "__main__":
    run()
