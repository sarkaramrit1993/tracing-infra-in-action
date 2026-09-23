"""Chapter 9 benchmark: how many exemplars still point at a stored trace.

An exemplar is a trace id riding on a metric bucket, and it is the whole of
the metric-to-trace bridge in section 9.3.1. The gateway config mints them on
both span-metrics connectors, one ahead of the tail sampler and one behind it,
which makes the two sides directly comparable on one workload.

The pre-sampler side mints its pointer before the sampler has decided
anything. The sampler then keeps every error trace and one success in a
hundred, so most pre-sampler pointers end up aiming at a trace that was never
stored. Nothing errors: query_exemplars returns the id, the drill-down runs,
and the trace viewer says the trace does not exist.

The direction is the claim and the direction is what this asserts: a larger
share of post-sampler exemplars resolve than pre-sampler ones. The magnitude
is not asserted, because it is a draw. Exemplars are minted one per series per
scrape rather than one per span, so what the pre side resolves depends on how
many series were live in the window and how many of those were error series,
and both are recorded here for exactly that reason. An error series is kept
whole by the sampler, so it resolves; a success series is the one that dangles.
That is why a pre-sampler resolution rate sits well above the share of spans
that are errors, and why quoting a single run's ratio as if it were a constant
is the same unsupported claim in a new font.

Run (stack up, after some /checkout traffic and one scrape):
    python3 benchmarks/exemplar_resolution.py
    WINDOW=1800 python3 benchmarks/exemplar_resolution.py
"""
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

CHAPTER = Path(__file__).resolve().parent.parent

PROM = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
SERVICE = os.environ.get("SERVICE", "checkout-service")
# Prometheus keeps exemplars in a fixed-size in-memory ring, so the window is
# a read over what is still buffered rather than a retention setting.
WINDOW = int(os.environ.get("WINDOW", "900"))

SIDES = {"pre": "pre_duration_milliseconds_bucket",
         "post": "post_duration_milliseconds_bucket"}


def ch(sql):
    """Run one query inside the ClickHouse container.

    stdin is closed rather than left on the terminal, or the client waits on
    an EOF that never arrives and the benchmark hangs with no output.
    """
    proc = subprocess.run(
        ["docker", "compose", "exec", "-T", "clickhouse",
         "clickhouse-client", "--database", "tracing", "--query", sql],
        cwd=CHAPTER, text=True, capture_output=True, input="")
    if proc.returncode != 0:
        print("[exemplar] ClickHouse rejected a query:\n" + proc.stderr.strip(),
              file=sys.stderr)
        raise SystemExit(1)
    return proc.stdout.strip()


def read_exemplars(metric, start, end):
    """Every exemplar on one metric in the window, with its series labels.

    Returns (trace ids in mint order, distinct series, error series). The two
    series counts are what make a resolution rate readable: exemplars are
    minted per series per scrape, so a side's rate is a statement about the
    series mix in the window and not about the share of spans that failed.
    """
    url = f"{PROM}/api/v1/query_exemplars?" + urllib.parse.urlencode(
        {"query": metric, "start": start, "end": end})
    with urllib.request.urlopen(url, timeout=30) as resp:
        data = json.load(resp).get("data", [])

    ids, series, error_series = [], 0, 0
    for block in data:
        labels = block.get("seriesLabels", {})
        minted = False
        for exemplar in block.get("exemplars", []):
            trace_id = exemplar.get("labels", {}).get("trace_id")
            if trace_id:
                ids.append(trace_id)
                minted = True
        if minted:
            series += 1
            if labels.get("status_code") == "STATUS_CODE_ERROR":
                error_series += 1
    return ids, series, error_series


def resolve(trace_ids):
    """How many of these trace ids have spans in the store.

    One query rather than one per id: the point is the count, and a loop of
    thirty-five round trips through docker exec is slower than the measurement
    is interesting.
    """
    if not trace_ids:
        return 0
    quoted = ",".join("'" + t + "'" for t in sorted(set(trace_ids)))
    return int(ch(f"SELECT uniqExact(trace_id) FROM tracing.otel_traces "
                  f"WHERE trace_id IN ({quoted})"))


def run():
    end = time.time()
    start = end - WINDOW
    print(f"[exemplar] Prometheus={PROM} service={SERVICE} window={WINDOW}s")

    measured = {}
    for side, metric in SIDES.items():
        ids, series, error_series = read_exemplars(metric, start, end)
        distinct = sorted(set(ids))
        resolved = resolve(distinct)
        rate = resolved / len(distinct) if distinct else 0.0
        measured[side] = {
            "metric": metric,
            "minted": len(ids),
            "distinct_trace_ids": len(distinct),
            "resolved": resolved,
            "resolution_rate": round(rate, 6),
            "distinct_series": series,
            "error_series": error_series,
        }
        print(f"[exemplar] {side:<4}: {resolved} of {len(distinct)} resolve "
              f"({rate:.1%}) across {series} series, {error_series} of them error")

    pre, post = measured["pre"], measured["post"]
    if not pre["distinct_trace_ids"] or not post["distinct_trace_ids"]:
        raise SystemExit(
            "[exemplar] one side minted no exemplars. Drive /checkout traffic, "
            "wait for a connector flush and a Prometheus scrape, then rerun.")
    if not post["resolution_rate"] > pre["resolution_rate"]:
        raise SystemExit(
            f"[exemplar] expected the post side to resolve better than the pre "
            f"side, got {post['resolution_rate']:.1%} against "
            f"{pre['resolution_rate']:.1%}. An exemplar minted behind the "
            f"sampler points at a trace the sampler already decided to keep.")

    print(f"[exemplar] PASS: post resolves {post['resolution_rate']:.1%} against "
          f"pre {pre['resolution_rate']:.1%}; direction holds, magnitude is a draw")

    stamp = datetime.now(timezone.utc)
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    out = out_dir / f"exemplar-resolution-{stamp.strftime('%Y-%m-%dT%H%M%S')}.json"
    out.write_text(json.dumps({
        "benchmark": "exemplar_resolution",
        "measured_at_utc": stamp.isoformat(),
        "prometheus": PROM,
        "window_seconds": WINDOW,
        "service": SERVICE,
        "pre": pre,
        "post": post,
        "note": "Direction is the claim: a larger share of post-sampler "
                "exemplars resolve against the span store than pre-sampler "
                "ones, because the pointer behind the sampler is minted after "
                "the decision to keep. The magnitude is a draw. Exemplars are "
                "minted one per series per scrape rather than one per span, so "
                "distinct_series and error_series are recorded beside the "
                "rates: an error series is kept whole and resolves, a success "
                "series is the one that dangles, and the mix of the two in the "
                "window sets the pre-side rate.",
    }, indent=2) + "\n")
    print(f"[exemplar] wrote {out}")


if __name__ == "__main__":
    run()
