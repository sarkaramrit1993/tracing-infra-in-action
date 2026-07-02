"""Bench 1: Routing distribution -- single-gateway vs trace-aware.

Phase A: all spans to one gateway (no routing_key). Gini ~0.5.
Phase B: traceID routing across two gateways. Gini ~0.
"""

import os
import time

from lib.metrics import (
    wait_for_collector,
    get_accepted_spans,
)
from lib.load import run_telemetrygen_blocking

RATE = 6250          # spans/sec per worker (--rate is spans, not traces)
WORKERS = 8
CHILD_SPANS = 1      # total = 6250 * 8 = 50K spans/sec
DURATION_SEC = 30


def gini_coefficient(values: list[float]) -> float:
    """Gini coefficient. 0 = equal, 0.5 = max skew for n=2."""
    if not values or sum(values) == 0:
        return 0.0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    total = sum(sorted_vals)
    cumulative = 0.0
    gini_sum = 0.0
    for i, v in enumerate(sorted_vals):
        cumulative += v
        gini_sum += (2 * (i + 1) - n - 1) * v
    return gini_sum / (n * total)


def run_phase(
    agent_endpoint: str,
    agent_metrics: str,
    gw1_metrics: str,
    gw2_metrics: str,
    label: str,
) -> dict:
    """Send load through an agent, measure span distribution across gateways."""
    wait_for_collector(agent_metrics)
    wait_for_collector(gw1_metrics)
    wait_for_collector(gw2_metrics)

    gw1_accepted_before = get_accepted_spans(gw1_metrics)
    gw2_accepted_before = get_accepted_spans(gw2_metrics)

    rc, stdout, stderr = run_telemetrygen_blocking(
        endpoint=agent_endpoint,
        rate=RATE,
        duration=DURATION_SEC,
        workers=WORKERS,
        child_spans=CHILD_SPANS,
        service_name=f"bench-routing-{label}",
    )
    if rc != 0:
        print(f"  [routing] telemetrygen {label} stderr: {stderr[:500]}")

    time.sleep(5)

    gw1_accepted_after = get_accepted_spans(gw1_metrics)
    gw2_accepted_after = get_accepted_spans(gw2_metrics)

    gw1_spans = gw1_accepted_after - gw1_accepted_before
    gw2_spans = gw2_accepted_after - gw2_accepted_before
    total_spans = gw1_spans + gw2_spans

    skew_ratio = max(gw1_spans, gw2_spans) / max(1, min(gw1_spans, gw2_spans))

    return {
        "gw1_spans": gw1_spans,
        "gw2_spans": gw2_spans,
        "total_spans": total_spans,
        "gini": gini_coefficient([gw1_spans, gw2_spans]),
        "skew_ratio": skew_ratio,
    }


def run() -> dict:
    """Run both routing phases and compare distribution."""
    # Phase A: round-robin agent (no routing_key)
    agent_random_endpoint = os.environ.get("AGENT_RANDOM_GRPC_ENDPOINT", "otel-agent-random:4317")
    agent_random_metrics = os.environ.get("AGENT_RANDOM_METRICS_URL", "http://otel-agent-random:8888/metrics")
    # Phase B: traceID agent
    agent_endpoint = os.environ.get("OTLP_GRPC_ENDPOINT", "otel-agent:4317")
    agent_metrics = os.environ.get("AGENT_METRICS_URL", "http://otel-agent:8888/metrics")
    # Shared gateways
    gw1_metrics = os.environ.get("GATEWAY1_METRICS_URL", "http://otel-gateway-1:8888/metrics")
    gw2_metrics = os.environ.get("GATEWAY2_METRICS_URL", "http://otel-gateway-2:8888/metrics")

    print("[routing] Phase A: single-gateway routing (no routing_key)...")
    single_gw_result = run_phase(agent_random_endpoint, agent_random_metrics, gw1_metrics, gw2_metrics, "single-gw")

    print("[routing] Phase B: traceID routing...")
    traceaware_result = run_phase(agent_endpoint, agent_metrics, gw1_metrics, gw2_metrics, "traceaware")

    single_total = single_gw_result["total_spans"]
    single_gw1_pct = (single_gw_result["gw1_spans"] / max(1, single_total)) * 100
    single_gw2_pct = (single_gw_result["gw2_spans"] / max(1, single_total)) * 100
    trace_total = traceaware_result["total_spans"]
    trace_gw1_pct = (traceaware_result["gw1_spans"] / max(1, trace_total)) * 100
    trace_gw2_pct = (traceaware_result["gw2_spans"] / max(1, trace_total)) * 100

    result = {
        "single_gw_gini": single_gw_result["gini"],
        "traceaware_gini": traceaware_result["gini"],
        "single_gw_skew_ratio": single_gw_result["skew_ratio"],
        "traceaware_skew_ratio": traceaware_result["skew_ratio"],
        "single_gw_gw1_spans": single_gw_result["gw1_spans"],
        "single_gw_gw2_spans": single_gw_result["gw2_spans"],
        "traceaware_gw1_spans": traceaware_result["gw1_spans"],
        "traceaware_gw2_spans": traceaware_result["gw2_spans"],
        "single_gw_gw1_pct": single_gw1_pct,
        "single_gw_gw2_pct": single_gw2_pct,
        "traceaware_gw1_pct": trace_gw1_pct,
        "traceaware_gw2_pct": trace_gw2_pct,
        "total_spans_target": RATE * WORKERS * DURATION_SEC,
    }

    print(f"[routing] Single-GW: Gini={single_gw_result['gini']:.3f}, skew={single_gw_result['skew_ratio']:.1f}x (GW1: {single_gw1_pct:.0f}%, GW2: {single_gw2_pct:.0f}%)")
    print(f"[routing] TraceID:   Gini={traceaware_result['gini']:.3f}, skew={traceaware_result['skew_ratio']:.1f}x (GW1: {trace_gw1_pct:.0f}%, GW2: {trace_gw2_pct:.0f}%)")

    return result


if __name__ == "__main__":
    run()
