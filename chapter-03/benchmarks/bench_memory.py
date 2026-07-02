"""Bench 4: Collector memory -- agent vs gateway RSS under load."""

import os
import time

from lib.metrics import (
    wait_for_collector,
    get_process_memory_bytes,
)
from lib.load import create_tracer, send_spans_at_rate, flush_and_shutdown


LOAD_LEVELS = [
    ("idle", 0, 0),
    ("1K/sec", 1_000, 15),
    ("10K/sec", 10_000, 15),
    ("50K/sec", 50_000, 15),
]


def measure_memory_profile(otlp_endpoint: str, metrics_url: str, label: str) -> list[dict]:
    """Measure RSS at idle and increasing load levels."""
    wait_for_collector(metrics_url)

    results = []

    # Idle measurement
    time.sleep(3)
    idle_mem = get_process_memory_bytes(metrics_url)
    results.append({
        "load": "idle",
        "rss_bytes": idle_mem,
        "rss_mb": idle_mem / (1024 * 1024),
    })
    print(f"  [{label}] Idle: {idle_mem / (1024 * 1024):.0f} MB")

    # Load levels
    for level_name, rate, duration in LOAD_LEVELS:
        if rate == 0:
            continue

        tracer, provider = create_tracer(otlp_endpoint, service_name=f"bench-memory-{label}")
        print(f"  [{label}] Loading at {level_name} for {duration}s...")
        send_spans_at_rate(tracer, rate, duration)

        time.sleep(2)  # let metrics update
        mem = get_process_memory_bytes(metrics_url)
        results.append({
            "load": level_name,
            "rss_bytes": mem,
            "rss_mb": mem / (1024 * 1024),
        })
        print(f"  [{label}] {level_name}: {mem / (1024 * 1024):.0f} MB")

        flush_and_shutdown(provider)
        time.sleep(3)  # cool down between levels

    return results


def run() -> dict:
    otlp_endpoint = os.environ.get("OTLP_GRPC_ENDPOINT", "otel-agent:4317")
    agent_metrics = os.environ.get("AGENT_METRICS_URL", "http://otel-agent:8888/metrics")
    gw1_metrics = os.environ.get("GATEWAY1_METRICS_URL", "http://otel-gateway-1:8888/metrics")

    print("[memory] Measuring agent (sidecar-like) memory profile...")
    agent_results = measure_memory_profile(otlp_endpoint, agent_metrics, "agent")

    print("[memory] Measuring gateway (daemonset-like) memory profile...")
    gateway_results = measure_memory_profile(otlp_endpoint, gw1_metrics, "gateway")

    result = {
        "agent": agent_results,
        "gateway": gateway_results,
    }

    # Summary
    agent_idle = agent_results[0]["rss_mb"] if agent_results else 0
    gateway_idle = gateway_results[0]["rss_mb"] if gateway_results else 0
    agent_peak = max(r["rss_mb"] for r in agent_results) if agent_results else 0
    gateway_peak = max(r["rss_mb"] for r in gateway_results) if gateway_results else 0

    print(f"[memory] Agent: idle={agent_idle:.0f}MB, peak={agent_peak:.0f}MB")
    print(f"[memory] Gateway: idle={gateway_idle:.0f}MB, peak={gateway_peak:.0f}MB")

    return result


if __name__ == "__main__":
    run()
