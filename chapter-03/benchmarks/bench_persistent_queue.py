"""Bench 5: Persistent queue throughput -- memory vs disk.

Measures throughput delta between in-memory and file_storage-backed queues.
Both agents export to gateway-1 with identical configs except queue backing.
"""

import os
import time

from lib.metrics import (
    wait_for_collector,
    get_sent_spans,
    get_process_cpu_seconds,
)
from lib.load import run_telemetrygen_blocking


RATE = 6250          # spans/sec per worker
WORKERS = 8
CHILD_SPANS = 1      # total = 6250 * 8 = 50K spans/sec
DURATION_SEC = 30


def measure_phase(
    agent_endpoint: str,
    agent_metrics: str,
    label: str,
) -> dict:
    """Send load and measure agent-side export throughput."""
    wait_for_collector(agent_metrics)

    sent_before = get_sent_spans(agent_metrics)
    cpu_before = get_process_cpu_seconds(agent_metrics)

    start = time.time()
    rc, stdout, stderr = run_telemetrygen_blocking(
        endpoint=agent_endpoint,
        rate=RATE,
        duration=DURATION_SEC,
        workers=WORKERS,
        child_spans=CHILD_SPANS,
        service_name=f"bench-pq-{label}",
    )
    send_duration = time.time() - start
    if rc != 0:
        print(f"  [{label}] telemetrygen stderr: {stderr[:300]}")

    time.sleep(6)  # exceed batch timeout (5s) for full drain

    sent_after = get_sent_spans(agent_metrics)
    cpu_after = get_process_cpu_seconds(agent_metrics)

    sent = sent_after - sent_before
    throughput = sent / send_duration if send_duration > 0 else 0
    cpu_delta = cpu_after - cpu_before

    return {
        "label": label,
        "agent_sent": sent,
        "duration_sec": send_duration,
        "throughput_spans_per_sec": throughput,
        "cpu_seconds": cpu_delta,
    }


def run() -> dict:
    # Phase A: memory queue (identical config to PQ except no file_storage)
    agent_mem_endpoint = os.environ.get("AGENT_MEMORY_GRPC_ENDPOINT", "otel-agent-memory:4317")
    agent_mem_metrics = os.environ.get("AGENT_MEMORY_METRICS_URL", "http://otel-agent-memory:8888/metrics")
    # Phase B: disk queue (file_storage backed)
    agent_pq_endpoint = os.environ.get("AGENT_PQ_GRPC_ENDPOINT", "otel-agent-pq:4317")
    agent_pq_metrics = os.environ.get("AGENT_PQ_METRICS_URL", "http://otel-agent-pq:8888/metrics")

    print("[persistent_queue] Phase A: Memory-only queue...")
    memory_result = measure_phase(agent_mem_endpoint, agent_mem_metrics, "memory")

    print("[persistent_queue] Phase B: Disk-backed queue (file_storage)...")
    disk_result = measure_phase(agent_pq_endpoint, agent_pq_metrics, "disk")

    mem_throughput = memory_result["throughput_spans_per_sec"]
    disk_throughput = disk_result["throughput_spans_per_sec"]
    throughput_delta = ((mem_throughput - disk_throughput) / mem_throughput * 100) if mem_throughput > 0 else 0

    result = {
        "memory_throughput": mem_throughput,
        "disk_throughput": disk_throughput,
        "throughput_delta_pct": throughput_delta,
        "memory_cpu_seconds": memory_result["cpu_seconds"],
        "disk_cpu_seconds": disk_result["cpu_seconds"],
        "memory_phase": memory_result,
        "disk_phase": disk_result,
        "total_spans_target": RATE * WORKERS * DURATION_SEC,
    }

    print(f"[persistent_queue] Memory: {mem_throughput:,.0f} spans/sec, Disk: {disk_throughput:,.0f} spans/sec")
    print(f"[persistent_queue] Throughput delta: {throughput_delta:.1f}%")
    print(f"[persistent_queue] CPU: memory={memory_result['cpu_seconds']:.2f}s, disk={disk_result['cpu_seconds']:.2f}s")

    return result


if __name__ == "__main__":
    run()
