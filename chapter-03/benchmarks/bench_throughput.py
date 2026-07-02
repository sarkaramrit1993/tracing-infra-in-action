"""Bench 2: Gateway throughput ceiling.

Sends spans directly to a gateway at increasing rates (10K--500K/sec).
Measures export throughput = exporter_sent_spans / wall_time.
Ceiling = rate where throughput plateaus.
"""

import os
import time

from lib.metrics import (
    wait_for_collector,
    get_accepted_spans,
    get_refused_spans,
    get_sent_spans,
    get_process_memory_bytes,
    get_process_cpu_seconds,
)
from lib.load import run_telemetrygen_blocking


RATE_STEPS = [10_000, 25_000, 50_000, 75_000, 100_000, 150_000, 200_000, 300_000, 400_000, 500_000]
STEP_DURATION_SEC = 30
WORKERS = 8
CHILD_SPANS = 1
DRAIN_POLL_SEC = 2       # poll interval while waiting for queue drain
DRAIN_TIMEOUT_SEC = 60   # max time to wait for queue to drain after send


def wait_for_drain(gw_metrics: str, sent_before: float, timeout: float = DRAIN_TIMEOUT_SEC) -> float:
    """Wait until exporter_sent_spans stabilizes. Returns final count."""
    deadline = time.time() + timeout
    prev_sent = get_sent_spans(gw_metrics)
    stable_count = 0
    while time.time() < deadline:
        time.sleep(DRAIN_POLL_SEC)
        curr_sent = get_sent_spans(gw_metrics)
        if curr_sent == prev_sent:
            stable_count += 1
            if stable_count >= 2:  # stable for 2 consecutive polls
                return curr_sent
        else:
            stable_count = 0
            prev_sent = curr_sent
    return get_sent_spans(gw_metrics)


def run() -> dict:
    # Target gateway directly (not through agent)
    gw_endpoint = os.environ.get("GATEWAY1_GRPC_ENDPOINT", "otel-gateway-1:4317")
    gw_metrics = os.environ.get("GATEWAY1_METRICS_URL", "http://otel-gateway-1:8888/metrics")

    wait_for_collector(gw_metrics)

    steps = []

    for target_rate in RATE_STEPS:
        # --rate = spans/sec/worker, so rate_per_worker = target / workers
        rate_per_worker = max(1, target_rate // WORKERS)
        actual_total = rate_per_worker * WORKERS

        print(f"[throughput] Testing ~{actual_total:,} spans/sec (rate={rate_per_worker}/worker) for {STEP_DURATION_SEC}s...")

        accepted_before = get_accepted_spans(gw_metrics)
        refused_before = get_refused_spans(gw_metrics)
        sent_before = get_sent_spans(gw_metrics)
        cpu_before = get_process_cpu_seconds(gw_metrics)

        wall_start = time.time()

        rc, stdout, stderr = run_telemetrygen_blocking(
            endpoint=gw_endpoint,
            rate=rate_per_worker,
            duration=STEP_DURATION_SEC,
            workers=WORKERS,
            child_spans=CHILD_SPANS,
            service_name="bench-throughput",
        )
        if rc != 0:
            print(f"  telemetrygen stderr: {stderr[:300]}")

        send_end = time.time()

        # Wait for the gateway to fully drain its export queue
        print(f"  Waiting for gateway queue drain...")
        sent_final = wait_for_drain(gw_metrics, sent_before)

        wall_end = time.time()

        accepted_after = get_accepted_spans(gw_metrics)
        refused_after = get_refused_spans(gw_metrics)
        cpu_after = get_process_cpu_seconds(gw_metrics)
        memory_mb = get_process_memory_bytes(gw_metrics) / (1024 * 1024)

        accepted = accepted_after - accepted_before
        refused = refused_after - refused_before
        sent = sent_final - sent_before
        wall_time = wall_end - wall_start
        send_time = send_end - wall_start
        cpu_delta = cpu_after - cpu_before

        # Actual throughput = spans exported / total wall time (send + drain)
        actual_throughput = sent / wall_time if wall_time > 0 else 0
        # Send-phase throughput = spans accepted by receiver / send duration
        receive_throughput = accepted / send_time if send_time > 0 else 0

        step_result = {
            "target_rate": target_rate,
            "actual_rate": actual_total,
            "accepted": accepted,
            "refused": refused,
            "sent": sent,
            "actual_throughput": actual_throughput,
            "receive_throughput": receive_throughput,
            "wall_time_sec": wall_time,
            "send_time_sec": send_time,
            "cpu_seconds": cpu_delta,
            "memory_mb": memory_mb,
        }
        steps.append(step_result)

        print(f"  Accepted: {accepted:,.0f}, Sent: {sent:,.0f}, Refused: {refused:,.0f}")
        print(f"  Actual throughput: {actual_throughput:,.0f} spans/sec (over {wall_time:.1f}s wall time)")
        print(f"  Receive throughput: {receive_throughput:,.0f} spans/sec (over {send_time:.1f}s send time)")

    # The gateway exports to a real backend (Jaeger), so export throughput
    # is bounded by both gateway capacity AND backend ingest rate.
    # Report peak actual throughput and the highest sustainable receive rate.
    throughputs = [s["actual_throughput"] for s in steps]
    peak_throughput = max(throughputs) if throughputs else 0

    # Sustainable rate = highest target where send_time ≈ STEP_DURATION
    # (i.e., gateway didn't need excessive drain time)
    sustainable_rate = 0
    for s in steps:
        drain_overhead = s["wall_time_sec"] - s["send_time_sec"]
        if drain_overhead < s["send_time_sec"] * 0.5:  # drain < 50% of send time
            sustainable_rate = s["target_rate"]

    result = {
        "steps": steps,
        "peak_throughput": peak_throughput,
        "sustainable_rate": sustainable_rate,
        "rate_steps_tested": [s["target_rate"] for s in steps],
    }

    print(f"[throughput] Peak export throughput: {peak_throughput:,.0f} spans/sec")
    print(f"[throughput] Sustainable receive rate (drain < 50% of send time): {sustainable_rate:,} spans/sec")
    return result


if __name__ == "__main__":
    run()
