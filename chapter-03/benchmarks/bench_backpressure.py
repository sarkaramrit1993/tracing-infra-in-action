"""Bench 3: Backpressure cascade timeline.

Topology: telemetrygen -> agent (queue=500) -> gateway (queue=100) -> nonexistent.
Gateway fills first (T1/T2), then agent fills (T3/T4).
"""

import os
import time

from lib.metrics import (
    wait_for_collector,
    get_queue_size,
    get_queue_capacity,
    get_refused_spans,
    get_accepted_spans,
    get_enqueue_failed_spans,
    get_enqueue_failed_spans_strict,
)
from lib.load import run_telemetrygen


RATE = 6250          # spans/sec per worker (--rate is spans, not traces)
WORKERS = 8
CHILD_SPANS = 1      # total = 6250 * 8 = 50K spans/sec
MAX_DURATION = 90
POLL_INTERVAL = 0.5



def run() -> dict:
    agent_bh_endpoint = os.environ.get("AGENT_BLACKHOLE_GRPC_ENDPOINT", "otel-agent-blackhole:4317")
    agent_bh_metrics = os.environ.get("AGENT_BLACKHOLE_METRICS_URL", "http://otel-agent-blackhole:8888/metrics")
    gw_bh_metrics = os.environ.get("GATEWAY_BLACKHOLE_METRICS_URL", "http://otel-gateway-blackhole:8888/metrics")

    wait_for_collector(agent_bh_metrics)
    wait_for_collector(gw_bh_metrics)

    timestamps = {
        "t0_stall_start": 0.0,
        "t1_gateway_queue_full": None,
        "t2_gateway_refusing": None,
        "t3_agent_queue_full": None,
        "t4_agent_refusing": None,
    }

    gw_refused_baseline = get_refused_spans(gw_bh_metrics)
    gw_enqueue_failed_baseline = get_enqueue_failed_spans(gw_bh_metrics)
    gw_enqueue_failed_strict_baseline = get_enqueue_failed_spans_strict(gw_bh_metrics)
    agent_refused_baseline = get_refused_spans(agent_bh_metrics)
    agent_enqueue_failed_baseline = get_enqueue_failed_spans(agent_bh_metrics)

    # Start telemetrygen (non-blocking) targeting blackhole agent
    proc = run_telemetrygen(
        endpoint=agent_bh_endpoint,
        rate=RATE,
        duration=MAX_DURATION,
        workers=WORKERS,
        child_spans=CHILD_SPANS,
        service_name="bench-backpressure",
    )

    t0 = time.time()
    print(f"[backpressure] Sending ~{RATE * WORKERS:,} spans/sec to blackhole cascade...")

    try:
        deadline = t0 + MAX_DURATION
        while time.time() < deadline:
            elapsed = time.time() - t0

            try:
                # Fetch gateway metrics
                gw_queue = get_queue_size(gw_bh_metrics)
                gw_cap = get_queue_capacity(gw_bh_metrics)
                gw_refused_now = get_refused_spans(gw_bh_metrics)
                gw_enqueue_failed_now = get_enqueue_failed_spans(gw_bh_metrics)

                # T1: Gateway saturated — queue full OR queue dropping (enqueue_failed)
                gw_enqueue_strict_now = get_enqueue_failed_spans_strict(gw_bh_metrics)
                if timestamps["t1_gateway_queue_full"] is None:
                    # Detect queue filling (>= 10% or any enqueue failures)
                    if gw_cap > 0 and gw_queue >= max(1, gw_cap * 0.1):
                        timestamps["t1_gateway_queue_full"] = elapsed
                        pct = (gw_queue / gw_cap) * 100
                        print(f"  T1: Gateway queue filling at +{elapsed:.1f}s (queue={gw_queue:.0f}/{gw_cap:.0f}, {pct:.0f}%)")
                    elif gw_enqueue_strict_now > gw_enqueue_failed_strict_baseline:
                        timestamps["t1_gateway_queue_full"] = elapsed
                        dropped = gw_enqueue_strict_now - gw_enqueue_failed_strict_baseline
                        print(f"  T1: Gateway saturated at +{elapsed:.1f}s (enqueue_failed={dropped:.0f})")

                # T2: Gateway refusing spans back to agent (receiver-level rejection)
                if timestamps["t2_gateway_refusing"] is None and gw_refused_now > gw_refused_baseline:
                    timestamps["t2_gateway_refusing"] = elapsed
                    refused = gw_refused_now - gw_refused_baseline
                    print(f"  T2: Gateway refusing spans at +{elapsed:.1f}s (refused={refused:.0f})")

                # Check agent queue
                agent_queue = get_queue_size(agent_bh_metrics)
                agent_cap = get_queue_capacity(agent_bh_metrics)
                if timestamps["t3_agent_queue_full"] is None and agent_cap > 0:
                    if agent_queue >= agent_cap * 0.9:
                        timestamps["t3_agent_queue_full"] = elapsed
                        print(f"  T3: Agent queue full at +{elapsed:.1f}s (queue={agent_queue:.0f}, cap={agent_cap:.0f})")

                # Check agent refusing (enqueue failures in 0.148+, or receiver refused)
                agent_refused_now = get_refused_spans(agent_bh_metrics)
                agent_enqueue_failed_now = get_enqueue_failed_spans(agent_bh_metrics)
                agent_dropping = (agent_refused_now > agent_refused_baseline) or (agent_enqueue_failed_now > agent_enqueue_failed_baseline)
                if timestamps["t4_agent_refusing"] is None and agent_dropping:
                    timestamps["t4_agent_refusing"] = elapsed
                    agent_dropped = max(agent_refused_now - agent_refused_baseline, agent_enqueue_failed_now - agent_enqueue_failed_baseline)
                    print(f"  T4: Agent dropping spans at +{elapsed:.1f}s (dropped={agent_dropped:.0f})")

            except Exception as e:
                print(f"  [backpressure] Metrics poll error: {e}")

            if all(v is not None for v in timestamps.values()):
                print("[backpressure] All cascade stages observed.")
                break

            time.sleep(POLL_INTERVAL)

    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()

    for key in timestamps:
        if timestamps[key] is None:
            timestamps[key] = MAX_DURATION
            print(f"  {key}: NOT observed within {MAX_DURATION}s")

    t1 = timestamps["t1_gateway_queue_full"]
    t4 = timestamps["t4_agent_refusing"]
    cascade_sec = t4 - t1 if isinstance(t1, (int, float)) and isinstance(t4, (int, float)) else MAX_DURATION

    print(f"[backpressure] Cascade timeline: T1=+{timestamps['t1_gateway_queue_full']:.1f}s, "
          f"T2=+{timestamps['t2_gateway_refusing']:.1f}s, T3=+{timestamps['t3_agent_queue_full']:.1f}s, "
          f"T4=+{timestamps['t4_agent_refusing']:.1f}s")
    print(f"[backpressure] Cascade duration (T1->T4): {cascade_sec:.1f}s")

    timestamps["cascade_duration_sec"] = cascade_sec

    gw_refused_final = get_refused_spans(gw_bh_metrics) - gw_refused_baseline
    gw_enqueue_failed_final = get_enqueue_failed_spans(gw_bh_metrics) - gw_enqueue_failed_baseline
    agent_refused_final = get_refused_spans(agent_bh_metrics) - agent_refused_baseline
    agent_enqueue_failed_final = get_enqueue_failed_spans(agent_bh_metrics) - agent_enqueue_failed_baseline
    # Total dropped = max of refused or enqueue_failed (they track different stages)
    gw_total_dropped = max(gw_refused_final, gw_enqueue_failed_final)
    agent_total_dropped = max(agent_refused_final, agent_enqueue_failed_final)
    agent_accepted = get_accepted_spans(agent_bh_metrics)
    total_attempted = agent_accepted + agent_total_dropped
    loss_pct = (agent_total_dropped / max(1, total_attempted)) * 100
    timestamps["agent_refused_spans"] = agent_refused_final
    timestamps["agent_enqueue_failed_spans"] = agent_enqueue_failed_final
    timestamps["gateway_refused_spans"] = gw_refused_final
    timestamps["gateway_enqueue_failed_spans"] = gw_enqueue_failed_final
    timestamps["span_loss_pct"] = loss_pct
    print(f"[backpressure] Span loss: {loss_pct:.1f}% ({agent_total_dropped:.0f}/{total_attempted:.0f} spans dropped)")
    print(f"[backpressure] Gateway enqueue failures: {gw_enqueue_failed_final:.0f}, Agent enqueue failures: {agent_enqueue_failed_final:.0f}")

    return timestamps


if __name__ == "__main__":
    run()
