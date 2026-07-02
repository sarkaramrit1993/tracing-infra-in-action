"""Scenario-based load generator for Chapter 3 collector testing.

Usage: python scripts/load-generator.py --scenario <name> [--duration <secs>] [--rate <rps>]
"""

import argparse
import json
import random
import sys
import time
import urllib.request
import urllib.error

BASE_URL = "http://localhost:8080"


def send_request(path, method="GET", data=None, headers=None):
    url = f"{BASE_URL}{path}"
    req_data = json.dumps(data).encode() if data else None
    req_headers = headers or {}
    if data:
        req_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=req_data, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    except urllib.error.URLError as e:
        print(f"  request failed: {e}")
        return 0


def scenario_steady(duration, rate):
    """Constant low-rate traffic for baseline measurements."""
    print(f"[steady] {rate} rps for {duration}s")
    interval = 1.0 / rate
    end = time.time() + duration
    count = 0
    while time.time() < end:
        send_request("/checkout")
        count += 1
        time.sleep(interval)
    print(f"[steady] sent {count} requests")


def scenario_spike(duration, rate):
    """Sudden 10x traffic spike after 30s of baseline."""
    baseline_duration = min(30, duration // 3)
    spike_duration = duration - baseline_duration
    print(f"[spike] baseline {rate} rps for {baseline_duration}s, then {rate * 10} rps for {spike_duration}s")

    interval = 1.0 / rate
    end = time.time() + baseline_duration
    count = 0
    while time.time() < end:
        send_request("/checkout")
        count += 1
        time.sleep(interval)
    print(f"[spike] baseline phase: {count} requests")

    spike_interval = 1.0 / (rate * 10)
    end = time.time() + spike_duration
    spike_count = 0
    while time.time() < end:
        send_request("/checkout")
        spike_count += 1
        time.sleep(spike_interval)
    print(f"[spike] spike phase: {spike_count} requests")


def scenario_backpressure(duration, rate):
    """Sustained high volume to overwhelm collector buffers."""
    effective_rate = max(rate, 50)
    print(f"[backpressure] {effective_rate} rps for {duration}s with burst endpoints")
    interval = 1.0 / effective_rate
    end = time.time() + duration
    count = 0
    while time.time() < end:
        if count % 10 == 0:
            send_request("/burst", method="POST", data={"count": 200})
        else:
            send_request("/checkout")
        count += 1
        time.sleep(interval)
    print(f"[backpressure] sent {count} requests")


def scenario_multi_tenant(duration, rate):
    """Mixed tenant traffic with different tiers."""
    tenants = [
        {"id": "enterprise-a", "tier": "premium"},
        {"id": "enterprise-b", "tier": "premium"},
        {"id": "startup-1", "tier": "standard"},
        {"id": "startup-2", "tier": "standard"},
        {"id": "free-user-1", "tier": "free"},
        {"id": "free-user-2", "tier": "free"},
        {"id": "free-user-3", "tier": "free"},
    ]
    print(f"[multi-tenant] {rate} rps for {duration}s across {len(tenants)} tenants")
    interval = 1.0 / rate
    end = time.time() + duration
    count = 0
    while time.time() < end:
        tenant = random.choice(tenants)
        headers = {
            "X-Tenant-ID": tenant["id"],
            "X-Tenant-Tier": tenant["tier"],
        }
        send_request("/multi-tenant", headers=headers)
        count += 1
        time.sleep(interval)
    print(f"[multi-tenant] sent {count} requests")


def scenario_hot_trace(duration, rate):
    """Large batch jobs creating trace hot spots.

    Alternates between normal checkout traffic and large batch jobs
    that generate thousands of spans in a single trace.
    """
    print(f"[hot-trace] {rate} rps baseline + batch jobs every 15s for {duration}s")
    interval = 1.0 / rate
    end = time.time() + duration
    last_batch = time.time()
    count = 0
    batches = 0
    while time.time() < end:
        now = time.time()
        if now - last_batch >= 15:
            span_count = random.choice([500, 1000, 2000])
            print(f"  batch job: {span_count} spans")
            send_request("/batch-job", method="POST", data={"span_count": span_count})
            last_batch = now
            batches += 1
        else:
            send_request("/checkout")
        count += 1
        time.sleep(interval)
    print(f"[hot-trace] sent {count} requests + {batches} batch jobs")


def scenario_failover(duration, rate):
    """Alternating bursts to test gateway failover behavior.

    Sends traffic in waves: 10s burst, 5s pause, repeat.
    Pause windows simulate gateway restarts.
    """
    print(f"[failover] burst/pause pattern at {rate * 5} rps for {duration}s")
    end = time.time() + duration
    count = 0
    burst_interval = 1.0 / (rate * 5)
    while time.time() < end:
        burst_end = time.time() + 10
        while time.time() < min(burst_end, end):
            send_request("/checkout")
            count += 1
            time.sleep(burst_interval)
        if time.time() < end:
            print(f"  pause (simulating gateway restart)...")
            time.sleep(5)
    print(f"[failover] sent {count} requests")


SCENARIOS = {
    "steady": scenario_steady,
    "spike": scenario_spike,
    "backpressure": scenario_backpressure,
    "multi-tenant": scenario_multi_tenant,
    "hot-trace": scenario_hot_trace,
    "failover": scenario_failover,
}


def main():
    parser = argparse.ArgumentParser(description="Load generator for Chapter 3 collector testing")
    parser.add_argument("--scenario", required=True, choices=SCENARIOS.keys(),
                        help="Traffic scenario to run")
    parser.add_argument("--duration", type=int, default=60,
                        help="Duration in seconds (default: 60)")
    parser.add_argument("--rate", type=int, default=10,
                        help="Base request rate per second (default: 10)")
    args = parser.parse_args()

    print(f"Target: {BASE_URL}")
    print(f"Scenario: {args.scenario}, Duration: {args.duration}s, Base rate: {args.rate} rps\n")

    try:
        send_request("/health")
        print("Health check passed\n")
    except Exception:
        print("WARNING: health check failed, app may not be running\n", file=sys.stderr)

    SCENARIOS[args.scenario](args.duration, args.rate)
    print("\nDone.")


if __name__ == "__main__":
    main()
