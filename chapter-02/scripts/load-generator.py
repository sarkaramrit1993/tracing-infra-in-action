#!/usr/bin/env python3
"""
Load Generator for Chapter 2 Demo

Generates realistic traffic patterns to populate Jaeger with traces.

Usage:
    python load-generator.py              # Default: 60 seconds, medium load
    python load-generator.py --duration 120 --rate high
    python load-generator.py --rate low   # Gentle load for demos
"""

import argparse
import random
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

BASE_URL = "http://localhost:8080"

ENDPOINTS = {
    "checkout": {"weight": 30, "path": "/checkout"},
    "user": {"weight": 25, "path": "/users/{user_id}"},
    "order": {"weight": 20, "path": "/orders/{order_id}"},
    "error": {"weight": 10, "path": "/error"},
    "slow": {"weight": 10, "path": "/slow"},
    "batch": {"weight": 5, "path": "/batch"},
}

RATES = {
    "low": {"rps": 1, "workers": 2},
    "medium": {"rps": 5, "workers": 5},
    "high": {"rps": 20, "workers": 10},
}

stats = {"success": 0, "error": 0, "total": 0}


def make_request(endpoint_name: str) -> None:
    """Make a single request to the specified endpoint."""
    endpoint = ENDPOINTS[endpoint_name]
    path = endpoint["path"]

    # Replace path parameters with random values
    if "{user_id}" in path:
        path = path.replace("{user_id}", str(random.randint(1000, 9999)))
    if "{order_id}" in path:
        path = path.replace("{order_id}", f"ORD-{random.randint(10000, 99999)}")

    url = f"{BASE_URL}{path}"

    try:
        response = requests.get(url, timeout=10)
        stats["total"] += 1
        if response.status_code < 500:
            stats["success"] += 1
        else:
            stats["error"] += 1
    except requests.RequestException:
        stats["total"] += 1
        stats["error"] += 1


def select_endpoint() -> str:
    """Select an endpoint based on weights."""
    total_weight = sum(e["weight"] for e in ENDPOINTS.values())
    r = random.randint(1, total_weight)

    cumulative = 0
    for name, config in ENDPOINTS.items():
        cumulative += config["weight"]
        if r <= cumulative:
            return name

    return "checkout"


def run_load(duration: int, rate: str) -> None:
    """Run load generation for specified duration."""
    config = RATES[rate]
    rps = config["rps"]
    workers = config["workers"]
    delay = 1.0 / rps

    print(f"\n{'='*50}")
    print(f"Load Generator - Chapter 2 Demo")
    print(f"{'='*50}")
    print(f"Target URL: {BASE_URL}")
    print(f"Duration: {duration}s | Rate: {rate} ({rps} req/s)")
    print(f"Workers: {workers}")
    print(f"{'='*50}\n")

    # Check if service is up
    try:
        requests.get(f"{BASE_URL}/health", timeout=5)
        print("[OK] Service is healthy\n")
    except requests.RequestException:
        print("[ERROR] Service not available. Run: docker-compose up")
        return

    start_time = time.time()
    end_time = start_time + duration

    print(f"Started at {datetime.now().strftime('%H:%M:%S')}")
    print("Generating traffic...\n")

    with ThreadPoolExecutor(max_workers=workers) as executor:
        while time.time() < end_time:
            endpoint = select_endpoint()
            executor.submit(make_request, endpoint)
            time.sleep(delay + random.uniform(-delay*0.2, delay*0.2))

            # Progress update every 10 seconds
            elapsed = int(time.time() - start_time)
            if elapsed > 0 and elapsed % 10 == 0 and stats["total"] % 10 == 0:
                print(f"  [{elapsed}s] Requests: {stats['total']} "
                      f"(success: {stats['success']}, errors: {stats['error']})")

    print(f"\n{'='*50}")
    print("COMPLETE")
    print(f"{'='*50}")
    print(f"Total requests: {stats['total']}")
    print(f"Successful: {stats['success']}")
    print(f"Errors: {stats['error']}")
    print(f"\nView traces at: http://localhost:16686")
    print(f"  - Search for service: checkout-service")
    print(f"  - Look for operations: GET /checkout, fetch_user_data, etc.")


def main():
    parser = argparse.ArgumentParser(description="Load generator for Chapter 2")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--rate", choices=["low", "medium", "high"], default="medium")
    args = parser.parse_args()

    run_load(args.duration, args.rate)


if __name__ == "__main__":
    main()
