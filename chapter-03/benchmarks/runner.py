"""Benchmark orchestrator. Runs benchmarks in order (backpressure last) and averages across N runs."""

import argparse
import copy
import os
import sys
import time
import traceback
import math
from datetime import datetime, timezone

from lib.report import save_results

# Ordered so backpressure (which pauses Jaeger) runs last
RUN_ORDER = ["memory", "routing", "throughput", "persistent_queue", "backpressure"]

BENCHMARKS = {
    "routing": ("bench_routing", "Routing distribution (single-gateway vs trace-aware)"),
    "throughput": ("bench_throughput", "Gateway throughput ceiling"),
    "backpressure": ("bench_backpressure", "Backpressure cascade timeline"),
    "memory": ("bench_memory", "Collector memory (sidecar vs daemonset)"),
    "persistent_queue": ("bench_persistent_queue", "Persistent queue throughput"),
}


def run_benchmark(name: str) -> dict:
    """Import and run a single benchmark."""
    module_name, description = BENCHMARKS[name]
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"{'='*60}\n")

    module = __import__(module_name)
    start = time.time()
    result = module.run()
    elapsed = time.time() - start

    print(f"\n[{name}] Completed in {elapsed:.1f}s")
    return result


def average_numeric(values: list) -> float:
    """Mean of numeric values, skipping None."""
    nums = [v for v in values if isinstance(v, (int, float))]
    return sum(nums) / len(nums) if nums else 0.0


def stddev_numeric(values: list) -> float:
    """Sample standard deviation."""
    nums = [v for v in values if isinstance(v, (int, float))]
    if len(nums) < 2:
        return 0.0
    mean = sum(nums) / len(nums)
    variance = sum((x - mean) ** 2 for x in nums) / (len(nums) - 1)
    return math.sqrt(variance)


def cv_numeric(values: list) -> float:
    """Coefficient of variation (stddev/mean * 100)."""
    mean = average_numeric(values)
    if mean == 0:
        return 0.0
    return (stddev_numeric(values) / abs(mean)) * 100


def average_results(all_runs: list[dict]) -> dict:
    """Average numeric fields across runs."""
    if not all_runs:
        return {}
    if len(all_runs) == 1:
        return all_runs[0]

    averaged = {}
    keys = set()
    for run in all_runs:
        keys.update(run.keys())

    for key in keys:
        values = [run.get(key) for run in all_runs if key in run]
        if not values:
            continue

        # Skip error entries
        if any(isinstance(v, dict) and "error" in v for v in values):
            averaged[key] = values[0]
            continue

        sample = values[0]
        if isinstance(sample, (int, float)):
            averaged[key] = average_numeric(values)
        elif isinstance(sample, dict):
            # Recurse into nested dicts
            averaged[key] = average_results(values)
        elif isinstance(sample, list):
            # Average lists element-wise (e.g., throughput steps, memory levels)
            if all(isinstance(v, list) and len(v) == len(sample) for v in values):
                averaged[key] = []
                for i in range(len(sample)):
                    elements = [v[i] for v in values]
                    if all(isinstance(e, dict) for e in elements):
                        averaged[key].append(average_results(elements))
                    elif all(isinstance(e, (int, float)) for e in elements):
                        averaged[key].append(average_numeric(elements))
                    else:
                        averaged[key].append(elements[0])
            else:
                averaged[key] = sample
        else:
            averaged[key] = sample

    return averaged


def warmup_collectors():
    """Send warmup traffic to all collectors so Go runtime is hot."""
    from lib.load import run_telemetrygen_blocking
    endpoints = {
        "otel-agent": os.environ.get("OTLP_GRPC_ENDPOINT", "otel-agent:4317"),
        "otel-agent-random": os.environ.get("AGENT_RANDOM_GRPC_ENDPOINT", "otel-agent-random:4317"),
        "otel-agent-memory": os.environ.get("AGENT_MEMORY_GRPC_ENDPOINT", "otel-agent-memory:4317"),
        "otel-agent-pq": os.environ.get("AGENT_PQ_GRPC_ENDPOINT", "otel-agent-pq:4317"),
        "otel-gateway-1": os.environ.get("GATEWAY1_GRPC_ENDPOINT", "otel-gateway-1:4317"),
    }
    print(f"[warmup] Sending 10s of warm-up traffic to {len(endpoints)} collectors at 10K/s...")
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _warmup_one(args):
        name, ep = args
        print(f"  [warmup] {name} -> {ep}")
        run_telemetrygen_blocking(ep, rate=10000, duration=10, workers=4, child_spans=1)
        return name

    with ThreadPoolExecutor(max_workers=len(endpoints)) as pool:
        futures = {pool.submit(_warmup_one, item): item[0] for item in endpoints.items()}
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                print(f"  [warmup] {futures[fut]} failed: {e}")
    time.sleep(5)
    print("[warmup] Done.")


def main():
    parser = argparse.ArgumentParser(description="Chapter 3 benchmark suite")
    parser.add_argument("--all", action="store_true", help="Run all benchmarks")
    parser.add_argument("--bench", nargs="+", choices=list(BENCHMARKS.keys()), help="Run specific benchmarks")
    parser.add_argument("--runs", type=int, default=1, help="Number of runs to average (default: 1)")
    parser.add_argument("--output-dir", default="/app/results", help="Output directory for results")
    args = parser.parse_args()

    if not args.all and not args.bench:
        parser.print_help()
        sys.exit(1)

    requested = set(BENCHMARKS.keys()) if args.all else set(args.bench)
    to_run = [b for b in RUN_ORDER if b in requested]

    warmup_collectors()

    num_runs = max(1, args.runs)
    all_run_results = {name: [] for name in to_run}
    total_start = time.time()

    for run_idx in range(num_runs):
        if num_runs > 1:
            print(f"\n{'#'*60}")
            print(f"# Run {run_idx + 1} of {num_runs}")
            print(f"{'#'*60}")

        for name in to_run:
            try:
                result = run_benchmark(name)
                all_run_results[name].append(result)
            except Exception as e:
                print(f"\n[{name}] FAILED: {e}")
                traceback.print_exc()
                all_run_results[name].append({"error": str(e)})

        # Wait between runs for metrics to settle and Go GC to stabilize
        if run_idx < num_runs - 1:
            print("\n[runner] Waiting 30s between runs for metrics and GC to settle...")
            time.sleep(30)

    # Average results across runs and preserve per-run data
    results = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "platform": "Docker",
        "benchmarks_run": to_run,
        "num_runs": num_runs,
    }

    for name in to_run:
        valid_runs = [r for r in all_run_results[name] if "error" not in r]
        if valid_runs:
            results[name] = average_results(valid_runs)
            results[name]["_runs_succeeded"] = len(valid_runs)
            results[name]["_runs_failed"] = len(all_run_results[name]) - len(valid_runs)
            # Preserve individual run data for outlier analysis (deepcopy to avoid circular refs)
            results[name]["_individual_runs"] = copy.deepcopy(valid_runs)
        else:
            results[name] = {"error": "all runs failed"}

    total_elapsed = time.time() - total_start
    results["total_duration_sec"] = total_elapsed

    # Generate per-run stats for key metrics
    stats = _compute_stats(results, all_run_results, to_run)
    results["_stats"] = stats

    json_path, md_path = save_results(results, args.output_dir)
    print(f"\n{'='*60}")
    print(f"All benchmarks completed in {total_elapsed:.0f}s ({num_runs} runs)")
    print(f"Results: {json_path}")
    print(f"Report:  {md_path}")
    print(f"{'='*60}")


def _compute_stats(results: dict, all_run_results: dict, to_run: list) -> dict:
    """Compute per-metric stats across runs."""
    stats = {}

    if "throughput" in to_run:
        valid = [r for r in all_run_results["throughput"] if "error" not in r]
        if valid:
            # Extract peak throughput from each run
            peaks = []
            for run in valid:
                steps = run.get("steps", [])
                if steps:
                    run_peak = max(s.get("actual_throughput", 0) for s in steps)
                    peaks.append(run_peak)
            stats["throughput_peak"] = {
                "mean": average_numeric(peaks),
                "stddev": stddev_numeric(peaks),
                "cv_pct": cv_numeric(peaks),
                "values": peaks,
            }
            # Per-step throughput stats
            step_stats = []
            num_steps = len(valid[0].get("steps", []))
            for i in range(num_steps):
                step_throughputs = [r["steps"][i].get("actual_throughput", 0)
                                   for r in valid if len(r.get("steps", [])) > i]
                target = valid[0]["steps"][i]["target_rate"] if valid[0].get("steps") else 0
                step_stats.append({
                    "target_rate": target,
                    "mean": average_numeric(step_throughputs),
                    "stddev": stddev_numeric(step_throughputs),
                    "cv_pct": cv_numeric(step_throughputs),
                    "values": step_throughputs,
                })
            stats["throughput_per_step"] = step_stats

    if "memory" in to_run:
        valid = [r for r in all_run_results["memory"] if "error" not in r]
        if valid:
            for component in ["agent", "gateway"]:
                for load_idx, load_level in enumerate(valid[0].get(component, [])):
                    key = f"{component}_{load_level['load']}_mb"
                    values = [r[component][load_idx]["rss_mb"]
                              for r in valid if len(r.get(component, [])) > load_idx]
                    stats[key] = {
                        "mean": average_numeric(values),
                        "stddev": stddev_numeric(values),
                        "cv_pct": cv_numeric(values),
                        "values": values,
                    }

    if "persistent_queue" in to_run:
        valid = [r for r in all_run_results["persistent_queue"] if "error" not in r]
        if valid:
            mem_vals = [r.get("memory_throughput", 0) for r in valid]
            disk_vals = [r.get("disk_throughput", 0) for r in valid]
            delta_vals = [r.get("throughput_delta_pct", 0) for r in valid]
            stats["pq_memory_throughput"] = {
                "mean": average_numeric(mem_vals),
                "stddev": stddev_numeric(mem_vals),
                "cv_pct": cv_numeric(mem_vals),
                "values": mem_vals,
            }
            stats["pq_disk_throughput"] = {
                "mean": average_numeric(disk_vals),
                "stddev": stddev_numeric(disk_vals),
                "cv_pct": cv_numeric(disk_vals),
                "values": disk_vals,
            }
            stats["pq_delta_pct"] = {
                "mean": average_numeric(delta_vals),
                "stddev": stddev_numeric(delta_vals),
                "cv_pct": cv_numeric(delta_vals),
                "values": delta_vals,
            }

    if "backpressure" in to_run:
        valid = [r for r in all_run_results["backpressure"] if "error" not in r]
        if valid:
            for metric in ["t1_gateway_queue_full", "t2_gateway_refusing",
                           "gateway_enqueue_failed_spans"]:
                values = [r.get(metric, 0) for r in valid]
                stats[f"bp_{metric}"] = {
                    "mean": average_numeric(values),
                    "stddev": stddev_numeric(values),
                    "cv_pct": cv_numeric(values),
                    "values": values,
                }

    return stats


if __name__ == "__main__":
    main()
