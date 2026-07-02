"""JSON and Markdown report generation."""

import json
import os
from datetime import datetime, timezone


def save_results(results: dict, output_dir: str = "/app/results"):
    """Save results as JSON + Markdown. Timestamped to avoid overwrites."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M")

    json_path = os.path.join(output_dir, f"benchmark-{timestamp}.json")
    md_path = os.path.join(output_dir, f"benchmark-{timestamp}.md")

    with open(json_path, "w") as f:
        json.dump(results, f, indent=2, default=str)

    md = generate_markdown(results)
    with open(md_path, "w") as f:
        f.write(md)

    return json_path, md_path


def generate_markdown(results: dict) -> str:
    """Convert results dict to Markdown."""
    num_runs = results.get("num_runs", 1)
    lines = [
        "# Chapter 3 Benchmark Results",
        f"\n**Date**: {results.get('timestamp', 'N/A')}",
        f"**Platform**: {results.get('platform', 'Docker')}",
        f"**Runs**: {num_runs}",
        "",
        "> **Note**: These benchmarks run in Docker Compose on a shared host.",
        "> Absolute numbers vary by hardware. Relative comparisons within the same run are valid.",
        "",
    ]

    if "routing" in results and "error" not in results["routing"]:
        r = results["routing"]
        lines.extend([
            "## Bench 1: Routing Distribution",
            "",
            "| Metric | Single gateway | Trace-aware (traceID) |",
            "|---|---|---|",
            f"| GW1 spans | {r.get('single_gw_gw1_spans', 0):,.0f} ({r.get('single_gw_gw1_pct', 0):.0f}%) | {r.get('traceaware_gw1_spans', 0):,.0f} ({r.get('traceaware_gw1_pct', 0):.0f}%) |",
            f"| GW2 spans | {r.get('single_gw_gw2_spans', 0):,.0f} ({r.get('single_gw_gw2_pct', 0):.0f}%) | {r.get('traceaware_gw2_spans', 0):,.0f} ({r.get('traceaware_gw2_pct', 0):.0f}%) |",
            f"| Gini (distribution skew) | {r.get('single_gw_gini', 0):.3f} | {r.get('traceaware_gini', 0):.3f} |",
            f"| Skew ratio (max/min) | {r.get('single_gw_skew_ratio', 0):.1f}x | {r.get('traceaware_skew_ratio', 0):.1f}x |",
            "",
            "> This benchmark demonstrates span distribution. Downstream cost savings depend on your stream processor.",
            "",
        ])

    if "throughput" in results and "error" not in results["throughput"]:
        t = results["throughput"]
        lines.extend([
            "## Bench 2: Gateway Throughput",
            "",
            "| Target Rate | Accepted | Sent (exported) | Actual Throughput | Wall Time | Memory MB |",
            "|---|---|---|---|---|---|",
        ])
        for step in t.get("steps", []):
            lines.append(
                f"| {step['target_rate']:,} | {step['accepted']:,.0f} | {step.get('sent', 0):,.0f} "
                f"| {step.get('actual_throughput', 0):,.0f}/s | {step.get('wall_time_sec', 0):.1f}s | {step['memory_mb']:.0f} |"
            )
        lines.extend([
            "",
            f"> Peak export throughput: **{t.get('peak_throughput', 0):,.0f} spans/sec**",
            f"> Sustainable receive rate: **{t.get('sustainable_rate', 0):,} spans/sec**",
            "",
            "> Note: Gateway exports to Jaeger (real backend). Export throughput is bounded by",
            "> both gateway capacity and backend ingest rate. Sustainable rate = highest target",
            "> where queue drain time stays under 50% of send time.",
            "",
        ])

    if "backpressure" in results and "error" not in results["backpressure"]:
        b = results["backpressure"]
        lines.extend([
            "## Bench 3: Backpressure Cascade",
            "",
            f"- **T0**: Load begins (0s)",
            f"- **T1**: Gateway queue filling at **+{b.get('t1_gateway_queue_full', 0.0):.1f}s**",
            f"- **T2**: Gateway refusing spans at **+{b.get('t2_gateway_refusing', 0.0):.1f}s**",
            f"- **T3**: Agent queue (500) full at **+{b.get('t3_agent_queue_full', 0.0):.1f}s**",
            f"- **T4**: Agent refusing (SDK drops) at **+{b.get('t4_agent_refusing', 0.0):.1f}s**",
            "",
            f"> Cascade duration (T1->T4): **{b.get('cascade_duration_sec', 0.0):.1f}s**",
            f"- **Span loss**: {b.get('span_loss_pct', 0.0):.1f}% of spans refused during cascade",
            "",
        ])

    if "memory" in results and "error" not in results["memory"]:
        m = results["memory"]
        lines.extend([
            "## Bench 4: Collector Memory",
            "",
            "### Agent (sidecar-like)",
            "| Load | RSS (MB) |",
            "|---|---|",
        ])
        for level in m.get("agent", []):
            lines.append(f"| {level['load']} | {level['rss_mb']:.0f} |")
        lines.extend([
            "",
            "### Gateway (daemonset-like)",
            "| Load | RSS (MB) |",
            "|---|---|",
        ])
        for level in m.get("gateway", []):
            lines.append(f"| {level['load']} | {level['rss_mb']:.0f} |")
        lines.append("")

    if "persistent_queue" in results and "error" not in results["persistent_queue"]:
        p = results["persistent_queue"]
        lines.extend([
            "## Bench 5: Persistent Queue Throughput",
            "",
            f"- **Memory queue throughput**: {p.get('memory_throughput', 0):,.0f} spans/sec",
            f"- **Disk queue throughput**: {p.get('disk_throughput', 0):,.0f} spans/sec",
            f"- **Throughput delta**: {p.get('throughput_delta_pct', 0):.1f}%",
            "",
            f"> Persistent queue reduced throughput by **{p.get('throughput_delta_pct', 0):.1f}%**",
            "",
        ])

    # Stats section
    stats = results.get("_stats", {})
    if stats:
        lines.extend([
            "## Statistical Summary (per-run breakdown)",
            "",
        ])

        if "throughput_peak" in stats:
            s = stats["throughput_peak"]
            lines.extend([
                "### Throughput Peak (spans/sec)",
                f"- **Mean**: {s['mean']:,.0f}",
                f"- **Stddev**: {s['stddev']:,.0f}",
                f"- **CV**: {s['cv_pct']:.1f}%",
                f"- **Per-run values**: {', '.join(f'{v:,.0f}' for v in s['values'])}",
                "",
            ])

        if "throughput_per_step" in stats:
            lines.extend([
                "### Throughput Per Step",
                "| Target Rate | Mean | Stddev | CV% | Per-Run Values |",
                "|---|---|---|---|---|",
            ])
            for step in stats["throughput_per_step"]:
                vals_str = ", ".join(f"{v:,.0f}" for v in step["values"])
                lines.append(
                    f"| {step['target_rate']:,.0f} | {step['mean']:,.0f} | "
                    f"{step['stddev']:,.0f} | {step['cv_pct']:.1f}% | {vals_str} |"
                )
            lines.append("")

        # Memory stats
        mem_keys = [k for k in stats if k.startswith("agent_") or k.startswith("gateway_")]
        if mem_keys:
            lines.extend([
                "### Memory (MB)",
                "| Component | Mean | Stddev | CV% | Per-Run Values |",
                "|---|---|---|---|---|",
            ])
            for k in sorted(mem_keys):
                s = stats[k]
                vals_str = ", ".join(f"{v:.0f}" for v in s["values"])
                lines.append(
                    f"| {k} | {s['mean']:.0f} | {s['stddev']:.1f} | {s['cv_pct']:.1f}% | {vals_str} |"
                )
            lines.append("")

        # PQ stats
        if "pq_delta_pct" in stats:
            s = stats["pq_delta_pct"]
            lines.extend([
                "### Persistent Queue Delta (%)",
                f"- **Mean**: {s['mean']:.1f}%",
                f"- **Stddev**: {s['stddev']:.1f}%",
                f"- **CV**: {s['cv_pct']:.1f}%",
                f"- **Per-run values**: {', '.join(f'{v:.1f}%' for v in s['values'])}",
                "",
            ])

        # Backpressure stats
        bp_keys = [k for k in stats if k.startswith("bp_")]
        if bp_keys:
            lines.extend([
                "### Backpressure Timing",
                "| Metric | Mean | Stddev | CV% | Per-Run Values |",
                "|---|---|---|---|---|",
            ])
            for k in sorted(bp_keys):
                s = stats[k]
                vals_str = ", ".join(f"{v:.2f}" for v in s["values"])
                lines.append(
                    f"| {k} | {s['mean']:.2f} | {s['stddev']:.2f} | {s['cv_pct']:.1f}% | {vals_str} |"
                )
            lines.append("")

    lines.extend([
        "---",
        "*Generated by chapter-03 benchmark suite*",
    ])

    return "\n".join(lines)
