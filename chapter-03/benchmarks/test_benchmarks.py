"""Tests for benchmark code -- run standalone without chapter text."""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))


def test_metrics_parser_preserves_labels():
    """scrape_collector_metrics must keep label keys intact."""
    from lib.metrics import scrape_collector_metrics
    from unittest.mock import patch, MagicMock

    fake_response = MagicMock()
    fake_response.text = (
        '# HELP receiver_accepted_spans\n'
        'receiver_accepted_spans{transport="grpc"} 5000\n'
        'receiver_accepted_spans{transport="http"} 100\n'
        'simple_metric 42\n'
    )
    fake_response.raise_for_status = MagicMock()

    with patch("lib.metrics.requests.get", return_value=fake_response):
        result = scrape_collector_metrics("http://fake:8888/metrics")

    assert 'receiver_accepted_spans{transport="grpc"}' in result
    assert 'receiver_accepted_spans{transport="http"}' in result
    assert result['receiver_accepted_spans{transport="grpc"}'] == 5000.0
    assert result['receiver_accepted_spans{transport="http"}'] == 100.0
    assert result["simple_metric"] == 42.0


def test_gini_coefficient():
    from bench_routing import gini_coefficient

    assert gini_coefficient([100, 0]) == 0.5
    assert gini_coefficient([50, 50]) == 0.0
    assert gini_coefficient([0, 0]) == 0.0


def test_routing_phase_returns_skew_ratio():
    """run_phase returns skew_ratio, not cpu_per_1k_spans."""
    import bench_routing
    import inspect
    source = inspect.getsource(bench_routing.run_phase)
    assert "skew_ratio" in source
    assert "cpu_per_1k" not in source


def test_report_docker_caveat():
    from lib.report import generate_markdown
    results = {"timestamp": "2026-01-01", "platform": "Docker", "num_runs": 1}
    md = generate_markdown(results)
    assert "Docker Compose" in md
    assert "Absolute numbers vary" in md


def test_report_routing_uses_distribution():
    from lib.report import generate_markdown
    results = {
        "timestamp": "2026-01-01", "platform": "Docker", "num_runs": 1,
        "routing": {
            "single_gw_gini": 1.0, "traceaware_gini": 0.05,
            "single_gw_skew_ratio": 999.0, "traceaware_skew_ratio": 1.1,
            "single_gw_gw1_spans": 100000, "single_gw_gw2_spans": 0,
            "traceaware_gw1_spans": 52000, "traceaware_gw2_spans": 48000,
            "single_gw_gw1_pct": 100, "single_gw_gw2_pct": 0,
            "traceaware_gw1_pct": 52, "traceaware_gw2_pct": 48,
        },
    }
    md = generate_markdown(results)
    assert "cpu_overhead" not in md.lower()
    assert "CPU / 1K" not in md
    assert "Skew ratio" in md


def test_blackhole_configs_infinite_retry():
    base = os.path.join(os.path.dirname(__file__), "..", "collector")
    for name in ["gateway-blackhole-config.yaml", "agent-blackhole-config.yaml"]:
        path = os.path.join(base, name)
        with open(path) as f:
            content = f.read()
        assert "max_elapsed_time: 0s" in content, f"{name} missing infinite retry"
        assert "max_elapsed_time: 30s" not in content


def test_runner_calls_warmup():
    import runner
    import inspect
    source = inspect.getsource(runner.main)
    assert "warmup_collectors()" in source


def test_routing_no_cpu_import():
    with open(os.path.join(os.path.dirname(__file__), "bench_routing.py")) as f:
        source = f.read()
    assert "get_process_cpu_seconds" not in source
    assert "cpu_before" not in source
    assert "cpu_after" not in source


def test_routing_docstrings_say_distribution():
    import bench_routing
    import inspect
    source = inspect.getsource(bench_routing)
    assert "distribution" in source.lower()
    assert "CPU + distribution" not in source
    assert "routing overhead benchmark" not in source


def test_runner_descriptions():
    import runner
    descs = {v[1] for v in runner.BENCHMARKS.values()}
    assert "Routing distribution (single-gateway vs trace-aware)" in descs
    assert "Persistent queue throughput" in descs


def test_warmup_covers_all_collectors():
    import runner
    import inspect
    source = inspect.getsource(runner.warmup_collectors)
    for svc in ["otel-agent", "otel-agent-random", "otel-agent-memory",
                 "otel-agent-pq", "otel-gateway-1"]:
        assert svc in source, f"warmup missing {svc}"


def test_persistent_queue_docstring():
    with open(os.path.join(os.path.dirname(__file__), "bench_persistent_queue.py")) as f:
        source = f.read()
    assert "throughput delta" in source.lower()
    assert "~2ms disk write per span" not in source


def test_backpressure_computes_span_loss():
    with open(os.path.join(os.path.dirname(__file__), "bench_backpressure.py")) as f:
        source = f.read()
    assert "get_accepted_spans" in source
    assert "span_loss_pct" in source


def test_report_includes_span_loss():
    from lib.report import generate_markdown
    results = {
        "timestamp": "2026-01-01", "platform": "Docker", "num_runs": 1,
        "backpressure": {
            "t1_gateway_queue_full": 2.0, "t2_gateway_refusing": 3.0,
            "t3_agent_queue_full": 15.0, "t4_agent_refusing": 20.0,
            "cascade_duration_sec": 18.0, "span_loss_pct": 42.5,
        },
    }
    md = generate_markdown(results)
    assert "Span loss" in md
    assert "42.5%" in md


if __name__ == "__main__":
    test_metrics_parser_preserves_labels()
    test_gini_coefficient()
    test_routing_phase_returns_skew_ratio()
    test_report_docker_caveat()
    test_report_routing_uses_distribution()
    test_blackhole_configs_infinite_retry()
    test_runner_calls_warmup()
    test_routing_no_cpu_import()
    test_routing_docstrings_say_distribution()
    test_runner_descriptions()
    test_warmup_covers_all_collectors()
    test_persistent_queue_docstring()
    test_backpressure_computes_span_loss()
    test_report_includes_span_loss()
    print(f"\nAll 14 tests passed.")
