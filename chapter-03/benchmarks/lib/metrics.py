"""Collector and Prometheus metrics scraping."""

import time
import requests


def scrape_collector_metrics(metrics_url: str) -> dict[str, float]:
    """Scrape Prometheus-format metrics. Preserves label keys."""
    import re
    resp = requests.get(metrics_url, timeout=5)
    resp.raise_for_status()
    result = {}
    for line in resp.text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        # Split metric name (possibly with labels) from value
        match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)(\{[^}]*\})?\s+(.+)$', line)
        if match:
            name = match.group(1)
            labels = match.group(2) or ""
            key = name + labels
            value = match.group(3).strip()
            try:
                result[key] = float(value)
            except ValueError:
                pass
    return result


def get_metric_value(metrics_url: str, metric_name: str) -> float:
    """Get a metric value, returning 0.0 if not found."""
    metrics = scrape_collector_metrics(metrics_url)
    if metric_name in metrics:
        return metrics[metric_name]
    for key, val in metrics.items():
        if key.startswith(metric_name):
            return val
    return 0.0


def require_metric_value(metrics_url: str, metric_name: str) -> float:
    """Get a metric value, raising ValueError if missing."""
    metrics = scrape_collector_metrics(metrics_url)
    if metric_name in metrics:
        return metrics[metric_name]
    for key, val in metrics.items():
        if key.startswith(metric_name):
            return val
    available = ', '.join(sorted(metrics.keys())[:15])
    raise ValueError(
        f"Metric '{metric_name}' not found at {metrics_url}. "
        f"Available: {available}"
    )


def query_prometheus(prom_url: str, query: str) -> list[dict]:
    """Instant PromQL query."""
    resp = requests.get(f"{prom_url}/api/v1/query", params={"query": query}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if data["status"] != "success":
        raise RuntimeError(f"Prometheus query failed: {data}")
    return data["data"]["result"]


def query_prometheus_range(prom_url: str, query: str, start: float, end: float, step: str = "1s") -> list[dict]:
    """Range PromQL query."""
    resp = requests.get(
        f"{prom_url}/api/v1/query_range",
        params={"query": query, "start": start, "end": end, "step": step},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if data["status"] != "success":
        raise RuntimeError(f"Prometheus range query failed: {data}")
    return data["data"]["result"]


def wait_for_prometheus(prom_url: str, timeout: int = 60):
    """Block until Prometheus responds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(f"{prom_url}/-/ready", timeout=3)
            if resp.status_code == 200:
                return
        except requests.ConnectionError:
            pass
        time.sleep(2)
    raise TimeoutError(f"Prometheus not ready after {timeout}s")


def wait_for_collector(metrics_url: str, timeout: int = 60):
    """Block until a collector's metrics endpoint responds."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(metrics_url, timeout=3)
            if resp.status_code == 200:
                return
        except requests.ConnectionError:
            pass
        time.sleep(2)
    raise TimeoutError(f"Collector not ready at {metrics_url} after {timeout}s")


def get_process_memory_bytes(metrics_url: str) -> float:
    """RSS memory in bytes."""
    val = get_metric_value(metrics_url, "otelcol_process_memory_rss")
    if val > 0:
        return val
    return get_metric_value(metrics_url, "process_resident_memory_bytes")


def get_process_cpu_seconds(metrics_url: str) -> float:
    """Cumulative CPU seconds."""
    val = get_metric_value(metrics_url, "otelcol_process_cpu_seconds")
    if val > 0:
        return val
    return get_metric_value(metrics_url, "process_cpu_seconds_total")


def get_accepted_spans(metrics_url: str) -> float:
    """Total accepted spans across all receivers."""
    metrics = scrape_collector_metrics(metrics_url)
    total = 0.0
    for key, val in metrics.items():
        if "receiver_accepted_spans" in key:
            total += val
    return total


def get_refused_spans(metrics_url: str) -> float:
    """Total refused spans across all receivers."""
    metrics = scrape_collector_metrics(metrics_url)
    total = 0.0
    for key, val in metrics.items():
        if "receiver_refused_spans" in key:
            total += val
    return total


def get_sent_spans(metrics_url: str) -> float:
    """Total exported spans."""
    metrics = scrape_collector_metrics(metrics_url)
    total = 0.0
    for key, val in metrics.items():
        if "exporter_sent_spans" in key:
            total += val
    return total


def get_queue_size(metrics_url: str) -> float:
    """Current exporter queue size."""
    return get_metric_value(metrics_url, "otelcol_exporter_queue_size")


def get_queue_capacity(metrics_url: str) -> float:
    """Exporter queue capacity."""
    return get_metric_value(metrics_url, "otelcol_exporter_queue_capacity")


def get_enqueue_failed_spans(metrics_url: str) -> float:
    """Spans that failed to enqueue (0.148+). Falls back to refused_spans."""
    metrics = scrape_collector_metrics(metrics_url)
    total = 0.0
    for key, val in metrics.items():
        if "enqueue_failed_spans" in key:
            total += val
    if total > 0:
        return total
    # Fallback for older collector versions
    return get_refused_spans(metrics_url)


def get_enqueue_failed_spans_strict(metrics_url: str) -> float:
    """Spans that failed to enqueue — no fallback to refused_spans."""
    metrics = scrape_collector_metrics(metrics_url)
    total = 0.0
    for key, val in metrics.items():
        if "enqueue_failed_spans" in key:
            total += val
    return total
