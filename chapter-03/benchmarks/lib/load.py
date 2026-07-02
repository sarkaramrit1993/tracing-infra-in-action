"""Span generation via OTel SDK and telemetrygen."""

import subprocess
import time
import uuid
import random
from concurrent.futures import ThreadPoolExecutor

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource


def create_tracer(endpoint: str, service_name: str = "benchmark") -> tuple[trace.Tracer, TracerProvider]:
    """Create a tracer exporting to the given gRPC endpoint."""
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    processor = BatchSpanProcessor(
        exporter,
        max_export_batch_size=512,
        schedule_delay_millis=1000,
        max_queue_size=65536,
    )
    provider.add_span_processor(processor)
    tracer = provider.get_tracer("benchmark")
    return tracer, provider


def send_traces(tracer: trace.Tracer, num_traces: int, spans_per_trace: int = 10):
    """Send num_traces traces, each with spans_per_trace spans."""
    for _ in range(num_traces):
        with tracer.start_as_current_span("root") as root:
            root.set_attribute("trace.id", str(uuid.uuid4()))
            for j in range(spans_per_trace - 1):
                with tracer.start_as_current_span(f"child-{j}") as child:
                    child.set_attribute("item.index", j)
                    child.set_attribute("work.type", random.choice(["db", "http", "compute"]))


def send_spans_at_rate(tracer: trace.Tracer, rate_per_sec: int, duration_sec: int) -> int:
    """Send spans at ~rate_per_sec for duration_sec. Returns total sent."""
    total_sent = 0
    batch_size = max(1, rate_per_sec // 10)
    interval = 1.0 / 10  # 10 batches per second

    end_time = time.time() + duration_sec
    while time.time() < end_time:
        batch_start = time.time()
        traces_needed = max(1, batch_size // 10)
        send_traces(tracer, traces_needed, spans_per_trace=min(10, batch_size))
        total_sent += batch_size
        elapsed = time.time() - batch_start
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    return total_sent


def send_burst(tracer: trace.Tracer, total_spans: int, concurrency: int = 4) -> float:
    """Send total_spans as fast as possible. Returns duration in seconds."""
    spans_per_thread = total_spans // concurrency

    def _worker():
        traces = max(1, spans_per_thread // 10)
        send_traces(tracer, traces, spans_per_trace=10)

    start = time.time()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(_worker) for _ in range(concurrency)]
        for f in futures:
            f.result()
    return time.time() - start


def flush_and_shutdown(provider: TracerProvider, timeout_ms: int = 30000):
    """Flush pending spans and shut down."""
    provider.force_flush(timeout_millis=timeout_ms)
    provider.shutdown()


def run_telemetrygen(
    endpoint: str,
    rate: int,
    duration: int,
    workers: int = 8,
    child_spans: int = 1,
    service_name: str = "bench-telemetrygen",
) -> subprocess.Popen:
    """Start telemetrygen (non-blocking). Returns Popen handle."""
    cmd = [
        "telemetrygen", "traces",
        "--otlp-endpoint", endpoint,
        "--otlp-insecure",
        "--rate", str(rate),
        "--duration", f"{duration}s",
        "--workers", str(workers),
        "--child-spans", str(child_spans),
        "--service", service_name,
    ]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def run_telemetrygen_blocking(
    endpoint: str,
    rate: int,
    duration: int,
    workers: int = 8,
    child_spans: int = 1,
    service_name: str = "bench-telemetrygen",
    timeout: int | None = None,
) -> tuple[int, str, str]:
    """Run telemetrygen, block until done. Returns (rc, stdout, stderr)."""
    proc = run_telemetrygen(endpoint, rate, duration, workers, child_spans, service_name)
    wait_timeout = timeout or (duration + 30)
    try:
        stdout, stderr = proc.communicate(timeout=wait_timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        stdout, stderr = proc.communicate()
    return proc.returncode, stdout.decode(), stderr.decode()
