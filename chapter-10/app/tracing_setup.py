"""Shared SDK setup for the two services: OTLP spans out to the edge Collector."""

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def init_tracing():
    provider = TracerProvider(resource=Resource.create({}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    return trace.get_tracer("ch10")


def fails(i):
    """The fixed error pattern: 29 failures in every 1000 requests (2.9%)."""
    return (i * 29) % 1000 < 29
