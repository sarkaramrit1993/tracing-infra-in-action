"""
Chapter 5: Storage-time consumer for the Jaeger path.

The docker-compose stack runs this path via the OpenTelemetry Collector's
built-in Kafka receiver (see collector/consumer-config.yaml). That collector
is the production-grade choice: it handles offset commit, cooperative
rebalancing, and backpressure internally.

This Python file exists as a reference implementation for readers who want to
see what the Collector does under the hood and run the storage-time path
outside the Collector binary. It reads OTLP-encoded spans from Kafka, forwards
them to the Jaeger OTLP gRPC endpoint, and commits offsets after the forward
returns success.
"""

import os
import signal
import logging

from opentelemetry.proto.trace.v1.trace_pb2 import TracesData

logging.basicConfig(level=logging.INFO, format="%(asctime)s [jaeger-sink] %(message)s")
log = logging.getLogger("jaeger-sink")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-1:9093,kafka-2:9093,kafka-3:9093")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "otlp_spans")
KAFKA_GROUP = os.environ.get("KAFKA_GROUP", "trace-storage-jaeger-py")
JAEGER_ENDPOINT = os.environ.get("JAEGER_ENDPOINT", "jaeger:4317")


def main():
    import grpc
    from confluent_kafka import Consumer
    from opentelemetry.proto.collector.trace.v1 import trace_service_pb2_grpc
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
        ExportTraceServiceRequest,
    )

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])

    channel = grpc.insecure_channel(JAEGER_ENDPOINT)
    stub = trace_service_pb2_grpc.TraceServiceStub(channel)
    log.info("consuming from %s, forwarding to %s", KAFKA_TOPIC, JAEGER_ENDPOINT)

    running = True

    def _stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            log.warning("kafka error: %s", msg.error())
            continue

        try:
            td = TracesData()
            td.ParseFromString(msg.value())
            req = ExportTraceServiceRequest(resource_spans=td.resource_spans)
            stub.Export(req, timeout=5)
        except Exception as e:
            # Do not commit this message. The offset stays put, so a restart
            # replays it. We continue to the next message rather than blocking
            # the partition; on a clean restart the uncommitted range is reread.
            log.warning("forward failed, offset not committed (replays on restart): %s", e)
            continue
        consumer.commit(message=msg, asynchronous=False)

    consumer.close()


if __name__ == "__main__":
    main()
