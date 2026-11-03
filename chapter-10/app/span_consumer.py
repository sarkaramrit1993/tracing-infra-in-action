"""Reads spans off Kafka two ways.

otlp_spans (every span)    -> spans.flat, one JSON row per span, for the Flink job
otlp_sampled (survivors)   -> tracing.otel_traces in ClickHouse

Flink SQL has no OTLP format, so the first path decodes the protobuf and writes
the fields the windowed query reads. The second is chapter 9's storage consumer:
adjusted_count is left at its column default of 1.0.
"""

import json
import logging
import os
import signal
import threading
import time
from datetime import datetime, timezone

from opentelemetry.proto.trace.v1.trace_pb2 import Span, TracesData

logging.basicConfig(level=logging.INFO, format="%(asctime)s [span-consumer] %(message)s")
log = logging.getLogger("span-consumer")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9093")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
BATCH_SIZE = 1000
BATCH_TIMEOUT_S = 2.0

STATUS_NAMES = {0: "STATUS_CODE_UNSET", 1: "STATUS_CODE_OK", 2: "STATUS_CODE_ERROR"}
INSERT_SQL = """
    INSERT INTO tracing.otel_traces (
        timestamp, trace_id, span_id, parent_span_id, service_name, span_name,
        status_code, duration_ns, attributes
    ) VALUES
"""
stop = threading.Event()


def attr_pairs(attrs):
    for kv in attrs:
        v = kv.value
        if v.HasField("string_value"):
            yield kv.key, v.string_value
        elif v.HasField("int_value"):
            yield kv.key, str(v.int_value)
        elif v.HasField("double_value"):
            yield kv.key, str(v.double_value)
        elif v.HasField("bool_value"):
            yield kv.key, "true" if v.bool_value else "false"
        else:
            yield kv.key, ""


def spans_of(payload):
    td = TracesData()
    td.ParseFromString(payload)
    for rs in td.resource_spans:
        service = dict(attr_pairs(rs.resource.attributes)).get("service.name", "unknown")
        for ss in rs.scope_spans:
            for span in ss.spans:
                yield service, span


def event_time(ns):
    dt = datetime.fromtimestamp(ns // 1_000_000_000, tz=timezone.utc)
    return f"{dt:%Y-%m-%d %H:%M:%S}.{(ns // 1_000_000) % 1000:03d}"


def flatten(service, span):
    return {
        "service_name": service,
        "span_name": span.name,
        "span_kind": Span.SpanKind.Name(span.kind).replace("SPAN_KIND_", ""),
        "status_code": STATUS_NAMES.get(span.status.code, "STATUS_CODE_UNSET"),
        "duration_ms": max(0, span.end_time_unix_nano - span.start_time_unix_nano) / 1e6,
        "event_time": event_time(span.start_time_unix_nano),
    }


def clickhouse_row(service, span):
    parent = span.parent_span_id.hex()
    if not parent or set(parent) == {"0"}:
        parent = ""
    return (
        span.start_time_unix_nano, span.trace_id.hex(), span.span_id.hex(), parent,
        service, span.name, STATUS_NAMES.get(span.status.code, "STATUS_CODE_UNSET"),
        max(0, span.end_time_unix_nano - span.start_time_unix_nano),
        dict(attr_pairs(span.attributes)),
    )


def consumer_for(topic, group):
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group,
                  "auto.offset.reset": "earliest", "enable.auto.commit": True})
    c.subscribe([topic])
    return c


def run_flat():
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50,
                         "compression.type": "lz4"})
    consumer = consumer_for("otlp_spans", "span-flattener")
    sent, logged = 0, time.time()
    while not stop.is_set():
        msg = consumer.poll(1.0)
        producer.poll(0)
        if msg is None or msg.error():
            continue
        for service, span in spans_of(msg.value()):
            producer.produce("spans.flat", json.dumps(flatten(service, span)).encode())
            sent += 1
        if time.time() - logged >= 30:
            log.info("flattened %d spans so far", sent)
            logged = time.time()
    producer.flush(10)
    consumer.close()


def run_store():
    from clickhouse_driver import Client
    client = Client(host=CLICKHOUSE_HOST, port=9000, database="tracing")
    consumer = consumer_for("otlp_sampled", "span-store")
    batch, last = [], time.time()

    def flush():
        nonlocal batch, last
        if batch:
            client.execute(INSERT_SQL, batch, types_check=True)
            log.info("stored %d rows", len(batch))
        batch, last = [], time.time()

    while not stop.is_set():
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error():
            batch.extend(clickhouse_row(s, sp) for s, sp in spans_of(msg.value()))
        if len(batch) >= BATCH_SIZE or (batch and time.time() - last >= BATCH_TIMEOUT_S):
            flush()
    flush()
    consumer.close()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    threads = [threading.Thread(target=run_flat), threading.Thread(target=run_store)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
