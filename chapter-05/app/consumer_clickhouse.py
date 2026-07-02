"""
Chapter 5: Storage-time consumer for the ClickHouse path.

Reads raw OTLP-encoded spans from the otlp_spans Kafka topic, decodes the
protobuf, flattens each span into one row, and batch-inserts into
tracing.otel_traces. Every span lands as soon as it arrives. Trace assembly
is deferred to query time, which is the store-then-stitch contract.

The atomicity imperative shapes the commit policy: offsets commit only after
the batch insert returns success, so a crash mid-batch replays the same span
range on restart. A duplicate (span_id, trace_id) row is fine because the
MergeTree dedups on the sorting key during background merges.
"""

import os
import time
import signal
import logging
from typing import List

from opentelemetry.proto.trace.v1.trace_pb2 import TracesData, Span
from opentelemetry.proto.common.v1.common_pb2 import KeyValue, AnyValue

logging.basicConfig(level=logging.INFO, format="%(asctime)s [storage] %(message)s")
log = logging.getLogger("storage")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-1:9093,kafka-2:9093,kafka-3:9093")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "otlp_spans")
KAFKA_GROUP = os.environ.get("KAFKA_GROUP", "trace-storage-clickhouse")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
BATCH_TIMEOUT_S = float(os.environ.get("BATCH_TIMEOUT_S", "2.0"))

SPAN_KIND_NAMES = {
    0: "SPAN_KIND_UNSPECIFIED",
    1: "SPAN_KIND_INTERNAL",
    2: "SPAN_KIND_SERVER",
    3: "SPAN_KIND_CLIENT",
    4: "SPAN_KIND_PRODUCER",
    5: "SPAN_KIND_CONSUMER",
}

STATUS_CODE_NAMES = {
    0: "STATUS_CODE_UNSET",
    1: "STATUS_CODE_OK",
    2: "STATUS_CODE_ERROR",
}

INSERT_SQL = """
    INSERT INTO tracing.otel_traces (
        timestamp, trace_id, span_id, parent_span_id, trace_state,
        span_name, span_kind, service_name, resource_attributes,
        scope_name, scope_version, span_attributes, duration,
        status_code, status_message,
        events_timestamp, events_name, events_attributes,
        links_trace_id, links_span_id, links_trace_state, links_attributes
    ) VALUES
"""


def _kv_to_pair(kv: KeyValue) -> tuple:
    v: AnyValue = kv.value
    if v.HasField("string_value"):
        return kv.key, v.string_value
    if v.HasField("int_value"):
        return kv.key, str(v.int_value)
    if v.HasField("double_value"):
        return kv.key, str(v.double_value)
    if v.HasField("bool_value"):
        return kv.key, "true" if v.bool_value else "false"
    return kv.key, ""


def _attrs_to_map(attrs) -> dict:
    return dict(_kv_to_pair(kv) for kv in attrs)


def _span_to_row(span: Span, resource_attrs: dict, scope_name: str, scope_version: str) -> tuple:
    service_name = resource_attrs.get("service.name", "unknown")
    start_ns = span.start_time_unix_nano
    end_ns = span.end_time_unix_nano
    duration = max(0, end_ns - start_ns)

    return (
        start_ns,
        span.trace_id.hex(),
        span.span_id.hex(),
        span.parent_span_id.hex() if span.parent_span_id else "",
        span.trace_state or "",
        span.name,
        SPAN_KIND_NAMES.get(span.kind, "SPAN_KIND_UNSPECIFIED"),
        service_name,
        resource_attrs,
        scope_name,
        scope_version,
        _attrs_to_map(span.attributes),
        duration,
        STATUS_CODE_NAMES.get(span.status.code, "STATUS_CODE_UNSET"),
        span.status.message or "",
        [e.time_unix_nano for e in span.events],
        [e.name for e in span.events],
        [_attrs_to_map(e.attributes) for e in span.events],
        [l.trace_id.hex() for l in span.links],
        [l.span_id.hex() for l in span.links],
        [l.trace_state or "" for l in span.links],
        [_attrs_to_map(l.attributes) for l in span.links],
    )


def _decode_message(payload: bytes) -> List[tuple]:
    rows: List[tuple] = []
    td = TracesData()
    td.ParseFromString(payload)
    for rs in td.resource_spans:
        resource_attrs = _attrs_to_map(rs.resource.attributes) if rs.HasField("resource") else {}
        for ss in rs.scope_spans:
            scope_name = ss.scope.name if ss.HasField("scope") else ""
            scope_version = ss.scope.version if ss.HasField("scope") else ""
            for span in ss.spans:
                rows.append(_span_to_row(span, resource_attrs, scope_name, scope_version))
    return rows


def main():
    from confluent_kafka import Consumer, TopicPartition
    from clickhouse_driver import Client

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([KAFKA_TOPIC])

    client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database="tracing")
    log.info("consuming from %s, writing to %s:%d/tracing", KAFKA_TOPIC, CLICKHOUSE_HOST, CLICKHOUSE_PORT)

    batch: List[tuple] = []
    # Highest offset consumed per (topic, partition) for messages now in batch.
    # The commit offset is this max + 1, so a restart resumes at the first
    # unflushed message and never skips a span.
    pending_offsets: dict = {}
    last_flush = time.time()
    running = True

    def _stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    def _commit_targets():
        return [
            TopicPartition(tp[0], tp[1], offset + 1)
            for tp, offset in pending_offsets.items()
        ]

    while running:
        msg = consumer.poll(timeout=1.0)
        now = time.time()
        if msg is None:
            if batch and now - last_flush >= BATCH_TIMEOUT_S:
                if _flush(client, consumer, batch, _commit_targets()):
                    batch = []
                    pending_offsets.clear()
                    last_flush = now
            continue
        if msg.error():
            log.warning("kafka error: %s", msg.error())
            continue

        try:
            rows = _decode_message(msg.value())
        except Exception as e:
            log.warning("decode failed, skipping message: %s", e)
            continue
        batch.extend(rows)
        pending_offsets[(msg.topic(), msg.partition())] = msg.offset()

        if len(batch) >= BATCH_SIZE:
            if _flush(client, consumer, batch, _commit_targets()):
                batch = []
                pending_offsets.clear()
                last_flush = now

    if batch and _flush(client, consumer, batch,
                        [TopicPartition(tp[0], tp[1], offset + 1)
                         for tp, offset in pending_offsets.items()]):
        pending_offsets.clear()
    consumer.close()


def _flush(client, consumer, batch: List[tuple], commit_offsets) -> bool:
    """Insert the batch, then commit only the offsets it covers.

    Returns True only when the insert succeeded and offsets were committed.
    On failure we keep the batch and skip the commit, so a restart replays the
    same span range. That honors the atomicity imperative: no span is committed
    as stored until it actually landed in ClickHouse.
    """
    try:
        client.execute(INSERT_SQL, batch, types_check=True)
    except Exception as e:
        log.error("insert failed, holding %d rows for retry: %s", len(batch), e)
        return False
    consumer.commit(offsets=commit_offsets, asynchronous=False)
    log.info("flushed %d rows", len(batch))
    return True


if __name__ == "__main__":
    main()
