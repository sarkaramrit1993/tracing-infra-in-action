"""
Chapter 7: storage-time consumer for the listing 7.1 ClickHouse table.

Trimmed from chapter-05/app/consumer_clickhouse.py to the eight columns of
listing 7.1: timestamp, trace_id, span_id, service_name, span_name, status_code,
duration_ns, attributes. (Chapter 5 used a wider SigNoz-style row; chapter 7's
listing is the compression-focused narrow schema.)

Reads raw OTLP-encoded spans from the otlp_spans Kafka topic, decodes the
protobuf, flattens each span into one listing-7.1 row, and batch-inserts into
tracing.otel_traces. Offsets commit only after the insert returns success, so a
crash mid-batch replays the same span range on restart (the atomicity imperative
from chapter 5).
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

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9093")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "otlp_spans")
KAFKA_GROUP = os.environ.get("KAFKA_GROUP", "trace-storage-clickhouse")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
BATCH_TIMEOUT_S = float(os.environ.get("BATCH_TIMEOUT_S", "2.0"))

STATUS_CODE_NAMES = {
    0: "STATUS_CODE_UNSET",
    1: "STATUS_CODE_OK",
    2: "STATUS_CODE_ERROR",
}

# Columns match listing 7.1 exactly. tenant_id is omitted here: it has a column
# DEFAULT ('tenant_a') applied by tenancy.sql, so live ingest tags all rows as
# tenant_a unless the ingest path is extended to set it from the principal
# (section 7.5.2's "validate tenant_id at ingest" rule). The README seeds an
# explicit tenant_b row through tenancy.sql to demonstrate the row policy.
INSERT_SQL = """
    INSERT INTO tracing.otel_traces (
        timestamp, trace_id, span_id, service_name, span_name,
        status_code, duration_ns, attributes
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


def _span_to_row(span: Span, service_name: str) -> tuple:
    start_ns = span.start_time_unix_nano
    end_ns = span.end_time_unix_nano
    duration_ns = max(0, end_ns - start_ns)
    return (
        start_ns,
        span.trace_id.hex(),
        span.span_id.hex(),
        service_name,
        span.name,
        STATUS_CODE_NAMES.get(span.status.code, "STATUS_CODE_UNSET"),
        duration_ns,
        _attrs_to_map(span.attributes),
    )


def _decode_message(payload: bytes) -> List[tuple]:
    rows: List[tuple] = []
    td = TracesData()
    td.ParseFromString(payload)
    for rs in td.resource_spans:
        resource_attrs = _attrs_to_map(rs.resource.attributes) if rs.HasField("resource") else {}
        service_name = resource_attrs.get("service.name", "unknown")
        for ss in rs.scope_spans:
            for span in ss.spans:
                rows.append(_span_to_row(span, service_name))
    return rows


def _flush(client, consumer, batch: List[tuple], commit_offsets) -> bool:
    """Insert the batch, then commit only the offsets it covers.

    Returns True only when the insert succeeded and offsets were committed. On
    failure we keep the batch and skip the commit, so a restart replays the same
    span range. No span is committed as stored until it actually landed.
    """
    try:
        client.execute(INSERT_SQL, batch, types_check=True)
    except Exception as e:
        log.error("insert failed, holding %d rows for retry: %s", len(batch), e)
        return False
    consumer.commit(offsets=commit_offsets, asynchronous=False)
    log.info("flushed %d rows", len(batch))
    return True


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

    if batch and _flush(client, consumer, batch, _commit_targets()):
        pending_offsets.clear()
    consumer.close()


if __name__ == "__main__":
    main()
