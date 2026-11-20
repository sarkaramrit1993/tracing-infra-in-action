"""Reads one trace back from the three Kafka topics and prints when its spans arrived.

Runs inside the consumer-clickhouse container (it has confluent-kafka and the
OTLP protobuf classes), fed on stdin by scripts/show-long-run.sh. Argument: trace ID.
"""
import json
import sys
import time
import uuid

from confluent_kafka import Consumer, TopicPartition
from opentelemetry.proto.trace.v1.trace_pb2 import TracesData

TRACE = bytes.fromhex(sys.argv[1])
TOPICS = ["otlp_spans", "traces.assembled", "spans.late"]


def read(topic):
    c = Consumer({
        "bootstrap.servers": "kafka-1:9093",
        "group.id": "timeline-" + uuid.uuid4().hex,
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
    })
    meta = c.list_topics(topic, timeout=10).topics[topic]
    parts = [TopicPartition(topic, p, 0) for p in meta.partitions]
    c.assign(parts)
    out, idle = [], 0
    while idle < 3:
        msg = c.poll(1.0)
        if msg is None:
            idle += 1
            continue
        if msg.error():
            continue
        idle = 0
        if TRACE not in msg.value():
            continue
        td = TracesData()
        td.ParseFromString(msg.value())
        spans = [s for rs in td.resource_spans for ss in rs.scope_spans for s in ss.spans
                 if s.trace_id == TRACE]
        if spans:
            out.append({"kafka_ms": msg.timestamp()[1], "spans": [
                {"name": s.name, "start_ms": s.start_time_unix_nano // 1_000_000,
                 "end_ms": s.end_time_unix_nano // 1_000_000} for s in spans]})
    c.close()
    return sorted(out, key=lambda r: r["kafka_ms"])


print(json.dumps({t: read(t) for t in TOPICS}))
