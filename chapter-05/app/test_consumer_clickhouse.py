"""Unit tests for consumer_clickhouse decode and row mapping.

Builds a synthetic OTLP TracesData payload (one resource, one scope, three
spans) and asserts the decoder produces the expected wide-table rows. No
Kafka, no ClickHouse, just the protobuf->row mapping.
"""

import os
import sys
import time
import unittest
from pathlib import Path

# stub env so the module import works
os.environ.setdefault("KAFKA_BOOTSTRAP", "localhost:9092")

sys.path.insert(0, str(Path(__file__).parent))
import consumer_clickhouse as cc

from opentelemetry.proto.trace.v1.trace_pb2 import TracesData, Span, ResourceSpans, ScopeSpans, Status
from opentelemetry.proto.common.v1.common_pb2 import KeyValue, AnyValue, InstrumentationScope
from opentelemetry.proto.resource.v1.resource_pb2 import Resource


def _kv(k: str, sv: str) -> KeyValue:
    return KeyValue(key=k, value=AnyValue(string_value=sv))


def _build_payload() -> bytes:
    td = TracesData()
    rs = td.resource_spans.add()
    rs.resource.CopyFrom(Resource(attributes=[
        _kv("service.name", "checkout-service"),
        _kv("service.version", "1.0.0"),
    ]))
    ss = rs.scope_spans.add()
    ss.scope.CopyFrom(InstrumentationScope(name="checkout", version="v1"))
    now = int(time.time() * 1e9)
    for i in range(3):
        s = ss.spans.add()
        s.trace_id = b"\xab" * 16
        s.span_id = bytes([i]) * 8
        s.parent_span_id = bytes([0]) * 8 if i > 0 else b""
        s.name = f"op_{i}"
        s.kind = Span.SPAN_KIND_SERVER
        s.start_time_unix_nano = now
        s.end_time_unix_nano = now + 5_000_000
        s.attributes.append(_kv("http.method", "GET"))
        s.status.code = Status.STATUS_CODE_OK
    return td.SerializeToString()


class DecodeTests(unittest.TestCase):
    def test_decode_three_spans(self):
        payload = _build_payload()
        rows = cc._decode_message(payload)
        self.assertEqual(len(rows), 3)
        for row in rows:
            (ts, trace_id, _sid, _pid, _state, _name, kind, service, *_) = row
            self.assertEqual(trace_id, "ab" * 16)
            self.assertEqual(kind, "SPAN_KIND_SERVER")
            self.assertEqual(service, "checkout-service")
            # Timestamp is integer nanoseconds (DateTime64(9)), not float
            # seconds: float seconds would lose sub-microsecond precision.
            self.assertIsInstance(ts, int)

    def test_timestamp_preserves_nanoseconds(self):
        # A nanosecond value that is not on a microsecond boundary survives
        # only if we keep integer ns. start_ns / 1e9 would round it away.
        td = TracesData()
        rs = td.resource_spans.add()
        rs.resource.CopyFrom(Resource(attributes=[_kv("service.name", "svc")]))
        ss = rs.scope_spans.add()
        ss.scope.CopyFrom(InstrumentationScope(name="s", version="v1"))
        s = ss.spans.add()
        s.trace_id = b"\xab" * 16
        s.span_id = b"\x01" * 8
        s.name = "op"
        s.kind = Span.SPAN_KIND_SERVER
        odd_ns = 1_700_000_000_123_456_789
        s.start_time_unix_nano = odd_ns
        s.end_time_unix_nano = odd_ns + 1
        s.status.code = Status.STATUS_CODE_OK
        rows = cc._decode_message(td.SerializeToString())
        self.assertEqual(rows[0][0], odd_ns)

    def test_duration_is_non_negative(self):
        payload = _build_payload()
        rows = cc._decode_message(payload)
        durations = [r[12] for r in rows]
        for d in durations:
            self.assertGreaterEqual(d, 0)
            self.assertEqual(d, 5_000_000)

    def test_attribute_map_string_typed(self):
        payload = _build_payload()
        rows = cc._decode_message(payload)
        span_attrs = rows[0][11]
        self.assertEqual(span_attrs.get("http.method"), "GET")
        for v in span_attrs.values():
            self.assertIsInstance(v, str)


if __name__ == "__main__":
    unittest.main()
