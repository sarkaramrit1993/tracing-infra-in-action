"""Unit tests for the pure-Python helpers inside assembly_job.

Skips the PyFlink runtime (which requires Java) and exercises only the
span-explode and trace-id extraction logic. Verifies the keyed-state input
shape: every span turns into one record, each record carries the trace_id
that the key_by operator will hash on.
"""

import sys
import time
import types
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from opentelemetry.proto.trace.v1.trace_pb2 import TracesData
from opentelemetry.proto.common.v1.common_pb2 import KeyValue, AnyValue, InstrumentationScope
from opentelemetry.proto.resource.v1.resource_pb2 import Resource


def _load_assembly_module():
    """Import assembly_job for its pure-Python helpers without a live PyFlink.

    PyFlink needs a JVM, which the unit environment does not have, so we stub
    the pyflink.* import tree with empty module objects before importing. The
    helpers under test (_explode_payload, _trace_id_of, _event_time_ms_of) touch
    only protobuf, so the stubs are never exercised. Importing the real module
    (rather than slicing source text) means the tests break loudly if a helper
    signature changes.
    """
    import importlib.util

    class _AnyMeta(type):
        """Metaclass so the stand-in works as a base class, a callable, an
        attribute root, and a subscript target. Module-level pyflink
        expressions (Types.PICKLED_BYTE_ARRAY(), OutputTag(...),
        `class X(ProcessFunction)`, enum lookups) all resolve without a JVM.
        The helpers under test never touch these objects."""

        def __getattr__(cls, _name):
            return _Any

        def __getitem__(cls, _key):
            return _Any

    class _Any(metaclass=_AnyMeta):
        def __init__(self, *_args, **_kwargs):
            pass

        def __getattr__(self, _name):
            return _Any()

        def __call__(self, *_args, **_kwargs):
            return _Any()

    pyflink_mods = [
        "pyflink",
        "pyflink.common",
        "pyflink.common.serialization",
        "pyflink.common.typeinfo",
        "pyflink.datastream",
        "pyflink.datastream.connectors",
        "pyflink.datastream.connectors.kafka",
        "pyflink.datastream.state",
    ]
    saved = {name: sys.modules.get(name) for name in pyflink_mods}
    try:
        for name in pyflink_mods:
            stub = types.ModuleType(name)
            # Any attribute access returns the permissive _Any class so the
            # module-level `from pyflink... import X` lines resolve and X works
            # as a callable or a base class.
            stub.__getattr__ = lambda _attr: _Any  # type: ignore[attr-defined]
            sys.modules[name] = stub
        spec = importlib.util.spec_from_file_location(
            "_assembly_job_under_test",
            Path(__file__).parent / "assembly_job.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for name, mod in saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod


def _build_payload(trace_ids: list, spans_per_trace: int = 2) -> bytes:
    td = TracesData()
    rs = td.resource_spans.add()
    rs.resource.CopyFrom(Resource(attributes=[
        KeyValue(key="service.name", value=AnyValue(string_value="t")),
    ]))
    ss = rs.scope_spans.add()
    ss.scope.CopyFrom(InstrumentationScope(name="t", version="v1"))
    now = int(time.time() * 1e9)
    for trace_id in trace_ids:
        for i in range(spans_per_trace):
            s = ss.spans.add()
            s.trace_id = trace_id
            s.span_id = bytes([i]) * 8
            s.name = f"op-{i}"
            s.start_time_unix_nano = now + i * 1_000_000
            s.end_time_unix_nano = now + (i + 1) * 1_000_000
    return td.SerializeToString()


class AssemblyHelpersTests(unittest.TestCase):
    """Tests pure-python helpers from assembly_job without loading PyFlink."""

    @classmethod
    def setUpClass(cls):
        mod = _load_assembly_module()
        cls.explode = staticmethod(mod._explode_payload)
        cls.trace_id_of = staticmethod(mod._trace_id_of)
        cls.event_time_ms_of = staticmethod(mod._event_time_ms_of)

    def test_explode_emits_one_record_per_span(self):
        tid_a = b"\xaa" * 16
        tid_b = b"\xbb" * 16
        payload = _build_payload([tid_a, tid_b], spans_per_trace=3)
        records = list(self.explode(payload))
        self.assertEqual(len(records), 6)

    def test_explode_preserves_trace_id(self):
        tid_a = b"\xaa" * 16
        tid_b = b"\xbb" * 16
        payload = _build_payload([tid_a, tid_b], spans_per_trace=2)
        records = list(self.explode(payload))
        trace_ids = {self.trace_id_of(r) for r in records}
        self.assertEqual(trace_ids, {"aa" * 16, "bb" * 16})

    def test_event_time_ms_extraction(self):
        tid = b"\xcc" * 16
        payload = _build_payload([tid], spans_per_trace=1)
        records = list(self.explode(payload))
        ts_ms = self.event_time_ms_of(records[0])
        self.assertGreater(ts_ms, 1_700_000_000_000)


class _FakeValueState:
    """In-memory stand-in for a Flink ValueState."""

    def __init__(self, initial=None):
        self._v = initial

    def value(self):
        return self._v

    def update(self, v):
        self._v = v

    def clear(self):
        self._v = None


class _FakeListState:
    def __init__(self):
        self._items = []

    def add(self, v):
        self._items.append(v)

    def get(self):
        return list(self._items)

    def clear(self):
        self._items = []


class _FakeTimerService:
    def __init__(self, watermark=0):
        self._watermark = watermark
        self.registered = []

    def current_watermark(self):
        return self._watermark

    def register_event_time_timer(self, t):
        self.registered.append(t)


class _FakeCtx:
    def __init__(self, ts, watermark=0):
        self._ts = ts
        self._svc = _FakeTimerService(watermark)

    def timestamp(self):
        return self._ts

    def timer_service(self):
        return self._svc


class TraceAssemblerLogicTests(unittest.TestCase):
    """Exercises the atomicity-critical control flow of TraceAssembler without
    a JVM by wiring fake keyed-state and timer-service objects onto a bare
    instance. Covers the emitted-tombstone guard (defect 1) and _merge_spans."""

    @classmethod
    def setUpClass(cls):
        cls.mod = _load_assembly_module()

    def _new_assembler(self, emitted=False, watermark=0):
        TraceAssembler = self.mod.TraceAssembler
        a = TraceAssembler.__new__(TraceAssembler)
        a.spans = _FakeListState()
        a.fire_at = _FakeValueState(None)
        a.emitted = _FakeValueState(emitted)
        return a

    def test_emitted_trace_rejects_restraggler_no_reemit(self):
        # A span arriving after the trace shipped (tombstone set) and NOT
        # classified late must route to the late side output, never re-buffer,
        # never register a timer. This is the headline atomicity guarantee.
        a = self._new_assembler(emitted=True)
        ctx = _FakeCtx(ts=10_000, watermark=0)  # watermark 0 => not "late"
        out = list(a.process_element(b"straggler", ctx))
        self.assertEqual(len(out), 1)
        tag, payload = out[0]
        self.assertEqual(payload, b"straggler")
        self.assertEqual(a.spans.get(), [])
        self.assertEqual(ctx.timer_service().registered, [])

    def test_first_span_buffers_and_registers_timer(self):
        a = self._new_assembler(emitted=False)
        ctx = _FakeCtx(ts=5_000, watermark=0)
        out = list(a.process_element(b"span-1", ctx))
        self.assertEqual(out, [])
        self.assertEqual(a.spans.get(), [b"span-1"])
        self.assertEqual(len(ctx.timer_service().registered), 1)

    def test_late_span_routes_to_side_output(self):
        a = self._new_assembler(emitted=False)
        # event time (1000) older than watermark (5000) => late
        ctx = _FakeCtx(ts=1_000, watermark=5_000)
        out = list(a.process_element(b"late", ctx))
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0][1], b"late")
        self.assertEqual(a.spans.get(), [])

    def test_merge_reassembles_all_spans(self):
        tid = b"\xee" * 16
        payload = _build_payload([tid], spans_per_trace=3)
        singles = list(self.mod._explode_payload(payload))
        a = self._new_assembler()
        merged = a._merge_spans(singles)
        td = TracesData()
        td.ParseFromString(merged)
        total = sum(len(ss.spans)
                    for rs in td.resource_spans
                    for ss in rs.scope_spans)
        self.assertEqual(total, 3)


if __name__ == "__main__":
    unittest.main()
