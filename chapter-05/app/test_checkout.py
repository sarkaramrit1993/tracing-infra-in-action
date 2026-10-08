"""Unit test for the trace shape checkout.py emits.

Runs one checkout through Flask's test client and captures every span in
memory. Listing 5.6 can only find an edge where a child span's service.name
differs from its parent's, so the test checks the exact cross-service edges
and the span count every script waits for. No collector, no Kafka.
"""
import sys
import unittest
from collections import Counter
from pathlib import Path

from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

sys.path.insert(0, str(Path(__file__).parent))
import checkout  # noqa: E402

# Stop the real OTLP export before any span is made: there is no collector here.
checkout.processor.shutdown()
EXPORTER = InMemorySpanExporter()
for _provider in checkout.providers.values():
    _provider.add_span_processor(SimpleSpanProcessor(EXPORTER))


def _run(path):
    EXPORTER.clear()
    assert checkout.app.test_client().get(path).status_code == 200
    return EXPORTER.get_finished_spans()


def _edges(spans):
    by_id = {s.context.span_id: s for s in spans}
    edges = Counter()
    for s in spans:
        parent = by_id.get(s.parent.span_id) if s.parent else None
        if parent is None:
            continue
        a, b = parent.resource.attributes["service.name"], s.resource.attributes["service.name"]
        if a != b:
            edges[(a, b)] += 1
    return edges


class CheckoutTraceShape(unittest.TestCase):
    def test_one_checkout_is_one_trace_of_eleven_spans(self):
        spans = _run("/checkout")
        self.assertEqual(len(spans), 11)
        self.assertEqual(len({s.context.trace_id for s in spans}), 1)

    def test_each_downstream_call_crosses_into_the_called_service(self):
        self.assertEqual(_edges(_run("/checkout")), Counter({
            ("checkout-service", "inventory-service"): 1,
            ("checkout-service", "payment-service"): 1,
            ("payment-service", "fraud-service"): 1,
            ("checkout-service", "notification-service"): 1,
        }))

    def test_the_callee_side_is_a_receiving_span(self):
        from opentelemetry.trace import SpanKind
        spans = _run("/checkout")
        received = {s.resource.attributes["service.name"]: s.kind for s in spans
                    if s.resource.attributes["service.name"] != "checkout-service"
                    and s.kind in (SpanKind.SERVER, SpanKind.CONSUMER)}
        self.assertEqual(received, {
            "inventory-service": SpanKind.SERVER,
            "payment-service": SpanKind.SERVER,
            "fraud-service": SpanKind.SERVER,
            "notification-service": SpanKind.CONSUMER,
        })

    def test_the_client_span_names_stay_put(self):
        names = {s.name for s in _run("/checkout")}
        for name in ("GET /checkout", "validate_cart", "inventory.reserve",
                     "payment.charge", "fraud.score", "order.create", "notification.send"):
            self.assertIn(name, names)

    def test_the_slow_lookup_reaches_inventory_too(self):
        self.assertEqual(_edges(_run("/checkout/slow")),
                         Counter({("checkout-service", "inventory-service"): 1}))


if __name__ == "__main__":
    unittest.main()
