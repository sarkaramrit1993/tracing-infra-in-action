"""
Chapter 5: Checkout producer.

Carries the Chapter 4 multi-step checkout forward. Every downstream call is a
client span in the caller plus a receiving span in the called service, each
under its own service.name, so the trace crosses services the way listing 5.6
expects: checkout -> inventory, checkout -> payment -> fraud, checkout ->
notification. The called services are simulated in this one process with a
TracerProvider each. The trace shape makes Figure 5.8's service graph
derivation visible in ClickHouse and gives Flink's keyed-state assembler
something with depth.
"""

import random
import time
from flask import Flask, jsonify

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import Status, StatusCode, SpanKind

# One processor shared by every provider. The exporter groups spans by their
# resource, so each service keeps its own service.name on the wire, while one
# queue keeps a trace's spans in the same export batches they always rode in.
# Separate queues would flush up to a schedule delay apart, and the late one
# could fall behind Flink's 5-second out-of-order bound.
processor = BatchSpanProcessor(OTLPSpanExporter())

providers = {}
for _service in ("checkout-service", "inventory-service", "payment-service",
                 "fraud-service", "notification-service"):
    providers[_service] = TracerProvider(resource=Resource.create({
        "service.name": _service,
        "service.version": "1.0.0",
        "deployment.environment": "development",
    }))
    providers[_service].add_span_processor(processor)

trace.set_tracer_provider(providers["checkout-service"])

tracer = trace.get_tracer(__name__)
inventory = providers["inventory-service"].get_tracer(__name__)
payment = providers["payment-service"].get_tracer(__name__)
fraud = providers["fraud-service"].get_tracer(__name__)
notification = providers["notification-service"].get_tracer(__name__)

app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)


def _simulated_downstream(name: str, kind: SpanKind, duration_s: float, attrs: dict,
                          callee, handler: str, handler_kind: SpanKind = SpanKind.SERVER):
    with tracer.start_as_current_span(name, kind=kind) as span:
        for k, v in attrs.items():
            span.set_attribute(k, v)
        with callee.start_as_current_span(handler, kind=handler_kind):
            time.sleep(duration_s)


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


@app.route("/checkout")
def checkout():
    cart_id = f"cart-{random.randint(1000, 9999)}"
    item_count = random.randint(1, 8)

    with tracer.start_as_current_span("validate_cart") as span:
        span.set_attribute("cart.id", cart_id)
        span.set_attribute("cart.item_count", item_count)
        time.sleep(0.02)

    _simulated_downstream("inventory.reserve", SpanKind.CLIENT, 0.03, {
        "peer.service": "inventory-service",
        "inventory.warehouse": "us-east-1",
        "inventory.items_reserved": item_count,
    }, inventory, "POST /inventory/reserve")

    amount = round(random.uniform(15, 450), 2)
    with tracer.start_as_current_span("payment.charge", kind=SpanKind.CLIENT) as span:
        span.set_attribute("peer.service", "payment-service")
        span.set_attribute("payment.method", "credit_card")
        span.set_attribute("payment.amount", amount)
        span.set_attribute("payment.currency", "USD")
        with payment.start_as_current_span("POST /payments/charge", kind=SpanKind.SERVER):
            time.sleep(0.05)

            with payment.start_as_current_span("fraud.score", kind=SpanKind.CLIENT) as child:
                child.set_attribute("peer.service", "fraud-service")
                score = round(random.uniform(0, 1), 3)
                child.set_attribute("fraud.score", score)
                child.set_attribute("fraud.model_version", "v2.1")
                with fraud.start_as_current_span("POST /fraud/score", kind=SpanKind.SERVER) as served:
                    time.sleep(0.04)
                    if score > 0.95:
                        served.set_status(Status(StatusCode.ERROR, "High fraud risk"))
                if score > 0.95:
                    child.set_status(Status(StatusCode.ERROR, "High fraud risk"))

    order_id = f"ord-{random.randint(10000, 99999)}"
    with tracer.start_as_current_span("order.create") as span:
        span.set_attribute("order.id", order_id)
        span.set_attribute("order.total", amount)
        time.sleep(0.02)

    _simulated_downstream("notification.send", SpanKind.PRODUCER, 0.01, {
        "peer.service": "notification-service",
        "notification.channel": "email",
        "notification.order_id": order_id,
    }, notification, "process notification", SpanKind.CONSUMER)

    return jsonify({
        "status": "completed",
        "cart_id": cart_id,
        "order_id": order_id,
        "amount": amount,
    })


@app.route("/checkout/slow")
def checkout_slow():
    with tracer.start_as_current_span("inventory.slow_lookup", kind=SpanKind.CLIENT) as span:
        span.set_attribute("peer.service", "inventory-service")
        span.set_attribute("inventory.warehouse", "eu-west-1")
        delay = random.uniform(1.0, 3.0)
        span.set_attribute("lookup.duration_estimate", delay)
        with inventory.start_as_current_span("GET /inventory/lookup", kind=SpanKind.SERVER):
            time.sleep(delay)

    return jsonify({"status": "completed", "delay_seconds": round(delay, 2)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
