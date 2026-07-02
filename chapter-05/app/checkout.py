"""
Chapter 5: Checkout producer.

Carries the Chapter 4 multi-step checkout forward and adds nested service spans
that produce a meaningful service graph downstream (checkout -> inventory ->
warehouse, checkout -> payment -> fraud, checkout -> notification). The trace
shape is designed to make Figure 5.7's service graph derivation visible in
ClickHouse and to give Flink's keyed-state assembler something with depth.
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

resource = Resource.create({
    "service.name": "checkout-service",
    "service.version": "1.0.0",
    "deployment.environment": "development",
})

provider = TracerProvider(resource=resource)
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)

app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)


def _simulated_downstream(name: str, kind: SpanKind, duration_s: float, attrs: dict):
    with tracer.start_as_current_span(name, kind=kind) as span:
        for k, v in attrs.items():
            span.set_attribute(k, v)
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
    })

    amount = round(random.uniform(15, 450), 2)
    with tracer.start_as_current_span("payment.charge", kind=SpanKind.CLIENT) as span:
        span.set_attribute("peer.service", "payment-service")
        span.set_attribute("payment.method", "credit_card")
        span.set_attribute("payment.amount", amount)
        span.set_attribute("payment.currency", "USD")
        time.sleep(0.05)

        with tracer.start_as_current_span("fraud.score", kind=SpanKind.CLIENT) as child:
            child.set_attribute("peer.service", "fraud-service")
            score = round(random.uniform(0, 1), 3)
            child.set_attribute("fraud.score", score)
            child.set_attribute("fraud.model_version", "v2.1")
            time.sleep(0.04)
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
    })

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
        time.sleep(delay)

    return jsonify({"status": "completed", "delay_seconds": round(delay, 2)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
