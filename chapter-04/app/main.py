"""
Chapter 4: Scaling Trace Collection -- Checkout Service

Demonstrates the OTel pipeline with Kafka-buffered trace collection:
- Spans flow: App → OTel Agent → OTel Gateway → Kafka → OTel Consumer → Jaeger
- Multi-step checkout with nested spans
- Simulated downstream service calls
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
from opentelemetry.trace import Status, StatusCode

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


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


@app.route("/checkout")
def checkout():
    """Multi-step checkout producing nested spans through the Kafka pipeline."""
    cart_id = f"cart-{random.randint(1000, 9999)}"
    item_count = random.randint(1, 8)

    with tracer.start_as_current_span("validate_cart") as span:
        span.set_attribute("cart.id", cart_id)
        span.set_attribute("cart.item_count", item_count)
        time.sleep(0.02)

    with tracer.start_as_current_span("reserve_inventory") as span:
        span.set_attribute("inventory.warehouse", "us-east-1")
        span.set_attribute("inventory.items_reserved", item_count)
        time.sleep(0.03)

    with tracer.start_as_current_span("process_payment") as span:
        amount = round(random.uniform(15, 450), 2)
        span.set_attribute("payment.method", "credit_card")
        span.set_attribute("payment.amount", amount)
        span.set_attribute("payment.currency", "USD")
        time.sleep(0.05)

        with tracer.start_as_current_span("fraud_check") as child:
            score = round(random.uniform(0, 1), 3)
            child.set_attribute("fraud.score", score)
            child.set_attribute("fraud.model_version", "v2.1")
            time.sleep(0.04)
            if score > 0.95:
                child.set_status(Status(StatusCode.ERROR, "High fraud risk"))

    with tracer.start_as_current_span("create_order") as span:
        order_id = f"ord-{random.randint(10000, 99999)}"
        span.set_attribute("order.id", order_id)
        span.set_attribute("order.total", amount)
        time.sleep(0.02)

    with tracer.start_as_current_span("send_confirmation") as span:
        span.set_attribute("notification.channel", "email")
        span.set_attribute("notification.order_id", order_id)
        time.sleep(0.01)

    return jsonify({
        "status": "completed",
        "cart_id": cart_id,
        "order_id": order_id,
        "amount": amount,
    })


@app.route("/checkout/slow")
def checkout_slow():
    """Simulates a slow checkout for latency investigation."""
    with tracer.start_as_current_span("slow_inventory_lookup") as span:
        span.set_attribute("inventory.warehouse", "eu-west-1")
        delay = random.uniform(1.0, 3.0)
        span.set_attribute("lookup.duration_estimate", delay)
        time.sleep(delay)

    return jsonify({"status": "completed", "delay_seconds": round(delay, 2)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
