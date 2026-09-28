"""
Chapter 2: OpenTelemetry Fundamentals - Sample Application

Demonstrates:
- Three-layer OTel architecture (SDK → Collector → Backend)
- Context propagation across spans
- Proper cardinality handling
- Error recording
- Span links for batch processing
"""

import random
import time
from flask import Flask, jsonify

from opentelemetry.context import Context
from opentelemetry.propagate import inject
from opentelemetry.trace import Status, StatusCode

# --- OTel Setup ---
# Listing 2.9: Automatic instrumentation setup with Flask
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor)
from opentelemetry.exporter.otlp.proto.grpc \
    .trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import (
    FlaskInstrumentor)
from opentelemetry.sdk.resources import Resource

resource = Resource.create({
    "service.name": "checkout-service",
    "service.version": "1.0.0",
})

provider = TracerProvider(resource=resource)
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(provider)

app = Flask(__name__)
FlaskInstrumentor().instrument_app(app)

tracer = trace.get_tracer(__name__)


# --- Health Check ---
@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


# --- Checkout Endpoint ---
# Listing 2.3: Context propagation in a checkout endpoint
# Demonstrates: nested spans, context propagation, business attributes

def validate(cart_id, item_count):
    time.sleep(0.02)


def reserve_stock(warehouse):
    time.sleep(0.03)


def charge_card(amount):
    time.sleep(0.05)


def score_fraud_risk(amount):
    time.sleep(0.04)
    return round(random.uniform(0, 1), 3)


def send_email(cart_id):
    time.sleep(0.01)


# validate(), charge_card() and the rest stand in for
# the downstream services a real checkout would call
@app.route("/checkout")
def checkout():
    cart_id = f"cart-{random.randint(1000, 9999)}"
    item_count = random.randint(1, 10)
    with tracer.start_as_current_span(
            "validate_cart") as span:
        span.set_attribute("cart.id", cart_id)
        span.set_attribute("cart.items", item_count)
        validate(cart_id, item_count)

    with tracer.start_as_current_span(
            "check_inventory") as span:
        span.set_attribute(
            "inventory.warehouse", "us-west-2")
        reserve_stock("us-west-2")

    with tracer.start_as_current_span(
            "process_payment") as span:
        amount = round(random.uniform(10, 500), 2)
        span.set_attribute(
            "payment.method", "credit_card")
        span.set_attribute("payment.amount", amount)
        charge_card(amount)
        with tracer.start_as_current_span(
                "fraud_check") as child:
            child.set_attribute(
                "fraud.score", score_fraud_risk(amount))

    with tracer.start_as_current_span(
            "send_confirmation") as span:
        span.set_attribute(
            "notification.channel", "email")
        send_email(cart_id)

    return jsonify({"status": "completed", "cart_id": cart_id})


# --- User Endpoint ---
# Listing 2.8: Attribute placement for high-cardinality values
# Demonstrates: low-cardinality span naming (user_id in attribute, not span name)
@app.route("/users/<user_id>")
def get_user(user_id):
    with tracer.start_as_current_span(
            "fetch_user_data") as span:
        span.set_attribute("user.id", user_id)
        time.sleep(0.02)
    return jsonify(
        {"user_id": user_id,
         "name": f"User {user_id}"})


# --- Error Endpoint ---
# Listing 2.7: Recording errors with status, attributes, and events
# Demonstrates: error recording with status, attributes, and exception events
@app.route("/error")
def error_endpoint():
    with tracer.start_as_current_span(
            "risky_operation") as span:
        span.set_attribute(
            "operation.type", "database_write")
        try:
            raise ValueError(
                "Database connection timeout")
        except Exception as e:
            span.set_status(
                Status(StatusCode.ERROR, str(e)))
            span.set_attribute(
                "error.type", type(e).__name__)
            span.record_exception(e)
            return jsonify({"error": str(e)}), 500


# --- Batch Endpoint ---
class Message:
    def __init__(self, message_id, headers):
        self.id = message_id
        self.headers = headers


def process_message(msg):
    time.sleep(0.01)


# Listing 2.6: Batch consumer with span links
# Demonstrates: span links for message queue patterns
from opentelemetry import trace
from opentelemetry.trace import Link
from opentelemetry.propagate import extract

def process_batch(messages):
    """Process a batch of messages, linking
    to originating traces."""

    # Collect links to all originating traces
    links = []
    for msg in messages:
        ctx = extract(msg.headers)
        span_ctx = trace.get_current_span(
            ctx).get_span_context()
        if span_ctx.is_valid:
            links.append(Link(span_ctx))

    # Create batch processing span with links
    with tracer.start_as_current_span(
        "process_batch",
        links=links
    ) as batch_span:
        batch_span.set_attribute(
            "batch.size", len(messages))
        batch_span.set_attribute(
            "messaging.system", "kafka")

        for msg in messages:
            process_message(msg)


# Each producer span starts from an empty context, so every message
# comes from its own trace, as if sent by a different service, and
# carries that trace's context in its headers.
@app.route("/batch")
def batch_endpoint():
    messages = []
    for i in range(5):
        headers = {}
        producer_tracer = trace.get_tracer(f"producer-{i}")
        with producer_tracer.start_as_current_span(
                "send_message", context=Context(),
                attributes={"message.id": f"msg-{i}",
                            "messaging.system": "kafka"}):
            inject(headers)
        messages.append(Message(f"msg-{i}", headers))

    process_batch(messages)
    return jsonify({"processed": len(messages)})


# --- Slow Endpoint ---
# Demonstrates: identifying slow operations in traces
@app.route("/slow")
def slow_endpoint():
    with tracer.start_as_current_span("slow_database_query") as span:
        span.set_attribute("db.system", "postgresql")
        span.set_attribute("db.operation", "SELECT")
        # Simulate slow query
        delay = random.uniform(0.5, 2.0)
        span.set_attribute("db.query.duration_estimate", delay)
        time.sleep(delay)

    return jsonify({"status": "completed", "delay": delay})


# --- Cardinality Demo Endpoints ---
# Shows contrast between good and bad cardinality patterns

@app.route("/orders/<order_id>")
def get_order(order_id):
    """GOOD: order_id in attribute, not span name"""
    with tracer.start_as_current_span("fetch_order") as span:
        span.set_attribute("order.id", order_id)
        span.set_attribute("order.status", random.choice(["pending", "shipped", "delivered"]))
        time.sleep(0.015)

    return jsonify({"order_id": order_id, "items": random.randint(1, 5)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
