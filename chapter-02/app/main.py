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

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import Status, StatusCode, Link

# --- OTel Setup (Chapter 2, lines 489-510) ---
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


# --- Health Check ---
@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


# --- Checkout Endpoint (Chapter 2, lines 170-194) ---
# Demonstrates: nested spans, context propagation, business attributes
@app.route("/checkout")
def checkout():
    cart_id = f"cart-{random.randint(1000, 9999)}"

    with tracer.start_as_current_span("validate_cart") as span:
        span.set_attribute("cart.id", cart_id)
        span.set_attribute("cart.items", random.randint(1, 10))
        time.sleep(0.02)

    with tracer.start_as_current_span("check_inventory") as span:
        span.set_attribute("inventory.warehouse", "us-west-2")
        time.sleep(0.03)

    with tracer.start_as_current_span("process_payment") as span:
        span.set_attribute("payment.method", "credit_card")
        span.set_attribute("payment.amount", round(random.uniform(10, 500), 2))
        time.sleep(0.05)

        # Nested span: fraud_check is child of process_payment
        with tracer.start_as_current_span("fraud_check") as child:
            child.set_attribute("fraud.score", round(random.uniform(0, 1), 3))
            time.sleep(0.04)

    with tracer.start_as_current_span("send_confirmation") as span:
        span.set_attribute("notification.channel", "email")
        time.sleep(0.01)

    return jsonify({"status": "completed", "cart_id": cart_id})


# --- User Endpoint (Chapter 2, lines 391-396) ---
# Demonstrates: low-cardinality span naming (user_id in attribute, not span name)
@app.route("/users/<user_id>")
def get_user(user_id):
    with tracer.start_as_current_span("fetch_user_data") as span:
        # High-cardinality value goes in ATTRIBUTE, not span name
        span.set_attribute("user.id", user_id)
        time.sleep(0.02)

    return jsonify({"user_id": user_id, "name": f"User {user_id}"})


# --- Error Endpoint (Chapter 2, lines 300-310) ---
# Demonstrates: error recording with status, attributes, and exception events
@app.route("/error")
def error_endpoint():
    with tracer.start_as_current_span("risky_operation") as span:
        span.set_attribute("operation.type", "database_write")
        try:
            raise ValueError("Database connection timeout")
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, str(e)))
            span.set_attribute("error.type", type(e).__name__)
            span.record_exception(e)
            return jsonify({"error": str(e)}), 500


# --- Batch Endpoint (Chapter 2, Listing 2.6) ---
# Demonstrates: span links for message queue patterns
@app.route("/batch")
def batch_endpoint():
    # Simulate producer spans from separate traces (as if messages
    # arrived from different services, each with its own trace)
    links = []
    for i in range(5):
        producer_tracer = trace.get_tracer(f"producer-{i}")
        with producer_tracer.start_as_current_span(
                f"send_message",
                attributes={"message.id": f"msg-{i}",
                             "messaging.system": "kafka"}) as span:
            ctx = span.get_span_context()
            links.append(Link(ctx, {"message.id": f"msg-{i}"}))

    # Consumer creates a new span linked back to each producer
    with tracer.start_as_current_span(
            "process_batch", links=links) as batch_span:
        batch_span.set_attribute("batch.size", len(links))
        batch_span.set_attribute("messaging.system", "kafka")

        for i, link in enumerate(links):
            with tracer.start_as_current_span(
                    "process_message") as msg_span:
                msg_span.set_attribute("message.id", f"msg-{i}")
                time.sleep(0.01)

    return jsonify({"processed": len(links)})


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
