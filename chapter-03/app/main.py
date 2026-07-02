"""Sample checkout service for Chapter 3 collector demos."""

import random
import time
from flask import Flask, jsonify, request

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk.resources import Resource

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
    """Multi-span checkout flow for baseline trace generation."""
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

        with tracer.start_as_current_span("fraud_check") as child:
            child.set_attribute("fraud.score", round(random.uniform(0, 1), 3))
            time.sleep(0.04)

    with tracer.start_as_current_span("send_confirmation") as span:
        span.set_attribute("notification.channel", "email")
        time.sleep(0.01)

    return jsonify({"status": "completed", "cart_id": cart_id})


@app.route("/batch-job", methods=["POST"])
def batch_job():
    """Generate N spans in one trace for hot spot testing."""
    data = request.get_json(silent=True) or {}
    span_count = min(int(data.get("span_count", 500)), 5000)

    with tracer.start_as_current_span("batch_job") as root:
        root.set_attribute("batch.span_count", span_count)
        root.set_attribute("batch.type", "promotional")

        for i in range(span_count):
            with tracer.start_as_current_span("process_item") as span:
                span.set_attribute("item.index", i)
                span.set_attribute("item.id", f"item-{random.randint(10000, 99999)}")
                if i % 100 == 0:
                    time.sleep(0.001)

    return jsonify({
        "status": "completed",
        "spans_generated": span_count,
    })


@app.route("/multi-tenant")
def multi_tenant():
    """Sets tenant attributes from headers for routing connector demo."""
    tenant_id = request.headers.get("X-Tenant-ID", "default")
    tenant_tier = request.headers.get("X-Tenant-Tier", "standard")

    with tracer.start_as_current_span("tenant_request") as span:
        span.set_attribute("tenant.id", tenant_id)
        span.set_attribute("tenant.tier", tenant_tier)
        span.set_attribute("http.route", "/multi-tenant")

        with tracer.start_as_current_span("tenant_db_query") as child:
            child.set_attribute("db.system", "postgresql")
            child.set_attribute("db.operation", "SELECT")
            child.set_attribute("tenant.id", tenant_id)
            time.sleep(0.03)

    return jsonify({
        "tenant_id": tenant_id,
        "tenant_tier": tenant_tier,
        "status": "processed",
    })


@app.route("/burst", methods=["POST"])
def burst():
    """Rapid-fire spans for backpressure testing."""
    data = request.get_json(silent=True) or {}
    count = min(int(data.get("count", 100)), 1000)

    for i in range(count):
        with tracer.start_as_current_span("burst_request") as span:
            span.set_attribute("burst.index", i)
            span.set_attribute("burst.total", count)

    return jsonify({
        "status": "completed",
        "spans_generated": count,
    })


@app.route("/orders/<order_id>")
def get_order(order_id):
    """Low-priority spans for the gateway filter demo (Listing 3.7)."""
    with tracer.start_as_current_span("fetch_order") as span:
        span.set_attribute("order.id", order_id)
        span.set_attribute("priority", "low")
        span.set_attribute("order.status",
                           random.choice(["pending", "shipped", "delivered"]))
        time.sleep(0.015)

    return jsonify({"order_id": order_id, "items": random.randint(1, 5)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
