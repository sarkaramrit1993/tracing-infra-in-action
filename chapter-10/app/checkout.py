"""Checkout service: one SERVER span, one CLIENT span to payment.

/checkout?i=N is the measured request: it calls payment, and request N fails on
the fixed pattern in tracing_setup.fails. /cart is the background request: it
makes no downstream call, so dashboards stay populated without touching the
counts of the measured workload.
"""

import random
import time
import urllib.error
import urllib.request

from flask import Flask, jsonify, request
from opentelemetry import trace
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.propagate import inject
from opentelemetry.trace import SpanKind, Status, StatusCode

from tracing_setup import init_tracing

tracer = init_tracing()
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app, excluded_urls="health")

USER_POOL = 6000


def user_for(i):
    return f"user-{(i * 7919) % USER_POOL}"


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


@app.route("/checkout")
def checkout():
    i = int(request.args.get("i", "0"))
    user = user_for(i)
    trace.get_current_span().set_attribute("user.id", user)
    time.sleep(random.uniform(0.005, 0.015))
    with tracer.start_as_current_span("payment.charge", kind=SpanKind.CLIENT) as span:
        headers = {}
        inject(headers)
        req = urllib.request.Request(
            f"http://payment-service:8082/charge?i={i}&user={user}", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                resp.read()
        except urllib.error.HTTPError as exc:
            span.set_status(Status(StatusCode.ERROR, f"payment answered {exc.code}"))
            return jsonify({"status": "failed"}), 500
    return jsonify({"status": "completed"})


@app.route("/cart")
def cart():
    time.sleep(random.uniform(0.002, 0.008))
    return jsonify({"items": random.randint(1, 8)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, threaded=True)
