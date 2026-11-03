"""Payment service: the downstream half of the checkout -> payment edge."""

import random
import time

from flask import Flask, jsonify, request
from opentelemetry import trace
from opentelemetry.instrumentation.flask import FlaskInstrumentor

from tracing_setup import fails, init_tracing

init_tracing()
app = Flask(__name__)
FlaskInstrumentor().instrument_app(app, excluded_urls="health")


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


@app.route("/charge")
def charge():
    i = int(request.args.get("i", "0"))
    trace.get_current_span().set_attribute("user.id", request.args.get("user", ""))
    time.sleep(random.uniform(0.010, 0.040))
    if fails(i):
        return jsonify({"status": "declined"}), 500
    return jsonify({"status": "charged"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8082, threaded=True)
