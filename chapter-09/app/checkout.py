"""
Chapter 9: Checkout producer.

Carries chapter 5's multi-step checkout forward. The trace shape is unchanged:
checkout -> inventory -> warehouse, checkout -> payment -> fraud, checkout ->
notification, which gives the service graph depth and gives the fraud span a
place to fail.

Three things chapter 9 adds, and all three are about what a downstream system
can learn without the application telling it anything special:

  1. THE FAILURE IS RECORDED THE ORDINARY WAY. The fraud call raises, the
     handler catches it and calls record_exception(). Nothing sets an
     exception.* attribute by hand. The OTel SDK writes exception.type,
     exception.message and exception.stacktrace onto a span EVENT, and the
     Collector's transform processor moves them onto the span. That is what
     makes the listing 9.2 error-issue index a real artifact: it fingerprints
     what any instrumented service already emits, not what this file was
     rigged to emit.

  2. THE PROCESS COUNTS ITS OWN SPANS. A SpanProcessor increments a Prometheus
     counter as each span ends, and /metrics publishes it. Prometheus scrapes
     that counter directly, never through the Collector, so listing 9.5 can
     compare what was emitted against what was received. Two counts that
     travelled the same path cannot detect that path losing spans.

  3. LOGS CARRY TRACE CONTEXT BECAUSE THEY SHARE A PROCESS WITH THE SPAN. The
     OTel logs SDK bridges the stdlib logger, so a log written inside a span
     gets that span's TraceId and SpanId from the active context. Nothing
     downstream parses a line to recover them.
"""

import itertools
import logging
import random
import time

from flask import Flask, Response, jsonify, request

from opentelemetry import trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode
from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

resource = Resource.create({
    "service.name": "checkout-service",
    "service.version": "1.0.0",
    "deployment.environment": "development",
})

# ---- Listing 9.5 (producer half): the process counts the spans it finished ----
# This counter is the independent "expected" input. It is incremented in-process
# as each span ends, which is the last moment the application knows about a span
# for certain, and Prometheus reads it off this container. Deriving it from
# anything the Collector reports would make it agree with the Collector by
# construction, which is exactly the agreement listing 9.5 is trying to test.
SPANS_EMITTED = Counter(
    "checkout_spans_emitted",
    "Spans this process finished and handed to the exporter.",
)


class SpanEmitCounter(SpanProcessor):
    """Counts every span the SDK ends, independent of whether export succeeds."""

    def on_start(self, span, parent_context=None):
        pass

    def on_end(self, span):
        SPANS_EMITTED.inc()

    def shutdown(self):
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return True


provider = TracerProvider(resource=resource)
provider.add_span_processor(SpanEmitCounter())
provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)

# ---- The logs half of section 9.3 ----------------------------------------------
# LoggingHandler bridges the stdlib logger into the OTel logs SDK. It reads the
# ambient context when a record is emitted, so a log written inside a span comes
# out carrying that span's TraceId and SpanId. It is attached to the "checkout"
# logger rather than the root logger on purpose: on the root logger the OTLP
# exporter's own failure messages would be routed back into the exporter.
logger_provider = LoggerProvider(resource=resource)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
set_logger_provider(logger_provider)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [checkout] %(message)s")
log = logging.getLogger("checkout")
log.addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))

app = Flask(__name__)
# /health and /metrics are excluded so a liveness probe and a Prometheus scrape
# do not each produce a span. Left in, every 15s scrape adds a span to the store,
# to the span metrics, and to both sides of the listing 9.5 comparison.
FlaskInstrumentor().instrument_app(app, excluded_urls="health,metrics")

# Deterministic demo error injection: every 100th checkout (~1%) fails at the
# deepest span (fraud.score). Real fraud scoring is stochastic; a fixed 1-in-100
# cadence keeps the tail sampler's keep-errors policy and the chapter 9 error
# demos reproducible. itertools.count().__next__ is atomic under the GIL.
_checkout_seq = itertools.count(1)


def _simulated_downstream(name: str, kind: SpanKind, duration_s: float, attrs: dict):
    with tracer.start_as_current_span(name, kind=kind) as span:
        for k, v in attrs.items():
            span.set_attribute(k, v)
        time.sleep(duration_s)


def _fraud_backend_call(deadline_ms: int, request_id: str) -> float:
    """Stands in for the network call to the fraud scorer.

    It raises rather than returning a sentinel so the exception carries a real
    traceback. The chapter's error-issue index fingerprints on the innermost
    frame, and a hand-written frame string would make that index look like it
    works on data no real service produces.
    """
    raise TimeoutError(
        f"fraud scoring backend timed out after {deadline_ms}ms (req {request_id})")


def _score_fraud(deadline_ms: int, request_id: str) -> float:
    return _fraud_backend_call(deadline_ms, request_id)


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


@app.route("/checkout")
def checkout():
    cart_id = f"cart-{random.randint(1000, 9999)}"
    item_count = random.randint(1, 8)
    seq = next(_checkout_seq)
    # ?fail=1 forces the failure path. The 1-in-100 cadence is what ordinary
    # traffic looks like; this is how a test gets one error trace on demand,
    # which matters because the tail sampler keeps error traces unconditionally
    # and keeps only a tenth of everything else.
    force_fail = request.args.get("fail") == "1"

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
    failed = False
    with tracer.start_as_current_span("payment.charge", kind=SpanKind.CLIENT) as span:
        span.set_attribute("peer.service", "payment-service")
        span.set_attribute("payment.method", "credit_card")
        span.set_attribute("payment.amount", amount)
        span.set_attribute("payment.currency", "USD")
        time.sleep(0.05)

        with tracer.start_as_current_span("fraud.score", kind=SpanKind.CLIENT) as child:
            child.set_attribute("peer.service", "fraud-service")
            child.set_attribute("fraud.model_version", "v2.1")
            time.sleep(0.04)
            if force_fail or seq % 100 == 0:
                # The latency and the request id vary per failure, which is what
                # gives the listing 9.2 index something to normalize: the regex
                # strips the digits and the hex, so every distinct raw message
                # collapses to one issue template.
                deadline_ms = 30000 + (seq % 1000)
                req_hex = f"{random.randrange(16 ** 8):08x}"
                try:
                    _score_fraud(deadline_ms, req_hex)
                except TimeoutError as exc:
                    # The ordinary two lines. record_exception writes the
                    # exception event with type, message and stacktrace; the
                    # status is what the tail sampler's keep-errors policy and
                    # the RED error rate both read.
                    child.record_exception(exc)
                    child.set_status(Status(StatusCode.ERROR, str(exc)))
                    log.error("fraud scoring failed for %s: %s", cart_id, exc)
                    failed = True
            else:
                child.set_attribute("fraud.score", round(random.uniform(0, 1), 3))

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

    # Written inside the Flask server span, so the emitted log record carries
    # that span's TraceId and SpanId. This is the line section 9.3's trace-to-log
    # jump lands on.
    log.info("checkout complete cart=%s order=%s amount=%.2f fraud_failed=%s",
             cart_id, order_id, amount, failed)

    return jsonify({
        "status": "completed",
        "cart_id": cart_id,
        "order_id": order_id,
        "amount": amount,
        "fraud_failed": failed,
    })


@app.route("/checkout/slow")
def checkout_slow():
    with tracer.start_as_current_span("inventory.slow_lookup", kind=SpanKind.CLIENT) as span:
        span.set_attribute("peer.service", "inventory-service")
        span.set_attribute("inventory.warehouse", "eu-west-1")
        delay = random.uniform(1.0, 3.0)
        span.set_attribute("lookup.duration_estimate", delay)
        time.sleep(delay)

    log.info("slow checkout complete delay=%.2fs", delay)
    return jsonify({"status": "completed", "delay_seconds": round(delay, 2)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
