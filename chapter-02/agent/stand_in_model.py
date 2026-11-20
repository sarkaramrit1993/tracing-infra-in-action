import contextlib
import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"
MODEL = "stand-in-1"
MODELS = (MODEL, "stand-in-2")
FAILING_ORDER = "ORD-1042"

CALL_DELAY_SECONDS = {
    "short": [0.4, 0.3, 0.3, 0.6],
    "long": [20.0, 15.0, 15.0, 30.0],
}
REFUND_DELAY_SECONDS = {"short": 0.2, "long": 6.0}
DELAY_SCALE = float(os.environ.get("STAND_IN_DELAY_SCALE", "1"))

TRACER = None

TOOL_CALLS = [
    ("call_standin_1", "lookup_order", {"order_id": FAILING_ORDER}),
    ("call_standin_2", "search_policy", {"query": "refund window for delivered orders"}),
    ("call_standin_3", "issue_refund", {"order_id": FAILING_ORDER, "amount": "84.50"}),
]


def call_number(messages):
    return sum(1 for m in messages if m.get("role") == "assistant")


def _tokens(text):
    return (len(text) + 3) // 4


def completion(request_body, total_calls=4):
    messages = request_body.get("messages", [])
    n = call_number(messages)
    if n < total_calls - 1:
        call_id, name, args = TOOL_CALLS[n] if n < len(TOOL_CALLS) else (f"call_standin_{n + 1}", *TOOL_CALLS[1][1:])
        message = {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": call_id,
                    "type": "function",
                    "function": {"name": name, "arguments": json.dumps(args, sort_keys=True)},
                }
            ],
        }
        finish = "tool_calls"
        completion_text = json.dumps(args, sort_keys=True)
    else:
        text = (FIXTURES / "final_answer.txt").read_text().strip()
        message = {"role": "assistant", "content": text}
        finish = "stop"
        completion_text = text
    prompt_tokens = _tokens(json.dumps(messages, sort_keys=True))
    completion_tokens = _tokens(completion_text)
    return {
        "id": f"chatcmpl-standin-{n + 1}",
        "object": "chat.completion",
        "created": 1790000000 + n,
        "model": request_body.get("model") if request_body.get("model") in MODELS else MODEL,
        "choices": [{"index": 0, "message": message, "finish_reason": finish}],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }


def refund_response(body):
    attempt = int(body.get("attempt", 1))
    if body.get("always_fail"):
        return 503, {"error": "payment provider unavailable"}
    if body.get("order_id") == FAILING_ORDER and attempt == 1:
        return 503, {"error": "payment provider unavailable"}
    return 200, json.loads((FIXTURES / "tool_issue_refund.json").read_text())


def start_tracing():
    global TRACER
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(resource=Resource.create({}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    TRACER = provider.get_tracer("stand-in-model")


def refund_span(headers):
    if TRACER is None:
        return contextlib.nullcontext()
    from opentelemetry.propagate import extract
    from opentelemetry.trace import SpanKind

    return TRACER.start_as_current_span(
        "POST /payments/refund", context=extract(headers), kind=SpanKind.SERVER
    )


def mark_refund_span(span, status):
    from opentelemetry.trace import Status, StatusCode

    span.set_attribute("http.request.method", "POST")
    span.set_attribute("http.route", "/payments/refund")
    span.set_attribute("http.response.status_code", status)
    if status >= 500:
        span.set_status(Status(StatusCode.ERROR))


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, status, payload):
        data = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length) or b"{}")

    def _mode(self):
        mode = self.headers.get("X-Run-Mode", "short")
        return mode if mode in CALL_DELAY_SECONDS else "short"

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"status": "ok"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        body = self._body()
        mode = self._mode()
        if self.path == "/v1/chat/completions":
            n = min(call_number(body.get("messages", [])), 3)
            time.sleep(CALL_DELAY_SECONDS[mode][n] * DELAY_SCALE)
            total = int(self.headers.get("X-Model-Calls", 4))
            self._send(200, completion(body, total))
        elif self.path == "/payments/refund":
            with refund_span(self.headers) as span:
                time.sleep(REFUND_DELAY_SECONDS[mode] * DELAY_SCALE)
                status, payload = refund_response(body)
                if span is not None:
                    mark_refund_span(span, status)
            self._send(status, payload)
        else:
            self._send(404, {"error": "not found"})

    def log_message(self, fmt, *args):
        sys.stderr.write("stand-in: " + fmt % args + "\n")


def serve(port=None):
    port = int(os.environ.get("STAND_IN_PORT", 8000)) if port is None else port
    if os.environ.get("STAND_IN_SPANS") == "1":
        start_tracing()
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()


if __name__ == "__main__":
    serve()
