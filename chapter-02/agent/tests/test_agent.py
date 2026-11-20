import json
import sys
import threading
import urllib.error
import urllib.request
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import agent  # noqa: E402
import stand_in_model  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter  # noqa: E402
from opentelemetry.trace import SpanKind  # noqa: E402

BODY = {
    "model": "stand-in-1",
    "messages": [{"role": "system", "content": "s"}, {"role": "user", "content": "q"}],
}


class Checkout(BaseHTTPRequestHandler):
    traceparents = []

    def do_GET(self):
        Checkout.traceparents.append(self.headers.get("traceparent"))
        data = b'{"status":"delivered"}'
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


def _serve(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_port}"


@pytest.fixture(scope="module")
def servers():
    stand_in, stand_in_url = _serve(stand_in_model.Handler)
    checkout, checkout_url = _serve(Checkout)
    yield stand_in_url, checkout_url
    stand_in.shutdown()
    checkout.shutdown()


def _post(url, path, body):
    request = urllib.request.Request(url + path, data=json.dumps(body).encode(), method="POST")
    with urllib.request.urlopen(request) as response:
        return response.read()


def test_stand_in_returns_same_bytes_for_the_same_call(servers):
    stand_in_url, _ = servers
    first = _post(stand_in_url, "/v1/chat/completions", BODY)
    second = _post(stand_in_url, "/v1/chat/completions", BODY)
    assert first == second


def test_stand_in_walks_the_fixture_by_call_number(servers):
    stand_in_url, _ = servers
    messages = list(BODY["messages"])
    names = []
    for _ in range(4):
        reply = json.loads(_post(stand_in_url, "/v1/chat/completions", {**BODY, "messages": messages}))
        message = reply["choices"][0]["message"]
        names.append(message["tool_calls"][0]["function"]["name"] if "tool_calls" in message else "final")
        messages = messages + [message]
    assert names == ["lookup_order", "search_policy", "issue_refund", "final"]


def _run(servers, capture, monkeypatch):
    stand_in_url, checkout_url = servers
    monkeypatch.setenv("OTEL_SEMCONV_STABILITY_OPT_IN", "gen_ai_latest_experimental")
    monkeypatch.setenv("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", capture)
    settings = agent.settings_from_env(
        {"AGENT_PROMPT_SIZE": "1k", "STAND_IN_URL": stand_in_url, "CHECKOUT_URL": checkout_url}
    )
    exporter = InMemorySpanExporter()
    tracer_provider, _ = agent.build_providers(exporter)
    agent.run_once(settings, tracer_provider)
    tracer_provider.force_flush()
    return exporter.get_finished_spans()


def test_agent_emits_the_expected_spans(servers, monkeypatch):
    spans = _run(servers, "no_content", monkeypatch)
    operations = Counter(s.attributes.get("gen_ai.operation.name", "http") for s in spans)
    assert operations == {"invoke_agent": 1, "chat": 4, "execute_tool": 2, "retrieval": 1, "http": 3}
    assert len({s.context.trace_id for s in spans}) == 1
    retrieval = next(s for s in spans if s.attributes.get("gen_ai.operation.name") == "retrieval")
    assert retrieval.kind == SpanKind.INTERNAL
    assert retrieval.attributes["gen_ai.data_source.id"] == "refund-policy"
    roots = [s for s in spans if s.parent is None]
    assert [r.name for r in roots] == ["invoke_agent support-agent"]
    http = [s for s in spans if "gen_ai.operation.name" not in s.attributes]
    assert sorted(s.attributes["http.status_code"] for s in http) == [200, 200, 503]


def test_capture_switch_moves_content_onto_spans(servers, monkeypatch):
    off = _run(servers, "no_content", monkeypatch)
    on = _run(servers, "span_only", monkeypatch)
    assert not any("gen_ai.input.messages" in s.attributes for s in off)
    assert sum("gen_ai.input.messages" in s.attributes for s in on) == 4


def test_fail_last_tool_switch_ends_the_run_in_error_on_the_refund_span(servers, monkeypatch):
    stand_in_url, checkout_url = servers
    monkeypatch.setenv("OTEL_SEMCONV_STABILITY_OPT_IN", "gen_ai_latest_experimental")
    settings = agent.settings_from_env(
        {
            "AGENT_PROMPT_SIZE": "1k",
            "AGENT_FAIL_LAST_TOOL": "1",
            "STAND_IN_URL": stand_in_url,
            "CHECKOUT_URL": checkout_url,
        }
    )
    exporter = InMemorySpanExporter()
    tracer_provider, _ = agent.build_providers(exporter)
    with pytest.raises(agent.AgentRunFailed):
        agent.run_once(settings, tracer_provider)
    tracer_provider.force_flush()
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["execute_tool issue_refund"].status.status_code.name == "ERROR"


def test_failed_refund_keeps_its_arguments_when_capture_is_on(servers, monkeypatch):
    stand_in_url, checkout_url = servers
    monkeypatch.setenv("OTEL_SEMCONV_STABILITY_OPT_IN", "gen_ai_latest_experimental")
    monkeypatch.setenv("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "span_only")
    settings = agent.settings_from_env(
        {
            "AGENT_PROMPT_SIZE": "1k",
            "AGENT_FAIL_LAST_TOOL": "1",
            "STAND_IN_URL": stand_in_url,
            "CHECKOUT_URL": checkout_url,
        }
    )
    exporter = InMemorySpanExporter()
    tracer_provider, _ = agent.build_providers(exporter)
    with pytest.raises(agent.AgentRunFailed):
        agent.run_once(settings, tracer_provider)
    tracer_provider.force_flush()
    refund = {s.name: s for s in exporter.get_finished_spans()}["execute_tool issue_refund"]
    assert "gen_ai.tool.call.arguments" in refund.attributes
    assert "gen_ai.tool.call.result" not in refund.attributes


def test_checkout_call_carries_the_agent_trace_context(servers, monkeypatch):
    Checkout.traceparents.clear()
    spans = _run(servers, "no_content", monkeypatch)
    [traceparent] = Checkout.traceparents
    lookup_http = [s for s in spans if s.attributes.get("http.method") == "GET"]
    assert len(lookup_http) == 1
    context = lookup_http[0].context
    assert traceparent.rsplit("-", 1)[0] == f"00-{context.trace_id:032x}-{context.span_id:016x}"


def test_the_switch_is_off_by_default():
    assert agent.settings_from_env({})["fail_last_tool"] is False


def _chat_models(servers, monkeypatch, extra):
    stand_in_url, checkout_url = servers
    monkeypatch.setenv("OTEL_SEMCONV_STABILITY_OPT_IN", "gen_ai_latest_experimental")
    settings = agent.settings_from_env(
        {"AGENT_PROMPT_SIZE": "1k", "STAND_IN_URL": stand_in_url, "CHECKOUT_URL": checkout_url, **extra}
    )
    exporter = InMemorySpanExporter()
    tracer_provider, _ = agent.build_providers(exporter)
    agent.run_once(settings, tracer_provider)
    tracer_provider.force_flush()
    chats = [s for s in exporter.get_finished_spans() if s.attributes.get("gen_ai.operation.name") == "chat"]
    chats.sort(key=lambda s: s.start_time)
    return [(s.attributes["gen_ai.request.model"], s.attributes["gen_ai.response.model"]) for s in chats]


def test_second_model_is_off_by_default(servers, monkeypatch):
    assert agent.settings_from_env({})["second_model"] == ""
    assert _chat_models(servers, monkeypatch, {}) == [("stand-in-1", "stand-in-1")] * 4


def test_second_model_switch_moves_only_the_final_answer(servers, monkeypatch):
    models = _chat_models(servers, monkeypatch, {"AGENT_SECOND_MODEL": "stand-in-2"})
    assert models == [("stand-in-1", "stand-in-1")] * 3 + [("stand-in-2", "stand-in-2")]


def test_stand_in_answers_the_second_model_with_the_same_body_but_its_name(servers):
    stand_in_url, _ = servers
    one = json.loads(_post(stand_in_url, "/v1/chat/completions", BODY))
    two = json.loads(_post(stand_in_url, "/v1/chat/completions", {**BODY, "model": "stand-in-2"}))
    assert two["model"] == "stand-in-2"
    assert {**two, "model": "stand-in-1"} == one


def _refund_with_traceparent(stand_in_url, body, traceparent):
    request = urllib.request.Request(
        stand_in_url + "/payments/refund",
        data=json.dumps(body).encode(),
        method="POST",
        headers={"traceparent": traceparent},
    )
    try:
        urllib.request.urlopen(request).read()
    except urllib.error.HTTPError as err:
        return err.code
    return 200


def test_refund_route_makes_no_span_unless_switched_on(servers):
    stand_in_url, _ = servers
    assert stand_in_model.TRACER is None
    assert _refund_with_traceparent(stand_in_url, {"order_id": "X", "always_fail": True}, "") == 503


def test_refund_span_is_a_server_span_in_the_callers_trace(servers, monkeypatch):
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    stand_in_url, _ = servers
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(stand_in_model, "TRACER", provider.get_tracer("test"))
    caller = "00-" + "ab" * 16 + "-" + "cd" * 8 + "-01"
    assert _refund_with_traceparent(stand_in_url, {"order_id": "X", "always_fail": True}, caller) == 503
    assert _refund_with_traceparent(stand_in_url, {"order_id": "X"}, caller) == 200
    failed, ok = exporter.get_finished_spans()
    assert failed.kind.name == "SERVER"
    assert failed.name == "POST /payments/refund"
    assert format(failed.context.trace_id, "032x") == "ab" * 16
    assert format(failed.parent.span_id, "016x") == "cd" * 8
    assert failed.status.status_code.name == "ERROR"
    assert failed.attributes["http.response.status_code"] == 503
    assert ok.status.status_code.name == "UNSET"
