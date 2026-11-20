import json
import os
import sys
import time
from pathlib import Path

import requests
from openai import OpenAI
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.openai_v2 import OpenAIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind
from opentelemetry.util.genai.handler import TelemetryHandler
from opentelemetry.util.genai.utils import gen_ai_json_dumps, should_capture_content_on_spans

FIXTURES = Path(__file__).parent / "fixtures"
AGENT_NAME = "support-agent"
MODEL = "stand-in-1"
QUESTION = "Order ORD-1042 arrived damaged. Can I get my money back?"
SIZES = ("1k", "8k", "32k")
EVENT_MODES = ("event_only", "span_and_event")

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "lookup_order",
            "description": "Look up one order by its id.",
            "parameters": {
                "type": "object",
                "properties": {"order_id": {"type": "string"}},
                "required": ["order_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_policy",
            "description": "Search the refund policy.",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "issue_refund",
            "description": "Refund an order to its original payment method.",
            "parameters": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "string"},
                    "amount": {"type": "string"},
                },
                "required": ["order_id", "amount"],
            },
        },
    },
]


def settings_from_env(env=None):
    env = os.environ if env is None else env
    size = env.get("AGENT_PROMPT_SIZE", "8k")
    mode = env.get("AGENT_RUN_SECONDS", "short")
    if size not in SIZES:
        raise SystemExit(f"AGENT_PROMPT_SIZE must be one of {', '.join(SIZES)}, got {size}")
    if mode not in ("short", "long"):
        raise SystemExit(f"AGENT_RUN_SECONDS must be short or long, got {mode}")
    return {
        "size": size,
        "mode": mode,
        "runs": int(env.get("AGENT_RUNS", "1")),
        "model_calls": int(env.get("AGENT_MODEL_CALLS", "4")),
        "fail_last_tool": env.get("AGENT_FAIL_LAST_TOOL", "") == "1",
        "second_model": env.get("AGENT_SECOND_MODEL", ""),
        "capture": env.get("OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT", "no_content"),
        "stand_in_url": env.get("STAND_IN_URL", "http://stand-in-model:8000"),
        "checkout_url": env.get("CHECKOUT_URL", "http://checkout-service:8080"),
    }


def _text(name):
    return (FIXTURES / name).read_text()


def run_tool(handler, tracer, call, settings):
    name = call.function.name
    args = json.loads(call.function.arguments)
    if name == "lookup_order":
        with handler.tool(name, tool_call_id=call.id, tool_type="function") as tool:
            requests.get(f"{settings['checkout_url']}/orders/{args['order_id']}", timeout=10)
            result = _text("tool_lookup_order.json")
            if tool.should_capture_content_on_span:
                tool.arguments = args
                tool.tool_result = result
        return result
    if name == "search_policy":
        attributes = {"gen_ai.operation.name": "retrieval", "gen_ai.data_source.id": "refund-policy"}
        with tracer.start_as_current_span(
            "retrieval refund-policy", kind=SpanKind.INTERNAL, attributes=attributes
        ) as span:
            chunks = json.loads(_text("policy_chunks.json"))
            if should_capture_content_on_spans():
                span.set_attribute("gen_ai.retrieval.query.text", args["query"])
                documents = [{"id": c["id"], "content": c["text"]} for c in chunks]
                span.set_attribute("gen_ai.retrieval.documents", gen_ai_json_dumps(documents))
        return json.dumps(chunks)
    if name == "issue_refund":
        with handler.tool(name, tool_call_id=call.id, tool_type="function") as tool:
            if tool.should_capture_content_on_span:
                tool.arguments = args
            for attempt in (1, 2, 3):
                response = requests.post(
                    f"{settings['stand_in_url']}/payments/refund",
                    json={**args, "attempt": attempt, **({"always_fail": True} if settings["fail_last_tool"] else {})},
                    headers={"X-Run-Mode": settings["mode"]},
                    timeout=30,
                )
                if response.status_code < 500:
                    break
                time.sleep(0.1)
            response.raise_for_status()
            result = response.text
            if tool.should_capture_content_on_span:
                tool.tool_result = result
        return result
    raise ValueError(f"unknown tool {name}")


def _run_tool_or_end(handler, tracer, call, settings, trace_id):
    try:
        return run_tool(handler, tracer, call, settings)
    except requests.HTTPError as err:
        if not settings["fail_last_tool"]:
            raise
        raise AgentRunFailed(trace_id) from err


class AgentRunFailed(Exception):
    def __init__(self, trace_id):
        super().__init__("the last tool call failed")
        self.trace_id = trace_id


def run_agent(client, handler, tracer, settings):
    messages = [
        {"role": "system", "content": _text(f"system_prompt_{settings['size']}.txt")},
        {"role": "user", "content": QUESTION},
    ]
    with handler.invoke_local_agent(request_model=MODEL, agent_name=AGENT_NAME):
        trace_id = format(trace.get_current_span().get_span_context().trace_id, "032x")
        while True:
            answering = sum(1 for m in messages if m.get("role") == "assistant") == settings["model_calls"] - 1
            response = client.chat.completions.create(
                model=settings["second_model"] if answering and settings["second_model"] else MODEL,
                messages=messages,
                tools=TOOLS,
                extra_headers={
                    "X-Run-Mode": settings["mode"],
                    "X-Model-Calls": str(settings["model_calls"]),
                },
            )
            message = response.choices[0].message
            messages.append(message.model_dump(exclude_none=True))
            if not message.tool_calls:
                return trace_id, message.content
            for call in message.tool_calls:
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": call.id,
                        "content": _run_tool_or_end(handler, tracer, call, settings, trace_id),
                    }
                )


def build_providers(span_exporter, log_exporter=None):
    resource = Resource.create({})
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter))
    logger_provider = None
    if log_exporter is not None:
        logger_provider = LoggerProvider(resource=resource)
        logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    return tracer_provider, logger_provider


def make_client(settings):
    return OpenAI(base_url=f"{settings['stand_in_url']}/v1", api_key="stand-in", max_retries=0)


def run_once(settings, tracer_provider, logger_provider=None):
    OpenAIInstrumentor().instrument(
        tracer_provider=tracer_provider, logger_provider=logger_provider
    )
    RequestsInstrumentor().instrument(tracer_provider=tracer_provider)
    try:
        handler = TelemetryHandler(
            tracer_provider=tracer_provider, logger_provider=logger_provider
        )
        tracer = tracer_provider.get_tracer(AGENT_NAME)
        return run_agent(make_client(settings), handler, tracer, settings)
    finally:
        OpenAIInstrumentor().uninstrument()
        RequestsInstrumentor().uninstrument()


def main():
    os.environ.setdefault("OTEL_SEMCONV_STABILITY_OPT_IN", "gen_ai_latest_experimental")
    settings = settings_from_env()
    record_path = os.environ.get("MEASURE_RECORD")
    span_exporter = OTLPSpanExporter()
    log_exporter = OTLPLogExporter() if settings["capture"] in EVENT_MODES else None
    recorder = None
    if record_path:
        from measure import Recorder

        recorder = Recorder(record_path, os.environ.get("MEASURE_OTLP_JSON"))
        span_exporter = recorder.wrap_spans(span_exporter)
        if log_exporter is not None:
            log_exporter = recorder.wrap_logs(log_exporter)
    tracer_provider, logger_provider = build_providers(span_exporter, log_exporter)
    if settings["mode"] == "long":
        print("Long mode: each run takes about 90 seconds. Waiting for the stand-in model.", flush=True)
    for n in range(1, settings["runs"] + 1):
        started = time.time()
        outcome = ""
        try:
            trace_id, answer = run_once(settings, tracer_provider, logger_provider)
        except AgentRunFailed as failed:
            trace_id, outcome = failed.trace_id, " ended=error"
        print(
            f"run {n}/{settings['runs']}: trace_id={trace_id} size={settings['size']} "
            f"capture={settings['capture']} seconds={time.time() - started:.1f}{outcome}",
            flush=True,
        )
    tracer_provider.shutdown()
    if logger_provider is not None:
        logger_provider.shutdown()
    if recorder is not None:
        recorder.finish()


if __name__ == "__main__":
    sys.exit(main())
