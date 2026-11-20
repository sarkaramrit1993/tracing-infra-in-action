import argparse
import base64
import gzip
import json
import os
import platform
import statistics
import struct
import subprocess
import sys
import tempfile
import threading
import time
from datetime import date
from importlib.metadata import version
from pathlib import Path

import zstandard
from google.protobuf.json_format import MessageToDict
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.exporter.otlp.proto.common._log_encoder import encode_logs
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk._logs.export import LogExporter, LogExportResult
from opentelemetry.sdk.trace.export import SpanExporter

HERE = Path(__file__).parent
CONDITIONS = ("no_content", "span_only", "event_only", "span_and_event", "ref")
TRUNC_BYTES = 2048
KAFKA_CAP = 1_000_000
SIZES = ("1k", "8k", "32k")
PACKAGES = (
    "openai",
    "opentelemetry-api",
    "opentelemetry-sdk",
    "opentelemetry-exporter-otlp-proto-grpc",
    "opentelemetry-exporter-otlp-proto-common",
    "opentelemetry-instrumentation",
    "opentelemetry-semantic-conventions",
    "opentelemetry-instrumentation-openai-v2",
    "opentelemetry-util-genai",
    "opentelemetry-instrumentation-requests",
    "requests",
    "zstandard",
    "protobuf",
)


class MeasuringSpanExporter(SpanExporter):
    def __init__(self, recorder, inner):
        self._recorder = recorder
        self._inner = inner

    def export(self, spans):
        self._recorder.spans.extend(spans)
        self._recorder.request_bytes.append(encode_spans(spans).ByteSize())
        return self._inner.export(spans)

    def shutdown(self):
        self._inner.shutdown()

    def force_flush(self, timeout_millis=30000):
        return self._inner.force_flush(timeout_millis)


class MeasuringLogExporter(LogExporter):
    """Measures event bytes and does not forward them: the chapter's Collector has no logs pipeline."""

    def __init__(self, recorder, inner):
        self._recorder = recorder
        self._inner = inner

    def export(self, batch):
        self._recorder.event_bytes += encode_logs(batch).ByteSize()
        return LogExportResult.SUCCESS

    def shutdown(self):
        self._inner.shutdown()

    def force_flush(self, timeout_millis=30000):
        return True


def _hex_ids(node):
    if isinstance(node, dict):
        for key, value in node.items():
            if key in ("traceId", "spanId", "parentSpanId") and isinstance(value, str):
                node[key] = base64.b64decode(value).hex()
            else:
                _hex_ids(value)
    elif isinstance(node, list):
        for item in node:
            _hex_ids(item)
    return node


def _attr_string(span, key):
    for attribute in span.attributes:
        if attribute.key == key:
            return attribute.value.string_value
    return None


def _cut(value):
    kind = value.WhichOneof("value")
    if kind == "string_value":
        raw = value.string_value.encode()
        if len(raw) > TRUNC_BYTES:
            value.string_value = raw[:TRUNC_BYTES].decode(errors="ignore")
    elif kind == "array_value":
        for item in value.array_value.values:
            _cut(item)
    elif kind == "kvlist_value":
        for item in value.kvlist_value.values:
            _cut(item.value)


def truncation(request):
    """Span sizes after every string attribute value is cut to TRUNC_BYTES."""
    longest = {"key": None, "bytes": 0}
    before, after = [], []
    over = 0
    cut = ExportTraceServiceRequest()
    cut.CopyFrom(request)
    for resource_spans in cut.resource_spans:
        for scope_spans in resource_spans.scope_spans:
            for span in scope_spans.spans:
                before.append(span.ByteSize())
                for attribute in span.attributes:
                    if attribute.value.WhichOneof("value") == "string_value":
                        size = len(attribute.value.string_value.encode())
                        if size > longest["bytes"]:
                            longest = {"key": attribute.key, "bytes": size}
                        over += size > TRUNC_BYTES
                    _cut(attribute.value)
                after.append(span.ByteSize())
    return {
        "cut_to_bytes": TRUNC_BYTES,
        "longest_string_attribute": longest,
        "attributes_over_limit": over,
        "span_bytes_before": before,
        "span_bytes_after": after,
        "span_bytes_lost": [b - a for b, a in zip(before, after)],
        "trace_bytes_after": cut.ByteSize(),
        "largest_span_after": max(after),
    }


def read_collector_file(path, offset=0):
    """Length-prefixed (4 bytes, big endian) ExportTraceServiceRequest messages written by the file exporter."""
    data = Path(path).read_bytes()[offset:]
    messages, position = [], 0
    while position + 4 <= len(data):
        (size,) = struct.unpack(">I", data[position : position + 4])
        if position + 4 + size > len(data):
            break
        request = ExportTraceServiceRequest()
        request.ParseFromString(data[position + 4 : position + 4 + size])
        service = ""
        for attribute in request.resource_spans[0].resource.attributes:
            if attribute.key == "service.name":
                service = attribute.value.string_value
        count = sum(len(ss.spans) for rs in request.resource_spans for ss in rs.scope_spans)
        messages.append({"service": service, "bytes": size, "spans": count})
        position += 4 + size
    return messages, offset + position


class Recorder:
    def __init__(self, path, otlp_json_path=None):
        self.path = path
        self.otlp_json_path = otlp_json_path
        self.spans = []
        self.request_bytes = []
        self.event_bytes = 0

    def wrap_spans(self, inner):
        return MeasuringSpanExporter(self, inner)

    def wrap_logs(self, inner):
        return MeasuringLogExporter(self, inner)

    def finish(self):
        spans = sorted(self.spans, key=lambda s: s.start_time)
        request = encode_spans(spans)
        raw = request.SerializeToString()
        rows = []
        for resource_spans in request.resource_spans:
            for scope_spans in resource_spans.scope_spans:
                for span in scope_spans.spans:
                    rows.append(
                        {
                            "name": span.name,
                            "type": _attr_string(span, "gen_ai.operation.name") or "http_client",
                            "bytes": span.ByteSize(),
                            "attribute_keys": sorted(a.key for a in span.attributes),
                            "start": span.start_time_unix_nano,
                            "is_root": not span.parent_span_id,
                            "duration_ns": span.end_time_unix_nano - span.start_time_unix_nano,
                        }
                    )
        rows.sort(key=lambda r: r["start"])
        root = next(r for r in rows if r["is_root"])
        record = {
            "spans": rows,
            "span_count": len(rows),
            "sum_span_bytes": sum(r["bytes"] for r in rows),
            "trace_bytes": request.ByteSize(),
            "trace_gzip": len(gzip.compress(raw, 6, mtime=0)),
            "trace_zstd": len(zstandard.ZstdCompressor(level=3).compress(raw)),
            "request_bytes": self.request_bytes,
            "largest_request": max(self.request_bytes),
            "event_bytes": self.event_bytes,
            "root_duration_ns": root["duration_ns"],
            "truncation": truncation(request),
        }
        record["envelope_bytes"] = record["trace_bytes"] - record["sum_span_bytes"]
        Path(self.path).write_text(json.dumps(record))
        if self.otlp_json_path:
            document = _hex_ids(MessageToDict(request))
            Path(self.otlp_json_path).write_text(json.dumps(document, indent=1) + "\n")


def run_one(size, condition, mode, otlp_json=None, calls=4, collector_file=None, service=None):
    with tempfile.TemporaryDirectory() as tmp:
        record_path = os.path.join(tmp, "record.json")
        offset = Path(collector_file).stat().st_size if collector_file else 0
        env = {
            **os.environ,
            "AGENT_PROMPT_SIZE": size,
            "AGENT_RUN_SECONDS": mode,
            "AGENT_RUNS": "1",
            "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT": condition,
            "AGENT_MODEL_CALLS": str(calls),
            "MEASURE_RECORD": record_path,
        }
        if condition == "ref":
            env["OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"] = os.environ.get("REF_CAPTURE", "no_content")
            env["OTEL_INSTRUMENTATION_GENAI_COMPLETION_HOOK"] = "upload"
            os.makedirs(os.path.join(tmp, "uploads"))
            env["OTEL_INSTRUMENTATION_GENAI_UPLOAD_BASE_PATH"] = os.path.join(tmp, "uploads")
        if service:
            env["OTEL_SERVICE_NAME"] = service
        if otlp_json:
            env["MEASURE_OTLP_JSON"] = otlp_json
        started = time.time()
        subprocess.run([sys.executable, str(HERE / "agent.py")], env=env, check=True, stdout=subprocess.DEVNULL)
        record = json.loads(Path(record_path).read_text())
        record["wall_seconds"] = round(time.time() - started, 2)
        if condition == "ref":
            uploads = Path(tmp) / "uploads"
            record["uploaded_files"] = sorted(p.name for p in uploads.rglob("*") if p.is_file()) if uploads.exists() else []
            record["uploaded_bytes"] = sum(p.stat().st_size for p in uploads.rglob("*") if p.is_file()) if uploads.exists() else 0
        if collector_file:
            record["collector"] = collector_check(collector_file, offset, record["request_bytes"])
        return record


def collector_check(path, offset, sdk_requests, wait=15):
    """The Collector's file exporter output for this run against the SDK-side request sizes."""
    deadline = time.time() + wait
    while True:
        messages, _ = read_collector_file(path, offset)
        agent = [m for m in messages if m["service"] == "support-agent"]
        if len(agent) >= len(sdk_requests) or time.time() > deadline:
            break
        time.sleep(0.25)
    time.sleep(0.5)
    messages, _ = read_collector_file(path, offset)
    agent = sorted(m["bytes"] for m in messages if m["service"] == "support-agent")
    other = {}
    for m in messages:
        if m["service"] != "support-agent":
            other[m["service"]] = other.get(m["service"], 0) + m["spans"]
    return {
        "agent_request_bytes": agent,
        "sdk_request_bytes": sorted(sdk_requests),
        "delta_bytes": [a - s for a, s in zip(agent, sorted(sdk_requests))] if len(agent) == len(sdk_requests) else None,
        "agent_spans_received": sum(m["spans"] for m in messages if m["service"] == "support-agent"),
        "other_service_spans": other,
    }


def step_shares(runs):
    per_run = []
    for r in runs:
        total = r["root_duration_ns"]
        per_run.append([(row["type"], row["name"], row["duration_ns"] / total) for row in r["spans"] if row["type"] not in ("http_client", "invoke_agent")])
    steps = []
    for i in range(min(len(p) for p in per_run)):
        values = [p[i][2] for p in per_run]
        steps.append({"type": per_run[0][i][0], "name": per_run[0][i][1], "share_median": round(statistics.median(values), 4), "share_min": round(min(values), 4), "share_max": round(max(values), 4)})
    return steps


def summarize(size, condition, runs):
    first = runs[0]
    by_type = {}
    for row in first["spans"]:
        by_type.setdefault(row["type"], []).append(row["bytes"])
    chat = by_type.get("chat", [])
    durations = [r["root_duration_ns"] / 1e6 for r in runs]
    return {
        "condition": condition,
        "prompt_size": size,
        "runs": len(runs),
        "bytes_identical_across_runs": len({r["trace_bytes"] for r in runs}) == 1
        and len({tuple(s["bytes"] for s in r["spans"]) for r in runs}) == 1,
        "trace_bytes": [r["trace_bytes"] for r in runs],
        "span_count": first["span_count"],
        "spans_by_type": {t: {"count": len(v), "bytes": v} for t, v in by_type.items()},
        "largest_span": max(r["bytes"] for r in first["spans"]),
        "chat_growth_last_over_first": round(chat[-1] / chat[0], 3) if chat else None,
        "sum_span_bytes": first["sum_span_bytes"],
        "envelope_bytes": first["envelope_bytes"],
        "span_bytes_per_run": [[s["bytes"] for s in r["spans"]] for r in runs],
        "trace_gzip": [r["trace_gzip"] for r in runs],
        "trace_zstd": [r["trace_zstd"] for r in runs],
        "largest_request": [r["largest_request"] for r in runs],
        "event_bytes": [r["event_bytes"] for r in runs],
        "truncation": first["truncation"],
        "step_shares": step_shares(runs),
        "collector_check": [r["collector"] for r in runs if "collector" in r],
        "uploaded_files": first.get("uploaded_files"),
        "uploaded_bytes": first.get("uploaded_bytes"),
        "span_attribute_keys": sorted({k for r in first["spans"] for k in r["attribute_keys"]}),
        "root_duration_ms_runs": [round(d, 1) for d in durations],
        "root_duration_ms": {
            "median": round(statistics.median(durations), 1),
            "min": round(min(durations), 1),
            "max": round(max(durations), 1),
        },
    }


def cap_search(collector_file, repeats=3):
    """Smallest model-call count at which one trace's uncompressed OTLP export reaches the Kafka exporter cap, at 32 KB, span_only."""
    sweep, calls, crossing = [], 4, None
    while crossing is None:
        record = run_one("32k", "span_only", "short", calls=calls, collector_file=collector_file)
        sweep.append({"calls": calls, "trace_bytes": record["trace_bytes"], "span_count": record["span_count"], "largest_request": record["largest_request"]})
        if record["trace_bytes"] >= KAFKA_CAP:
            crossing = calls
        calls += 1
    confirm = {}
    for n in (crossing - 1, crossing):
        confirm[str(n)] = [run_one("32k", "span_only", "short", calls=n)["trace_bytes"] for _ in range(repeats)]
    return {"cap_bytes": KAFKA_CAP, "size": "32k", "condition": "span_only", "sweep": sweep, "calls_to_cap": crossing, "repeat_trace_bytes": confirm}


def duration_block(runs, mode):
    values = [r["root_duration_ns"] / 1e6 for r in runs]
    return {
        "mode": mode,
        "size": "8k",
        "condition": "no_content",
        "runs": len(values),
        "root_duration_ms_runs": [round(v, 1) for v in values],
        "median_ms": round(statistics.median(values), 1),
        "min_ms": round(min(values), 1),
        "max_ms": round(max(values), 1),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Measure the agent trace at each capture condition and prompt size.")
    parser.add_argument("--sizes", default=",".join(SIZES))
    parser.add_argument("--conditions", default=",".join(CONDITIONS))
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--mode", default="short", choices=("short", "long"))
    parser.add_argument("--out-dir", default=str(HERE / "results"))
    parser.add_argument("--otlp-json", action="store_true", help="also write results/agent-traces-otlp.json (capture off, 8k)")
    parser.add_argument("--collector-file", help="the Collector file exporter output to compare each run against")
    parser.add_argument("--duration-runs", type=int, default=0, help="runs per mode for the duration block (0 skips it)")
    parser.add_argument("--cap-search", action="store_true", help="find the model-call count that reaches the Kafka exporter cap at 32k")
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cells = []
    for condition in [c for c in args.conditions.split(",") if c]:
        for size in args.sizes.split(","):
            runs = [run_one(size, condition, args.mode, collector_file=args.collector_file) for _ in range(args.runs)]
            cell = summarize(size, condition, runs)
            print(
                f"{condition:15} {size:>3}  spans={cell['span_count']}  trace_bytes={cell['trace_bytes']}  "
                f"identical={cell['bytes_identical_across_runs']}",
                flush=True,
            )
            cells.append(cell)
    if args.otlp_json:
        run_one("8k", "no_content", "short", otlp_json=str(out_dir / "agent-traces-otlp.json"))
        print(f"wrote {out_dir / 'agent-traces-otlp.json'}")
    durations = {}
    if args.duration_runs:
        long_runs = []

        def run_long():
            long_runs.extend(run_one("8k", "no_content", "long", service="support-agent-long") for _ in range(args.duration_runs))

        worker = threading.Thread(target=run_long)
        worker.start()
        runs = [run_one("8k", "no_content", "short") for _ in range(args.duration_runs)]
        durations["short"] = duration_block(runs, "short")
        print(f"duration short: {durations['short']}", flush=True)
    cap = cap_search(args.collector_file) if args.cap_search else None
    if args.duration_runs:
        worker.join()
        durations["long"] = duration_block(long_runs, "long")
        print(f"duration long: {durations['long']}", flush=True)
    if cap:
        print(f"calls to cap: {cap['calls_to_cap']}", flush=True)
    result = {
        "date": date.today().isoformat(),
        "mode": args.mode,
        "runs_per_cell": args.runs,
        "host": {"platform": platform.platform(), "python": platform.python_version(), "cpus": os.cpu_count()},
        "versions": {name: version(name) for name in PACKAGES},
        "semconv_stability_opt_in": os.environ.get("OTEL_SEMCONV_STABILITY_OPT_IN", "gen_ai_latest_experimental"),
        "cells": cells,
        "durations": durations,
        "calls_to_cap": cap,
    }
    path = out_dir / f"agent-trace-{result['date']}.json"
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
