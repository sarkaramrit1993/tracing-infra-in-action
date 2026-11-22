#!/usr/bin/env python3
"""Print an OTLP JSON trace file as JSONEachRow rows for tracing.otel_traces.

Usage:  python3 generate/otlp_to_rows.py FILE
"""
import json
import sys
from datetime import datetime, timezone

STATUS = {0: "STATUS_CODE_UNSET", 1: "STATUS_CODE_OK", 2: "STATUS_CODE_ERROR"}


def scalar(value):
    """One OTLP AnyValue as the string the attributes map stores."""
    if "arrayValue" in value:
        return json.dumps([scalar(v) for v in value["arrayValue"].get("values", [])])
    (kind, raw), = value.items()
    return str(raw).lower() if kind == "boolValue" else str(raw)


def timestamp(nanos):
    seconds, rest = divmod(int(nanos), 1_000_000_000)
    when = datetime.fromtimestamp(seconds, timezone.utc)
    return f"{when:%Y-%m-%d %H:%M:%S}.{rest:09d}"


def rows(doc):
    for resource in doc["resourceSpans"]:
        service = scalar(next(a["value"] for a in resource["resource"]["attributes"]
                              if a["key"] == "service.name"))
        for scope in resource["scopeSpans"]:
            for span in scope["spans"]:
                status = span.get("status", {}).get("code", 0)
                yield {
                    "timestamp": timestamp(span["startTimeUnixNano"]),
                    "trace_id": span["traceId"],
                    "span_id": span["spanId"],
                    "parent_span_id": span.get("parentSpanId", ""),
                    "service_name": service,
                    "span_name": span["name"],
                    "status_code": STATUS.get(status, status),
                    "duration_ns": int(span["endTimeUnixNano"]) - int(span["startTimeUnixNano"]),
                    "attributes": {a["key"]: scalar(a["value"]) for a in span.get("attributes", [])},
                }


if __name__ == "__main__":
    with open(sys.argv[1]) as handle:
        for row in rows(json.load(handle)):
            print(json.dumps(row))
