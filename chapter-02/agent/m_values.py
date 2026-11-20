import json
import sys
from pathlib import Path

DECLARED = "8k"
KB = 1024


def r10(value):
    return int(round(value / 10.0) * 10)


def kb(value):
    return round(value / KB, 1)


def spread(values):
    return {"min": min(values), "max": max(values), "varies": min(values) != max(values)}


def cell(result, condition, size):
    for c in result["cells"]:
        if c["condition"] == condition and c["prompt_size"] == size:
            return c
    raise KeyError((condition, size))


def compute(result):
    off = cell(result, "no_content", DECLARED)
    on = cell(result, "span_only", DECLARED)
    out = {"declared_size": DECLARED, "kb_is_bytes": KB}

    def types(c):
        return c["spans_by_type"]

    out["M-OVH-CHAT"] = {
        "chat_span_bytes_off": types(off)["chat"]["bytes"],
        "min": min(types(off)["chat"]["bytes"]),
        "max": max(types(off)["chat"]["bytes"]),
        "rounded_nearest_10": [r10(b) for b in types(off)["chat"]["bytes"]],
        "by_size": {s: cell(result, "no_content", s)["spans_by_type"]["chat"]["bytes"] for s in ("1k", "8k", "32k")},
    }
    out["M-OVH-TOOL"] = {
        "tool_span_bytes_off": types(off)["execute_tool"]["bytes"],
        "rounded_nearest_10": [r10(b) for b in types(off)["execute_tool"]["bytes"]],
        "retrieval_span_bytes_off": types(off).get("retrieval", {}).get("bytes"),
        "agent_span_bytes_off": types(off).get("invoke_agent", {}).get("bytes"),
        "http_client_span_bytes_off": types(off).get("http_client", {}).get("bytes"),
    }
    out["M-SPANS"] = {
        "agent_process_spans": off["span_count"],
        "by_type": {t: v["count"] for t, v in types(off).items()},
    }
    out["M-TRACE-OFF"] = {"bytes": off["trace_bytes"], **spread(off["trace_bytes"]), "rounded_nearest_10": [r10(b) for b in off["trace_bytes"]]}
    out["M-SPAN-MAX"] = {"bytes": on["largest_span"], "kb": kb(on["largest_span"])}
    out["M-TRACE-ON"] = {"bytes": on["trace_bytes"], **spread(on["trace_bytes"]), "kb": [kb(b) for b in on["trace_bytes"]]}
    chat_on = types(on)["chat"]["bytes"]
    out["M-GROWTH"] = {
        "chat_span_bytes_on": chat_on,
        "last_over_first": on["chat_growth_last_over_first"],
        "first_bytes": chat_on[0],
        "last_bytes": chat_on[-1],
    }
    for name, c in (("M-TRACE-ON-Z", on), ("M-TRACE-OFF-Z", off)):
        out[name] = {
            "for_this_fixture": True,
            "gzip": {"bytes": c["trace_gzip"], **spread(c["trace_gzip"])},
            "zstd": {"bytes": c["trace_zstd"], **spread(c["trace_zstd"])},
        }
    out["M-STEP-SHARE"] = {"short_mode_8k_no_content": off["step_shares"]}
    out["M-SWEEP"] = [
        {
            "condition": c["condition"],
            "size": c["prompt_size"],
            "trace_bytes": c["trace_bytes"],
            "largest_span": c["largest_span"],
            "span_count": c["span_count"],
            "event_bytes": c["event_bytes"],
            "trace_gzip": c["trace_gzip"],
            "trace_zstd": c["trace_zstd"],
            "bytes_identical_across_runs": c["bytes_identical_across_runs"],
        }
        for c in result["cells"]
    ]
    if result.get("durations", {}).get("long"):
        out["M-DUR-LONG"] = result["durations"]["long"]
    cap = result.get("calls_to_cap")
    if cap:
        out["M-CALLS-TO-CAP"] = {"calls": cap["calls_to_cap"], "size": cap["size"], "condition": cap["condition"], "cap_bytes": cap["cap_bytes"]}
    out["M-TRUNC"] = {
        s: {
            "longest_string_attribute": cell(result, "span_only", s)["truncation"]["longest_string_attribute"],
            "attributes_over_limit": cell(result, "span_only", s)["truncation"]["attributes_over_limit"],
            "span_bytes_lost": cell(result, "span_only", s)["truncation"]["span_bytes_lost"],
            "trace_bytes_after": cell(result, "span_only", s)["truncation"]["trace_bytes_after"],
            "largest_span_after": cell(result, "span_only", s)["truncation"]["largest_span_after"],
        }
        for s in ("1k", "8k", "32k")
    }
    return out


if __name__ == "__main__":
    path = Path(sys.argv[1])
    result = json.loads(path.read_text())
    result["m_values"] = compute(result)
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result["m_values"], indent=1)[:6000])
