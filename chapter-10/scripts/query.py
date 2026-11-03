#!/usr/bin/env python3
"""HTTP and JSON plumbing shared by the scripts in this directory.

Not a reader step. The shell scripts call it so that every JSON parse and
every connection failure ends in one readable line instead of a traceback.

Usage: python3 scripts/query.py <command> [args...]
"""
import calendar
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

PROM = "http://localhost:9090"
EDGE = "http://localhost:8889/metrics"
SAMPLER = "http://localhost:8890/metrics"
FLINK = "http://localhost:18081"
HINT = "Is the stack up? From chapter-10/ run: docker compose up -d --build, then ./scripts/wait-until-ready.sh"

# The one operation every measured comparison reads: the SERVER span of the
# checkout request. payment-service's GET /charge carries the same numbers.
SELECTOR = 'service_name="checkout-service",span_name="GET /checkout",span_kind="SPAN_KIND_SERVER"'
ERROR = ',status_code="STATUS_CODE_ERROR"'
SERIES = (
    ("edge", "traces_span_metrics_calls_total", EDGE),
    ("uniform", "uniform_calls_total", SAMPLER),
    ("policy", "policy_calls_total", SAMPLER),
    ("weighted", "weighted_calls_total", SAMPLER),
)


def fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(2)


def get(url, params=None, headers=None):
    if params:
        url += "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(request, timeout=15) as resp:
            return resp.read().decode()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace").strip().replace("\n", " ")
        fail(f"{url.split('?')[0]} answered HTTP {exc.code}: {body[:200]}")
    except (urllib.error.URLError, OSError) as exc:
        reason = getattr(exc, "reason", exc)
        fail(f"no answer from {url.split('?')[0]} ({reason}). {HINT}")


def number(value):
    value = float(value)
    return str(int(value)) if value.is_integer() else str(round(value, 3))


def prom(query):
    data = json.loads(get(PROM + "/api/v1/query", {"query": query}))
    if data.get("status") != "success":
        fail(f"Prometheus rejected the query {query}: {data.get('error')}")
    return data["data"]["result"]


def prom_sum(query):
    return sum(float(r["value"][1]) for r in prom(query))


def parse_exposition(text):
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        head, _, value = line.rpartition(" ")
        if " # " in line:
            head, _, value = line.split(" # ", 1)[0].rpartition(" ")
        name, _, labels = head.partition("{")
        yield name, labels, value


def endpoint_sum(url, name, *needles):
    total = 0.0
    for metric, labels, value in parse_exposition(get(url)):
        if metric == name and all(n in labels for n in needles):
            total += float(value)
    return total


def cmd_sum(query):
    """Sum of an instant vector, 0 when nothing matches."""
    print(number(prom_sum(query)))


def cmd_count(query):
    print(len(prom(query)))


def cmd_exposition(url, name, *needles):
    print(number(endpoint_sum(url, name, *needles)))


def cmd_instance():
    """The edge Collector's instance id, which changes whenever it restarts."""
    for _, labels, _ in parse_exposition(get(EDGE)):
        if 'collector_instance_id="' in labels:
            print(labels.split('collector_instance_id="', 1)[1].split('"', 1)[0])
            return
    print("none")


def cmd_snapshot(source="prometheus"):
    """key=value for the eight series every comparison reads."""
    for key, name, url in SERIES:
        for suffix, extra in (("calls", ""), ("errors", ERROR)):
            if source == "endpoint":
                needles = [f'span_name="GET /checkout"', 'service_name="checkout-service"',
                           'span_kind="SPAN_KIND_SERVER"']
                if extra:
                    needles.append('status_code="STATUS_CODE_ERROR"')
                value = endpoint_sum(url, name, *needles)
            else:
                value = prom_sum(f"sum({name}{{{SELECTOR}{extra}}})")
            print(f"{key}_{suffix}={number(value)}")


def cmd_exemplars(metric, since):
    """Distinct trace IDs Prometheus holds as exemplars on one metric."""
    data = json.loads(get(PROM + "/api/v1/query_exemplars", {
        "query": metric, "start": since, "end": time.time()}))
    seen = []
    for series in data.get("data") or []:
        for exemplar in series.get("exemplars", []):
            trace_id = exemplar["labels"].get("trace_id")
            if trace_id and re.fullmatch(r"[0-9a-f]{32}", trace_id) and trace_id not in seen:
                seen.append(trace_id)
    print("\n".join(seen))


def cmd_targets():
    data = json.loads(get(PROM + "/api/v1/targets"))["data"]["activeTargets"]
    for t in sorted(data, key=lambda t: t["labels"]["job"]):
        error = (t.get("lastError") or "").strip()
        print(t["labels"]["job"], t["health"], error[:60])


def cmd_label_sets(url):
    """Distinct calls series per service on a span-metrics endpoint, and overflow."""
    per_service = {}
    overflow = 0
    for name, labels, _ in parse_exposition(get(url)):
        if name != "traces_span_metrics_calls_total":
            continue
        service = re.search(r'service_name="([^"]*)"', labels)
        job = re.search(r'job="([^"]*)"', labels)
        key = service.group(1) if service else (job.group(1) if job else "?")
        per_service[key] = per_service.get(key, 0) + 1
        if 'otel_metric_overflow="true"' in labels:
            overflow += 1
    for key, count in sorted(per_service.items()):
        print(f"{key} {count}")
    print(f"overflow series (users past the limit, folded together) {overflow}")


def cmd_series_names(query):
    """One line per series an instant vector holds: its name, plus le for a bucket."""
    def edge(le):
        return float("inf") if le == "+Inf" else float(le)
    rows = []
    for r in prom(query):
        metric = r["metric"]
        le = metric.get("le")
        rows.append((metric["__name__"], edge(le) if le else -1,
                     metric["__name__"] + (f'{{le="{le}"}}' if le else "")))
    for _, _, line in sorted(rows):
        print(line)


def cmd_weight_per_span():
    """What the probabilistic sampler's connector counted per span it kept."""
    counted = endpoint_sum(SAMPLER, "weighted_calls_total")
    kept = 0.0
    for metric, labels, value in parse_exposition(get("http://localhost:8892/metrics")):
        if metric == "otelcol_processor_outgoing_items_total" and 'processor="probabilistic_sampler"' in labels:
            kept += float(value)
    print("n/a" if kept == 0 else f"{counted / kept:.0f}")


def cmd_endpoint_count(url, *needles):
    """Series on an endpoint whose line carries every needle."""
    print(sum(1 for line in get(url).splitlines()
              if line and not line.startswith("#") and all(n in line for n in needles)))


def cmd_series_count(url):
    print(sum(1 for _ in parse_exposition(get(url))))


def utc_minute(epoch):
    return time.strftime("%Y-%m-%d %H:%M:00", time.gmtime(int(epoch)))


def cmd_red_totals(span_name, since_epoch="0"):
    """Sum calls and errors for one span across red_metrics rows read on stdin.

    Windows that start before the minute of since_epoch are left out, so an
    older run's windows are not counted against this one."""
    since = utc_minute(since_epoch)
    calls = errors = rows = 0
    for line in sys.stdin:
        line = line.strip()
        if not line.startswith("{"):
            continue
        row = json.loads(line)
        if row["span_name"] != span_name or row["window_start"] < since:
            continue
        calls += row["calls"]
        errors += row["errors"]
        rows += 1
    print(calls, errors, rows)


def cmd_latest_window(span_name):
    """The newest window_start red_metrics holds for one span, read on stdin."""
    latest = ""
    for line in sys.stdin:
        line = line.strip()
        if line.startswith("{"):
            row = json.loads(line)
            if row["span_name"] == span_name and row["window_start"] > latest:
                latest = row["window_start"]
    print(latest)


def cmd_late_span(span_name, window_start):
    """One spans.flat row whose event time is 30 s into an already-fired window."""
    start = calendar.timegm(time.strptime(window_start, "%Y-%m-%d %H:%M:%S"))
    event = time.strftime("%Y-%m-%d %H:%M:%S.000", time.gmtime(start + 30))
    print(json.dumps({"service_name": "checkout-service", "span_name": span_name,
                      "span_kind": "SERVER", "status_code": "STATUS_CODE_UNSET",
                      "duration_ms": 12.0, "event_time": event}))


def cmd_now_ms():
    print(int(time.time() * 1000))


def cmd_window_row(span_name, window_start):
    for line in sys.stdin:
        line = line.strip()
        if line.startswith("{"):
            row = json.loads(line)
            if row["span_name"] == span_name and row["window_start"] == window_start:
                print(f'{row["window_start"]}  calls {row["calls"]}  errors {row["errors"]}  '
                      f'p99 {round(row["p99_ms"], 1)}ms')
                return
    print("no row")


def flink_window_vertex():
    jobs = json.loads(get(FLINK + "/jobs/overview"))["jobs"]
    running = [j for j in jobs if j["state"] == "RUNNING"]
    if not running:
        fail("no Flink job is running. Check docker compose logs flink-sql-submit")
    job = json.loads(get(f"{FLINK}/jobs/{running[0]['jid']}"))
    for vertex in job["vertices"]:
        if "WindowAggregate" in vertex["name"]:
            return running[0]["jid"], vertex["id"]
    fail("the Flink job has no window operator")


def cmd_flink_late():
    """numLateRecordsDropped on the window operator, or `none` while REST has no value."""
    jid, vid = flink_window_vertex()
    listing = json.loads(get(f"{FLINK}/jobs/{jid}/vertices/{vid}/metrics"))
    ids = [m["id"] for m in listing if m["id"].endswith("numLateRecordsDropped")]
    if not ids:
        print("none")
        return
    data = json.loads(get(f"{FLINK}/jobs/{jid}/vertices/{vid}/metrics",
                          {"get": ",".join(ids)}))
    print(number(sum(float(m["value"]) for m in data)))


def cmd_table():
    """Align tab-separated rows read from stdin into columns."""
    rows = [line.split("\t") for line in sys.stdin.read().splitlines() if line]
    if not rows:
        return
    widths = [max(len(r[i]) for r in rows if i < len(r)) for i in range(len(rows[0]))]
    for r in rows:
        print("  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip())


def cmd_percent(part, whole):
    print("n/a" if float(whole) == 0 else f"{100 * float(part) / float(whole):.2f}%")


def cmd_ratio(part, whole):
    print("n/a" if float(whole) == 0 else f"{float(part) / float(whole):.2f}")


def cmd_delta(after, before):
    print(number(float(after) - float(before)))


def cmd_newer_than(started, *paths):
    """Print the files edited after the container's StartedAt (RFC 3339)."""
    m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?Z", started)
    if not m:
        fail(f"cannot read the container start time {started!r}")
    start = calendar.timegm(time.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S")) + float(m.group(2) or 0)
    print(" ".join(p for p in paths if os.path.getmtime(p) > start))


COMMANDS = {
    "sum": cmd_sum, "count": cmd_count, "exposition": cmd_exposition,
    "instance": cmd_instance, "snapshot": cmd_snapshot, "exemplars": cmd_exemplars,
    "targets": cmd_targets, "label-sets": cmd_label_sets,
    "series-count": cmd_series_count, "series-names": cmd_series_names,
    "weight-per-span": cmd_weight_per_span, "endpoint-count": cmd_endpoint_count, "red-totals": cmd_red_totals,
    "latest-window": cmd_latest_window, "late-span": cmd_late_span, "now-ms": cmd_now_ms, "window-row": cmd_window_row,
    "flink-late": cmd_flink_late, "table": cmd_table, "percent": cmd_percent,
    "ratio": cmd_ratio, "delta": cmd_delta, "newer-than": cmd_newer_than,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        fail("usage: query.py <" + "|".join(COMMANDS) + "> [args...]")
    COMMANDS[sys.argv[1]](*sys.argv[2:])
