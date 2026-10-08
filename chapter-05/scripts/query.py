#!/usr/bin/env python3
"""HTTP and JSON plumbing shared by the scripts in this directory.

Not a reader step. The shell scripts call it so that every JSON parse and
every connection failure ends in one readable line instead of a traceback.

Usage: python3 scripts/query.py <command> [args...]
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

FLINK = f"http://localhost:{os.environ.get('FLINK_PORT', '8081')}"
JAEGER = "http://localhost:16686"
JOB = "chapter5-trace-assembly"
# Flink's metrics.fetcher.update-interval, which the stack leaves at its default.
METRICS_REFRESH_SECONDS = 10
HINT = "Is the stack up? From chapter-05/ run: docker compose up -d --build"


def fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(2)


def get(url, missing_ok=False):
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        if missing_ok and exc.code == 404:
            return None
        fail(f"{url} answered HTTP {exc.code}")
    except (urllib.error.URLError, OSError) as exc:
        fail(f"no answer from {url} ({getattr(exc, 'reason', exc)}). {HINT}")
    except json.JSONDecodeError:
        fail(f"{url} did not answer with JSON. Is something else using that port?")


def flink_job():
    jobs = [j for j in get(FLINK + "/jobs/overview").get("jobs", []) if j.get("name") == JOB]
    jobs.sort(key=lambda j: j.get("start-time", 0))
    return jobs[-1] if jobs else None


def cmd_flink_state():
    job = flink_job()
    print(job["state"] if job else "NONE")


def assembly_vertex(jid):
    detail = get(f"{FLINK}/jobs/{jid}")
    vertex = next((v for v in detail["vertices"] if "trace-assembly" in v["name"]), None)
    if vertex is None:
        fail(f"job {jid} has no trace-assembly operator")
    marks = get(f"{FLINK}/jobs/{jid}/vertices/{vertex['id']}/watermarks") or []
    watermark = max((int(m["value"]) for m in marks if m["value"].lstrip("-").isdigit()), default=0)
    return vertex, watermark


def cmd_flink_summary():
    job = flink_job()
    if not job:
        fail(f"no {JOB} job on {FLINK}. Check docker compose logs flink-job-submit")
    jid = job["jid"]
    # Flink's REST API serves metrics from a cache it refreshes every 10
    # seconds, so one read can be up to 10 seconds old, and a job that has been
    # working for a minute can still read zero. Wait for a first count, then
    # keep reading until it moves or a full refresh has passed: either way the
    # number printed is no older than the last refresh.
    vertex, watermark = assembly_vertex(jid)
    deadline = time.time() + 30
    while vertex["metrics"]["read-records"] == 0 or watermark == 0:
        if time.time() > deadline:
            fail("Flink has reported no spans read yet. It publishes metrics every 10 seconds: run this again.")
        time.sleep(2)
        vertex, watermark = assembly_vertex(jid)
    seen = vertex["metrics"]["read-records"]
    settled = time.time() + METRICS_REFRESH_SECONDS + 2
    while time.time() < settled:
        time.sleep(1)
        vertex, watermark = assembly_vertex(jid)
        if vertex["metrics"]["read-records"] != seen:
            break
    checkpoints = get(f"{FLINK}/jobs/{jid}/checkpoints")
    latest = (checkpoints.get("latest") or {}).get("completed") or {}
    print(f"job state                      {job['state']}")
    print(f"spans into trace-assembly      {vertex['metrics']['read-records']}")
    if watermark > 0:
        lag = time.time() - watermark / 1000
        print(f"watermark behind wall clock    {lag:.1f}s")
    else:
        print("watermark behind wall clock    no watermark yet")
    print(f"checkpoints completed          {checkpoints['counts']['completed']}")
    size = latest.get("checkpointed_size", latest.get("state_size"))
    if size is not None:
        print(f"last checkpoint size           {size / 1024:.1f} KiB")


def cmd_jaeger_sources(trace_id):
    """Spans of one trace in Jaeger, counted by their assembly.source label."""
    data = get(f"{JAEGER}/api/traces/{trace_id}", missing_ok=True)
    counts = {"query-time": 0, "stream-time": 0}
    for trace in (data or {}).get("data") or []:
        sources = {}
        for pid, process in trace.get("processes", {}).items():
            tags = {t["key"]: t["value"] for t in process.get("tags", [])}
            sources[pid] = tags.get("assembly.source", "none")
        for span in trace.get("spans", []):
            source = sources.get(span.get("processID"), "none")
            counts[source] = counts.get(source, 0) + 1
    for source, n in counts.items():
        print(source, n)


def cmd_count_records(text=""):
    """Count the records topic_records pipes in, or those containing TEXT."""
    records = sys.stdin.buffer.read().split(os.environ["RECORD_END"].encode())[:-1]
    print(sum(text.encode() in r for r in records))


def cmd_table():
    """Align tab-separated rows read from stdin into columns."""
    rows = [line.split("\t") for line in sys.stdin.read().splitlines() if line]
    if not rows:
        return
    widths = [max(len(r[i]) for r in rows if i < len(r)) for i in range(len(rows[0]))]
    for r in rows:
        print("  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip())


COMMANDS = {
    "flink-state": cmd_flink_state, "flink-summary": cmd_flink_summary,
    "jaeger-sources": cmd_jaeger_sources, "count-records": cmd_count_records,
    "table": cmd_table,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        fail("usage: query.py <" + "|".join(COMMANDS) + "> [args...]")
    COMMANDS[sys.argv[1]](*sys.argv[2:])
