#!/usr/bin/env python3
"""HTTP and JSON plumbing shared by the scripts in this directory.

Not a reader step. The shell scripts call it so that every JSON parse and
every connection failure ends in one readable line instead of a traceback.

Usage: python3 scripts/query.py <command> [args...]
"""
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

PROM = "http://localhost:9090"
LOKI = "http://localhost:3100"
HINT = "Is the stack up? From chapter-09/ run: docker compose up -d --build"


def fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(2)


def get(url, params=None):
    if params:
        url += "?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return resp.read().decode()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace").strip().replace("\n", " ")
        fail(f"{url.split('?')[0]} answered HTTP {exc.code}: {body[:200]}")
    except (urllib.error.URLError, OSError) as exc:
        reason = getattr(exc, "reason", exc)
        fail(f"no answer from {url.split('?')[0]} ({reason}). {HINT}")


def prom(query):
    data = json.loads(get(PROM + "/api/v1/query", {"query": query}))
    if data.get("status") != "success":
        fail(f"Prometheus rejected the query {query}: {data.get('error')}")
    return data["data"]


def number(value):
    value = float(value)
    return str(int(value)) if value.is_integer() else str(value)


def cmd_sum(query):
    """Sum of an instant vector, or `none` when nothing matches."""
    result = prom(query)["result"]
    print(number(sum(float(r["value"][1]) for r in result)) if result else "none")


def cmd_time():
    print(prom("time()")["result"][0])


def cmd_scrape_time(job):
    result = prom(f'timestamp(up{{job="{job}"}})')["result"]
    print(result[0]["value"][1] if result else "0")


def parse_exposition(text):
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        head, _, value = line.rpartition(" ")
        name, _, labels = head.partition("{")
        yield name, labels, value


def cmd_exposition(url, name, *needles):
    """Sum of one metric in a Prometheus text exposition, 0 when absent."""
    total = 0.0
    for metric, labels, value in parse_exposition(get(url)):
        if metric == name and all(n in labels for n in needles):
            total += float(value)
    print(number(total))


def cmd_instance(url):
    """The Collector's service_instance_id, which changes on every restart."""
    for metric, labels, _ in parse_exposition(get(url)):
        if metric == "target_info" and "service_instance_id=" in labels:
            print(labels.split('service_instance_id="', 1)[1].split('"', 1)[0])
            return
    fail(f"no target_info on {url}. {HINT}")


def cmd_edges():
    result = prom("traces_service_graph_request_total")["result"]
    rows = sorted(result, key=lambda r: (r["metric"].get("failed") == "true",
                                         r["metric"].get("client", ""),
                                         r["metric"].get("server", "")))
    for r in rows:
        m = r["metric"]
        tag = " (failed)" if m.get("failed") == "true" else ""
        print(f"{m.get('client', '?')} -> {m.get('server', '?')} "
              f"{number(r['value'][1])}{tag}")
    if not rows:
        print("no edges")


def cmd_rules():
    data = json.loads(get(PROM + "/api/v1/rules"))
    for g in data["data"]["groups"]:
        for r in g["rules"]:
            print(g["name"], r["type"], r.get("name"), r["health"])


def cmd_exemplars(metric, since):
    data = json.loads(get(PROM + "/api/v1/query_exemplars", {
        "query": metric, "start": since, "end": time.time()}))
    seen = []
    for s in data.get("data") or []:
        for e in s.get("exemplars", []):
            tid = e["labels"].get("trace_id")
            if tid and tid not in seen:
                seen.append(tid)
    print("\n".join(seen))


def cmd_exemplar_count(metric, since):
    data = json.loads(get(PROM + "/api/v1/query_exemplars", {
        "query": metric, "start": since, "end": time.time()}))
    series = data.get("data") or []
    print("exemplar series:", len(series), " exemplars:",
          sum(len(s.get("exemplars", [])) for s in series))


def cmd_buckets(metric):
    result = prom(metric)["result"]
    print(len(result), "series; le values:",
          sorted({r["metric"].get("le") for r in result}))


def loki(query, since, limit):
    data = json.loads(get(LOKI + "/loki/api/v1/query_range", {
        "query": query, "start": int(float(since) * 1e9),
        "end": int(time.time() * 1e9), "limit": limit}))
    return data["status"], [line for s in data["data"]["result"]
                            for _, line in s["values"]]


def cmd_loki(query, since, limit="20"):
    status, lines = loki(query, since, limit)
    print("status:", status, " lines:", len(lines))
    for line in lines:
        print("   ", line)


def cmd_loki_count(query, since):
    print(len(loki(query, since, "100")[1]))


def cmd_table():
    """Align tab-separated rows read from stdin into columns."""
    rows = [line.split("\t") for line in sys.stdin.read().splitlines() if line]
    if not rows:
        return
    widths = [max(len(r[i]) for r in rows if i < len(r)) for i in range(len(rows[0]))]
    for r in rows:
        print("  ".join(c.ljust(w) for c, w in zip(r, widths)).rstrip())


COMMANDS = {
    "sum": cmd_sum, "time": cmd_time, "scrape-time": cmd_scrape_time,
    "exposition": cmd_exposition, "instance": cmd_instance, "edges": cmd_edges,
    "rules": cmd_rules, "exemplars": cmd_exemplars,
    "exemplar-count": cmd_exemplar_count, "buckets": cmd_buckets,
    "loki": cmd_loki, "loki-count": cmd_loki_count, "table": cmd_table,
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        fail("usage: query.py <" + "|".join(COMMANDS) + "> [args...]")
    COMMANDS[sys.argv[1]](*sys.argv[2:])
