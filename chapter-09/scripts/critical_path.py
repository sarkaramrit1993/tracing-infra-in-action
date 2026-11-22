#!/usr/bin/env python3
"""Walks one stored trace backwards and prints each step's share of the run.

Not a reader step. show-agent-critical-path.sh pipes the trace's spans in as
JSON lines (span_id, parent_span_id, span_name, start_ns, duration_ns).

The walk is the one chapter 9 describes: from the parent's end, take the
child that finished last at or before the boundary, move the boundary to
that child's start, and repeat. Ties go to the longer span, then the span id.

Usage: python3 scripts/critical_path.py
"""
import json
import sys


def load(lines):
    spans = {}
    for line in lines:
        row = json.loads(line)
        start = int(row["start_ns"])
        spans[row["span_id"]] = {
            "id": row["span_id"],
            "parent": row["parent_span_id"],
            "name": row["span_name"],
            "start": start,
            "end": start + int(row["duration_ns"]),
            "children": [],
        }
    for span in spans.values():
        if span["parent"] in spans:
            spans[span["parent"]]["children"].append(span)
    return spans


def on_path(span):
    """Nanoseconds of each span on the critical path, by span id."""
    time_on = {span["id"]: 0}
    boundary = span["end"]
    left = list(span["children"])
    while True:
        done = [c for c in left if c["end"] <= boundary]
        if not done:
            break
        child = max(done, key=lambda c: (c["end"], c["end"] - c["start"], c["id"]))
        time_on[span["id"]] += boundary - child["end"]
        time_on.update(on_path(child))
        boundary = child["start"]
        left = [c for c in left if c["end"] <= boundary]
    time_on[span["id"]] += boundary - span["start"]
    return time_on


def subtree(span):
    yield span["id"]
    for child in span["children"]:
        yield from subtree(child)


def main():
    spans = load(sys.stdin)
    roots = [s for s in spans.values() if s["parent"] not in spans]
    if len(roots) != 1:
        sys.exit(f"expected one root span, found {len(roots)}")
    root = roots[0]
    time_on = on_path(root)
    total = root["end"] - root["start"]
    steps = []
    for child in sorted(root["children"], key=lambda c: (c["start"], c["id"])):
        held = sum(time_on.get(i, 0) for i in subtree(child))
        if held:
            steps.append((child["name"], held))
    own = time_on[root["id"]]
    width = max([len(name) for name, _ in steps] + [len("between steps")])
    for number, (name, held) in enumerate(steps, 1):
        print(f"{number}  {name.ljust(width)}  {100 * held / total:5.1f}%")
    print(f"   {'between steps'.ljust(width)}  {100 * own / total:5.1f}%")
    top = max(range(len(steps)), key=lambda i: steps[i][1])
    print(f"The call holding the most time: step {top + 1}, {steps[top][0]}, "
          f"{100 * steps[top][1] / total:.1f}% of the run")


if __name__ == "__main__":
    main()
