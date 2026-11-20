import json
import sys
from pathlib import Path

SETTINGS = (("no_content", "capture off"), ("span_only", "capture on"))
ORDER = ("invoke_agent", "chat", "execute_tool", "retrieval", "http_client")


def load(root, condition):
    files = sorted((Path(root) / condition).glob("agent-trace-*.json"))
    if not files:
        return None
    result = json.loads(files[-1].read_text())
    return next(c for c in result["cells"] if c["condition"] == condition)


def span_bytes(values):
    low, high = min(values), max(values)
    return f"{low}" if low == high else f"{low} to {high}"


def table(rows):
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    for n, row in enumerate(rows):
        print("  ".join(c.ljust(w) if i == 0 else c.rjust(w) for i, (c, w) in enumerate(zip(row, widths))).rstrip())
        if n == 0:
            print("  ".join("-" * w for w in widths))


def main(root):
    cells = [load(root, key) for key, _ in SETTINGS]
    missing = "not measured yet"
    rows = [["", "spans per run", "capture off (bytes per span)", "capture on (bytes per span)"]]
    for kind in ORDER:
        row = [kind]
        counts = [c["spans_by_type"][kind]["count"] for c in cells if c]
        row.append(str(counts[0]) if counts else "")
        for c in cells:
            row.append(span_bytes(c["spans_by_type"][kind]["bytes"]) if c else missing)
        rows.append(row)
    total = [c["span_count"] for c in cells if c]
    rows.append(["whole trace", str(total[0]) if total else ""] + [f"{c['trace_bytes'][0]}" if c else missing for c in cells])
    rows.append(["largest span", ""] + [f"{c['largest_span']}" if c else missing for c in cells])
    table(rows)
    same = [c for c in cells if c]
    if same:
        identical = "the same" if all(c["bytes_identical_across_runs"] for c in same) else "not the same"
        print(f"\nAcross the {same[0]['runs']} runs the byte counts were {identical} every time.")


if __name__ == "__main__":
    main(sys.argv[1])
