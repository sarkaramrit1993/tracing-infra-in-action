#!/usr/bin/env python3
"""Render git-tracked benchmark JSON into a readable RESULTS.md per chapter.

The benchmark result files live under the chapter whose stack produced them,
but several of them back listings in later chapters. BENCHMARK_CHAPTER is the
single place that mapping is recorded.

Usage:
    python3 scripts/render_results.py            # regenerate
    python3 scripts/render_results.py --check    # exit 1 if regeneration would change anything
"""
import argparse
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Which chapter each benchmark belongs to, independent of which chapter's
# stack produced the file. Several chapter-07 benchmarks back listings in
# chapters 8, 9 and 10.
BENCHMARK_CHAPTER = {
    "compression_ratio": "07",
    "tiering_automation": "07",
    "bloom_index_pruning": "07",
    "tenant_cardinality_blowup": "07",
    "attribute_search_pushdown": "08",
    "sampler_divergence": "09",
    "fingerprint_compression": "09",
    "red_mv_vs_query": "10",
    "store_then_stitch": "05",
    "stream_time": "05",
    # atomicity_audit files key on "mode" rather than "benchmark"
    "none": "05",
    "drop-whole-trace": "05",
    "producer-crash": "05",
    "buffer-overflow": "05",
}

TITLES = {
    "compression_ratio": "Per-column compression",
    "tiering_automation": "Hot-to-cold tiering",
    "bloom_index_pruning": "Bloom index granule pruning",
    "tenant_cardinality_blowup": "Per-tenant attribute cardinality",
    "attribute_search_pushdown": "Attribute search pushdown",
    "sampler_divergence": "Metric divergence across the sampler",
    "fingerprint_compression": "Error fingerprint compression",
    "red_mv_vs_query": "Materialized view against full scan",
    "store_then_stitch": "Store-then-stitch write cost",
    "stream_time": "Stream-time buffer cost",
}

# The four atomicity_audit modes are one experiment, not four. Control run
# first, then the two modes that preserve the invariant, then the one that
# violates it -- this order carries the chapter's argument, so it is not
# sorted alphabetically like every other section.
ATOMICITY_MODES = ["none", "drop-whole-trace", "producer-crash", "buffer-overflow"]
ATOMICITY_NOTE = ("Only buffer-overflow produces partial traces. That is the "
                   "failure the atomicity rule exists to prevent.")

# A handful of result files (store_then_stitch, stream_time) predate the
# "benchmark"/"mode" convention and carry no self-describing field at all.
# Their filenames follow "<name>-YYYY-MM-DD.json", so strip the date suffix
# as a fallback rather than rejecting them.
_DATE_SUFFIX = re.compile(r"-\d{4}-\d{2}-\d{2}$")


def _benchmark_name(data, path):
    name = data.get("benchmark") or data.get("mode")
    if name is not None:
        return name
    return _DATE_SUFFIX.sub("", path.stem)


def _measured_at(data, filename):
    """Parse measured_at_utc for tie-breaking reruns of the same benchmark.

    Only called when two files share a benchmark name and one has to be
    picked as "latest" -- a wrong pick here is a published wrong number, so
    an unparseable or missing timestamp fails loudly instead of silently
    falling back to string or filename order.
    """
    value = data.get("measured_at_utc")
    if value is None:
        raise ValueError(f"{filename}: no measured_at_utc to compare against "
                          "another run of the same benchmark")
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{filename}: measured_at_utc {value!r} is not a "
                          "valid ISO-8601 timestamp") from exc


RESULTS_PATHSPEC = "chapter-*/benchmarks/results/*.json"


def _result_paths():
    """Return the git-tracked result files, newest-wins handled by the caller.

    The glob would also pick up whatever the reader's own benchmark runs left
    behind, and `benchmarks/.gitignore` keeps those out of git on purpose. If
    they counted, `--check` would fail on a clean tree the moment somebody ran a
    benchmark, and re-rendering would publish numbers from a file git will never
    carry. Asking git for the list makes `--check` mean "the tables match the
    tracked record" instead of "the tables match this laptop".
    """
    try:
        out = subprocess.run(["git", "-C", str(ROOT), "ls-files", "-z",
                              "--", RESULTS_PATHSPEC],
                             capture_output=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        print("render_results: git unavailable or not a repository, falling "
              "back to every result file on disk", file=sys.stderr)
        return sorted(ROOT.glob(RESULTS_PATHSPEC))
    # A tracked file staged for deletion is still listed but no longer on disk.
    paths = [ROOT / name for name in out.decode().split("\0") if name]
    return sorted(p for p in paths if p.is_file())


def load_results():
    """Return {chapter: [(name, data, filename)]}, sorted for determinism.

    Reruns share a benchmark name (e.g. compression_ratio measured on two
    different dates). Only the most recent measurement -- by measured_at_utc,
    parsed and compared as real timestamps -- is kept, so a benchmark never
    gets two sections telling a reader two different numbers under the same
    heading.
    """
    latest_by_name = {}  # name -> (data, filename)
    for path in _result_paths():
        data = json.loads(path.read_text())
        name = _benchmark_name(data, path)
        if BENCHMARK_CHAPTER.get(name) is None:
            continue
        current = latest_by_name.get(name)
        if current is not None:
            existing_data, existing_filename = current
            if _measured_at(data, path.name) <= _measured_at(existing_data, existing_filename):
                continue
        latest_by_name[name] = (data, path.name)

    by_chapter = {}
    for name, (data, filename) in latest_by_name.items():
        chapter = BENCHMARK_CHAPTER[name]
        by_chapter.setdefault(chapter, []).append((name, data, filename))
    for entries in by_chapter.values():
        entries.sort(key=lambda entry: entry[2])
    return by_chapter


def _table(rows, headers):
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join("---" for _ in headers) + "|"]
    for row in rows:
        out.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(out)


def _render_compression(data):
    cols = sorted(data["columns"], key=lambda c: -c["ratio"])
    rows = [(c["column"], f"{c['stored_bytes']:,}", f"{c['raw_bytes']:,}",
             f"{c['ratio']:.2f}x") for c in cols]
    return (f"{data['num_spans_loaded']:,} spans at {data['spans_per_trace']} "
            f"spans per trace.\n\n"
            + _table(rows, ["Column", "Stored bytes", "Raw bytes", "Ratio"]))


def _render_generic(data):
    """Flat key-value table for the benchmarks without a bespoke renderer."""
    rows = []
    for key, value in data.items():
        if key in ("benchmark", "mode", "measured_at_utc", "note"):
            continue
        if isinstance(value, dict):
            for sub, subval in value.items():
                rows.append((f"{key}.{sub}", subval))
        else:
            rows.append((key, value))
    return _table(rows, ["Metric", "Value"])


RENDERERS = {"compression_ratio": _render_compression}


def _render_atomicity_section(by_mode):
    """One comparison table across the atomicity_audit modes present.

    Combining four files into one section means a note on any one of them
    would otherwise never reach the output. Collect notes in table row
    order, drop exact duplicates, and print each as its own blockquote
    after the table's closing sentence.
    """
    modes = [m for m in ATOMICITY_MODES if m in by_mode]
    sources = ", ".join(f"`{by_mode[m][1]}`" for m in modes)
    rows = [(m, by_mode[m][0]["result"]["whole"], by_mode[m][0]["result"]["absent"],
             by_mode[m][0]["result"]["partial"]) for m in modes]
    table = _table(rows, ["Scenario", "Whole traces", "Absent", "Partial"])

    notes = []
    for m in modes:
        note = by_mode[m][0].get("note")
        if note and note not in notes:
            notes.append(note)

    lines = [
        "## Atomicity audit", "",
        f"Source: {sources}", "",
        table, "",
        f"> {ATOMICITY_NOTE}",
    ]
    lines.extend(f"> {note}" for note in notes)
    lines.append("")
    return "\n".join(lines)


def _render_entry_section(name, data, filename):
    """One '## Title' section for a single benchmark JSON file."""
    lines = [f"## {TITLES.get(name, name)}", "",
             f"Source: `{filename}`"]
    measured = data.get("measured_at_utc")
    if measured:
        lines.append(f"Measured: {measured[:10]}")
    lines.append("")
    lines.append(RENDERERS.get(name, _render_generic)(data))
    lines.append("")
    note = data.get("note")
    if note:
        lines.append(f"> {note}")
        lines.append("")
    return "\n".join(lines)


def render_chapter(chapter):
    """Return the full RESULTS.md text for one chapter."""
    entries = load_results().get(chapter, [])
    if not entries:
        return ""

    lines = [f"# Chapter {int(chapter)} benchmark results", "",
             "Generated by `scripts/render_results.py` from the git-tracked JSON in",
             "`benchmarks/results/`. A run you have not added to git is ignored. That",
             "directory is gitignored, so add the file with `git add -f` first, then",
             "re-run the script.",
             ""]

    atomicity_by_mode = {name: (data, filename) for name, data, filename in entries
                         if name in ATOMICITY_MODES}
    remaining = [entry for entry in entries if entry[0] not in ATOMICITY_MODES]

    if atomicity_by_mode:
        lines.append(_render_atomicity_section(atomicity_by_mode))

    for name, data, filename in remaining:
        lines.append(_render_entry_section(name, data, filename))

    return "\n".join(lines).rstrip() + "\n"


def target_path(chapter):
    return ROOT / f"chapter-{chapter}" / "RESULTS.md"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true",
                        help="exit 1 if any RESULTS.md is out of date")
    args = parser.parse_args()

    stale = []
    for chapter in sorted(load_results()):
        path = target_path(chapter)
        if not path.parent.exists():
            continue
        rendered = render_chapter(chapter)
        current = path.read_text() if path.exists() else ""
        if rendered == current:
            continue
        if args.check:
            stale.append(str(path.relative_to(ROOT)))
        else:
            path.write_text(rendered)
            print(f"wrote {path.relative_to(ROOT)}")

    if stale:
        print("out of date, re-run scripts/render_results.py:", file=sys.stderr)
        for item in stale:
            print(f"  {item}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
