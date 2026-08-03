#!/usr/bin/env python3
"""Check that printed book listing numbers and companion-code files agree.

Each chapter README carries a Listing-to-File table. The file that backs a
printed listing carries an anchor comment naming that listing number. This
fails when the two disagree, which is what happens when listings renumber
between drafts.

Usage:
    python3 scripts/check_listing_anchors.py
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Matches both anchor comment conventions chapter 7 already established:
#   -- Chapter 7, listing 7.3: description
#   -- ---- Listing 7.2: description
# The trailing colon is load-bearing: source comments elsewhere in a file
# often mention another listing in passing (e.g. "listing 7.2's TO VOLUME
# clause") without a colon. Matching those too would let an incidental
# mention in one file shadow another file's real anchor for that number.
ANCHOR_RE = re.compile(r"[Ll]isting\s+(?P<num>\d+\.\d+):")

# README row looks like: | 7.2 | clickhouse/tiering.sql | hot-to-cold policy |
README_ROW_RE = re.compile(r"^\|\s*(?P<num>\d+\.\d+)\s*\|\s*(?P<path>[^|]+?)\s*\|")

SOURCE_GLOBS = ("**/*.sql", "**/*.yaml", "**/*.yml", "**/*.py", "**/*.xml")
SKIP_DIRS = {"__pycache__", ".pytest_cache", "results", "node_modules"}


def readme_listings(readme):
    """Return {listing_number: relative_path} from a README's listing table."""
    rows = {}
    if not readme.exists():
        return rows
    for line in readme.read_text().splitlines():
        match = README_ROW_RE.match(line)
        if match:
            rows[match.group("num")] = match.group("path").strip("` ")
    return rows


def file_anchors(chapter_dir):
    """Return {listing_number: relative_path} for anchors found in source files."""
    anchors = {}
    for pattern in SOURCE_GLOBS:
        for path in sorted(chapter_dir.glob(pattern)):
            if any(part in SKIP_DIRS for part in path.parts):
                continue
            for line in path.read_text(errors="ignore").splitlines():
                if not line.lstrip().startswith(("--", "#")):
                    continue
                match = ANCHOR_RE.search(line)
                if match:
                    anchors.setdefault(match.group("num"),
                                        str(path.relative_to(chapter_dir)))
    return anchors


def check_chapter(chapter_dir):
    """Return a list of problem strings for one chapter directory."""
    problems = []
    listed = readme_listings(chapter_dir / "README.md")
    found = file_anchors(chapter_dir)

    for num, path in sorted(listed.items()):
        anchor_path = found.get(num)
        if anchor_path is None:
            problems.append(
                f"{chapter_dir.name}: README lists listing {num} -> {path}, "
                f"but no source file carries a matching anchor comment")
        elif anchor_path != path:
            problems.append(
                f"{chapter_dir.name}: README lists listing {num} -> {path}, "
                f"but the anchor comment is in {anchor_path}")

    for num, path in sorted(found.items()):
        if num not in listed:
            problems.append(
                f"{chapter_dir.name}: {path} anchors listing {num}, "
                f"which is missing from the README table")

    return problems


def main():
    problems = []
    for chapter_dir in sorted(ROOT.glob("chapter-*")):
        problems.extend(check_chapter(chapter_dir))

    if problems:
        print("Listing traceability problems:", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1

    print("All listing anchors match README tables.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
