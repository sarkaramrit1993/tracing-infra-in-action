#!/usr/bin/env python3
"""Every pinned image, base image and Python package must match versions.yaml.

Usage: python3 scripts/check_versions.py
Exit 1 on any disagreement. Known drift listed under open_drift is printed, not failed.
"""
import re
import shlex
import sys
from collections import namedtuple
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
SKIP_DIRS = {".git", "node_modules", "__pycache__", ".venv", "venv", ".pytest_cache"}

Pin = namedtuple("Pin", "chapter path line kind name spec")

IMAGE_LINE = re.compile(r"^\s*-?\s*image:\s*[\"']?([^\s\"'#]+)")
FROM_LINE = re.compile(r"^\s*FROM\s+(?:--platform=\S+\s+)?(\S+)(?:\s+AS\s+(\S+))?", re.IGNORECASE)
REQ_LINE = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]*)(?:\[[^\]]*\])?\s*(.*)$")


def _files(root):
    for path in sorted(root.rglob("*")):
        if path.is_file() and not SKIP_DIRS & set(path.relative_to(root).parts):
            yield path


def _chapter(root, path):
    first = path.relative_to(root).parts[0]
    return first if re.fullmatch(r"chapter-\d\d", first) else "root"


def _split_image(ref):
    name, _, tag = ref.rpartition(":") if ":" in ref.rsplit("/", 1)[-1] else (ref, "", "")
    name = name.removeprefix("docker.io/").removeprefix("library/")
    return name, tag


PIP_OPTIONS_WITH_VALUE = {"-r", "--requirement", "-c", "--constraint", "-i", "--index-url",
                          "--extra-index-url", "-t", "--target", "-f", "--find-links"}


def _logical_lines(text):
    """Join backslash continuations; yield (first line number, joined line)."""
    start, parts = None, []
    for no, line in enumerate(text.splitlines(), 1):
        start = start or no
        if line.rstrip().endswith("\\"):
            parts.append(line.rstrip()[:-1])
            continue
        yield start, " ".join(parts + [line])
        start, parts = None, []
    if parts:
        yield start, " ".join(parts)


def _pip_installs(line):
    """Package specs named on a `pip install` inside a RUN line."""
    specs = []
    for command in re.split(r"&&|\|\||;|\|", line):
        try:
            words = shlex.split(command)
        except ValueError:
            continue
        for i, word in enumerate(words[:-1]):
            if re.fullmatch(r"(?:python3?\s+-m\s+)?pip3?", word) and words[i + 1] == "install":
                skip = False
                for arg in words[i + 2:]:
                    if skip:
                        skip = False
                    elif arg in PIP_OPTIONS_WITH_VALUE:
                        skip = True
                    elif not arg.startswith("-"):
                        specs.append(arg)
                break
    return specs


def normalize(package):
    return re.sub(r"[-_.]+", "-", package).lower()


def collect_pins(root):
    root = Path(root)
    pins = []
    for path in _files(root):
        rel, chapter = str(path.relative_to(root)), _chapter(root, path)
        name = path.name
        if name.endswith((".yml", ".yaml")) and (name.startswith("docker-compose") or path.parent.name == "k8s"):
            for no, line in enumerate(path.read_text().splitlines(), 1):
                if m := IMAGE_LINE.match(line):
                    pins.append(Pin(chapter, rel, no, "image", *_split_image(m.group(1))))
        elif name.startswith("Dockerfile"):
            stages = set()
            for no, line in _logical_lines(path.read_text()):
                if m := FROM_LINE.match(line):
                    if m.group(1) not in stages:
                        pins.append(Pin(chapter, rel, no, "image", *_split_image(m.group(1))))
                    if m.group(2):
                        stages.add(m.group(2))
                for spec in _pip_installs(line):
                    if m := REQ_LINE.match(spec):
                        pins.append(Pin(chapter, rel, no, "package", normalize(m.group(1)),
                                        m.group(2).replace(" ", "")))
        elif name.startswith("requirements") and name.endswith(".txt"):
            for no, line in enumerate(path.read_text().splitlines(), 1):
                line = line.split("#", 1)[0].strip()
                if line and not line.startswith("-") and (m := REQ_LINE.match(line)):
                    pins.append(Pin(chapter, rel, no, "package", normalize(m.group(1)),
                                    m.group(2).replace(" ", "").split(";", 1)[0]))
    return pins


def load_registry(path):
    return yaml.safe_load(Path(path).read_text())


def check(pins, registry):
    """Return (errors, open_drift_hits)."""
    errors, drift_hits = [], []
    tables = {"image": registry.get("images") or {}, "package": registry.get("packages") or {}}
    local = registry.get("local_images") or {}
    drift = registry.get("open_drift") or []
    used_drift, used = set(), set()

    for pin in pins:
        where = f"{pin.path}:{pin.line}"
        shown = f"{pin.name}:{pin.spec}" if pin.kind == "image" else f"{pin.name}{pin.spec}"
        if pin.kind == "image" and f"{pin.name}:{pin.spec}" in local:
            used.add(("local", f"{pin.name}:{pin.spec}"))
            continue
        entry = tables[pin.kind].get(pin.name)
        if entry is None:
            errors.append(f"{where}: {shown} is not in versions.yaml")
            continue
        used.add((pin.kind, pin.name))
        if not pin.spec or pin.spec == "latest":
            got = None
        elif pin.kind == "image":
            got = pin.spec
        elif pin.spec.startswith("==") and "," not in pin.spec:
            got = pin.spec[2:]
        else:
            got = pin.spec
        for i, d in enumerate(drift):
            if (d["component"], d["chapter"], str(d["version"])) == (pin.name, pin.chapter, str(got or pin.spec)):
                used_drift.add(i)
                drift_hits.append(f"{where}: {shown} (open drift, registry says "
                                  f"{entry.get('version') or entry.get('range')})")
                break
        else:
            if got is None:
                errors.append(f"{where}: {shown} is unpinned")
                continue
            allowed = {str(entry.get("version") or entry.get("range"))}
            allowed |= {str(x["version"]) for x in entry.get("exceptions") or [] if x["chapter"] == pin.chapter}
            if got not in allowed:
                errors.append(f"{where}: {shown} disagrees with versions.yaml "
                              f"({' or '.join(sorted(allowed))})")

    for i, d in enumerate(drift):
        if i not in used_drift:
            errors.append(f"stale open_drift entry {d['component']} {d['version']} in {d['chapter']}: "
                          "no pin matches it any more, delete it")
    for kind, table in tables.items():
        for name in table:
            if (kind, name) not in used:
                errors.append(f"versions.yaml lists {name}, which no file pins")
    for ref in local:
        if ("local", ref) not in used:
            errors.append(f"versions.yaml lists local image {ref}, which no file uses")
    return errors, drift_hits


def main():
    errors, drift_hits = check(collect_pins(ROOT), load_registry(ROOT / "versions.yaml"))
    for line in drift_hits:
        print("OPEN  " + line)
    for line in errors:
        print("FAIL  " + line)
    print(f"{len(errors)} failing, {len(drift_hits)} open drift")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
