#!/usr/bin/env python3
"""Chapter 8 offline tests. No Docker, no network.

Every printed listing is compared against the chapter itself, line for line.
The earlier version of this file compared them against fragments typed out
here instead, which is not the same claim: it proves the shipped file agrees
with a copy of the book, and a copy goes stale the first time the book is
edited. Both suites were fully green while two of the three `.sql` files had
drifted off the page, and chapter 7 lost a listing the same way before that.

The comparison needs the manuscript, which lives in its own repository. Without
it those tests skip and the rest still run. See tests/chapter_source.py.

Usage:  python3 tests/test_static.py
"""
import re
import sys
import unittest
from difflib import unified_diff
from pathlib import Path

import yaml

import chapter_source

CHAPTER = Path(__file__).resolve().parent.parent
RESULTS = []

# Which file backs which printed listing, and the anchor the book gives it.
LISTINGS = (
    ("8.1", "ch8-listing-1", "clickhouse/unbiased.sql"),
    ("8.2", "ch8-listing-2", "clickhouse/skipindex.sql"),
    ("8.3", "ch8-listing-3", "clickhouse/rollup.sql"),
)


def test(fn):
    RESULTS.append(fn)
    return fn


# The decorator is named test, so pytest tries to collect it as one and reports
# an error about a missing fixture. It is not a test.
test.__test__ = False


def read(rel):
    return (CHAPTER / rel).read_text()


def the_chapter():
    """The one chapter 8 source on this machine, or a reason there is no comparison."""
    found = chapter_source.find_chapters(8)
    told = chapter_source.configured_location()
    if not found:
        if told is not None:
            raise AssertionError(
                f"this machine points at {told}, and there is no chapter 8 source "
                "there. A check that was told where to look and could not look "
                "there has to fail; skipping would read as a pass")
        raise unittest.SkipTest(
            "the book's chapter 8 source is not on this machine, so the printed "
            f"listings cannot be compared against. Set {chapter_source.ENV_VAR} to "
            "a manuscript checkout, or to the chapter file, to run this check")
    if len(found) > 1:
        raise AssertionError(
            "several chapter 8 sources are here and they do not have to agree, so "
            "comparing against whichever sorts first proves nothing. Name the one "
            f"that counts, in {chapter_source.ENV_VAR} or in "
            f"{chapter_source.POINTER}:\n  "
            + "\n  ".join(str(p) for p in found))
    return found[0]


def assert_matches_the_book(number, anchor, rel):
    """Fail unless the shipped file reproduces the printed listing exactly."""
    chapter = the_chapter()
    printed = chapter_source.listing_sql(chapter, anchor)
    shipped = chapter_source.shipped_sql(CHAPTER / rel, number)
    if printed == shipped:
        return
    diff = "\n".join(unified_diff(
        printed.splitlines(), shipped.splitlines(),
        fromfile=f"listing {number} as printed in {chapter.name}",
        tofile=f"chapter-08/{rel}",
        lineterm=""))
    raise AssertionError(
        f"chapter-08/{rel} no longer reproduces printed listing {number}:\n{diff}")


# ---------------------------------------------------------------- the schema

@test
def test_schema_has_parent_span_id():
    """Without it there is no way to ask a question about requests."""
    body = read("clickhouse/init.sql")
    assert "parent_span_id String" in body, \
        "init.sql must carry parent_span_id; every count in chapter 8 filters on it"


@test
def test_schema_has_adjusted_count_defaulting_to_one():
    body = read("clickhouse/init.sql")
    assert re.search(r"adjusted_count\s+Float64\s+DEFAULT\s+1\.0", body), \
        "adjusted_count must default to 1.0 so an unsampled span weighs one"


@test
def test_schema_ships_without_the_trace_id_bloom():
    """Listing 8.2 adds it. If the table already has one there is nothing to show."""
    body = read("clickhouse/init.sql")
    assert "bloom_filter" not in body, \
        "init.sql must NOT create a bloom index; listing 8.2 exists to add it"


@test
def test_schema_keeps_listing_7_1_column_types():
    """Chapter 8 inherits chapter 7's table. The shared columns must not drift."""
    body = read("clickhouse/init.sql")
    for column in ("timestamp      DateTime64(9) CODEC(Delta(8), ZSTD(1))",
                   "trace_id       String CODEC(ZSTD(1))",
                   "service_name   LowCardinality(String) CODEC(ZSTD(1))",
                   "status_code    LowCardinality(String) CODEC(ZSTD(1))",
                   "duration_ns    UInt64 CODEC(T64, ZSTD(1))",
                   "attributes     Map(LowCardinality(String), String) CODEC(ZSTD(3))"):
        assert column in body, f"listing 7.1 column drifted: {column}"


@test
def test_ground_truth_and_policy_tables_exist():
    body = read("clickhouse/init.sql")
    assert "tracing.ground_truth" in body, \
        "without ground_truth the exercises can compare but never grade"
    assert "tracing.sampling_policy" in body, \
        "the keep rate has to be readable as data, not buried in a script"


# -------------------------------------------------------------- the listings

@test
def test_listing_8_1_matches_the_book():
    assert_matches_the_book(*LISTINGS[0])


@test
def test_listing_8_2_matches_the_book():
    assert_matches_the_book(*LISTINGS[1])


@test
def test_listing_8_3_matches_the_book():
    assert_matches_the_book(*LISTINGS[2])


@test
def test_every_listing_file_fences_its_printed_region():
    """The comparison above reads between these markers. Lose them and it reads nothing."""
    for number, _, rel in LISTINGS:
        body = read(rel)
        assert f"-- ---- Listing {number}:" in body, f"{rel} does not open listing {number}"
        assert f"-- ---- end listing {number} ----" in body, \
            f"{rel} does not close listing {number}"


@test
def test_the_setup_stays_outside_the_fences():
    """What the page does not print has to sit where the comparison will not see it.

    Each file carries a line or two the reader needs and the book leaves out. A
    second run of listing 8.2 fails on ADD INDEX unless the old index is dropped
    first, and TRUNCATE does not drop index definitions. Listing 8.3 is the same
    story for the view. Neither belongs in the printed listing, so both live
    above the opening marker, and unbiased.sql keeps its ground-truth query below
    the closing one. Move any of them inside and the comparison fails, which is
    correct but reads as drift; leave them out of the file and the reader is the
    one who finds out.
    """
    for rel, statement in (
            ("clickhouse/skipindex.sql",
             "ALTER TABLE tracing.otel_traces DROP INDEX IF EXISTS idx_trace_id;"),
            ("clickhouse/rollup.sql", "DROP VIEW IF EXISTS tracing.red_by_service;"),
            ("clickhouse/unbiased.sql", "FROM tracing.ground_truth;")):
        body = read(rel)
        assert statement in body, f"{rel} lost its setup: {statement}"
        number = next(n for n, _, r in LISTINGS if r == rel)
        assert statement not in chapter_source.shipped_sql(CHAPTER / rel, number), \
            f"{rel} moved setup inside listing {number}, which the book does not print"


@test
def test_listing_8_2_keeps_all_three_explains():
    """A shape check that runs with or without the book on this machine.

    The comparison above is the real one, and it skips for anyone who has the
    code and not the manuscript, which includes every automated run. These two
    pin the things that went wrong before, so a drift back does not wait for a
    machine that happens to hold both.
    """
    printed = chapter_source.shipped_sql(CHAPTER / "clickhouse/skipindex.sql", "8.2")
    explains = printed.count("EXPLAIN indexes = 1")
    assert explains == 3, (
        "listing 8.2 prints three EXPLAINs, the reading before the index, the "
        "reading after it, and the hour-bounded lookup of callout 6; this file "
        f"has {explains}")
    for setting in ("use_query_condition_cache = 0",
                    "use_skip_indexes_on_data_read = 0"):
        assert printed.count(setting) == explains, (
            f"every EXPLAIN has to carry `{setting}`, or that one answers from "
            "state the server already holds and its granule count never moves")


@test
def test_listing_8_3_keeps_minute_first_in_the_sort_key():
    """Trailing `minute` is the anti-pattern callout 2 exists to warn against."""
    printed = chapter_source.shipped_sql(CHAPTER / "clickhouse/rollup.sql", "8.3")
    for clause in ("PARTITION BY toYYYYMM(minute)",
                   "ORDER BY (minute, service_name, status_code)",
                   "TTL minute + INTERVAL 90 DAY"):
        assert clause in printed, f"listing 8.3 lost `{clause}`"


@test
def test_the_rollup_exercise_changes_one_thing_at_a_time():
    """Each demo view in the exercise is listing 8.3 with a single edit.

    The prose says so: one is "one word shorter than listing 8.3", the other is
    "one keyword apart", and the reader is asked to believe the number moved for
    that reason. Let the partition, the sort key or the TTL drift too and the
    demonstration is of nothing in particular. The sort key is the one that
    bites. Trailing `minute` is what callout 2 of listing 8.3 warns against, so
    an exercise shipping it teaches the opposite of the lesson it is printed for.

    The views are built by scripts and printed in the exercise, so both are read.
    The SummingMergeTree demos further down build different views on purpose and
    are not listing 8.3 variants.
    """
    sources = {"exercises/rollup.md": read("exercises/rollup.md")}
    for path in sorted((CHAPTER / "scripts").glob("build-view-*.sh")):
        sources[f"scripts/{path.name}"] = path.read_text()
    seen = set()
    for rel, body in sources.items():
        for block in body.split("CREATE MATERIALIZED VIEW ")[1:]:
            name = block.split("\n", 1)[0].strip()
            if not name.startswith("tracing.red_no_"):
                continue
            seen.add((rel.startswith("scripts/"), name))
            head = block.split("AS SELECT", 1)[0]
            for clause in ("ENGINE = SummingMergeTree",
                           "PARTITION BY toYYYYMM(minute)",
                           "ORDER BY (minute, service_name, status_code)",
                           "TTL minute + INTERVAL 90 DAY"):
                assert clause in head, (
                    f"{rel}: the {name} view leaves out `{clause}`, so it differs from "
                    "listing 8.3 by more than the one edit the exercise demonstrates")
    for where in (True, False):
        names = {n for w, n in seen if w is where}
        assert names == {"tracing.red_no_populate", "tracing.red_no_root"}, (
            f"expected both demo views in {'the scripts' if where else 'exercises/rollup.md'}, "
            f"found {sorted(names)}")


# ----------------------------------------------------------------- the stack

@test
def test_compose_parses_and_pins_one_tag():
    compose = yaml.safe_load(read("docker-compose.yml"))
    services = compose["services"]
    assert set(services) == {"clickhouse"}, \
        "chapter 8 is about the store's answers, not the path into it; one service"
    image = services["clickhouse"]["image"]
    assert image == "clickhouse/clickhouse-server:26.1", f"unpinned or moved: {image}"


@test
def test_the_readme_names_the_tag_the_compose_file_pins():
    """The manifest is a claim about what runs, so it has to track the repin.

    The tag moved once already, for listing 8.2's `use_skip_indexes_on_data_read`,
    and the README went on naming the old one and asserting parity with chapter 7
    that had stopped being true. A version table nobody checks is worse than none,
    because it is read as checked.
    """
    compose = yaml.safe_load(read("docker-compose.yml"))
    image = compose["services"]["clickhouse"]["image"]
    readme = read("README.md")
    assert f"`{image}`" in readme, \
        f"README's version manifest does not name {image}, which is what compose pins"


@test
def test_compose_mounts_init_sql_into_the_entrypoint():
    compose = yaml.safe_load(read("docker-compose.yml"))
    mounts = compose["services"]["clickhouse"]["volumes"]
    assert any("docker-entrypoint-initdb.d" in m for m in mounts), \
        "init.sql must be applied on first boot or nothing exists to query"


@test
def test_generator_arithmetic_closes():
    """The reader is told they can check the weights on paper. Check it here too."""
    src = read("generate/generate.py")
    ns = {}
    consts = "\n".join(l for l in src.splitlines()
                       if re.match(r"^[A-Z_]+(, [A-Z_]+)* = [0-9_, /*+()-]+$", l))
    exec(compile(consts, "<consts>", "exec"), ns)
    for name in ("POPULATION", "NORMAL", "SLOW", "ERROR",
                 "NORMAL_KEEP", "SLOW_KEEP", "ERROR_KEEP"):
        assert name in ns, f"generate.py no longer defines {name}"
    pop, normal, slow, error = ns["POPULATION"], ns["NORMAL"], ns["SLOW"], ns["ERROR"]
    nk, sk, ek = ns["NORMAL_KEEP"], ns["SLOW_KEEP"], ns["ERROR_KEEP"]
    assert normal + slow + error == pop, \
        f"classes sum to {normal + slow + error}, not the population {pop}"
    weighted = (normal // nk) * nk + (slow // sk) * sk + (error // ek) * ek
    assert weighted == pop, \
        f"weights sum to {weighted}, so sum(adjusted_count) will not reproduce {pop}"
    assert (slow + error) / pop < 0.01, \
        ("slow and error must stay under one percent of the population, or the true p99 "
         "leaves the normal band and every printed number in the chapter moves. The "
         "weighted query keeps finding the truth at any split; what closes is its "
         "distance from the unweighted one, and by about three percent there is no "
         "contrast left to show")


@test
def test_no_hash_comments_inside_bash_blocks():
    """A reader pastes the whole block. zsh turns a bare # into an argument."""
    offenders = []
    for md in sorted(CHAPTER.glob("*.md")) + sorted(CHAPTER.glob("exercises/*.md")):
        inside = False
        for n, line in enumerate(md.read_text().splitlines(), 1):
            s = line.strip()
            if s.startswith("```"):
                inside = s.startswith("```bash") or s.startswith("```sh")
                continue
            if inside and re.search(r"(^|\s)#", line):
                offenders.append(f"{md.relative_to(CHAPTER)}:{n}")
    assert not offenders, "bare # inside a bash block: " + ", ".join(offenders)


def _shell_commands(text):
    """Yield (first line number, whole command) with backslash continuations joined.

    A `clickhouse-client \\` at the end of one line puts its redirect on the
    next one. Read line by line and that command looks like it has no redirect.
    """
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        start = i + 1
        parts = [lines[i]]
        while parts[-1].rstrip().endswith("\\") and i + 1 < len(lines):
            i += 1
            parts.append(lines[i])
        yield start, " ".join(part.rstrip().rstrip("\\") for part in parts)
        i += 1


@test
def test_every_clickhouse_helper_closes_stdin():
    """The trap that hung both of chapter 7's scripts for a reviewer."""
    offenders = []
    for path in (sorted(CHAPTER.glob("*.md")) + sorted(CHAPTER.glob("exercises/*.md"))
                 + sorted(CHAPTER.glob("tests/*.sh"))
                 + sorted(CHAPTER.glob("scripts/*.sh"))):
        for n, command in _shell_commands(path.read_text()):
            if "clickhouse-client" not in command or command.strip().startswith("#"):
                continue
            if "< /dev/null" in command or "--multiquery <" in command or "--query" not in command:
                continue
            offenders.append(f"{path.relative_to(CHAPTER)}:{n}")
    assert not offenders, "clickhouse-client without stdin closed: " + ", ".join(offenders)


# ------------------------------------------------------- the reader's path

READER_DOCS = ("README.md", "exercises/unbiased.md", "exercises/rollup.md")
GENERATE = "python3 generate/generate.py"


def _fences(rel, lang):
    """Yield (first body line number, [body lines]) for each ```lang fence."""
    lines = read(rel).splitlines()
    i = 0
    while i < len(lines):
        if lines[i].strip() == f"```{lang}":
            start = i + 2
            body = []
            i += 1
            while i < len(lines) and lines[i].strip() != "```":
                body.append(lines[i])
                i += 1
            yield start, body
        i += 1


def _steps(rel):
    """Every command a page runs, in order: a script name, or GENERATE."""
    for start, body in _fences(rel, "bash"):
        for ln in body:
            if ln.strip() == GENERATE:
                yield start, GENERATE
            for m in re.findall(r"\./scripts/([\w-]+\.sh)", ln):
                yield start, m


def _scripts_that(marker):
    return {p.name for p in (CHAPTER / "scripts").glob("*.sh")
            if p.name != "lib.sh" and marker in p.read_text()}


@test
def test_no_bash_block_defines_a_function():
    """A reader pastes a block and expects a result, not a shell to set up.

    The `ch` and `ch_file` helpers used to be pasted in first. A function
    defined in one block and used in another breaks for anyone who opens a new
    terminal, or starts an exercise in the middle.
    """
    offenders = []
    for rel in READER_DOCS:
        for start, body in _fences(rel, "bash"):
            for ln in body:
                if re.match(r"\s*(function\s+)?[A-Za-z_][\w-]*\s*\(\)\s*\{?", ln):
                    offenders.append(f"{rel}:{start}: {ln.strip()}")
    assert not offenders, "shell function defined in a bash block: " + "; ".join(offenders)


@test
def test_no_bash_block_leans_on_a_shell_variable():
    """`MIN=$(...)` set in one line and read in the next was the old pattern. A
    new terminal, or a block skipped, and the query runs on an empty string."""
    offenders = []
    for rel in READER_DOCS:
        for start, body in _fences(rel, "bash"):
            for ln in body:
                if re.search(r"\$(?!\?)[{(A-Za-z_]", ln):
                    offenders.append(f"{rel}:{start}: {ln.strip()}")
    assert not offenders, "shell variable in a bash block: " + "; ".join(offenders)


@test
def test_every_script_a_page_runs_exists_and_is_executable():
    referenced = {name for rel in READER_DOCS for _, name in _steps(rel)} - {GENERATE}
    assert referenced, "no page runs a script; the regex has stopped matching"
    for name in sorted(referenced):
        path = CHAPTER / "scripts" / name
        assert path.exists(), f"a page runs scripts/{name}, which is not here"
        assert path.stat().st_mode & 0o111, f"scripts/{name} is not executable"


@test
def test_every_script_is_strict_bash_and_used():
    """Every script must fail loudly, which is what lib.sh's `set -euo pipefail`
    gives it, and run on the bash 3.2 macOS ships."""
    used = {name for rel in READER_DOCS for _, name in _steps(rel)}
    lib = read("scripts/lib.sh")
    assert "set -euo pipefail" in lib
    for path in sorted((CHAPTER / "scripts").glob("*.sh")):
        text = path.read_text()
        assert not re.search(r"declare -A|mapfile|readarray|\$\{\w+,,\}|\$\{\w+\^\^\}", text), \
            f"{path.name} uses a bash 4 feature that macOS bash 3.2 does not have"
        if path.name == "lib.sh":
            continue
        assert text.startswith("#!/usr/bin/env bash\n"), f"{path.name} has no bash shebang"
        assert 'source "$(dirname "$0")/lib.sh"' in text, f"{path.name} does not source lib.sh"
        assert path.name in used, f"scripts/{path.name} is never run by any page"


@test
def test_every_sql_block_on_a_page_is_sql_that_really_runs():
    """The SQL printed under a step is what the reader copies to run it by hand.

    It has to be the SQL the script runs, or the page teaches one query and the
    output comes from another. Whitespace is ignored; nothing else is.
    """
    def squash(text):
        return " ".join(text.split()).rstrip(";").strip()
    corpus = squash("\n".join(p.read_text() for p in
                              sorted((CHAPTER / "scripts").glob("*.sh"))
                              + sorted((CHAPTER / "clickhouse").glob("*.sql"))))
    offenders = []
    for rel in READER_DOCS:
        for start, body in _fences(rel, "sql"):
            if squash("\n".join(body)) not in corpus:
                offenders.append(f"{rel}:{start}")
    assert not offenders, "SQL block that no script or .sql file runs: " + ", ".join(offenders)


@test
def test_every_page_generates_before_it_reads():
    """Each page stands alone, so a reader who starts one cold gets the data its
    first read needs from the page itself, not from a page they skipped."""
    readers = _scripts_that("require_data")
    assert readers, "no script calls require_data any more; the check reads nothing"
    offenders = []
    for rel in READER_DOCS:
        generated = False
        for start, step in _steps(rel):
            if step == GENERATE:
                generated = True
            elif step in readers and not generated:
                offenders.append(f"{rel}:{start} runs {step} before {GENERATE}")
    assert not offenders, "; ".join(offenders)


@test
def test_every_page_puts_back_what_it_changed():
    """Each page cleans up after itself, so the next one starts from what boot
    left. A page that strips the weights, edits the generator, adds the index or
    builds a view has to undo it before it ends."""
    undo = {
        "strip-the-weights.sh": ({GENERATE}, "regenerate"),
        "add-trace-id-index.sh": ({"drop-trace-id-index.sh"}, "drop the index"),
        "build-rollup.sh": ({"drop-rollup-views.sh", "clean-up-rollup.sh"}, "drop the view"),
        "build-view-without-populate.sh": ({"drop-rollup-views.sh", "clean-up-rollup.sh"}, "drop the view"),
        "build-view-without-root-filter.sh": ({"drop-rollup-views.sh", "clean-up-rollup.sh"}, "drop the view"),
        "send-live-batch.sh": ({"clean-up-rollup.sh"}, "delete the demo rows"),
    }
    offenders = []
    for rel in READER_DOCS:
        pending = {}
        edited = False
        for start, step in _steps(rel):
            for made, (cleaners, _) in list(undo.items()):
                if step in cleaners:
                    pending.pop(made, None)
            if step == "widen-the-tail.sh":
                edited = True
                pending["widen-the-tail.sh"] = start
            elif step == "restore-edited-files.sh":
                edited = False
            elif step == GENERATE and not edited:
                pending.pop("widen-the-tail.sh", None)
            if step in undo:
                pending[step] = start
        for step, start in pending.items():
            offenders.append(f"{rel}:{start} runs {step} and the page never puts it back")
    assert not offenders, "; ".join(offenders)


def main():
    failed = skipped = 0
    for fn in sorted(RESULTS, key=lambda f: f.__name__):
        try:
            fn()
            print(f"PASS: {fn.__name__}")
        except unittest.SkipTest as exc:
            print(f"SKIP: {fn.__name__}\n      {exc}")
            skipped += 1
        except AssertionError as exc:
            print(f"FAIL: {fn.__name__}\n      {exc}", file=sys.stderr)
            failed += 1
    ran = len(RESULTS) - skipped
    print(f"\n{ran - failed}/{ran} passed"
          + (f", {skipped} skipped" if skipped else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
