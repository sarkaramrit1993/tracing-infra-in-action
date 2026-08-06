#!/usr/bin/env python3
"""Chapter 8 offline tests. No Docker, no network.

Every printed listing gets an exactness test here. That is not ceremony: in
chapter 7 the one listing without such a test was the one that silently drifted
away from the printed page, and nobody noticed until a reviewer ran it.

Usage:  python3 tests/test_static.py
"""
import re
import sys
from pathlib import Path

import yaml

CHAPTER = Path(__file__).resolve().parent.parent
RESULTS = []


def test(fn):
    RESULTS.append(fn)
    return fn


def read(rel):
    return (CHAPTER / rel).read_text()


def listing_body(rel, number):
    """Pull the text between the ---- listing N ---- fences."""
    text = read(rel)
    m = re.search(rf"-- ---- Listing {re.escape(number)}:.*?----\n(.*?)\n-- ---- end listing {re.escape(number)} ----",
                  text, re.S)
    assert m, f"{rel} has no fenced block for listing {number}"
    return m.group(1)


def normalize(sql):
    """Collapse whitespace so line wrapping is not a difference that matters."""
    return re.sub(r"\s+", " ", sql).strip()


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
def test_listing_8_1_unbiased_exact():
    body = normalize(listing_body("clickhouse/unbiased.sql", "8.1"))
    for fragment in (
            "SELECT service_name, count() AS requests",
            "sum(adjusted_count) AS requests",
            "round(quantile(0.99)(duration_ns) / 1e6, 1) AS p99_ms",
            "round(quantileExactWeighted(0.99)( duration_ns, "
            "toUInt64(round(adjusted_count))) / 1e6, 1) AS p99_ms"):
        assert normalize(fragment) in body, f"listing 8.1 lost: {fragment}"
    assert body.count("parent_span_id = ''") == 4, \
        "all four of listing 8.1's queries must filter to root spans, or they count spans"
    assert body.count("toStartOfMinute(") == 4, \
        "listing 8.1's window must snap to the minute grid so listing 8.3 can match it"
    assert "p99_ns" not in body, "durations are milliseconds with units, never raw nanoseconds"


@test
def test_listing_8_2_skipindex_exact():
    body = normalize(listing_body("clickhouse/skipindex.sql", "8.2"))
    assert "ADD INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1" in body
    assert "MATERIALIZE INDEX idx_trace_id" in body, \
        "ADD INDEX alone does not touch existing parts and prunes nothing"
    assert "mutations_sync = 2" in body, \
        "the materialize is asynchronous; without this EXPLAIN runs before it finishes"
    assert body.count("EXPLAIN indexes = 1") == 2, \
        "listing 8.2 needs a before and an after reading; one of them proves nothing"
    assert len(re.search(r"trace_id = '([0-9a-f]+)'", body).group(1)) == 32, \
        "a trace ID is 16 bytes, 32 hex characters"


@test
def test_listing_8_3_rollup_exact():
    body = normalize(listing_body("clickhouse/rollup.sql", "8.3"))
    assert "ENGINE = SummingMergeTree" in body
    assert "POPULATE" in body, \
        "without POPULATE the view starts empty on a store that already has data"
    assert "sum(adjusted_count) AS requests" in body, \
        "the rollup has to weight, or it is a biased dashboard with extra steps"
    assert "parent_span_id = ''" in body, \
        "the rollup counts requests, so it filters to roots exactly as listing 8.1 does"
    assert "sum(requests) AS requests" in body, \
        "SummingMergeTree needs re-summing on read; the background merge may not have run"
    assert "toStartOfMinute(" in body


# ----------------------------------------------------------------- the stack

@test
def test_compose_parses_and_pins_one_tag():
    compose = yaml.safe_load(read("docker-compose.yml"))
    services = compose["services"]
    assert set(services) == {"clickhouse"}, \
        "chapter 8 is about the store's answers, not the path into it; one service"
    image = services["clickhouse"]["image"]
    assert image == "clickhouse/clickhouse-server:25.8", f"unpinned or moved: {image}"


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


@test
def test_every_clickhouse_helper_closes_stdin():
    """The trap that hung both of chapter 7's scripts for a reviewer."""
    offenders = []
    for path in (sorted(CHAPTER.glob("*.md")) + sorted(CHAPTER.glob("exercises/*.md"))
                 + sorted(CHAPTER.glob("tests/*.sh"))):
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if "clickhouse-client" not in line or line.strip().startswith("#"):
                continue
            if "< /dev/null" in line or "--multiquery <" in line or "--query" not in line:
                continue
            offenders.append(f"{path.relative_to(CHAPTER)}:{n}")
    assert not offenders, "clickhouse-client without stdin closed: " + ", ".join(offenders)


def main():
    failed = 0
    for fn in sorted(RESULTS, key=lambda f: f.__name__):
        try:
            fn()
            print(f"PASS: {fn.__name__}")
        except AssertionError as exc:
            print(f"FAIL: {fn.__name__}\n      {exc}", file=sys.stderr)
            failed += 1
    print(f"\n{len(RESULTS) - failed}/{len(RESULTS)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
