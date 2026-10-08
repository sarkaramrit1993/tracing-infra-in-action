#!/usr/bin/env python3
"""Chapter 5 offline tests. No Docker, no network.

Two jobs. The first pins what the README promises a reader: every bash block
runs as pasted from chapter-05/, every script it names exists and fails with
one readable line, a step that reads traffic refuses to run before the traffic
exists, and the printed outputs that can be checked offline match the committed
measurements. The second pins the few facts about the stack that the README
and the book lean on: the listing files, the service set, the version pins.

The pytest files under app/, flink/ and benchmarks/ test the code itself and
stay where they are.

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


def normalize(text):
    return re.sub(r"\s+", " ", text).strip()


def listing_body(rel, number):
    """The text between `-- ---- Listing N:` and `-- ---- end listing N`."""
    m = re.search(rf"^-- -+ Listing {re.escape(number)}:.*?$\n(.*?)^-- -+ end listing {re.escape(number)}",
                  read(rel), re.S | re.M)
    assert m, f"{rel} has no fenced block for listing {number}"
    return m.group(1)


# ---------------------------------------------------------------- the stack

@test
def test_compose_service_set_and_pins():
    services = yaml.safe_load(read("docker-compose.yml"))["services"]
    assert set(services) == {
        "checkout-service", "otel-agent", "otel-gateway", "kafka-1", "kafka-2",
        "kafka-3", "kafka-init", "clickhouse", "consumer-clickhouse",
        "otel-consumer", "flink-jobmanager", "flink-taskmanager",
        "flink-job-submit", "otel-stream-consumer", "jaeger", "prometheus"}, \
        f"the service set moved: {sorted(services)}"
    tags = {}
    for name, svc in services.items():
        image = svc.get("image")
        if image is None:
            assert "build" in svc, f"{name} has neither an image nor a build"
            continue
        repo, _, tag = image.rpartition(":")
        assert repo and tag and tag != "latest", f"{name} runs {image}, which is not a pin"
        tags.setdefault(repo, set()).add(tag)
    for repo, seen in tags.items():
        assert len(seen) == 1, f"{repo} runs at {sorted(seen)}; one tag per repository"


@test
def test_readme_version_table_matches_the_pins():
    compose = yaml.safe_load(read("docker-compose.yml"))
    readme = read("README.md")
    for image in sorted({s["image"] for s in compose["services"].values() if "image" in s}):
        assert f"`{image}`" in readme, f"{image} runs and is not in the README's version table"
    flink = re.search(r"^FROM (\S+)", read("flink/Dockerfile"), re.M).group(1)
    assert f"`{flink}`" in readme, f"the Flink base image {flink} is not in the version table"


@test
def test_readme_port_table_matches_the_compose_file():
    services = yaml.safe_load(read("docker-compose.yml"))["services"]
    published = {p.split(":")[0] for s in services.values() for p in s.get("ports", [])}
    table = read("README.md").split("### Ports", 1)[1].split("###", 1)[0]
    listed = set(re.findall(r"\b(\d{4,5})\b", table))
    assert published == listed, \
        f"ports published but not listed: {sorted(published - listed)}; listed but not published: {sorted(listed - published)}"


@test
def test_schema_and_views_load_on_first_boot():
    mounts = yaml.safe_load(read("docker-compose.yml"))["services"]["clickhouse"]["volumes"]
    for f in ("init.sql", "materialized_views.sql"):
        assert any(f in m and "docker-entrypoint-initdb.d" in m for m in mounts), \
            f"clickhouse/{f} is not applied on first boot"


@test
def test_every_scrape_target_is_a_compose_service():
    services = yaml.safe_load(read("docker-compose.yml"))["services"]
    for job in yaml.safe_load(read("prometheus.yml"))["scrape_configs"]:
        for target in job["static_configs"][0]["targets"]:
            host = target.split(":")[0]
            assert host == "localhost" or host in services, \
                f"job {job['job_name']} scrapes {host}, which no service provides"


@test
def test_every_span_count_agrees_with_the_checkout_trace():
    """app/test_checkout.py runs a checkout and counts its spans. Every wait in
    the scripts and every check in the stack test counts the same number."""
    assert "self.assertEqual(len(spans), 11)" in read("app/test_checkout.py")
    assert "SPANS_PER_CHECKOUT=11" in read("scripts/lib.sh")
    assert "uniqExact(span_id) = $SPANS_PER_CHECKOUT" in read("scripts/lib.sh")
    assert re.search(r"^SPANS=11$", read("tests/test_stack.sh"), re.M)


@test
def test_listing_5_6_runs_as_printed():
    """Each downstream call lands in a service of its own, so the listing finds
    edges without help. No second, rewritten query hides behind it."""
    script = read("scripts/show-service-graph.sh")
    assert "ch_file clickhouse/service_graph.sql" in script
    assert "peer.service" not in script, "show-service-graph.sh runs a rewritten query again"


@test
def test_red_counts_receiving_spans_only():
    """Section 5.4.2: Rate is the count of receiving spans, kind SERVER or
    CONSUMER. The consumer stores span_kind as the OTLP enum name."""
    assert "WHERE span_kind IN ('SPAN_KIND_SERVER', 'SPAN_KIND_CONSUMER')" in \
        read("clickhouse/materialized_views.sql")
    assert '2: "SPAN_KIND_SERVER"' in read("app/consumer_clickhouse.py")
    assert '5: "SPAN_KIND_CONSUMER"' in read("app/consumer_clickhouse.py")


# ------------------------------------------------------------- the listings

@test
def test_every_readme_listing_row_names_a_file_with_its_anchor():
    rows = re.findall(r"^\|\s*(5\.\d)\s*\|\s*`([^`]+)`", read("README.md"), re.M)
    assert [n for n, _ in rows] == ["5.1", "5.2", "5.3", "5.5", "5.6"], f"listing rows: {rows}"
    for number, rel in rows:
        assert (CHAPTER / rel).exists(), f"listing {number} names {rel}, which is not here"
        assert re.search(rf"[Ll]isting\s+{re.escape(number)}:", read(rel)), \
            f"{rel} carries no anchor for listing {number}"


@test
def test_listing_5_6_is_the_printed_query():
    body = normalize(listing_body("clickhouse/service_graph.sql", "5.6"))
    for fragment in (
            "count() AS call_count",
            "quantileTDigest(0.99)(duration) AS p99_duration_ns",
            "countIf(status_code = 'STATUS_CODE_ERROR') AS error_count",
            "INNER JOIN tracing.otel_traces AS p ON s.trace_id = p.trace_id "
            "AND s.parent_span_id = p.span_id",
            "WHERE s.timestamp >= now() - INTERVAL 1 HOUR "
            "AND p.timestamp >= now() - INTERVAL 2 HOUR",
            "WHERE parent_service != child_service",
            "GROUP BY parent_service, child_service ORDER BY call_count DESC"):
        assert normalize(fragment) in body, f"listing 5.6 lost: {fragment}"


@test
def test_listing_5_1_keeps_the_layout_the_chapter_describes():
    body = normalize(read("clickhouse/init.sql"))
    for fragment in ("ENGINE = MergeTree", "PARTITION BY toStartOfHour(timestamp)",
                     "ORDER BY (trace_id, timestamp)",
                     "TTL toDateTime(timestamp) + INTERVAL 7 DAY"):
        assert fragment in body, f"listing 5.1 lost: {fragment}"


@test
def test_the_kafka_sinks_get_raw_bytes_not_pickles():
    """Both sinks serialize with ByteArraySchema, which writes the Java byte[]
    it is handed. A stream typed PICKLED_BYTE_ARRAY hands it the pickle of the
    Python bytes instead, every assembled trace lands on the topic with a pickle
    header in front of the protobuf, and the stream-time collector rejects all
    of them as malformed OTLP. Topic offsets still grow, so nothing upstream
    notices."""
    job = read("flink/assembly_job.py")
    raw = "PRIMITIVE_ARRAY(Types.BYTE())"
    assert f'LATE_TAG = OutputTag("late-spans", Types.{raw})' in job, \
        "the late-span side output is not typed as raw bytes"
    assert f".process(TraceAssembler(), output_type=PyTypes.{raw.replace('Types.', 'PyTypes.')})" in job, \
        "the assembled-trace stream is not typed as raw bytes"


# ------------------------------------------------------------ reader pages

READER_DOCS = ("README.md",)

# Scripts that read what send-traffic.sh produced. Run before it has arrived,
# each prints a number short by a batch that looks perfectly plausible.
NEEDS_READY = ("show-both-paths.sh", "assemble-trace.sh", "show-table-layout.sh",
               "show-red-metrics.sh", "show-service-graph.sh", "show-flink-job.sh")


def _bash_fences(rel):
    lines = read(rel).splitlines()
    fence, start = None, 0
    for n, line in enumerate(lines, 1):
        s = line.strip()
        if s.startswith("```"):
            if fence is None and (s.startswith("```bash") or s.startswith("```sh")):
                fence, start = [], n + 1
            elif fence is not None:
                yield start, fence
                fence = None
            continue
        if fence is not None:
            fence.append(line)


def _script_calls(rel):
    for start, body in _bash_fences(rel):
        yield start, [m for ln in body for m in re.findall(r"\./scripts/([\w-]+\.sh)", ln)]


@test
def test_no_bash_block_defines_a_function():
    offenders = [f"{rel}:{start}: {ln.strip()}"
                 for rel in READER_DOCS for start, body in _bash_fences(rel) for ln in body
                 if re.match(r"\s*(function\s+)?[A-Za-z_][\w-]*\s*\(\)\s*\{?", ln)]
    assert not offenders, "shell function defined in a bash block: " + "; ".join(offenders)


@test
def test_no_bash_block_leans_on_a_shell_variable():
    """`$TID` set in one block and read in the next was the old pattern."""
    offenders = [f"{rel}:{start}: {ln.strip()}"
                 for rel in READER_DOCS for start, body in _bash_fences(rel) for ln in body
                 if re.search(r"\$(?!\?)[{(A-Za-z_]", ln)]
    assert not offenders, "shell variable in a bash block: " + "; ".join(offenders)


@test
def test_no_bash_block_carries_a_placeholder_or_a_browser_opener():
    """`open` is macOS only, and an angle-bracket placeholder pastes as a redirect."""
    offenders = [f"{rel}:{start}: {ln.strip()}"
                 for rel in READER_DOCS for start, body in _bash_fences(rel) for ln in body
                 if re.search(r"<[\w-]+>|^\s*(open|xdg-open)\s", ln)]
    assert not offenders, "placeholder or browser opener in a bash block: " + "; ".join(offenders)


@test
def test_no_hash_comments_inside_bash_blocks():
    """A reader pastes the whole block. zsh turns a bare # into an argument."""
    offenders = [f"{md.name}:{start}"
                 for md in sorted(CHAPTER.glob("*.md")) + sorted(CHAPTER.glob("benchmarks/*.md"))
                 for start, body in _bash_fences(md.relative_to(CHAPTER))
                 for ln in body if re.search(r"(^|\s)#", ln)]
    assert not offenders, "bare # inside a bash block: " + ", ".join(offenders)


@test
def test_every_script_a_page_runs_exists_and_is_executable():
    referenced = {name for rel in READER_DOCS for _, calls in _script_calls(rel) for name in calls}
    assert referenced, "no page runs a script; the regex has stopped matching"
    for name in sorted(referenced):
        path = CHAPTER / "scripts" / name
        assert path.exists(), f"a page runs scripts/{name}, which is not here"
        assert path.stat().st_mode & 0o111, f"scripts/{name} is not executable"


@test
def test_every_script_is_strict_bash_and_used():
    used = {name for rel in READER_DOCS for _, calls in _script_calls(rel) for name in calls}
    assert "set -euo pipefail" in read("scripts/lib.sh")
    for path in sorted((CHAPTER / "scripts").glob("*.sh")):
        if path.name == "lib.sh":
            continue
        text = path.read_text()
        assert text.startswith("#!/usr/bin/env bash\n"), f"{path.name} has no bash shebang"
        assert 'source "$(dirname "$0")/lib.sh"' in text, f"{path.name} does not source lib.sh"
        assert not re.search(r"declare -A|mapfile|readarray|\$\{\w+,,\}|\$\{\w+\^\^\}", text), \
            f"{path.name} uses a bash 4 feature that macOS bash 3.2 does not have"
        assert path.name in used, f"scripts/{path.name} is never run by any page"


@test
def test_every_read_after_new_traffic_waits_for_it():
    offenders = []
    for rel in READER_DOCS:
        pending = None
        for start, calls in _script_calls(rel):
            for name in calls:
                if name == "send-traffic.sh":
                    pending = start
                elif name == "wait-until-ready.sh":
                    pending = None
                elif name in NEEDS_READY and pending is not None:
                    offenders.append(f"{rel}:{start} runs {name} before waiting for the traffic sent at line {pending}")
    assert not offenders, "; ".join(offenders)


@test
def test_readers_refuse_to_run_before_their_data_exists():
    """The page orders the steps; these guards cover the reader who skips one."""
    for name in NEEDS_READY:
        assert "require_ready" in read(f"scripts/{name}"), \
            f"{name} reads settled traffic and never checks wait-until-ready.sh ran"
    lib = read("scripts/lib.sh")
    assert 'SEND="nothing sent yet: run ./scripts/send-traffic.sh first"' in lib
    assert 'ARRIVING="still arriving: run ./scripts/wait-until-ready.sh first"' in lib
    send = read("scripts/send-traffic.sh")
    assert "save_state traffic" in send and "READY" not in send, \
        "sending traffic again must start a fresh state file, clearing the ready mark"
    assert "STACK_ID=$(stack_id)" in send, \
        "the state file must be tied to the running stack so it does not outlive down -v"
    assert "ps -q jaeger" in lib and "State.StartedAt" in lib, \
        "Jaeger keeps traces in memory, so the state file must not outlive a restart either"
    assert ">> \"$STATE_DIR" not in read("scripts/wait-until-ready.sh"), \
        "appending the ready mark gives a second run two LAST_TRACE lines, and every reader breaks"


@test
def test_no_page_ships_an_uncaptured_output():
    """Every expected-output block is a capture from a real run."""
    for md in sorted(CHAPTER.glob("*.md")) + sorted(CHAPTER.glob("benchmarks/*.md")):
        assert "PLACEHOLDER" not in md.read_text(), f"{md.name} still has a PLACEHOLDER block"


@test
def test_every_change_to_the_stack_is_undone():
    """A step that stops a service starts it again, on failure too."""
    for path in sorted((CHAPTER / "scripts").glob("*.sh")):
        text = path.read_text()
        for service in re.findall(r"docker compose stop (\S+)", text):
            assert f"docker compose start {service}" in text, \
                f"{path.name} stops {service} and never starts it again"
            assert re.search(r"^trap \w+ EXIT", text, re.M), \
                f"{path.name} stops {service} without a trap to start it on failure"


@test
def test_the_audit_runner_leaves_the_committed_results_alone():
    text = read("scripts/run-atomicity-audit.sh")
    assert "mktemp -d" in text and 'cp benchmarks/atomicity_audit.py "$scratch/"' in text, \
        "the audit writes a dated result file next to itself; run a copy"


def _shell_commands(text):
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
def test_every_clickhouse_client_closes_stdin():
    """clickhouse-client reads stdin even for a --query. From a terminal that
    never sends EOF, so a call without `< /dev/null` hangs with no output."""
    offenders = []
    for path in (sorted(CHAPTER.glob("*.md")) + sorted(CHAPTER.glob("tests/*.sh"))
                 + sorted(CHAPTER.glob("scripts/*.sh"))):
        for n, command in _shell_commands(path.read_text()):
            if "clickhouse-client" not in command or command.strip().startswith("#"):
                continue
            if "< /dev/null" in command or "--multiquery <" in command or "--query" not in command:
                continue
            offenders.append(f"{path.relative_to(CHAPTER)}:{n}")
    assert not offenders, "clickhouse-client without stdin closed: " + ", ".join(offenders)


# --------------------------------------------------- printed against recorded

@test
def test_the_readme_audit_table_is_the_committed_measurement():
    """The audit is seeded, so the README's table is exact, and RESULTS.md
    records the same four runs."""
    recorded = {m: tuple(int(x) for x in rest)
                for m, *rest in re.findall(r"^\| ([\w-]+) \| (\d+) \| (\d+) \| (\d+) \|$",
                                           read("RESULTS.md"), re.M)}
    printed = {m: (int(w), int(a), int(p))
               for m, w, a, p in re.findall(r"^([\w-]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:PASS|FAIL)$",
                                            read("README.md"), re.M)}
    assert len(printed) == 4, f"the README's audit table has {len(printed)} rows"
    for mode, row in printed.items():
        assert recorded.get(mode) == row, \
            f"the README prints {mode} as {row} and RESULTS.md records {recorded.get(mode)}"


@test
def test_the_book_finds_the_verify_it_works_step():
    """Section 5.5 sends the reader to the README's `Verify it works` walkthrough
    to see one trace under both assembly.source labels."""
    section = re.search(r"^## \d+\. Verify it works.*?(?=^## )", read("README.md"), re.S | re.M)
    assert section, "the README has no `Verify it works` step, and the chapter names it"
    assert "./scripts/show-both-paths.sh" in section.group(0)
    assert "assembly.source" in section.group(0)


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
