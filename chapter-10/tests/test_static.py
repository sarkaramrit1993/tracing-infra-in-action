#!/usr/bin/env python3
"""Chapter 10 offline tests. No Docker, no network.

Every printed listing gets a test here. tests/listings/10.N.txt holds the text
the book prints for listing 10.N, callout markers removed, so a listing that
drifts from the page fails here and not only in a reader's terminal. Whitespace
is collapsed before comparing, because the page wraps one long line that the
shipped file keeps on one.

The rest pins the things this stack gets right and that fail without an error:
a sampler that stamps no threshold, a Flink plan that drops a late span without
counting it, an exemplar the sampler tier never attaches, a Prometheus that
accepts exemplars and stores none, a port bound to every interface.

Usage: python3 tests/test_static.py
"""
import ast
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
    """Text between the `---- Listing N` and `---- end listing N` anchor lines."""
    n = re.escape(number)
    m = re.search(rf"^(?:--|#) -+ Listing {n}:.*?$\n(.*?)^(?:--|#) -+ end listing {n}",
                  read(rel), re.S | re.M)
    assert m, f"{rel} has no fenced block for listing {number}"
    return m.group(1)


LISTINGS = (
    ("10.1", "collector/edge-config.yaml"),
    ("10.2", "flink/red.sql"),
    ("10.3", "clickhouse/red_views.sql"),
    ("10.4", "clickhouse/red_query.sql"),
)


# ------------------------------------------------------------------ listings

@test
def test_every_listing_matches_the_printed_page():
    for number, rel in LISTINGS:
        printed = normalize(read(f"tests/listings/{number}.txt"))
        shipped = normalize(listing_body(rel, number))
        assert printed, f"tests/listings/{number}.txt is empty"
        assert shipped == printed, f"listing {number} in {rel} no longer matches the book's text"


@test
def test_every_readme_listing_row_names_a_file_that_exists():
    rows = re.findall(r"^\|\s*(10\.\d)\s*\|\s*`([^`]+)`", read("README.md"), re.M)
    assert [n for n, _ in rows] == [n for n, _ in LISTINGS], \
        f"the README table lists {[n for n, _ in rows]}, expected 10.1 to 10.4"
    for number, rel in rows:
        assert (CHAPTER / rel).exists(), f"listing {number} names {rel}, which is not here"
        listing_body(rel, number)


@test
def test_listing_10_1_samples_nowhere_before_the_connectors():
    """The chapter's claim: connectors see 100% of spans because the sampler is
    a later tier. A tail_sampling processor on traces/in would leave every
    count in the chapter measuring survivors, and the file would still be valid
    YAML."""
    import copy
    base = yaml.safe_load(read("collector/edge-base.yaml"))
    top = yaml.safe_load(read("collector/edge-config.yaml"))
    merged = copy.deepcopy(base)["service"]["pipelines"]["traces/in"]
    merged.update(top["service"]["pipelines"]["traces/in"])
    assert "tail_sampling" not in str(merged), "the edge pipeline samples"
    assert merged["processors"] == ["memory_limiter", "batch"], \
        "memory_limiter must run first, then batch: the chapter says the listing omits both"
    assert merged["exporters"] == ["spanmetrics", "servicegraph", "kafka"]
    assert top["connectors"]["spanmetrics"]["exemplars"]["enabled"] is True
    exporters = base["exporters"]
    assert exporters["prometheus"]["enable_open_metrics"] is True, \
        "classic exposition carries no exemplars"
    assert exporters["kafka"]["partition_traces_by_id"] is True, \
        "a trace's spans must reach one partition or the tail sampler decides on halves"


@test
def test_listing_10_2_runs_on_one_window_operator():
    """With the default two-phase plan a late span is dropped by the local half
    of the window operator, which has no late-record counter. The README's late
    span step reads that counter, so the SET has to come before the INSERT."""
    body = read("flink/red.sql")
    set_at = body.index("table.optimizer.agg-phase-strategy")
    assert "ONE_PHASE" in body[set_at:set_at + 80]
    assert set_at < body.index("INSERT INTO red_metrics")
    assert "WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND" in body
    assert "'topic' = 'spans.flat'" in body and "'topic' = 'red_metrics'" in body


@test
def test_listing_10_3_and_10_4_agree_on_the_columns_they_weigh_by():
    mv = normalize(listing_body("clickhouse/red_views.sql", "10.3"))
    query = normalize(listing_body("clickhouse/red_query.sql", "10.4"))
    for text in (mv, query):
        assert "toUInt64(round(adjusted_count))" in text
        assert "duration_ns / 1e6" in text
        assert "FROM tracing.otel_traces" in text
    target = read("clickhouse/red_views.sql")
    assert "ENGINE = AggregatingMergeTree" in target
    assert "AggregateFunction(quantileTDigestWeighted(0.99), Float64, UInt64)" in target
    assert target.index("CREATE TABLE") < target.index("CREATE MATERIALIZED VIEW"), \
        "the view's target table must exist first"


@test
def test_clickhouse_table_is_chapter_9s_with_adjusted_count():
    init = read("clickhouse/init.sql")
    flat = normalize(init)
    for column in ("timestamp DateTime64(9) CODEC(Delta, ZSTD(1))",
                   "trace_id String CODEC(ZSTD(1))",
                   "adjusted_count Float64 DEFAULT 1.0"):
        assert column in flat, f"init.sql lost: {column}"


# ------------------------------------------------------------------- sampler

@test
def test_sampler_tier_has_the_three_policies_the_chapter_compares():
    cfg = yaml.safe_load(read("collector/sampler-config.yaml"))
    uniform = cfg["processors"]["tail_sampling/uniform"]["policies"]
    assert [p["type"] for p in uniform] == ["probabilistic"]
    policy = cfg["processors"]["tail_sampling/policy"]["policies"]
    assert [p["type"] for p in policy] == ["status_code", "probabilistic"]
    assert cfg["processors"]["probabilistic_sampler"]["mode"] == "proportional"
    pipelines = cfg["service"]["pipelines"]
    assert "kafka/sampled" in pipelines["traces/uniform"]["exporters"], \
        "only the uniform survivors reach ClickHouse"
    assert "kafka/sampled" not in pipelines["traces/policy"]["exporters"]
    assert "kafka/sampled" not in pipelines["traces/weighted"]["exporters"]


@test
def test_sampler_exemplars_come_off_the_connector_after_the_decision():
    """The after-the-sampler half of the exemplar step. The connector behind
    the uniform sampler only sees kept traces, so its exemplars resolve. Without
    this setting nothing is attached there, and without OpenMetrics on the
    exporter nothing reaches the scrape even when it is."""
    cfg = yaml.safe_load(read("collector/sampler-config.yaml"))
    assert cfg["connectors"]["spanmetrics/uniform"]["exemplars"]["enabled"] is True
    assert cfg["exporters"]["prometheus"]["enable_open_metrics"] is True
    compose = yaml.safe_load(read("docker-compose.yml"))
    assert "--enable-feature=exemplar-storage" in compose["services"]["prometheus"]["command"], \
        "Prometheus accepts exemplars and stores none without the flag"


@test
def test_every_spanmetrics_connector_uses_the_same_twelve_buckets():
    buckets = set()
    edge = yaml.safe_load(read("collector/edge-config.yaml"))
    buckets.add(str(edge["connectors"]["spanmetrics"]["histogram"]["explicit"]["buckets"]))
    sampler = yaml.safe_load(read("collector/sampler-config.yaml"))
    for name, conn in sampler["connectors"].items():
        buckets.add(str(conn["histogram"]["explicit"]["buckets"]))
    assert len(buckets) == 1, f"the connectors disagree on buckets: {sorted(buckets)}"


# ----------------------------------------------------------------- the stack

COMPOSE_FILES = ("docker-compose.yml", "docker-compose.cardinality.yml")


@test
def test_compose_pins_one_tag_per_image():
    services = yaml.safe_load(read("docker-compose.yml"))["services"]
    tags = {}
    for name, svc in services.items():
        image = svc.get("image")
        if image is None:
            assert "build" in svc, f"{name} has neither an image nor a build"
            continue
        assert ":" in image, f"{name} runs {image}, which resolves to :latest"
        repo, tag = image.rsplit(":", 1)
        assert tag != "latest", f"{name} is pinned to :latest"
        tags.setdefault(repo, set()).add(tag)
    for repo, found in sorted(tags.items()):
        assert len(found) == 1, f"{repo} runs at {sorted(found)}; one tag per repository"
    override = yaml.safe_load(read("docker-compose.cardinality.yml"))["services"]
    edge = services["edge-collector"]["image"]
    assert override["edge-collector-userid"]["image"] == edge, \
        "the cardinality collector must run the same image as the edge it is compared with"


@test
def test_pinned_versions_are_the_ones_the_notes_and_results_claim():
    services = yaml.safe_load(read("docker-compose.yml"))["services"]
    images = {s["image"] for s in services.values() if "image" in s}
    flink = read("flink/Dockerfile")
    assert "flink:2.2.1-scala_2.12-java17" in flink
    assert "flink-sql-connector-kafka-5.0.0-2.2.jar" in flink
    for text in ("README.md", "RESULTS.md"):
        body = read(text)
        for image in sorted(images):
            assert image.rsplit(":", 1)[1] in body, f"{text} does not state {image}"


@test
def test_every_published_port_is_bound_to_loopback():
    offenders = []
    for rel in COMPOSE_FILES:
        for name, svc in yaml.safe_load(read(rel))["services"].items():
            for port in svc.get("ports", []):
                if not str(port).startswith("127.0.0.1:"):
                    offenders.append(f"{rel}: {name} publishes {port}")
    assert not offenders, "; ".join(offenders)


@test
def test_every_span_metrics_job_has_a_sample_limit():
    """The cardinality step's last line: Prometheus refuses a scrape instead of
    ingesting a hundred thousand series. A job without sample_limit never does."""
    prom = yaml.safe_load(read("prometheus.yml"))
    jobs = {j["job_name"]: j for j in prom["scrape_configs"]}
    for name in ("edge-spanmetrics", "sampler-spanmetrics", "edge-userid-spanmetrics"):
        assert jobs[name].get("sample_limit"), f"{name} has no sample_limit"


@test
def test_kafka_topics_match_what_the_stack_reads_and_writes():
    compose = read("docker-compose.yml")
    topics = dict(re.findall(r"^\s+create (otlp_spans|otlp_sampled|spans\.flat|red_metrics) (\d+)\s*$",
                             compose, re.M))
    assert topics == {"otlp_spans": "8", "otlp_sampled": "4", "spans.flat": "1", "red_metrics": "1"}, \
        f"topic partitions moved: {topics}"
    assert topics["spans.flat"] == "1", \
        "Flink's watermark is the minimum over partitions; an idle partition stalls every window"


@test
def test_compose_mounts_init_sql_and_the_view_into_the_entrypoint():
    mounts = yaml.safe_load(read("docker-compose.yml"))["services"]["clickhouse"]["volumes"]
    joined = " ".join(mounts)
    assert "01-init.sql" in joined and "02-red_views.sql" in joined


@test
def test_every_scrape_target_is_a_service_in_the_compose_file():
    services = set(yaml.safe_load(read("docker-compose.yml"))["services"])
    services |= set(yaml.safe_load(read("docker-compose.cardinality.yml"))["services"])
    for job in yaml.safe_load(read("prometheus.yml"))["scrape_configs"]:
        targets = list(job.get("static_configs", [{}])[0].get("targets", []))
        for dns in job.get("dns_sd_configs", []):
            targets += [n + ":0" for n in dns["names"]]
        for target in targets:
            assert target.split(":")[0] in services, \
                f"job {job['job_name']} scrapes {target}, which no service provides"


@test
def test_loadgen_failure_pattern_gives_exactly_two_point_nine_percent():
    """The ground truth every comparison is graded against: request i fails when
    i*29 % 1000 < 29. Read the rule out of the app and prove the arithmetic."""
    app = read("app/tracing_setup.py")
    assert re.search(r"\(i \* 29\) % 1000 < 29", app), \
        "tracing_setup.py no longer fails request i when i*29 % 1000 < 29"
    assert "(i * 7919) % USER_POOL" in read("app/checkout.py")
    assert re.search(r"USER_POOL\s*=\s*6000", read("app/checkout.py")), "the user pool is not 6000"
    failing = sum(1 for i in range(30000) if i * 29 % 1000 < 29)
    assert failing == 870 and 20000 * 29 // 1000 == sum(
        1 for i in range(20000) if i * 29 % 1000 < 29)
    users = {i * 7919 % 6000 for i in range(30000)}
    assert len(users) == 6000, "the user pool must exceed the connector's 5000 limit"


# ------------------------------------------------------------ the reader path

def _bash_fences(rel):
    """Yield (first line number, list of lines) for every ```bash fence."""
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


READER_DOCS = ("README.md",)


def _script_calls(rel):
    for start, body in _bash_fences(rel):
        yield start, [m for ln in body for m in re.findall(r"\./scripts/([\w-]+\.sh)", ln)]


@test
def test_no_bash_block_defines_a_function():
    offenders = []
    for rel in READER_DOCS:
        for start, body in _bash_fences(rel):
            for ln in body:
                if re.match(r"\s*(function\s+)?[A-Za-z_][\w-]*\s*\(\)\s*\{?", ln):
                    offenders.append(f"{rel}:{start}: {ln.strip()}")
    assert not offenders, "shell function defined in a bash block: " + "; ".join(offenders)


@test
def test_no_bash_block_leans_on_a_shell_variable():
    offenders = []
    for rel in READER_DOCS:
        for start, body in _bash_fences(rel):
            for ln in body:
                if re.search(r"\$(?!\?)[{(A-Za-z_]", ln):
                    offenders.append(f"{rel}:{start}: {ln.strip()}")
    assert not offenders, "shell variable in a bash block: " + "; ".join(offenders)


@test
def test_no_hash_comments_inside_bash_blocks():
    """A reader pastes the whole block. zsh turns a bare # into an argument."""
    offenders = []
    for md in sorted(CHAPTER.glob("*.md")):
        inside = False
        for n, line in enumerate(md.read_text().splitlines(), 1):
            s = line.strip()
            if s.startswith("```"):
                inside = s.startswith("```bash") or s.startswith("```sh")
                continue
            if inside and re.search(r"(^|\s)#", line):
                offenders.append(f"{md.name}:{n}")
    assert not offenders, "bare # inside a bash block: " + ", ".join(offenders)


@test
def test_every_script_a_page_runs_exists_and_is_executable():
    referenced = set()
    for rel in READER_DOCS:
        for _, calls in _script_calls(rel):
            referenced.update(calls)
    assert referenced, "no page runs a script; the regex has stopped matching"
    for name in sorted(referenced):
        path = CHAPTER / "scripts" / name
        assert path.exists(), f"a page runs scripts/{name}, which is not here"
        assert path.stat().st_mode & 0o111, f"scripts/{name} is not executable"


@test
def test_every_script_is_strict_bash_and_used():
    used = set(re.findall(r"\./scripts/([\w-]+\.sh)", read("README.md")))
    for other in (CHAPTER / "scripts").glob("*.sh"):
        used |= set(re.findall(r"\./scripts/([\w-]+\.sh)", other.read_text()))
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
def test_every_file_a_script_names_exists():
    for path in sorted((CHAPTER / "scripts").glob("*.sh")):
        for rel in re.findall(r"(?:clickhouse|collector|flink|app)/[\w.-]+\.(?:sql|yaml|yml|py)",
                              path.read_text()):
            assert (CHAPTER / rel).exists(), f"{path.name} names {rel}, which is not here"
        for name in re.findall(r"\./scripts/([\w-]+\.(?:sh|py))", path.read_text()):
            assert (CHAPTER / "scripts" / name).exists(), f"{path.name} runs scripts/{name}"
    for rel in ("docker-compose.cardinality.yml",):
        assert (CHAPTER / rel).exists()


@test
def test_the_readme_edits_no_file_with_perl_sed_or_awk():
    """A Try this is a shipped script that edits, restarts and verifies, and a
    reset that restores the file byte for byte. A one-line substitution in a
    bash block is the trap that left readers on a half-edited config."""
    offenders = []
    for start, body in _bash_fences("README.md"):
        for n, ln in enumerate(body):
            if re.search(r"(^|[\s|;&])(perl|sed|awk|ed)\s", ln):
                offenders.append(f"README.md:{start + n}: {ln.strip()}")
    assert not offenders, "an edit written out in a bash block: " + "; ".join(offenders)


@test
def test_every_readme_step_heading_carries_its_time_and_the_numbers_run_in_order():
    heads = re.findall(r"^## (\d+)\. (.+)$", read("README.md"), re.M)
    assert [int(n) for n, _ in heads] == list(range(1, len(heads) + 1)), \
        f"step numbers are not 1..{len(heads)} in order: {[n for n, _ in heads]}"
    assert len(heads) == 7, f"expected seven steps, found {len(heads)}"
    for n, title in heads:
        assert re.search(r"\(.*\b(?:minutes?|seconds?)\b.*\)$", title), \
            f"step {n} heading carries no time: {title}"


@test
def test_try_this_scripts_keep_the_shipped_file_and_restore_it_whole():
    lib = read("scripts/lib.sh")
    assert "keep_pristine()" in lib and "restore_pristine()" in lib
    for name in ("try-edge.sh",):
        text = read(f"scripts/{name}")
        assert "keep_pristine" in text and "restore_pristine" in text, \
            f"{name} must copy the shipped file aside before editing and restore it on reset"
        assert "reset" in text
        assert re.search(r"sed -i|perl ", text) is None, f"{name} edits with sed -i or perl"


@test
def test_weight_check_loads_real_weights_and_reads_them_both_ways():
    text = read("scripts/compare-mv-and-query.sh")
    assert "100" in text and "adjusted_count" in text and "'weight-check'" in text
    for needle in ("countMerge(calls)", "sumMerge(weighted_calls)",
                   "quantileTDigestWeightedMerge(0.99)(p99_state)", "red_query.sql",
                   "DELETE WHERE service_name = 'weight-check'"):
        assert needle in text, f"the weight check no longer reads {needle}"
    assert "trap forget_weight_check EXIT" in text, "the weighted rows must be removed even on Ctrl-C"


@test
def test_the_ci_stack_job_has_a_timeout():
    workflow = (CHAPTER.parent / ".github" / "workflows" / "verify.yml").read_text()
    stack = workflow[workflow.index("\n  stack:"):]
    assert re.search(r"^    timeout-minutes: 40$", stack, re.M), \
        "the live stack job has no timeout-minutes: 40"


@test
def test_every_script_guards_against_a_stack_that_is_down():
    """The guard is what turns a stranger's connection refused into one line
    that names the command to run first."""
    for path in sorted((CHAPTER / "scripts").glob("*.sh")):
        if path.name in ("lib.sh", "wait-until-ready.sh", "cardinality-down.sh"):
            continue
        text = path.read_text()
        assert re.search(r"\b(require|measure)\b", text), f"{path.name} never checks the stack is up"


def _shell_commands(text):
    """Yield (first line number, whole command) with backslash continuations joined."""
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
    """The trap that hung both of chapter 7's scripts for readers running them."""
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


@test
def test_every_kafka_client_in_a_script_closes_stdin_or_feeds_it():
    offenders = []
    for path in sorted(CHAPTER.glob("scripts/*.sh")) + sorted(CHAPTER.glob("tests/*.sh")):
        for n, command in _shell_commands(path.read_text()):
            if "kafka-" not in command or "docker compose" not in command:
                continue
            if "< /dev/null" in command or "kafka-console-producer" in command:
                continue
            offenders.append(f"{path.relative_to(CHAPTER)}:{n}")
    assert not offenders, "kafka client without stdin closed: " + ", ".join(offenders)


@test
def test_every_durations_in_a_script_carry_a_unit():
    """C1: no raw nanoseconds in anything a reader sees."""
    for path in sorted(CHAPTER.glob("scripts/*.sh")):
        for n, line in enumerate(path.read_text().splitlines(), 1):
            if "SELECT" in line and re.search(r"duration_ns(?!\s*/)", line):
                raise AssertionError(f"{path.name}:{n} selects raw duration_ns")


@test
def test_the_readme_states_one_measured_run_and_not_the_chapters_numbers():
    readme = read("README.md")
    assert "one measured run" in readme.lower()
    assert "ratios hold" in readme.lower()
    assert "RESULTS.md" in readme


@test
def test_the_readme_comparison_block_is_the_recorded_run():
    """The README's table and RESULTS.md come from the same run. The numbers a
    reader compares against must be the ones written down, not a later rerun."""
    results = read("RESULTS.md")
    recorded = dict(re.findall(r"^\| ([a-z_.]+) \| ([\d.]+) \|$", results, re.M))
    assert recorded, "RESULTS.md carries no measurement table"
    readme = read("README.md")
    truth = re.search(r"^ground truth \(load generator\)\s+(\d+)\s+(\d+)\s+", readme, re.M)
    edge = re.search(r"^1 Collector edge, before the sampler\s+(\d+)\s+(\d+)\s+", readme, re.M)
    assert truth and edge, "the comparison block is missing from the README"
    assert [int(truth.group(1)), int(truth.group(2))] == \
        [int(recorded["requests"]), int(recorded["errors"])]
    assert [int(edge.group(1)), int(edge.group(2))] == \
        [int(recorded["edge.calls"]), int(recorded["edge.errors"])]


def main():
    failed = 0
    for fn in sorted(RESULTS, key=lambda f: f.__name__):
        try:
            fn()
            print(f"PASS: {fn.__name__}")
        except AssertionError as exc:
            print(f"FAIL: {fn.__name__}\n  {exc}", file=sys.stderr)
            failed += 1
    print(f"\n{len(RESULTS) - failed}/{len(RESULTS)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
