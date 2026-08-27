#!/usr/bin/env python3
"""Chapter 9 offline tests. No Docker, no network.

Every printed listing gets a test here that pins the fragments of it this
directory depends on. Be exact about what that does and does not catch: the
fragments live in this file, not in the manuscript, so these tests catch a file
drifting away from what the REPOSITORY assumed about it, and they cannot catch
the file drifting away from the printed page. Checking a listing against the
page needs the page, and nothing here has it. What they do catch is the failure
that actually happened in chapter 7: an edit that quietly removes the line some
other artifact in this directory was relying on.

Chapter 9 adds a second job for the same suite. Four of the things this stack
gets right are things it is easy to get wrong in a way that produces no error
at all: exponential histogram buckets that swallow every exemplar, an exemplar
minted on the wrong side of the sampler, a Loki selector on a field that is not
a label, and a gap alert whose two counters came down the same pipe. Each of
those has a test below, because the stack cannot report them itself.

The two files under rules/ back no printed listing. They are held here anyway,
and to the same standard: what these tests pin is what the stack needs, and
every one of those needs was paid for once already.

Usage:  python3 tests/test_static.py
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


def listing_body(rel, number):
    """Pull the text between the ---- listing N ---- fences.

    Both comment leaders the repo uses are accepted, `--` for SQL and `#` for
    YAML, so a listing that moves between a .sql and a .yml file keeps its test.
    """
    text = read(rel)
    n = re.escape(number)
    m = re.search(rf"^(?:--|#) -+ Listing {n}:.*?$\n(.*?)^(?:--|#) -+ end listing {n}",
                  text, re.S | re.M)
    assert m, f"{rel} has no fenced block for listing {number}"
    return m.group(1)


def rules_body(rel, name):
    """Pull the text between the ---- <name> ---- fences in a rules/ file.

    Same shape as listing_body, different fence. The two rule files back no
    printed listing, so they carry a plain descriptive fence: a listing anchor
    in them would make scripts/check_listing_anchors.py demand a README row for
    a listing the book does not print.
    """
    text = read(rel)
    n = re.escape(name)
    m = re.search(rf"^(?:--|#) -+ {n}:.*?$\n(.*?)^(?:--|#) -+ end {n}",
                  text, re.S | re.M)
    assert m, f"{rel} has no fenced block named {name}"
    return m.group(1)


def normalize(text):
    """Collapse whitespace so line wrapping is not a difference that matters."""
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------- the schema

@test
def test_schema_has_parent_span_id():
    """An exemplar or a log line hands you a trace_id. Reassembling that into a
    trace a human reads means knowing which span hung off which."""
    body = read("clickhouse/init.sql")
    assert "parent_span_id String" in body, \
        "init.sql must carry parent_span_id; a trace_id with no tree is a list of spans"


# The consumer's INSERT binds positionally, so the column list in INSERT_SQL and
# the tuple _span_to_row returns have to agree in length AND in order. Neither
# ClickHouse nor clickhouse-driver raises when they do not: every value shifts
# one place and the table fills with plausible nonsense. This table is the
# bridge between the two, one row per column, in the order the INSERT declares
# them, paired with the expression that must supply that column.
CONSUMER_ROW = [
    ("timestamp",      "start_ns"),
    ("trace_id",       "span.trace_id.hex()"),
    ("span_id",        "span.span_id.hex()"),
    ("parent_span_id", "parent"),
    ("service_name",   "service_name"),
    ("span_name",      "span.name"),
    ("status_code",    "STATUS_CODE_NAMES.get(span.status.code, 'STATUS_CODE_UNSET')"),
    ("duration_ns",    "duration_ns"),
    ("attributes",     "_attrs_to_map(span.attributes)"),
]


def _consumer_module():
    return ast.parse(read("app/consumer_clickhouse.py"))


def _insert_columns(tree):
    """The column names inside INSERT INTO ... ( ... ) VALUES."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == "INSERT_SQL"
                for target in node.targets):
            sql = node.value.value
            break
    else:
        raise AssertionError("app/consumer_clickhouse.py no longer defines INSERT_SQL")
    inner = re.search(r"\(([^()]*)\)\s*VALUES", sql, re.S)
    assert inner, "INSERT_SQL no longer names its columns, so the bind is positional guesswork"
    return [c.strip() for c in inner.group(1).split(",") if c.strip()]


def _row_expressions(tree):
    """The expressions _span_to_row returns, in order, as source text."""
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_span_to_row":
            break
    else:
        raise AssertionError("app/consumer_clickhouse.py no longer defines _span_to_row")
    for stmt in ast.walk(node):
        if isinstance(stmt, ast.Return) and isinstance(stmt.value, ast.Tuple):
            return [ast.unparse(elt) for elt in stmt.value.elts]
    raise AssertionError("_span_to_row no longer returns a tuple, so there is no row order to check")


@test
def test_schema_populates_parent_span_id_from_the_consumer():
    """A declared column nothing writes to reads exactly like a correct one.

    Substring-matching "parent_span_id" against this file is what this test used
    to do, and the module docstring contains the literal, so it passed with the
    column deleted from BOTH the INSERT and the row. This compares the two lists
    the driver binds together.
    """
    tree = _consumer_module()
    columns = _insert_columns(tree)
    row = _row_expressions(tree)
    assert len(columns) == len(row), (
        f"INSERT_SQL names {len(columns)} columns and _span_to_row returns "
        f"{len(row)} values; clickhouse-driver binds positionally and will not "
        "complain, it will shift every value one place")
    assert columns == [name for name, _ in CONSUMER_ROW], \
        f"the INSERT column list moved: {columns}"
    for position, ((name, expected), actual) in enumerate(zip(CONSUMER_ROW, row)):
        assert actual == expected, (
            f"position {position} is bound to column {name}, and the row supplies "
            f"{actual!r} where {expected!r} is the value for that column")


@test
def test_schema_keeps_listing_7_1_column_types():
    """Chapter 9 reads chapter 7's table. The shared columns must not drift."""
    body = read("clickhouse/init.sql")
    for column in ("timestamp      DateTime64(9) CODEC(Delta, ZSTD(1))",
                   "trace_id       String CODEC(ZSTD(1))",
                   "service_name   LowCardinality(String) CODEC(ZSTD(1))",
                   "status_code    LowCardinality(String) CODEC(ZSTD(1))",
                   "duration_ns    UInt64 CODEC(T64, ZSTD(1))",
                   "attributes     Map(LowCardinality(String), String) CODEC(ZSTD(3))"):
        assert column in body, f"listing 7.1 column drifted: {column}"


@test
def test_schema_carries_no_chapter_7_storage_policy():
    """Chapter 7 pinned this table to a volume policy defined in its own
    config.d. That file is not here. Left in, CREATE TABLE fails on first boot
    and the stack comes up with an empty store and no obvious reason why."""
    sql = "\n".join(l for l in read("clickhouse/init.sql").splitlines()
                     if not l.lstrip().startswith("--"))
    assert "storage_policy" not in sql, \
        "init.sql names a storage policy this chapter does not ship"


# -------------------------------------------------------------- the listings

@test
def test_listing_9_1_connectors_sit_before_the_sampler():
    body = normalize(listing_body("collector/gateway-config.yaml", "9.1"))
    for fragment in (
            "dimensions: - name: http.route",
            "exemplars: enabled: true",
            "servicegraph: store: ttl: 2s max_items: 1000",
            "forward: {}",
            "traces/in: receivers: [otlp] processors: [memory_limiter] "
            "exporters: [spanmetrics/pre, servicegraph, forward]",
            "traces/sampled: receivers: [forward]",
            "exporters: [kafka, spanmetrics/post]",
            "metrics: receivers: [spanmetrics/pre, spanmetrics/post, servicegraph] "
            "exporters: [prometheus]"):
        assert normalize(fragment) in body, f"listing 9.1 lost: {fragment}"


@test
def test_listing_9_1_samples_in_exactly_one_pipeline():
    """The whole argument of section 9.2.4 is that the connectors count first.
    A tail_sampling processor on traces/in would leave both connectors reading
    survivors, the file would still be valid YAML, and the Collector would boot."""
    pipelines = yaml.safe_load(read("collector/gateway-config.yaml"))["service"]["pipelines"]
    assert "tail_sampling" not in pipelines["traces/in"].get("processors", []), \
        "the pre-sample pipeline must not sample, or 'pre' counts survivors too"
    assert pipelines["traces/in"].get("processors", [])[:1] == ["memory_limiter"], \
        ("traces/in must run memory_limiter first, or spanmetrics/pre counts a batch "
         "that memory_limiter downstream then refuses, the SDK retries it, and the "
         "series the chapter calls ground truth counts it twice")
    assert "tail_sampling" in pipelines["traces/sampled"]["processors"], \
        "nothing samples anywhere; pre and post would be the same series twice"
    assert "spanmetrics/pre" in pipelines["traces/in"]["exporters"], \
        "the pre connector must hang off the unsampled fork"
    assert "spanmetrics/post" in pipelines["traces/sampled"]["exporters"], \
        "the post connector must hang off the sampled fork"


@test
def test_listing_9_1_histograms_are_explicit_not_exponential():
    """Measured, not assumed. The Collector's prometheus exporter renders classic
    exposition, and an exponential histogram has no classic rendering: it comes
    out as one le="+Inf" bucket. histogram_quantile then reads +Inf, and an
    exemplar has no bucket to hang off, so /api/v1/query_exemplars returns
    nothing. Both halves of section 9.3's metric-to-trace jump die silently."""
    connectors = yaml.safe_load(read("collector/gateway-config.yaml"))["connectors"]
    for name in ("spanmetrics/pre", "spanmetrics/post"):
        histogram = connectors[name]["histogram"]
        assert "explicit" in histogram, \
            f"{name} must declare explicit buckets or its exemplars have nowhere to attach"
        assert "exponential" not in histogram, \
            f"{name} uses exponential buckets; classic exposition collapses them to +Inf"
        assert len(histogram["explicit"]["buckets"]) >= 8, \
            f"{name} has too few buckets to read a p99 off"


@test
def test_listing_9_1_the_post_connector_carries_exemplars():
    """A pre-sampler exemplar is minted before the drop decision, so it points at
    a trace the sampler is still free to throw away. Measured on this stack:
    10 of 35 pre-sampler exemplars resolved to a stored trace, against 17 of 17
    post-sampler ones. The post connector is the one section 9.3 reads."""
    connectors = yaml.safe_load(read("collector/gateway-config.yaml"))["connectors"]
    assert connectors["spanmetrics/post"]["exemplars"]["enabled"] is True, \
        "without post exemplars the metric-to-trace jump has no pointer that survives"


@test
def test_listing_9_2_error_index_exact():
    body = normalize(listing_body("clickhouse/error_index.sql", "9.2"))
    for fragment in (
            "CREATE MATERIALIZED VIEW IF NOT EXISTS tracing.exc_mv "
            "TO tracing.exceptions AS",
            "cityHash64(error_type, msg_template, top_frame) AS fingerprint",
            "attributes['exception.type'] AS error_type",
            "replaceRegexpAll(attributes['exception.message'], "
            "'[0-9a-f]{8,}|[0-9]+', '?') AS msg_template",
            "trace_id  AS sample_trace_id",
            "FROM tracing.otel_traces",
            "WHERE status_code = 'STATUS_CODE_ERROR'"):
        assert normalize(fragment) in body, f"listing 9.2 lost: {fragment}"


@test
def test_listing_9_2_fingerprints_on_the_innermost_frame():
    """The book prints splitByChar('\\n', stacktrace)[1]. On a Python traceback
    that is the literal line "Traceback (most recent call last):", identical for
    every exception ever raised, so every issue in the service collapses into
    one. The runnable file takes the LAST parsed frame and drops its line number
    so an edit above the raise does not fork one issue into two."""
    body = normalize(listing_body("clickhouse/error_index.sql", "9.2"))
    assert "extractAll(attributes['exception.stacktrace']" in body, \
        "top_frame must be parsed out of the traceback, not sliced off its first line"
    assert "-1)" in body, "top_frame must take the innermost frame, not the outermost"
    assert "', line [0-9]+', ''" in body, \
        "the line number must be stripped, or an edit above the raise forks the issue"
    assert "splitByChar" not in body, \
        "splitByChar('\\n', ...)[1] is 'Traceback (most recent call last):' on every error"


@test
def test_listing_9_2_weights_the_issue_count():
    """error_count sums adjusted_count, so an issue folded out of sampled spans
    reports the population it stands for rather than the rows that survived."""
    body = normalize(listing_body("clickhouse/error_index.sql", "9.2"))
    assert "toUInt64(adjusted_count) AS error_count" in body, \
        "an issue counted by rows is a sampled count wearing a total's name"


@test
def test_listing_9_2_target_table_folds_by_fingerprint():
    body = read("clickhouse/error_index.sql")
    assert "ENGINE = AggregatingMergeTree" in body
    assert "ORDER BY fingerprint" in body, \
        "the issue table has to key on the fingerprint, or nothing folds"
    assert "SimpleAggregateFunction(sum, UInt64)" in body


@test
def test_listing_9_3_declares_all_three_bridges():
    body = normalize(listing_body("collector/gateway-config.yaml", "9.3"))
    for bridge, fragment in (
            ("traces to logs", "otlphttp/loki: endpoint: http://loki:3100/otlp"),
            ("metrics to traces", "exemplars: enabled: true"),
            ("traces to metrics", "prometheus: endpoint: 0.0.0.0:8889")):
        assert normalize(fragment) in body, f"listing 9.3 lost the {bridge} bridge: {fragment}"
    assert "enable_open_metrics: true" in body, \
        "classic Prometheus exposition carries no exemplars; OpenMetrics is what exposes them"


@test
def test_listing_9_3_prometheus_stores_what_the_connector_emits():
    """The book's annotation D: without the flag Prometheus accepts exemplars and
    stores none. Nothing errors, and query_exemplars returns an empty list."""
    compose = yaml.safe_load(read("docker-compose.yml"))
    command = compose["services"]["prometheus"]["command"]
    assert "--enable-feature=exemplar-storage" in command, \
        "Prometheus drops every exemplar on the floor without this flag"


@test
def test_listing_9_3_loki_accepts_structured_metadata():
    """trace_id arrives as structured metadata, not as a label. With this off,
    Loki takes the write and drops the field, and the bridge-1 join returns
    nothing with no error anywhere."""
    loki = yaml.safe_load(read("loki/loki.yaml"))
    assert loki["limits_config"]["allow_structured_metadata"] is True, \
        "trace_id rides structured metadata; without it every log-to-trace join is empty"


@test
def test_burn_rate_rule_keeps_its_rules_and_both_alerts():
    body = normalize(rules_body("rules/burn_rate.yml", "burn-rate rules"))
    for fragment in (
            "- record: slo:checkout_errors:ratio_rate5m",
            "- record: slo:checkout_errors:ratio_rate1h",
            "- alert: CheckoutErrorBudgetBurnFast",
            "for: 2m",
            "severity: page",
            "- alert: CheckoutErrorBudgetBurnSlow",
            "for: 15m",
            "severity: ticket"):
        assert normalize(fragment) in body, f"the burn-rate rule lost: {fragment}"


@test
def test_burn_rate_thresholds_are_the_budget_arithmetic():
    """1.44% and 0.6% are 14.4x and 6x of a 0.1% budget. Written as the product
    rather than as 0.0144, so changing the objective changes both thresholds."""
    body = normalize(rules_body("rules/burn_rate.yml", "burn-rate rules"))
    assert "(14.4 * 0.001)" in body, "the fast threshold must show its burn rate and budget"
    assert "(6 * 0.001)" in body, "the slow threshold must show its burn rate and budget"


@test
def test_burn_rate_burns_against_the_pre_sample_series():
    """post_calls_total is the sampler's survivors, and the sampler keeps every
    error. An error ratio computed from post reads tens of times too high on this
    stack, so this alert would page on a healthy service."""
    body = rules_body("rules/burn_rate.yml", "burn-rate rules")
    assert "post_calls_total" not in body, \
        "a burn rate over the sampled survivors pages on a healthy service"
    assert body.count("pre_calls_total") >= 8, \
        "every window needs a numerator and a denominator off the pre series"


@test
def test_burn_rate_counts_requests_and_not_spans():
    """spanmetrics counts spans and a checkout makes seven of them. Divide error
    spans by all spans and the ratio comes out a seventh of the request error
    rate, under a label that promises 99.9% availability. The SERVER span is
    opened once per request, so selecting it is what makes the ratio a request
    rate, and app/checkout.py marking that span failed is what gives the
    selector something to count."""
    body = rules_body("rules/burn_rate.yml", "burn-rate rules")
    assert body.count('span_kind="SPAN_KIND_SERVER"') >= 8, \
        "every numerator and denominator has to be pinned to the server span"
    producer = ast.parse(read("app/checkout.py"))
    calls = {ast.unparse(node) for node in ast.walk(producer) if isinstance(node, ast.Call)}
    assert "trace.get_current_span()" in calls, \
        ("nothing reaches for the server span, so the request-grain selector in "
         "burn_rate.yml counts a flat zero and the burn-rate alert never fires")
    assert "span.set_status(Status(StatusCode.ERROR, str(failure)))" in calls, \
        "the server span is never marked failed, so no span of kind SERVER carries an error status"
    assert "span.record_exception(failure)" in calls, \
        ("the server span is marked ERROR without exception detail, so the listing 9.2 "
         "index fingerprints an empty type, template and frame and every failure adds "
         "a second, untitled issue")


@test
def test_burn_rate_every_window_an_alert_names_is_recorded():
    """A Prometheus alert whose expression names a rule that does not exist does
    not error. It evaluates to an empty vector and never fires. Both alerts read
    a short window against a long one, so the file has to carry all four."""
    doc = yaml.safe_load(read("rules/burn_rate.yml"))
    rules = doc["groups"][0]["rules"]
    recorded = {r["record"] for r in rules if "record" in r}
    for alert in (r for r in rules if "alert" in r):
        for name in re.findall(r"slo:[a-z_:0-9]+", alert["expr"]):
            assert name in recorded, \
                f"{alert['alert']} reads {name}, which no recording rule produces, " \
                "so it silently never fires"


@test
def test_ingest_gap_rule_keeps_both_sides_and_its_alert():
    body = normalize(rules_body("rules/span_ingest_gap.yml",
                                "ingest-gap rules"))
    for fragment in (
            "- record: spans:received:rate5m",
            "sum(otelcol_receiver_accepted_spans_total)",
            "sum(otelcol_receiver_accepted_spans_total offset 5m) or vector(0)",
            "- record: spans:expected:rate5m",
            "- alert: SpanIngestGap",
            "for: 10m"):
        assert normalize(fragment) in body, f"the ingest-gap rule lost: {fragment}"


@test
def test_ingest_gap_counts_two_things_that_took_different_paths():
    """The point of the rule. Comparing the Collector's received count against a
    number that also travelled through the Collector proves nothing: an outage
    takes both to zero and the ratio holds at 1.0."""
    body = normalize(rules_body("rules/span_ingest_gap.yml",
                                "ingest-gap rules"))
    assert "checkout_spans_emitted_total" in body, \
        "the expected side must come off the producer, not off the Collector"
    assert "otelcol_receiver_accepted_spans_total" in body
    prometheus = yaml.safe_load(read("prometheus.yml"))
    jobs = {j["job_name"]: j["static_configs"][0]["targets"] for j in prometheus["scrape_configs"]}
    assert jobs["checkout-producer"] == ["checkout-service:8080"], \
        "the producer counter must be scraped off the container, never through the Collector"


@test
def test_ingest_gap_survives_a_total_collector_outage():
    """The failure this alert exists for takes the received series stale, and
    rate() over a stale series returns an empty vector. An alert over an empty
    vector never fires, so the one case that matters most is the one it would
    miss. `or vector(0)` on each side is what keeps the arithmetic defined."""
    body = normalize(rules_body("rules/span_ingest_gap.yml",
                                "ingest-gap rules"))
    assert body.count("or vector(0)") >= 2, \
        "both counters need an empty-vector fallback or a total outage never pages"
    assert "clamp_min" in body, "a zero denominator makes the ratio NaN, which never alerts"


@test
def test_ingest_gap_counters_are_compared_from_the_same_starting_point():
    """The failure this rule shipped with. rate() measures from a series' first
    sample INSIDE the window. checkout_spans_emitted_total is published at zero
    from the producer's first scrape; otelcol_receiver_accepted_spans_total does
    not exist until the Collector accepts a batch, so it is born already in the
    hundreds and those hundreds can never enter a received-side window. A
    healthy stack whose counters read 2142 and 2142 reported every span lost.
    Subtracting the value five minutes ago, with `or vector(0)` for a series
    that did not exist then, counts them on both sides."""
    body = normalize(rules_body("rules/span_ingest_gap.yml",
                                "ingest-gap rules"))
    assert "rate(" not in body, \
        ("rate() drops the first sample of a series that was born non-zero, which is "
         "the whole defect: the two counters do not come into existence the same way")
    assert "offset 5m) or vector(0)" in body, \
        "the received side must treat a series that did not exist five minutes ago as zero"
    assert "min_over_time(spans:expected:rate5m[90s])" in body, \
        ("the expected side has to be taken at its low point over the last minute and "
         "a half, or every span still in flight between the producer and the Collector "
         "is charged to the gap and the first minute of a burst reads as total loss")
    assert "offset 1m" not in body and "offset 6m" not in body, \
        ("offsetting one window against the other was measured and is wrong: it fixes "
         "the start of a burst and puts a 100% gap on the end of one")


# ----------------------------------------------------------------- the stack

@test
def test_compose_parses_and_pins_one_tag_per_image():
    compose = yaml.safe_load(read("docker-compose.yml"))
    services = compose["services"]
    assert set(services) == {
        "checkout-service", "otel-collector", "kafka", "kafka-init",
        "clickhouse", "consumer-clickhouse", "prometheus", "loki"}, \
        f"the service set moved: {sorted(services)}"
    tags_by_repo = {}
    for name, svc in services.items():
        image = svc.get("image")
        if image is None:
            assert "build" in svc, f"{name} has neither an image nor a build"
            continue
        assert ":" in image, f"{name} runs {image}, which resolves to :latest"
        repo, tag = image.rsplit(":", 1)
        assert tag != "latest", f"{name} is pinned to :latest, which is not a pin"
        tags_by_repo.setdefault(repo, set()).add(tag)
    for repo, tags in sorted(tags_by_repo.items()):
        assert len(tags) == 1, f"{repo} runs at {sorted(tags)}; one tag per repository"


@test
def test_compose_mounts_the_rule_files_into_prometheus():
    """rules/*.yml is where the burn-rate and ingest-gap rules live. Unmounted,
    Prometheus
    starts clean and every rule this chapter prints is simply absent."""
    compose = yaml.safe_load(read("docker-compose.yml"))
    mounts = compose["services"]["prometheus"]["volumes"]
    assert any(m.startswith("./rules:") for m in mounts), \
        "rules/ is not mounted, so Prometheus loads neither rule file"
    prometheus = yaml.safe_load(read("prometheus.yml"))
    assert prometheus["rule_files"] == ["rules/*.yml"], \
        "the rule_files glob must match the mount point"


@test
def test_compose_mounts_init_sql_into_the_entrypoint():
    compose = yaml.safe_load(read("docker-compose.yml"))
    mounts = compose["services"]["clickhouse"]["volumes"]
    assert any("docker-entrypoint-initdb.d" in m for m in mounts), \
        "init.sql must be applied on first boot or nothing exists to query"


@test
def test_loki_ships_without_a_compose_healthcheck():
    """Not an oversight. grafana/loki is distroless, so there is no shell and no
    wget or curl to run a healthcheck with. Anything checking Loki has to poll
    /ready from outside the container, which is what the test scripts do."""
    compose = yaml.safe_load(read("docker-compose.yml"))
    assert "healthcheck" not in compose["services"]["loki"], \
        "the loki image is distroless; a compose healthcheck there can only fail"
    polls = [s.name for s in sorted((CHAPTER / "tests").glob("*.sh"))
             if "3100/ready" in s.read_text()]
    assert polls, "nothing polls Loki's /ready, so every suite races its first write"


@test
def test_every_scrape_target_is_a_service_in_the_compose_file():
    compose = yaml.safe_load(read("docker-compose.yml"))
    prometheus = yaml.safe_load(read("prometheus.yml"))
    for job in prometheus["scrape_configs"]:
        for target in job["static_configs"][0]["targets"]:
            host = target.split(":")[0]
            assert host in compose["services"], \
                f"job {job['job_name']} scrapes {host}, which no service provides"


@test
def test_no_hash_comments_inside_bash_blocks():
    """A reader pastes the whole block. zsh turns a bare # into an argument."""
    offenders = []
    for md in sorted(CHAPTER.glob("*.md")) + sorted(CHAPTER.glob("exercises/*.md")) \
            + sorted(CHAPTER.glob("benchmarks/*.md")):
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
    next one. Read line by line and that command looks like it has no redirect
    at all, which is how this check passed over two commands in this directory
    that were in fact correct, and would have passed over one that was not.
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
                 + sorted(CHAPTER.glob("benchmarks/*.md"))
                 + sorted(CHAPTER.glob("tests/*.sh"))):
        for n, command in _shell_commands(path.read_text()):
            if "clickhouse-client" not in command or command.strip().startswith("#"):
                continue
            if "< /dev/null" in command or "--multiquery <" in command or "--query" not in command:
                continue
            offenders.append(f"{path.relative_to(CHAPTER)}:{n}")
    assert not offenders, "clickhouse-client without stdin closed: " + ", ".join(offenders)


@test
def test_the_stdin_check_sees_a_continued_invocation():
    """The check above is only worth having if a wrapped command reaches it.

    Two commands in this directory put `clickhouse-client` on one line and the
    redirect on the next. Line-at-a-time, both were invisible, and so would a
    third that had lost its redirect be.
    """
    wrapped = ("docker compose exec -T clickhouse clickhouse-client \\\n"
               "  --query \"SELECT 1\"\n")
    joined = [command for _, command in _shell_commands(wrapped)]
    assert any("clickhouse-client" in c and "--query" in c for c in joined), \
        "a backslash-continued clickhouse-client invocation is still being read one line at a time"


@test
def test_every_readme_listing_row_names_a_file_that_exists():
    rows = re.findall(r"^\|\s*(9\.\d)\s*\|\s*`([^`]+)`", read("README.md"), re.M)
    assert len(rows) == 3, f"the listing table has {len(rows)} rows, expected 9.1 to 9.3"
    for number, rel in rows:
        assert (CHAPTER / rel).exists(), f"listing {number} names {rel}, which is not here"
        listing_body(rel, number)


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
