"""
Offline well-formedness checks for the Chapter 7 stack. No Docker required.

Asserts:
  - docker-compose.yml and every collector/*.yaml + prometheus.yml parse,
  - every image is pinned to exactly one tag (N1),
  - the four SQL files contain the listing 7.1/7.2/7.3/7.4 statements with the
    exact column names, codecs, ORDER BY, TTL, and row policy from the chapter,
  - the config.d XML files are well-formed and define the 'cold' volume that
    listing 7.2's `TO VOLUME 'cold'` resolves against,
  - the README and exercises are copy-paste: every bash block runs shipped
    scripts, defines nothing, carries no shell variable, and every script the
    pages run exists, reads traffic only after waiting for it, and has an undo.

Run:  python3 -m pytest tests/test_static.py   (or: python3 tests/test_static.py)
"""
import ast
import glob
import os
import re
import xml.etree.ElementTree as ET

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(rel):
    with open(os.path.join(ROOT, rel)) as f:
        return f.read()


def test_compose_parses_and_pins_one_tag_each():
    doc = yaml.safe_load(_read("docker-compose.yml"))
    images = [s["image"] for s in doc["services"].values() if "image" in s]
    assert images, "no images found"
    for img in images:
        assert ":" in img, f"image not pinned: {img}"
        tag = img.rsplit(":", 1)[1]
        assert tag not in ("latest", ""), f"unpinned/latest tag: {img}"
    # one tag per repository (N1)
    by_repo = {}
    for img in images:
        repo, tag = img.rsplit(":", 1)
        by_repo.setdefault(repo, set()).add(tag)
    for repo, tags in by_repo.items():
        assert len(tags) == 1, f"{repo} pinned to multiple tags: {tags}"


def test_collector_and_prometheus_yaml_parse():
    for rel in ("collector/gateway-config.yaml", "collector/tempo.yaml", "prometheus.yml"):
        yaml.safe_load(_read(rel))


def test_gateway_partitions_by_trace_id():
    cfg = yaml.safe_load(_read("collector/gateway-config.yaml"))
    kafka = cfg["exporters"]["kafka"]
    assert kafka["partition_traces_by_id"] is True
    assert kafka["traces"]["topic"] == "otlp_spans"


def test_gateway_fans_out_to_both_archetypes():
    """Section 7.3 claims one span stream reaches both stores. An exporter that
    is defined but left out of the pipeline reaches nothing and says nothing,
    so assert the pipeline list, not just the exporter block."""
    cfg = yaml.safe_load(_read("collector/gateway-config.yaml"))
    exporters = cfg["service"]["pipelines"]["traces"]["exporters"]
    assert "kafka" in exporters, "row archetype (Kafka -> ClickHouse) not wired"
    assert "otlp/tempo" in exporters, "block archetype (Tempo) not wired"
    assert cfg["exporters"]["otlp/tempo"]["endpoint"] == "tempo:4317"


def test_tempo_writes_blocks_to_the_same_object_store():
    """'Both writing to the same object storage' is only true if Tempo's backend
    is the SeaweedFS service, not a container filesystem."""
    cfg = yaml.safe_load(_read("collector/tempo.yaml"))
    trace = cfg["storage"]["trace"]
    assert trace["backend"] == "s3", "Tempo is not on object storage"
    assert trace["s3"]["endpoint"] == "seaweedfs:8333"
    assert trace["s3"]["bucket"] == "tempo-blocks"


def test_tempo_service_is_on_by_default_and_bucket_exists():
    """Tempo behind a profile is off by default, which makes the fan-out a lie.
    And its bucket has to be created or Tempo fails to start."""
    doc = yaml.safe_load(_read("docker-compose.yml"))
    tempo = doc["services"]["tempo"]
    assert "profiles" not in tempo, "Tempo behind a profile is off by default"
    buckets = _seaweedfs_buckets(doc)
    assert "tempo-blocks" in buckets, "SeaweedFS does not create the tempo-blocks bucket"
    assert tempo["depends_on"]["seaweedfs"]["condition"] == "service_healthy", \
        "Tempo can start before its bucket exists"


def test_tempo_config_has_no_pre_3_0_sections():
    """Tempo 3.0 removed these outright; leaving them in fails at startup with
    'field ingester not found in type app.Config'."""
    cfg = yaml.safe_load(_read("collector/tempo.yaml"))
    for gone in ("ingester", "compactor"):
        assert gone not in cfg, f"'{gone}' was removed in Tempo 3.0"


def test_listing_7_1_schema_exact():
    sql = _read("clickhouse/init.sql")
    # exact column + codec lines from listing 7.1
    for needle in (
        "timestamp      DateTime64(9) CODEC(Delta, ZSTD(1))",
        "trace_id       String CODEC(ZSTD(1))",
        "span_id        String CODEC(ZSTD(1))",
        "service_name   LowCardinality(String) CODEC(ZSTD(1))",
        "span_name      LowCardinality(String) CODEC(ZSTD(1))",
        "status_code    LowCardinality(String) CODEC(ZSTD(1))",
        "duration_ns    UInt64 CODEC(T64, ZSTD(1))",
        "attributes     Map(LowCardinality(String), String) CODEC(ZSTD(3))",
        "INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1",
        "ENGINE = MergeTree",
        "PARTITION BY toYYYYMMDD(timestamp)",
        "ORDER BY (service_name, span_name, toStartOfHour(timestamp), trace_id)",
        "TTL toDateTime(timestamp) + INTERVAL 15 DAY",
    ):
        assert needle in sql, f"listing 7.1 missing: {needle!r}"
    assert sql.count("(") == sql.count(")"), "unbalanced parentheses in init.sql"


def test_adjusted_count_column_present():
    # section 7.4.3 weight column: defaults to 1.0 (unsampled = weight 1)
    sql = _read("clickhouse/init.sql")
    assert "adjusted_count Float64 DEFAULT 1.0 CODEC(ZSTD(1))" in sql, \
        "adjusted_count column missing or wrong type/default/codec"


def test_listing_7_2_tiering_exact():
    sql = _read("clickhouse/tiering.sql")
    for needle in (
        "MODIFY TTL",
        "toDateTime(timestamp) + INTERVAL 2 DAY TO VOLUME 'cold'",
        "toDateTime(timestamp) + INTERVAL 15 DAY DELETE",
        "DROP PARTITION '20260601'",
    ):
        assert needle in sql, f"listing 7.2 missing: {needle!r}"


def test_listing_7_3_compression_exact():
    sql = _read("clickhouse/compression.sql")
    for needle in (
        "formatReadableSize(sum(data_compressed_bytes))",
        "formatReadableSize(sum(data_uncompressed_bytes))",
        "FROM system.columns",
        "WHERE table = 'otel_traces'",
        "ORDER BY ratio DESC",
    ):
        assert needle in sql, f"listing 7.3 missing: {needle!r}"


def test_listing_7_4_tenancy_exact():
    sql = _read("clickhouse/tenancy.sql")
    executable = "\n".join(
        l for l in sql.splitlines() if not l.lstrip().startswith("--"))
    norm = re.sub(r"\s+", " ", executable).strip()

    # The policy statement as listing 7.4 prints it, modulo three things that
    # cannot be literal here and are asserted in the shape they take instead:
    #   - `tracing.` qualifies both tables, because that is the database the
    #     repo puts them in and the book writes them unqualified,
    #   - OR REPLACE makes re-applying the file converge on this definition
    #     rather than keep an older policy,
    #   - the book's `admin, ingest` are the operator and the writer. This
    #     stack has one privileged login for both, `default`, and roles cannot
    #     stand in because `default` lives in read-only users.xml storage.
    assert (
        "CREATE ROW POLICY OR REPLACE tenant_filter ON tracing.otel_traces "
        "USING tenant_id IN (SELECT tenant_id FROM tracing.tenant_users "
        "WHERE user_name = currentUser()) "
        "TO ALL EXCEPT default;"
    ) in norm, "the row policy is not listing 7.4's"

    # The two ways this drifted before. `TO ALL` filters the operator to zero
    # rows, which annotation #D warns against, and comparing the username to
    # the tenant id skips the map that annotation #C exists to require.
    assert not re.search(r"TO ALL\s*;", norm), \
        "policy is TO ALL, which filters the operator (annotation #D)"
    assert "tenant_id = currentUser()" not in norm, \
        "policy compares the username to the tenant id (annotation #C)"

    for needle in (
        "ADD COLUMN IF NOT EXISTS tenant_id",
        "CREATE TABLE IF NOT EXISTS tracing.tenant_users",
        "CREATE USER IF NOT EXISTS acme_reader",
        "CREATE USER IF NOT EXISTS globex_reader",
        "GRANT SELECT ON tracing.tenant_users",
    ):
        assert needle in sql, f"listing 7.4 missing: {needle!r}"

    # Annotation #C only means anything if the map is not an identity function.
    # A login named after its own tenant id would pass every other assertion
    # here while teaching the opposite of what the annotation says.
    seed = re.search(r"INSERT INTO tracing\.tenant_users[^;]*?VALUES(.*?);",
                     sql, re.S)
    assert seed, "tenant_users is created but never seeded"
    pairs = re.findall(r"\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", seed.group(1))
    assert len(pairs) >= 2, f"expected two seeded logins, got {pairs}"
    for user_name, tenant_id in pairs:
        assert user_name != tenant_id, \
            f"login {user_name!r} is named after its tenant id (annotation #C)"

    # ClickHouse cannot ALTER a pre-existing column into the sort key, so the
    # tenant-leading layout is a CREATE-time concern, documented not executed;
    # any MODIFY ORDER BY line must stay commented out.
    for line in sql.splitlines():
        if "MODIFY ORDER BY" in line:
            assert line.lstrip().startswith("--"), \
                "tenancy.sql runs an un-runnable MODIFY ORDER BY (ClickHouse " \
                "rejects moving an existing column into the sort key)"


def test_storage_policy_defines_cold_volume():
    root = ET.fromstring(_read("clickhouse/config.d/storage.xml"))
    text = ET.tostring(root, encoding="unicode")
    assert "<cold>" in text and "<tiered>" in text, "storage policy 'tiered'/'cold' missing"
    # init.sql must bind the table to the policy or TO VOLUME 'cold' cannot resolve
    assert "storage_policy = 'tiered'" in _read("clickhouse/init.sql")


def test_storage_cold_tier_is_s3_seaweedfs():
    # the cold volume must be backed by a real S3 disk pointing at the SeaweedFS
    # service, not a local disk, so the tier move exercises object storage.
    xml = _read("clickhouse/config.d/storage.xml")
    root = ET.fromstring(xml)
    disks = root.find("./storage_configuration/disks")
    s3 = disks.find("./s3_cold")
    assert s3 is not None, "s3_cold disk missing from storage.xml"
    assert s3.findtext("type") == "s3", "s3_cold disk is not type s3"
    endpoint = s3.findtext("endpoint") or ""
    assert "seaweedfs:8333/traces-cold" in endpoint, f"s3_cold endpoint not SeaweedFS: {endpoint!r}"
    # the tiered policy's cold volume must resolve to that S3 disk
    cold_disk = root.findtext(
        "./storage_configuration/policies/tiered/volumes/cold/disk")
    assert cold_disk == "s3_cold", f"cold volume disk is {cold_disk!r}, expected 's3_cold'"


def _seaweedfs_buckets(doc):
    cmd = doc["services"]["seaweedfs"]["command"]
    flags = [a for a in cmd if a.startswith("-bucket=")]
    assert len(flags) == 1, f"seaweedfs command carries no -bucket= flag: {cmd}"
    return flags[0].split("=", 1)[1].split(",")


def test_object_store_pinned_and_creates_both_buckets():
    """minio/minio and minio/mc vanished from Docker Hub, which broke this stack
    for every reader. Pin the replacement to an exact release and keep bucket
    creation inside the store, so nothing else has to be pulled to bootstrap."""
    doc = yaml.safe_load(_read("docker-compose.yml"))
    images = [s["image"] for s in doc["services"].values() if "image" in s]
    assert not [i for i in images if i.startswith("minio/")], "a minio image is back"
    store = doc["services"]["seaweedfs"]
    repo, tag = store["image"].rsplit(":", 1)
    assert repo == "chrislusf/seaweedfs"
    assert re.fullmatch(r"\d+\.\d+", tag), f"seaweedfs tag is not a release number: {tag!r}"
    assert store["command"][0] == "mini"
    assert set(_seaweedfs_buckets(doc)) == {"traces-cold", "tempo-blocks"}
    assert doc["services"]["clickhouse"]["depends_on"]["seaweedfs"]["condition"] == "service_healthy", \
        "ClickHouse validates the s3_cold disk on boot, so it must wait for the bucket"


def test_s3_key_pair_matches_across_store_clickhouse_and_tempo():
    doc = yaml.safe_load(_read("docker-compose.yml"))
    env = doc["services"]["seaweedfs"]["environment"]
    s3 = ET.fromstring(_read("clickhouse/config.d/storage.xml")).find(
        "./storage_configuration/disks/s3_cold")
    tempo = yaml.safe_load(_read("collector/tempo.yaml"))["storage"]["trace"]["s3"]
    pair = (env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"])
    assert (s3.findtext("access_key_id"), s3.findtext("secret_access_key")) == pair
    assert (tempo["access_key"], tempo["secret_key"]) == pair


def test_benchmark_scripts_parse_clean():
    scripts = sorted(glob.glob(os.path.join(ROOT, "benchmarks", "*.py")))
    names = {os.path.basename(p) for p in scripts}
    for expected in ("compression_ratio.py", "bloom_index_pruning.py",
                     "tiering_automation.py", "chclient.py"):
        assert expected in names, f"benchmark script missing: {expected}"
    for path in scripts:
        with open(path) as f:
            ast.parse(f.read())  # raises SyntaxError if the script is malformed


def test_other_config_xml_well_formed():
    for rel in ("clickhouse/config.d/network.xml",
                "clickhouse/config.d/prometheus.xml",
                "clickhouse/users.d/z-allow-network.xml"):
        ET.fromstring(_read(rel))


def test_consumer_inserts_listing_7_1_columns():
    src = _read("app/consumer_clickhouse.py")
    m = re.search(r"INSERT INTO tracing\.otel_traces \((.*?)\) VALUES", src, re.S)
    assert m, "consumer INSERT not found"
    cols = {c.strip() for c in m.group(1).replace("\n", " ").split(",")}
    assert cols == {
        "timestamp", "trace_id", "span_id", "service_name",
        "span_name", "status_code", "duration_ns", "attributes",
    }, f"consumer columns drift from listing 7.1: {cols}"


def test_host_ports_bind_loopback_only():
    """ClickHouse runs a password-less user and the S3 key pair is in this
    file, so no published port may listen beyond this machine."""
    doc = yaml.safe_load(_read("docker-compose.yml"))
    published = [(name, p) for name, svc in doc["services"].items() for p in svc.get("ports", [])]
    assert published, "no published ports found"
    for name, port in published:
        assert str(port).startswith("127.0.0.1:"), f"{name} publishes {port} on every interface"


# ---- the reader path ------------------------------------------------------
# Every page is pasted a block at a time, often into a fresh terminal, often
# starting in the middle. These checks keep each block self-contained.

READER_DOCS = ("README.md", "exercises/compression.md", "exercises/tiering.md",
               "exercises/tenancy.md")


def _fences(rel, langs):
    """Yield (first line number, body lines) for each fence in one of langs."""
    fence, start = None, 0
    for n, line in enumerate(_read(rel).splitlines(), 1):
        s = line.strip()
        if s.startswith("```"):
            if fence is None:
                fence, start = (s[3:].strip() in langs and []), n
            else:
                if fence is not False:
                    yield start, fence
                fence = None
            continue
        if fence:
            fence.append(line)
        elif fence == []:
            fence = [line]


def _steps(rel):
    """Yield (fence start, script name) in the order a page runs them."""
    for start, body in _fences(rel, ("bash", "sh")):
        for ln in body:
            for name in re.findall(r"\./scripts/([\w-]+\.sh)", ln):
                yield start, name


def _scripts_that(marker):
    return {os.path.basename(p) for p in glob.glob(os.path.join(ROOT, "scripts", "*.sh"))
            if os.path.basename(p) != "lib.sh" and marker in _read(os.path.join("scripts", os.path.basename(p)))}


def test_no_hash_comments_inside_bash_blocks():
    """A reader pastes the whole block. zsh turns a bare # into an argument."""
    offenders = []
    for rel in READER_DOCS:
        for start, body in _fences(rel, ("bash", "sh")):
            offenders += [f"{rel}:{start}" for ln in body if re.search(r"(^|\s)#", ln)]
    assert not offenders, "bare # inside a bash block: " + ", ".join(offenders)


def test_no_bash_block_defines_a_function():
    """A helper pasted in one block and used in the next breaks anyone who
    opens a new terminal or starts an exercise in the middle."""
    offenders = []
    for rel in READER_DOCS:
        for start, body in _fences(rel, ("bash", "sh")):
            offenders += [f"{rel}:{start}: {ln.strip()}" for ln in body
                          if re.match(r"\s*(function\s+)?[A-Za-z_][\w-]*\s*\(\)\s*\{?", ln)]
    assert not offenders, "shell function defined in a bash block: " + "; ".join(offenders)


def test_no_bash_block_leans_on_a_shell_variable():
    """`TID=$(...)` in one block and `$TID` in the next was the old pattern. In a
    new terminal, or with a block skipped, the query runs on an empty string."""
    offenders = []
    for rel in READER_DOCS:
        for start, body in _fences(rel, ("bash", "sh")):
            offenders += [f"{rel}:{start}: {ln.strip()}" for ln in body
                          if re.search(r"\$(?!\?)[{(A-Za-z_]", ln)]
    assert not offenders, "shell variable in a bash block: " + "; ".join(offenders)


def test_every_script_a_page_runs_exists_and_is_executable():
    referenced = {name for rel in READER_DOCS for _, name in _steps(rel)}
    assert referenced, "no page runs a script; the regex stopped matching"
    for name in sorted(referenced):
        path = os.path.join(ROOT, "scripts", name)
        assert os.path.exists(path), f"a page runs scripts/{name}, which is not here"
        assert os.stat(path).st_mode & 0o111, f"scripts/{name} is not executable"


def test_every_script_is_strict_bash_and_used():
    """Every script fails loudly, which lib.sh's `set -euo pipefail` gives it, and
    runs on the bash 3.2 macOS ships."""
    used = {name for rel in READER_DOCS for _, name in _steps(rel)}
    assert "set -euo pipefail" in _read("scripts/lib.sh")
    for path in sorted(glob.glob(os.path.join(ROOT, "scripts", "*.sh"))):
        name = os.path.basename(path)
        text = _read(os.path.join("scripts", name))
        assert not re.search(r"declare -A|mapfile|readarray|\$\{\w+,,\}|\$\{\w+\^\^\}", text), \
            f"{name} needs bash 4, macOS ships 3.2"
        if name == "lib.sh":
            continue
        assert text.startswith("#!/usr/bin/env bash\n"), f"{name} has no bash shebang"
        assert 'source "$(dirname "$0")/lib.sh"' in text, f"{name} does not source lib.sh"
        assert name in used, f"scripts/{name} is never run by any page"


def test_every_sql_block_on_a_page_is_sql_that_really_runs():
    """A SQL block is what the reader copies to run by hand, so it has to be
    what a script or .sql file runs. Whitespace is ignored; nothing else is."""
    def squash(text):
        return " ".join(text.split()).rstrip(";").strip()
    corpus = squash("\n".join(
        _read(os.path.relpath(p, ROOT))
        for p in sorted(glob.glob(os.path.join(ROOT, "scripts", "*.sh")))
        + sorted(glob.glob(os.path.join(ROOT, "clickhouse", "*.sql")))))
    offenders = [f"{rel}:{start}" for rel in READER_DOCS
                 for start, body in _fences(rel, ("sql",))
                 if squash("\n".join(body)) not in corpus]
    assert not offenders, "SQL block no script or .sql file runs: " + ", ".join(offenders)


def test_every_read_of_new_traffic_waits_for_it():
    """A read that runs before the spans land prints a short, plausible
    answer. Every page that sends traffic waits before it reads."""
    readers = _scripts_that("require_ready")
    assert readers, "no script calls require_ready any more; the check reads nothing"
    offenders = []
    for rel in READER_DOCS:
        pending = False
        for start, name in _steps(rel):
            if name == "send-traffic.sh":
                pending = True
            elif name == "wait-until-ready.sh":
                pending = False
            elif name in readers and pending:
                offenders.append(f"{rel}:{start} runs {name} before wait-until-ready.sh")
    assert not offenders, "; ".join(offenders)


def test_the_guards_name_the_step_to_run():
    """A skipped step must stop with the command that fixes it, not a blank."""
    lib = _read("scripts/lib.sh")
    assert 'NO_TRAFFIC="nothing sent yet: run ./scripts/send-traffic.sh first"' in lib
    assert 'NOT_READY="still arriving: run ./scripts/wait-until-ready.sh first"' in lib
    named = set(re.findall(r"run (\./scripts/[\w-]+\.sh)", lib))
    for path in glob.glob(os.path.join(ROOT, "scripts", "*.sh")):
        named |= set(re.findall(r"run (\./scripts/[\w-]+\.sh)", open(path).read()))
    for cmd in sorted(named):
        assert os.path.exists(os.path.join(ROOT, cmd[2:])), f"a guard names {cmd}, which is not here"


def test_resending_traffic_clears_the_ready_mark():
    """send-traffic.sh rewrites the state without READY, and the state is tied
    to the running containers so it does not survive `down -v`."""
    send = _read("scripts/send-traffic.sh")
    assert 'rm -f "$STATE_DIR/traffic"' in send
    assert "READY" not in send
    assert "STACK_ID=$(stack_id)" in send
    assert "READY=1" in _read("scripts/wait-until-ready.sh")
    assert '"$(stack_id)"' in _read("scripts/lib.sh")


def test_every_exercise_puts_back_what_it_changed():
    """Each exercise starts from the state the stack boots in and leaves it
    there, so the next one, in any order, starts clean."""
    undo = {
        "build-compression-tables.sh": "drop-compression-tables.sh",
        "stage-tiering-partition.sh": "clean-up-tiering.sh",
        "let-the-rule-move-it.sh": "clean-up-tiering.sh",
        "apply-tenancy.sh": "clean-up-tenancy.sh",
        "add-unmapped-login.sh": "clean-up-tenancy.sh",
        "insert-as-tenant.sh": "clean-up-tenancy.sh",
    }
    offenders = []
    for rel in READER_DOCS[1:]:
        steps = [name for _, name in _steps(rel)]
        for made, cleaned in undo.items():
            if made in steps and (cleaned not in steps
                                  or steps.index(cleaned) < len(steps) - 1 - steps[::-1].index(made)):
                offenders.append(f"{rel} runs {made} and never {cleaned} after it")
    assert not offenders, "; ".join(offenders)


def test_every_clickhouse_helper_closes_stdin():
    """The `< /dev/null` trap that hung both test scripts for a reviewer."""
    offenders = []
    files = list(READER_DOCS) + [os.path.relpath(p, ROOT) for p in
                                 sorted(glob.glob(os.path.join(ROOT, "scripts", "*.sh")))
                                 + sorted(glob.glob(os.path.join(ROOT, "tests", "*.sh")))]
    for rel in files:
        lines = _read(rel).splitlines()
        i = 0
        while i < len(lines):
            start, parts = i + 1, [lines[i]]
            while parts[-1].rstrip().endswith("\\") and i + 1 < len(lines):
                i += 1
                parts.append(lines[i])
            command = " ".join(p.rstrip().rstrip("\\") for p in parts)
            i += 1
            if "clickhouse-client" not in command or command.strip().startswith("#"):
                continue
            if "< /dev/null" in command or "--multiquery <" in command or "--query" not in command:
                continue
            offenders.append(f"{rel}:{start}")
    assert not offenders, "clickhouse-client without stdin closed: " + ", ".join(offenders)


if __name__ == "__main__":
    import traceback
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for fn in fns:
        try:
            fn()
            print(f"PASS: {fn.__name__}")
        except Exception:
            failed += 1
            print(f"FAIL: {fn.__name__}")
            traceback.print_exc()
    print(f"\n{len(fns) - failed}/{len(fns)} passed")
    raise SystemExit(1 if failed else 0)
