"""
Offline well-formedness checks for the Chapter 7 stack. No Docker required.

Asserts:
  - docker-compose.yml and every collector/*.yaml + prometheus.yml parse,
  - every image is pinned to exactly one tag (N1),
  - the three SQL files contain the listing 7.1/7.2/7.4 statements with the
    exact column names, codecs, ORDER BY, TTL, and row policy from the chapter,
  - the config.d XML files are well-formed and define the 'cold' volume that
    listing 7.2's `TO VOLUME 'cold'` resolves against.

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


def test_tempo_writes_blocks_to_the_same_minio():
    """'Both writing to the same object storage' is only true if Tempo's backend
    is the MinIO service, not a container filesystem."""
    cfg = yaml.safe_load(_read("collector/tempo.yaml"))
    trace = cfg["storage"]["trace"]
    assert trace["backend"] == "s3", "Tempo is not on object storage"
    assert trace["s3"]["endpoint"] == "minio:9000"
    assert trace["s3"]["bucket"] == "tempo-blocks"


def test_tempo_service_is_on_by_default_and_bucket_exists():
    """Tempo behind a profile is off by default, which makes the fan-out a lie.
    And its bucket has to be created or Tempo fails to start."""
    doc = yaml.safe_load(_read("docker-compose.yml"))
    tempo = doc["services"]["tempo"]
    assert "profiles" not in tempo, "Tempo behind a profile is off by default"
    assert _read("docker-compose.yml").count("local/tempo-blocks") == 1, \
        "minio-init does not create the tempo-blocks bucket"


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
    # section 7.4.4 weight column: defaults to 1.0 (unsampled = weight 1)
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


def test_storage_cold_tier_is_s3_minio():
    # the cold volume must be backed by a real S3 disk pointing at the MinIO
    # service, not a local disk, so the tier move exercises object storage.
    xml = _read("clickhouse/config.d/storage.xml")
    root = ET.fromstring(xml)
    disks = root.find("./storage_configuration/disks")
    s3 = disks.find("./s3_cold")
    assert s3 is not None, "s3_cold disk missing from storage.xml"
    assert s3.findtext("type") == "s3", "s3_cold disk is not type s3"
    endpoint = s3.findtext("endpoint") or ""
    assert "minio:9000/traces-cold" in endpoint, f"s3_cold endpoint not MinIO: {endpoint!r}"
    # the tiered policy's cold volume must resolve to that S3 disk
    cold_disk = root.findtext(
        "./storage_configuration/policies/tiered/volumes/cold/disk")
    assert cold_disk == "s3_cold", f"cold volume disk is {cold_disk!r}, expected 's3_cold'"


def test_minio_images_pinned_to_real_release_tags():
    doc = yaml.safe_load(_read("docker-compose.yml"))
    images = {s["image"] for s in doc["services"].values() if "image" in s}
    minio = [i for i in images if i.startswith("minio/")]
    assert len(minio) == 2, f"expected minio/minio and minio/mc, found: {minio}"
    # MinIO ships versioned RELEASE tags, never 'latest'.
    release_re = re.compile(r"^RELEASE\.\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z$")
    for img in minio:
        repo, tag = img.rsplit(":", 1)
        assert release_re.match(tag), f"{repo} tag is not a RELEASE.* stamp: {tag!r}"


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
