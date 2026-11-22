"""Offline tests for the maintainer scripts. No Docker required."""
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))


def test_benchmark_chapter_map_covers_every_result_file():
    """Every committed result JSON maps to a chapter."""
    from render_results import BENCHMARK_CHAPTER, _benchmark_name

    for path in ROOT.glob("chapter-*/benchmarks/results/*.json"):
        data = json.loads(path.read_text())
        name = _benchmark_name(data, path)
        assert name in BENCHMARK_CHAPTER, f"{name} ({path.name}) is unmapped"


def test_render_is_deterministic(tmp_path):
    """Two renders of the same input produce byte-identical output."""
    from render_results import render_chapter

    first = render_chapter("07")
    second = render_chapter("07")
    assert first == second


def test_rendered_output_has_no_absolute_paths():
    from render_results import render_chapter

    out = render_chapter("07")
    assert "/Users/" not in out
    assert str(ROOT) not in out


def test_every_note_field_is_reproduced():
    """Caveats travel with the numbers they qualify.

    No committed result file carries a note today, so the walk over real
    files below never exercises the note-emitting code paths. The synthetic
    cases first make sure this doesn't stay dead code: they call both
    renderers directly with a note attached and check it survives -- one
    for the combined atomicity table, one for a regular single-file section.
    """
    from render_results import _render_atomicity_section, _render_entry_section, render_chapter

    atomicity_note = "synthetic-only: partial traces under sustained backpressure"
    by_mode = {
        "none": ({"result": {"whole": 1, "absent": 0, "partial": 0}}, "none.json"),
        "buffer-overflow": (
            {"result": {"whole": 0, "absent": 0, "partial": 1}, "note": atomicity_note},
            "buffer-overflow.json",
        ),
    }
    assert atomicity_note in _render_atomicity_section(by_mode)

    generic_note = "synthetic-only: directional only, not a wall-clock claim"
    generic_data = {"benchmark": "store_then_stitch", "num_spans": 1, "note": generic_note}
    assert generic_note in _render_entry_section("store_then_stitch", generic_data, "x.json")

    # Real coverage: starts asserting on actual data automatically once a
    # committed file carries a note. Scoped to the runs the renderer selects,
    # because it renders the newest file per benchmark and a superseded run's
    # note has nowhere to land. Globbing every file instead asserted that no
    # benchmark may ever be measured twice.
    from render_results import load_results

    results = load_results()
    out = "".join(render_chapter(chapter) for chapter in sorted(results))
    for entries in results.values():
        for _name, data, filename in entries:
            note = data.get("note")
            if note:
                assert note in out, f"note from {filename} is missing from the output"


def test_check_mode_passes_on_clean_tree():
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "render_results.py"), "--check"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_duplicate_benchmark_keeps_only_latest_measurement(tmp_path, monkeypatch):
    """Reruns of the same benchmark collapse to a single, most-recent section.

    Builds its own duplicate pair instead of reading committed files: no
    benchmark on this branch has two dated runs, and inventing a second real
    measurement would mean committing a fake one. Pointing the loader at two
    dicts for the same benchmark name with different measured_at_utc values
    tests the tie-break rule itself, so this fails if dedup ever reverts to
    keeping the first entry seen or the smaller timestamp.
    """
    import render_results

    monkeypatch.setattr(render_results, "ROOT", tmp_path)
    results_dir = tmp_path / "chapter-05" / "benchmarks" / "results"
    results_dir.mkdir(parents=True)

    earlier = {
        "benchmark": "store_then_stitch",
        "measured_at_utc": "2020-01-01T00:00:00+00:00",
        "num_spans": 111,
    }
    later = {
        "benchmark": "store_then_stitch",
        "measured_at_utc": "2020-01-02T00:00:00+00:00",
        "num_spans": 222,
    }
    (results_dir / "store_then_stitch-2020-01-01.json").write_text(json.dumps(earlier))
    (results_dir / "store_then_stitch-2020-01-02.json").write_text(json.dumps(later))

    out = render_results.render_chapter("05")
    assert out.count("## Query-time write cost") == 1
    assert "Measured: 2020-01-02" in out
    assert "Measured: 2020-01-01" not in out
    assert "store_then_stitch-2020-01-02.json" in out
    assert "store_then_stitch-2020-01-01.json" not in out
    assert "222" in out
    assert "111" not in out


def test_atomicity_audit_renders_as_one_comparison_table():
    """The four failure-mode runs are one experiment, not four sections."""
    from render_results import render_chapter

    out = render_chapter("05")
    assert out.count("## Atomicity audit") == 1
    for heading in ("## none", "## drop-whole-trace", "## producer-crash", "## buffer-overflow"):
        assert heading not in out

    modes = ["none", "drop-whole-trace", "producer-crash", "buffer-overflow"]
    positions = []
    for mode in modes:
        matches = sorted((ROOT / "chapter-05/benchmarks/results")
                         .glob(f"atomicity_audit-{mode}-*.json"))
        assert len(matches) == 1, f"expected one result file for {mode}"
        data = json.loads(matches[0].read_text())
        result = data["result"]
        row = f"| {mode} | {result['whole']} | {result['absent']} | {result['partial']} |"
        assert row in out, f"row for {mode} missing or numbers don't match the source file"
        positions.append(out.index(row))
    assert positions == sorted(positions), "rows must appear control-first, not alphabetically"


def test_anchor_regex_matches_both_chapter_seven_forms():
    """ANCHOR_RE recognizes both comment forms chapter 7 already established."""
    from check_listing_anchors import ANCHOR_RE

    header = "-- Chapter 7, listing 7.3: per-column compression"
    block = "-- ---- Listing 7.2: ClickHouse hot-to-cold tiering policy"
    assert ANCHOR_RE.search(header).group("num") == "7.3"
    assert ANCHOR_RE.search(block).group("num") == "7.2"


def test_readme_listings_parses_every_chapters_listings_table():
    """readme_listings finds at least one row in every chapter README that
    advertises a Listings heading.

    Checks every chapter with a Listings heading rather than pinning to one
    chapter number, so this keeps testing real content as chapters are
    added to or removed from the tree.
    """
    from check_listing_anchors import readme_listings

    chapters_with_listings = [
        chapter_dir for chapter_dir in sorted(ROOT.glob("chapter-*"))
        if (chapter_dir / "README.md").exists()
        and "## Listings" in (chapter_dir / "README.md").read_text()
    ]
    assert chapters_with_listings, "no chapter README advertises a Listings heading"

    for chapter_dir in chapters_with_listings:
        listings = readme_listings(chapter_dir / "README.md")
        assert listings, f"{chapter_dir.name}/README.md has a Listings heading but no rows parsed"


def test_agent_workload_has_one_copy():
    """The agent and its stand-in model live only in chapter-02/agent/."""
    canonical = ROOT / "chapter-02" / "agent"
    unrelated = {ROOT / "chapter-05" / "app" / "agent.py"}
    strays = [
        path for name in ("agent.py", "stand_in_model.py")
        for path in ROOT.rglob(name)
        if canonical not in path.parents and path not in unrelated and ".git" not in path.parts
    ]
    assert not strays, f"agent workload copied outside chapter-02/agent/: {strays}"
    assert (canonical / "agent.py").exists() and (canonical / "stand_in_model.py").exists()


def test_agent_scripts_are_runnable_and_strict():
    """Each shipped agent script has a shebang, is executable, and fails loudly."""
    scripts = ROOT / "chapter-02" / "scripts"
    for name in ("run-agent.sh", "measure-agent-trace.sh", "capture-on.sh", "capture-off.sh"):
        path = scripts / name
        text = path.read_text()
        assert text.startswith("#!/usr/bin/env bash\n"), f"{name}: no shebang"
        assert path.stat().st_mode & 0o111, f"{name}: not executable"
        assert 'source "$(dirname "$0")/lib.sh"' in text, f"{name}: does not source lib.sh"
        assert subprocess.run(["bash", "-n", str(path)]).returncode == 0, f"{name}: syntax error"
    assert "set -euo pipefail" in (scripts / "lib.sh").read_text()


def test_capture_scripts_round_trip_through_env_file(tmp_path):
    """capture-on then capture-off change only the capture line and survive between calls."""
    import shutil

    chapter = tmp_path / "chapter-02"
    shutil.copytree(ROOT / "chapter-02" / "scripts", chapter / "scripts", ignore=shutil.ignore_patterns("*.py"))
    (chapter / ".env").write_text("KEEP=1\n")
    var = "OTEL_INSTRUMENTATION_GENAI_CAPTURE_MESSAGE_CONTENT"

    def run(name):
        subprocess.run([str(chapter / "scripts" / name)], check=True, capture_output=True)
        return (chapter / ".env").read_text().splitlines()

    assert run("capture-on.sh") == ["KEEP=1", f"{var}=span_only"]
    assert run("capture-on.sh") == ["KEEP=1", f"{var}=span_only"]
    assert run("capture-off.sh") == ["KEEP=1", f"{var}=no_content"]


def test_agent_report_prints_both_settings():
    """report.py joins the off and on measurements into one table from the committed results."""
    import contextlib
    import io
    import shutil
    import tempfile

    sys.path.insert(0, str(ROOT / "chapter-02" / "agent"))
    import report

    results = ROOT / "chapter-02" / "agent" / "results" / "agent-trace-2026-11-18.json"
    data = json.loads(results.read_text())
    with tempfile.TemporaryDirectory() as root:
        for condition in ("no_content", "span_only"):
            cells = [c for c in data["cells"] if c["condition"] == condition and c["prompt_size"] == "8k"]
            target = Path(root) / condition
            target.mkdir()
            (target / "agent-trace-2026-11-18.json").write_text(json.dumps({"cells": cells}))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            report.main(root)
        shutil.rmtree(root, ignore_errors=True)
    text = out.getvalue()
    assert "whole trace              11" in text and "3777" in text and "42810" in text
    assert "not measured yet" not in text


def _run_agent_with_stubs(tmp_path, responses):
    """Run run-agent.sh with stubbed docker and curl. `responses` is the list of
    span counts curl reports per poll; the last one repeats."""
    import os
    import shutil

    chapter = tmp_path / "chapter-02"
    shutil.copytree(ROOT / "chapter-02" / "scripts", chapter / "scripts", ignore=shutil.ignore_patterns("*.py"))
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "docker").write_text(
        '#!/usr/bin/env bash\n'
        '[ "$2" = run ] && echo "run 1/1: trace_id=abc123 size=8k capture=no_content seconds=1.0"\n'
        'exit 0\n'
    )
    state = tmp_path / "polls"
    state.write_text("0")
    (bin_dir / "curl").write_text(
        '#!/usr/bin/env bash\n'
        f'n=$(cat {state}); echo $((n + 1)) > {state}\n'
        f'counts=({" ".join(map(str, responses))})\n'
        'i=$n; [ "$i" -ge "${#counts[@]}" ] && i=$((${#counts[@]} - 1))\n'
        '[ "${counts[$i]}" = F ] && exit 7\n'
        'k=0; while [ "$k" -lt "${counts[$i]}" ]; do echo \'{"spanId":"x"}\'; k=$((k + 1)); done\n'
        'exit 0\n'
    )
    for stub in bin_dir.iterdir():
        stub.chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:/usr/bin:/bin", "POLL_SECONDS": "0", "POLL_TRIES": "20"}
    result = subprocess.run([str(chapter / "scripts" / "run-agent.sh")], capture_output=True, text=True, env=env)
    return result, int(state.read_text())


def test_run_agent_keeps_waiting_through_empty_polls(tmp_path):
    """No spans yet must mean wait and retry, not a silent exit under pipefail."""
    result, polls = _run_agent_with_stubs(tmp_path, [0, 0, 0, 5, 11, 13])
    assert result.returncode == 0, result.stdout + result.stderr
    assert "has 13 spans" in result.stdout
    assert polls > 6


def test_run_agent_does_not_stop_at_a_partial_trace(tmp_path):
    """A count that holds for a few polls and then grows is not complete."""
    result, _ = _run_agent_with_stubs(tmp_path, [11, 11, 11, 13])
    assert result.returncode == 0, result.stdout + result.stderr
    assert "has 13 spans" in result.stdout


def test_run_agent_gives_up_with_a_message_when_trace_never_arrives(tmp_path):
    result, polls = _run_agent_with_stubs(tmp_path, [0])
    assert result.returncode != 0
    assert "did not reach Jaeger" in result.stderr
    assert polls == 20


def test_run_agent_survives_curl_failing_while_jaeger_is_down(tmp_path):
    """curl exit 7 (connection refused) counts as no spans for that poll."""
    result, polls = _run_agent_with_stubs(tmp_path, ["F", "F", "F", 13])
    assert result.returncode == 0, result.stdout + result.stderr
    assert "has 13 spans" in result.stdout
    assert polls >= 8


def test_run_agent_reports_timeout_when_curl_always_fails(tmp_path):
    result, polls = _run_agent_with_stubs(tmp_path, ["F"])
    assert result.returncode != 0
    assert "Is the stack up?" in result.stderr
    assert polls == 20


CH3 = ROOT / "chapter-03"
CH3_AGENT_SCRIPTS = ("send-agent-burst.sh", "send-oversized-agent-run.sh", "cap-batch-spans-at-agent-tier.sh", "trim-prompts-at-agent-tier.sh")


def test_chapter3_agent_scripts_are_runnable_and_strict():
    for name in CH3_AGENT_SCRIPTS:
        path = CH3 / "scripts" / name
        text = path.read_text()
        assert text.startswith("#!/usr/bin/env bash\n"), f"{name}: no shebang"
        assert path.stat().st_mode & 0o111, f"{name}: not executable"
        assert 'source "$(dirname "$0")/agent-lib.sh"' in text, f"{name}: does not source agent-lib.sh"
        assert subprocess.run(["bash", "-n", str(path)]).returncode == 0, f"{name}: syntax error"
    assert "set -euo pipefail" in (CH3 / "scripts" / "agent-lib.sh").read_text()


def test_chapter3_agent_builds_from_the_canonical_copy_behind_a_profile():
    text = (CH3 / "docker-compose.yml").read_text()
    for service in ("stand-in-model", "agent"):
        block = text.split(f"\n  {service}:\n")[1].split("\n\n")[0]
        assert "build: ../chapter-02/agent" in block
        assert 'profiles: ["agent"]' in block


def test_chapter3_demo_configs_differ_from_the_shipped_ones_in_the_one_thing_each_shows():
    shipped_gateway = (CH3 / "collector" / "gateway-config.yaml").read_text()
    demo_gateway = (CH3 / "collector" / "gateway-default-limit-config.yaml").read_text()
    assert "max_recv_msg_size_mib: 16" in shipped_gateway
    keep = lambda text: [l for l in text.splitlines() if not l.startswith("#") and "max_recv" not in l]
    assert not [l for l in demo_gateway.splitlines() if not l.startswith("#") and "max_recv" in l]
    assert keep(shipped_gateway) == keep(demo_gateway)

    plain = (CH3 / "collector" / "agent-genai-config.yaml").read_text()
    trim = (CH3 / "collector" / "agent-genai-trim-config.yaml").read_text()
    assert "compression: none" in plain and "compression: none" in trim
    assert "truncate_all" not in plain
    assert "processors: [memory_limiter, filter/drop_health, batch]" in plain
    assert "processors: [memory_limiter, filter/drop_health, transform/trim_prompts, batch]" in trim
    assert "truncate_all(attributes, 2048) where" in trim

    cap = (CH3 / "collector" / "agent-genai-batchcap-config.yaml").read_text()
    assert "compression: none" in cap and "truncate_all" not in cap
    assert "processors: [memory_limiter, filter/drop_health, batch]" in cap
    assert "send_batch_size: 100\n    send_batch_max_size: 100\n" in cap
    assert "send_batch_max_size" not in plain


def test_chapter3_default_stack_is_selected_by_unset_variables():
    text = (CH3 / "docker-compose.yml").read_text()
    assert "${AGENT_TIER_CONFIG:-agent-config.yaml}" in text
    assert text.count("${GATEWAY_CONFIG:-gateway-config.yaml}") == 2


def _chapter3_metrics(port, phase):
    """Prometheus text a stubbed Collector serves; `phase` is before or after the run."""
    import textwrap

    after = phase == "after"
    if port == "8888":
        return textwrap.dedent(f"""\
            otelcol_processor_outgoing_items{{otel_signal="traces",processor="filter/drop_health"}} {185 if after else 0}
            otelcol_processor_batch_batch_send_size_sum{{processor="batch"}} {185 if after else 0}
            otelcol_processor_batch_batch_send_size_bytes_count{{processor="batch"}} {1 if after else 0}
            otelcol_processor_batch_batch_send_size_bytes_sum{{processor="batch"}} {6.349353e+06 if after else 0}
            otelcol_exporter_send_failed_spans{{error_permanent="true",error_type="ResourceExhausted",exporter="otlp_grpc/gateway"}} {185 if after else 0}
            """)
    return 'otelcol_receiver_accepted_spans{receiver="otlp"} 0\notelcol_receiver_refused_spans{receiver="otlp"} 0\n'


def _run_chapter3_script(tmp_path, name, curl_mode="up", timeout_tries="3", jaeger="ab"):
    """Run a chapter 3 agent script with stubbed docker, curl and sleep. curl_mode is
    up (metrics change after the run), down (connection refused) or stuck (never exports)."""
    import os
    import shutil

    chapter = tmp_path / "chapter-03"
    shutil.copytree(CH3 / "scripts", chapter / "scripts", ignore=shutil.ignore_patterns("*.py"))
    (chapter / "collector").mkdir()
    for config in ("agent-genai-trim-config.yaml", "agent-genai-batchcap-config.yaml"):
        shutil.copy(CH3 / "collector" / config, chapter / "collector")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    phase = tmp_path / "phase"
    phase.write_text("before")
    after_8888 = _chapter3_metrics("8888", "after")
    before_8888 = _chapter3_metrics("8888", "before")
    gateway = _chapter3_metrics("8889", "after")
    (bin_dir / "docker").write_text(
        '#!/usr/bin/env bash\n'
        f'if [ "$2" = run ]; then echo after > {phase}; echo "run 1/1: trace_id=abc123 size=32k capture=span_only seconds=5.0"; fi\n'
        '[ "$1" = logs ] && echo \'ts error queue_sender "error": "not retryable error: Permanent error: rpc error: code = ResourceExhausted desc = grpc: received message larger than max (6349437 vs. 4194304)", "dropped_items": 185}\'\n'
        'exit 0\n'
    )
    (bin_dir / "curl").write_text(
        '#!/usr/bin/env bash\n'
        f'[ "{curl_mode}" = down ] && exit 7\n'
        'case "$*" in\n'
        f'  *api/v3/traces*) for s in $(echo {jaeger} | fold -w1); do echo "{{\\"spanId\\":\\"$s\\"}}"; done; exit 0;;\n'
        '  *:8888/metrics*)\n'
        f'    if [ "{curl_mode}" = stuck ] || [ "$(cat {phase})" = before ]; then cat <<\'M\'\n{before_8888}M\n'
        f'    else cat <<\'M\'\n{after_8888}M\n    fi; exit 0;;\n'
        f'  *:8889/metrics*|*:8890/metrics*) cat <<\'M\'\n{gateway}M\n    exit 0;;\n'
        'esac\nexit 0\n'
    )
    (bin_dir / "sleep").write_text("#!/usr/bin/env bash\nexit 0\n")
    for stub in bin_dir.iterdir():
        stub.chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:/usr/bin:/bin", "EXPORT_TRIES": timeout_tries,
           "POLL_TRIES": "2", "POLL_SECONDS": "0"}
    return subprocess.run([str(chapter / "scripts" / name)], capture_output=True, text=True, env=env)


def test_oversized_run_prints_where_the_refusal_shows():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter3_script(Path(tmp), "send-oversized-agent-run.sh")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Agent tier exporter: sent 0 spans, failed 185 spans" in result.stdout
    assert "Gateway 1 receiver: accepted 0 spans, refused 0 spans" in result.stdout
    assert "received message larger than max (6349437 vs. 4194304)" in result.stdout
    assert '"dropped_items": 185' in result.stdout
    assert "Next: ./scripts/cap-batch-spans-at-agent-tier.sh" in result.stdout


def test_cap_run_names_the_cap_and_points_to_the_trim():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter3_script(Path(tmp), "cap-batch-spans-at-agent-tier.sh")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Cap: send_batch_max_size: 100 spans per request" in result.stdout
    assert "Agent tier batch: 185 spans in 1 request" in result.stdout
    assert "Next: ./scripts/trim-prompts-at-agent-tier.sh" in result.stdout


def test_trim_fails_when_the_trace_never_reaches_jaeger():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter3_script(Path(tmp), "trim-prompts-at-agent-tier.sh", jaeger="")
    assert result.returncode != 0
    assert "Spans of that trace in Jaeger: 0" in result.stdout
    assert "Jaeger has 0 of the 0 spans the agent tier sent" in result.stderr


def test_chapter3_scripts_say_what_to_run_when_the_stack_is_down():
    import tempfile

    for name in CH3_AGENT_SCRIPTS:
        with tempfile.TemporaryDirectory() as tmp:
            result = _run_chapter3_script(Path(tmp), name, curl_mode="down")
        assert result.returncode != 0, name
        assert "The stack is not running. Run: docker compose up --build -d" in result.stderr, name


def test_chapter3_scripts_give_up_with_a_message_when_nothing_is_exported():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter3_script(Path(tmp), "send-agent-burst.sh", curl_mode="stuck")
    assert result.returncode != 0
    assert "did not finish exporting in 3 seconds" in result.stderr


def test_stand_in_delay_scale_defaults_to_one():
    """Chapter 3 shortens the stand-in's sleeps; every other chapter must keep them."""
    import os

    code = "import stand_in_model as m; print(m.DELAY_SCALE)"
    base = {k: v for k, v in os.environ.items() if k != "STAND_IN_DELAY_SCALE"}
    cwd = ROOT / "chapter-02" / "agent"
    default = subprocess.run([sys.executable, "-c", code], cwd=cwd, env=base, capture_output=True, text=True)
    scaled = subprocess.run([sys.executable, "-c", code], cwd=cwd, env={**base, "STAND_IN_DELAY_SCALE": "0.01"},
                            capture_output=True, text=True)
    assert default.stdout.strip() == "1.0", default.stderr
    assert scaled.stdout.strip() == "0.01", scaled.stderr


CH4 = ROOT / "chapter-04"
CH4_AGENT_SCRIPTS = ("send-agent-to-kafka.sh", "raise-kafka-limits.sh")


def test_chapter4_agent_scripts_are_runnable_and_strict():
    for name in CH4_AGENT_SCRIPTS:
        path = CH4 / "scripts" / name
        text = path.read_text()
        assert text.startswith("#!/usr/bin/env bash\n"), f"{name}: no shebang"
        assert path.stat().st_mode & 0o111, f"{name}: not executable"
        assert 'source "$(dirname "$0")/agent-lib.sh"' in text, f"{name}: does not source agent-lib.sh"
        assert subprocess.run(["bash", "-n", str(path)]).returncode == 0, f"{name}: syntax error"
    assert "set -euo pipefail" in (CH4 / "scripts" / "agent-lib.sh").read_text()


def test_chapter4_agent_builds_from_the_canonical_copy_behind_a_profile():
    text = (CH4 / "docker-compose.yml").read_text()
    for service in ("stand-in-model", "agent"):
        block = text.split(f"\n  {service}:\n")[1].split("\n\n")[0]
        assert "build: ../chapter-02/agent" in block
        assert 'profiles: ["agent"]' in block
    assert "${GATEWAY_CONFIG:-gateway-config.yaml}" in text


def test_chapter4_demo_gateways_differ_from_the_shipped_one_in_what_each_shows():
    shipped = (CH4 / "collector" / "gateway-config.yaml").read_text()
    plain = (CH4 / "collector" / "gateway-agent-config.yaml").read_text()
    raised = (CH4 / "collector" / "gateway-agent-raised-config.yaml").read_text()
    assert "max_message_bytes: 1000000" in shipped and "partition_traces_by_id: true" in shipped
    assert "max_message_bytes: 1000000" in plain and "partition_traces_by_id: true" in plain
    assert "max_message_bytes: 2000000" in raised and "partition_traces_by_id: true" in raised

    def body(text, drop=()):
        return [l for l in text.splitlines() if not l.startswith("#") and l.strip() and not any(d in l for d in drop)]

    assert "timeout: 30s" in plain and "timeout: 30s" in raised
    assert "processors: [memory_limiter, filter/drop_health, batch]" in plain
    assert body(plain, ("max_message_bytes",)) == body(raised, ("max_message_bytes",))


def _chapter4_gateway_metrics(phase, failing):
    after = phase == "after"
    taken = 53 if after else 0
    sent = (2 if failing else 53) if after else 0
    failed = (51 if failing else 0) if after else 0
    return (
        f'otelcol_processor_outgoing_items{{otel_signal="traces",processor="filter/drop_health"}} {taken}\n'
        f'otelcol_exporter_sent_spans{{exporter="kafka"}} {sent}\n'
        f'otelcol_exporter_send_failed_spans{{error_permanent="true",error_type="_OTHER",exporter="kafka"}} {failed}\n'
        'otelcol_receiver_refused_spans{receiver="otlp",transport="grpc"} 0\n'
    )


def _run_chapter4_script(tmp_path, name, mode="up", failing=True, exec_fails=False, logs_fail=False):
    """Run a chapter 4 agent script with stubbed docker, curl and sleep. mode is up (metrics
    change after the run), down (connection refused), jaeger_down (only the trace lookup
    fails), stuck (the exporter never reports) or dies (the metrics answer once, for the stack
    check, then the Collector stops answering)."""
    import os
    import shutil

    chapter = tmp_path / "chapter-04"
    shutil.copytree(CH4 / "scripts", chapter / "scripts")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    phase = tmp_path / "phase"
    phase.write_text("before")
    before = _chapter4_gateway_metrics("before", failing)
    after = _chapter4_gateway_metrics("after", failing)
    log_line = "ts error kafkaexporter record size 1037924 exceeds max 1000000: MESSAGE_TOO_LARGE: x"
    (bin_dir / "docker").write_text(
        '#!/usr/bin/env bash\n'
        f'if [ "$2" = run ]; then echo after > {phase}; echo "run 1/1: trace_id=abc123 size=32k capture=span_only seconds=1.4"; fi\n'
        f'[ "$2" = exec ] && [ "{int(exec_fails)}" = 1 ] && exit 1\n'
        '[ "$2" = exec ] && echo "Completed updating config for topic otlp_spans."\n'
        f'if [ "$2" = logs ]; then [ "{int(logs_fail)}" = 1 ] && exit 1; [ "{int(failing)}" = 1 ] && echo "{log_line}"; fi\n'
        'exit 0\n'
    )
    (bin_dir / "curl").write_text(
        '#!/usr/bin/env bash\n'
        f'[ "{mode}" = down ] && exit 7\n'
        'case "$*" in\n'
        f'  *api/v3/traces*) [ "{mode}" = jaeger_down ] && exit 7\n'
        f'    if [ "{int(failing)}" = 1 ]; then echo \'{{"spanId":"a"}}\'; else echo \'{{"spanId":"a"}}\'; echo \'{{"spanId":"b"}}\'; fi; exit 0;;\n'
        '  *:8889/metrics*)\n'
        f'    if [ "{mode}" = dies ]; then n=$(cat {tmp_path}/calls 2>/dev/null || echo 0); echo $((n + 1)) > {tmp_path}/calls; [ "$n" -ge 2 ] && exit 7; fi\n'
        f'    if [ "{mode}" = stuck ] || [ "$(cat {phase})" = before ]; then cat <<\'M\'\n{before}M\n'
        f'    else cat <<\'M\'\n{after}M\n    fi; exit 0;;\n'
        'esac\nexit 0\n'
    )
    (bin_dir / "sleep").write_text("#!/usr/bin/env bash\nexit 0\n")
    for stub in bin_dir.iterdir():
        stub.chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:/usr/bin:/bin", "EXPORT_TRIES": "3"}
    return subprocess.run([str(chapter / "scripts" / name)], capture_output=True, text=True, env=env)


def test_chapter4_failure_script_prints_the_log_line_and_the_counters():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter4_script(Path(tmp), "send-agent-to-kafka.sh")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Gateway: took in 53 spans, receiver refused 0 spans" in result.stdout
    assert "Kafka exporter: sent 2 spans, failed 51 spans" in result.stdout
    assert "record size 1037924 exceeds max 1000000: MESSAGE_TOO_LARGE" in result.stdout
    assert "Spans of that trace in Jaeger: 1" in result.stdout
    assert "Next: ./scripts/raise-kafka-limits.sh" in result.stdout


def test_chapter4_fix_script_shows_the_record_landing():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter4_script(Path(tmp), "raise-kafka-limits.sh", failing=False)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Completed updating config for topic otlp_spans." in result.stdout
    assert "Kafka exporter: sent 53 spans, failed 0 spans" in result.stdout
    assert "MESSAGE_TOO_LARGE lines in the gateway log: 0" in result.stdout
    assert "Spans of that trace in Jaeger: 2" in result.stdout


def test_chapter4_scripts_say_what_to_run_when_the_stack_is_down():
    import tempfile

    for name in CH4_AGENT_SCRIPTS:
        with tempfile.TemporaryDirectory() as tmp:
            result = _run_chapter4_script(Path(tmp), name, mode="down")
        assert result.returncode != 0, name
        assert "The stack is not running. Run: docker compose up -d" in result.stderr, name


def test_chapter4_scripts_give_up_with_a_message_when_nothing_is_exported():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter4_script(Path(tmp), "send-agent-to-kafka.sh", mode="stuck")
    assert result.returncode != 0
    assert "did not finish exporting in 3 seconds" in result.stderr


def test_chapter4_scripts_survive_a_jaeger_that_does_not_answer():
    import tempfile

    for name, failing in (("send-agent-to-kafka.sh", True), ("raise-kafka-limits.sh", False)):
        with tempfile.TemporaryDirectory() as tmp:
            result = _run_chapter4_script(Path(tmp), name, mode="jaeger_down", failing=failing)
        assert result.returncode == 0, name + result.stdout + result.stderr
        assert "Spans of that trace in Jaeger: 0" in result.stdout, name


def test_chapter4_scripts_survive_an_empty_gateway_log():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter4_script(Path(tmp), "send-agent-to-kafka.sh", logs_fail=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "MESSAGE_TOO_LARGE" not in result.stdout

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter4_script(Path(tmp), "raise-kafka-limits.sh", failing=False, logs_fail=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "MESSAGE_TOO_LARGE lines in the gateway log: 0" in result.stdout


def test_chapter4_fix_script_says_so_when_the_topic_cannot_be_altered():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter4_script(Path(tmp), "raise-kafka-limits.sh", exec_fails=True)
    assert result.returncode != 0
    assert "Could not raise the topic limit" in result.stderr


def test_chapter4_scripts_report_a_collector_that_stops_answering_mid_run():
    import tempfile

    for name in CH4_AGENT_SCRIPTS:
        with tempfile.TemporaryDirectory() as tmp:
            result = _run_chapter4_script(Path(tmp), name, mode="dies")
        assert result.returncode != 0, name
        assert "did not finish exporting" in result.stderr or "did not come back" in result.stderr, (
            name + ": silent exit " + result.stdout + result.stderr
        )


CH5 = ROOT / "chapter-05"
CH5_TRACE = "d" * 32
CH5_FAKE_QUERY = '''#!/usr/bin/env python3
import os, runpy, sys
cmd = sys.argv[1]
if cmd == "flink-state":
    print(os.environ.get("FAKE_FLINK", "RUNNING"))
elif cmd == "jaeger-sources":
    print("query-time " + os.environ.get("FAKE_QUERY_TIME", "3"))
    print("stream-time " + os.environ.get("FAKE_STREAM_TIME", "2"))
else:
    sys.argv[0] = os.path.join(os.path.dirname(__file__), "query_real.py")
    runpy.run_path(sys.argv[0], run_name="__main__")
'''


def _ch5_timeline(assembled=2, late=1, spans=3):
    def span(i):
        return {"name": f"span-{i}", "start_ms": 1000 * i, "end_ms": 1000 * i + 500}

    return json.dumps({
        "otlp_spans": [{"kafka_ms": 1000 * (i + 1), "spans": [span(i)]} for i in range(spans)],
        "traces.assembled": [{"kafka_ms": 11000, "spans": [span(i) for i in range(assembled)]}] if assembled else [],
        "spans.late": [{"kafka_ms": 12000, "spans": [span(i) for i in range(assembled, assembled + late)]}] if late else [],
    })


def _run_chapter5_long_run(tmp_path, wait="10000", up=True, build_ok=True, run="line", timeline=None,
                           clickhouse="3", flink="RUNNING", query_time="3", stream_time="2", prior_run=True):
    """Run send-long-agent-run.sh with stubbed docker, curl, sleep and date and a stand-in for the
    two query.py commands that need a stack. run is line, silent or fails; wait is what the Flink
    task manager reports for DECISION_WAIT_MS ('' means it printed nothing)."""
    import os
    import shutil

    chapter = tmp_path / "chapter-05"
    shutil.copytree(CH5 / "scripts", chapter / "scripts", ignore=shutil.ignore_patterns("__pycache__"))
    (chapter / "scripts" / "query_real.py").write_text((CH5 / "scripts" / "query.py").read_text())
    (chapter / "scripts" / "query.py").write_text(CH5_FAKE_QUERY)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    clock = tmp_path / "clock"
    clock.write_text("0")
    (bin_dir / "date").write_text(f'#!/usr/bin/env bash\necho $((1000000 + $(cat {clock})))\n')
    (bin_dir / "sleep").write_text(f'#!/usr/bin/env bash\necho $(($(cat {clock}) + ${{1:-1}})) > {clock}\n')
    (tmp_path / "timeline.json").write_text(_ch5_timeline() if timeline is None else timeline)
    run_lines = {
        "line": f"echo run 1/1: trace_id={CH5_TRACE} size=8k capture=no_content seconds=92.4; exit 0",
        "silent": "echo nothing useful; exit 0",
        "fails": "echo compose exploded; exit 1",
    }[run]
    (bin_dir / "docker").write_text(
        '#!/usr/bin/env bash\n'
        'case "$*" in\n'
        f'  *printenv*) printf "%s" "{wait}";;\n'
        f'  *" build "*) exit {0 if build_ok else 1};;\n'
        f'  *" run --rm"*) {run_lines};;\n'
        f'  *consumer-clickhouse*) cat {tmp_path}/timeline.json;;\n'
        f'  *clickhouse-client*) echo {clickhouse};;\n'
        '  *"ps -q jaeger"*) echo jaeger-id;;\n'
        '  inspect*) echo jaeger-id@started;;\n'
        'esac\nexit 0\n'
    )
    (bin_dir / "curl").write_text(f'#!/usr/bin/env bash\n{"exit 0" if up else "exit 7"}\n')
    for stub in bin_dir.iterdir():
        stub.chmod(0o755)
    if prior_run:
        state_dir = tmp_path / "tracing-in-action-ch05"
        state_dir.mkdir()
        (state_dir / "traffic").write_text(
            f"STACK_ID=jaeger-id@started\nCOUNT=120\nSTARTED=1\nREADY=1\nLAST_TRACE={CH5_TRACE}\n")
    env = {**os.environ, "PATH": f"{bin_dir}:/usr/bin:/bin", "TMPDIR": str(tmp_path),
           "FAKE_FLINK": flink, "FAKE_QUERY_TIME": query_time, "FAKE_STREAM_TIME": stream_time}
    return subprocess.run([str(chapter / "scripts" / "send-long-agent-run.sh")], capture_output=True,
                          text=True, env=env, timeout=60)


def test_chapter5_long_run_script_is_runnable_and_strict():
    path = CH5 / "scripts" / "send-long-agent-run.sh"
    text = path.read_text()
    assert text.startswith("#!/usr/bin/env bash\n")
    assert path.stat().st_mode & 0o111
    assert 'source "$(dirname "$0")/lib.sh"' in text
    assert subprocess.run(["bash", "-n", str(path)]).returncode == 0


def test_chapter5_agent_builds_from_the_canonical_copy_behind_a_profile():
    text = (CH5 / "docker-compose.yml").read_text()
    for service in ("stand-in-model", "agent"):
        block = text.split(f"\n  {service}:\n")[1].split("\n\n")[0]
        assert "build: ../chapter-02/agent" in block
        assert 'profiles: ["agent"]' in block
    assert "OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-agent:4317" in text.split("\n  agent:\n")[1]


def test_chapter5_long_run_prints_the_wait_the_arrivals_and_where_each_store_stands(tmp_path):
    result = _run_chapter5_long_run(tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Flink assembly window              10000 ms (DECISION_WAIT_MS)" in result.stdout
    assert "This takes about 90 seconds" in result.stdout
    assert "span-0" in result.stdout and "traces.assembled" in result.stdout and "spans.late" in result.stdout
    assert "the assembled trace holds 2 spans, stamped +11.0s" in result.stdout
    assert "spans.late holds 1 spans of this trace" in result.stdout
    assert "Jaeger, assembly.source=stream-time" in result.stdout


def test_chapter5_long_run_prints_a_wait_it_was_given(tmp_path):
    result = _run_chapter5_long_run(tmp_path, wait="30000")
    assert "30000 ms (DECISION_WAIT_MS)" in result.stdout


def test_chapter5_long_run_says_what_to_run_when_the_stack_is_down(tmp_path):
    result = _run_chapter5_long_run(tmp_path, up=False, prior_run=False)
    assert result.returncode != 0
    assert "docker compose up -d --build" in result.stderr


def test_chapter5_long_run_asks_for_traffic_first(tmp_path):
    result = _run_chapter5_long_run(tmp_path, prior_run=False)
    assert result.returncode != 0
    assert "./scripts/send-traffic.sh" in result.stderr


def test_chapter5_long_run_stops_with_a_message_on_each_failure(tmp_path):
    for kwargs, expected in (
        ({"wait": ""}, "could not read DECISION_WAIT_MS"),
        ({"build_ok": False}, "could not build the agent image"),
        ({"run": "fails"}, "the AI agent run failed"),
        ({"run": "silent"}, "the AI agent printed no trace ID"),
    ):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            result = _run_chapter5_long_run(Path(tmp), **kwargs)
        assert result.returncode != 0, kwargs
        assert expected in result.stderr, (kwargs, result.stderr)


def test_chapter5_long_run_gives_up_when_flink_never_accounts_for_every_span(tmp_path):
    result = _run_chapter5_long_run(tmp_path, timeline=_ch5_timeline(assembled=1, late=0, spans=3))
    assert result.returncode != 0
    assert "timed out after 180s waiting for Flink to assemble the trace" in result.stderr


def test_chapter5_long_run_gives_up_on_an_empty_or_failed_timeline_read(tmp_path):
    for timeline in ("", "not json"):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            result = _run_chapter5_long_run(Path(tmp), timeline=timeline)
        assert result.returncode != 0, repr(timeline)
        assert "timed out after 180s" in result.stderr, repr(timeline)


def test_chapter5_long_run_gives_up_when_the_stores_never_catch_up(tmp_path):
    result = _run_chapter5_long_run(tmp_path, clickhouse="0")
    assert result.returncode != 0
    assert "waiting for ClickHouse and Jaeger to hold the trace" in result.stderr


def test_chapter5_long_run_never_prints_a_zero_for_a_dead_clickhouse(tmp_path):
    result = _run_chapter5_long_run(tmp_path, clickhouse="")
    assert result.returncode != 0
    assert "where" not in result.stdout


def test_chapter5_timeline_marks_where_each_span_went():
    import contextlib
    import importlib.util
    import io

    spec = importlib.util.spec_from_file_location("ch5_query", CH5 / "scripts" / "query.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    out = io.StringIO()
    sys_stdin = sys.stdin
    sys.stdin = io.StringIO(_ch5_timeline())
    try:
        with contextlib.redirect_stdout(out):
            mod.cmd_timeline()
    finally:
        sys.stdin = sys_stdin
    lines = out.getvalue().splitlines()
    assert lines[0].split() == ["arrived", "span", "started", "ended", "went", "to"]
    assert lines[1].split() == ["+1.0s", "span-0", "+0.0s", "+0.5s", "traces.assembled"]
    assert lines[2].split() == ["+2.0s", "span-1", "+1.0s", "+1.5s", "traces.assembled"]
    assert lines[3].split() == ["+3.0s", "span-2", "+2.0s", "+2.5s", "spans.late"]


CH7 = ROOT / "chapter-07"
CH7_AGENT_SCRIPTS = ("send-agent-to-tempo.sh", "read-agent-prompt-back.sh", "capture-to-file.sh")
CH7_REF = "file:///uploads/0a1b2c3d-0000-4000-8000-000000000001_inputs.json"


def test_chapter7_agent_scripts_are_runnable_and_strict():
    for name in CH7_AGENT_SCRIPTS:
        path = CH7 / "scripts" / name
        text = path.read_text()
        assert text.startswith("#!/usr/bin/env bash\n"), f"{name}: no shebang"
        assert path.stat().st_mode & 0o111, f"{name}: not executable"
        assert 'source "$(dirname "$0")/agent-lib.sh"' in text, f"{name}: does not source agent-lib.sh"
        assert subprocess.run(["bash", "-n", str(path)]).returncode == 0, f"{name}: syntax error"
    assert "set -euo pipefail" in (CH7 / "scripts" / "lib.sh").read_text()
    assert "source \"$(dirname \"${BASH_SOURCE[0]}\")/lib.sh\"" in (CH7 / "scripts" / "agent-lib.sh").read_text()


def test_chapter7_agent_builds_from_the_canonical_copy_behind_a_profile():
    text = (CH7 / "docker-compose.yml").read_text()
    for service in ("stand-in-model", "agent"):
        block = text.split(f"\n  {service}:\n")[1].split("\n\n")[0]
        assert "build: ../chapter-02/agent" in block
        assert 'profiles: ["agent"]' in block
    agent = text.split("\n  agent:\n")[1]
    assert "OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317" in agent
    assert "agent-uploads:/uploads" in agent


def _tempo_trace_json(with_prompt, with_ref):
    import base64

    def span_id(n):
        return base64.b64encode(bytes([n] * 8)).decode()

    def attr(key, value):
        return {"key": key, "value": {"stringValue": value}}

    spans = []
    for n, start in ((1, 1000), (2, 2000)):
        attrs = []
        if with_prompt:
            attrs.append(attr("gen_ai.input.messages", "x" * 2048))
        if with_ref:
            attrs.append(attr("gen_ai.input.messages_ref", CH7_REF if n == 1 else CH7_REF.replace("0001", "0002")))
        spans.append({"spanId": span_id(n), "startTimeUnixNano": str(start), "attributes": attrs})
    spans.append({"spanId": span_id(3), "startTimeUnixNano": "3000", "attributes": []})
    return json.dumps({"batches": [{"scopeSpans": [{"spans": spans}]}]})


def _run_chapter7_script(tmp_path, name, mode="up", with_prompt=True, with_ref=False, tempo_json=None,
                         logs_fail=False, file_fails=False, ch_fails=False, spans_in_ch="3", prior_run=False):
    """Run a chapter 7 agent script with stubbed docker, curl and sleep. mode is up, down
    (connection refused) or tempo_empty (Tempo answers everything but trace lookups)."""
    import os
    import shutil

    chapter = tmp_path / "chapter-07"
    shutil.copytree(CH7 / "scripts", chapter / "scripts")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    trace_file = tmp_path / "trace.json"
    trace_file.write_text(tempo_json if tempo_json is not None else _tempo_trace_json(with_prompt, with_ref))
    log_line = ('tempo-1  | level=warn ts=2026-11-19T04:12:10Z caller=rate_limited_logger.go:38 msg="attributes truncated" '
                'tenant=single-tenant total_truncated=4 max_size_bytes=2048 example_scope=span '
                'example_name=gen_ai.input.messages example_field=value example_orig_size=8390')
    (bin_dir / "docker").write_text(
        '#!/usr/bin/env bash\n'
        'case "$*" in\n'
        '  *"compose ps -q"*) echo abc123;;\n'
        '  *"compose exec"*)\n'
        f'    [ "{int(ch_fails)}" = 1 ] && exit 1\n'
        '    case "$*" in\n'
        "      *mapContains*) printf 'C\\t0101010101010101\\t1000\\t8390\\nC\\t0202020202020202\\t2000\\t8818\\n';;\n"
        f'      *"SELECT count()"*) echo {spans_in_ch};;\n'
        '      *) echo 1;;\n'
        '    esac;;\n'
        '  *"compose run"*)\n'
        '    case "$*" in\n'
        f'      *"sh -c"*) [ "{int(file_fails)}" = 1 ] && exit 1; printf \'8391\\n{{"content":"Order ORD-1042"}}\\n\';;\n'
        '      *) echo "run 1/1: trace_id=deadbeef01 size=8k capture=span_only seconds=0.3";;\n'
        '    esac;;\n'
        f'  *"compose logs"*) [ "{int(logs_fail)}" = 1 ] && exit 1; echo \'{log_line}\' | sed "s/^tempo-1  | //";;\n'
        'esac\nexit 0\n'
    )
    collector = (
        'otelcol_receiver_refused_spans{receiver="otlp",transport="grpc"} 0\n'
        'otelcol_exporter_sent_spans{exporter="otlp_grpc/tempo"} 3\n'
    )
    tempo = (
        'tempo_receiver_refused_spans{receiver="otlp/otlp_receiver",transport="grpc"} 0\n'
        'tempo_distributor_attributes_truncated_total{scope="span",tenant="single-tenant"} 4\n'
        'tempo_distributor_attributes_truncated_total{scope="event",tenant="single-tenant"} 0\n'
    )
    (bin_dir / "curl").write_text(
        '#!/usr/bin/env bash\n'
        f'[ "{mode}" = down ] && exit 7\n'
        'case "$*" in\n'
        f'  *api/traces/*) [ "{mode}" = tempo_empty ] && exit 7; cat {trace_file}; exit 0;;\n'
        f'  *:8888/metrics*) cat <<\'M\'\n{collector}M\n    exit 0;;\n'
        f'  *:3200/metrics*) cat <<\'M\'\n{tempo}M\n    exit 0;;\n'
        'esac\nexit 0\n'
    )
    (bin_dir / "sleep").write_text("#!/usr/bin/env bash\nexit 0\n")
    for stub in bin_dir.iterdir():
        stub.chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:/usr/bin:/bin", "TMPDIR": str(tmp_path), "POLL_TRIES": "5",
           "POLL_SECONDS": "0", "READ_BUDGET": "2"}
    if prior_run:
        state_dir = tmp_path / "tracing-in-action-ch07"
        state_dir.mkdir()
        (state_dir / "agent").write_text("STACK_ID=abc123\nTRACE_ID=deadbeef01\n")
    return subprocess.run([str(chapter / "scripts" / name)], capture_output=True, text=True, env=env)


def test_chapter7_send_script_reports_the_trace_in_tempo():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter7_script(Path(tmp), "send-agent-to-tempo.sh")
        assert (Path(tmp) / "tracing-in-action-ch07" / "agent").read_text() == "STACK_ID=abc123\nTRACE_ID=deadbeef01\n"
    assert result.returncode == 0, result.stdout + result.stderr
    assert "run 1/1: trace_id=deadbeef01 size=8k capture=span_only" in result.stdout
    assert "Spans of that trace in Tempo: 3" in result.stdout
    assert "Next: ./scripts/read-agent-prompt-back.sh" in result.stdout


def test_chapter7_read_back_shows_the_push_succeeded_and_the_value_came_back_cut():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter7_script(Path(tmp), "read-agent-prompt-back.sh", prior_run=True)
    assert result.returncode == 0, result.stdout + result.stderr
    out = result.stdout
    assert "Collector: refused 0 spans, failed to send 0 spans to Tempo" in out
    assert "Tempo: refused 0 spans, discarded 0 spans" in out
    assert "attribute values cut: 4" in out
    assert "max_size_bytes=2048 example_scope=span example_name=gen_ai.input.messages" in out
    rows = [line.split() for line in out.splitlines() if line.strip()[:1] in ("1", "2")]
    assert rows == [["1", "8390", "2048"], ["2", "8818", "2048"]]


def test_chapter7_read_back_needs_a_run_first():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter7_script(Path(tmp), "read-agent-prompt-back.sh")
    assert result.returncode != 0
    assert "no agent run yet: run ./scripts/send-agent-to-tempo.sh first" in result.stderr


def test_chapter7_read_back_survives_an_empty_tempo_log_and_gives_up_when_the_stores_disagree():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter7_script(Path(tmp), "read-agent-prompt-back.sh", prior_run=True, logs_fail=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "log:" not in result.stdout

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter7_script(Path(tmp), "read-agent-prompt-back.sh", prior_run=True, spans_in_ch="0")
    assert result.returncode != 0
    assert "timed out after 2s waiting for the trace in ClickHouse and Tempo" in result.stderr


def test_chapter7_capture_script_prints_one_reference_and_the_file():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter7_script(Path(tmp), "capture-to-file.sh", with_prompt=False, with_ref=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert f"gen_ai.input.messages_ref = {CH7_REF}" in result.stdout
    assert "Spans with gen_ai.input.messages in Tempo: 0" in result.stdout
    assert "  8391" in result.stdout
    assert 'Order ORD-1042' in result.stdout


def test_chapter7_capture_script_refuses_a_reference_outside_uploads():
    import tempfile

    bad = _tempo_trace_json(False, True).replace(CH7_REF, "file:///etc/passwd")
    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter7_script(Path(tmp), "capture-to-file.sh", tempo_json=bad)
    assert result.returncode != 0
    assert "Unexpected reference file:///etc/passwd" in result.stderr


def test_chapter7_capture_script_says_so_when_there_is_no_reference_or_no_file():
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter7_script(Path(tmp), "capture-to-file.sh", with_prompt=True, with_ref=False)
    assert result.returncode != 0
    assert "has no gen_ai.input.messages_ref" in result.stderr

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter7_script(Path(tmp), "capture-to-file.sh", with_prompt=False, with_ref=True, file_fails=True)
    assert result.returncode != 0
    assert "Could not read /uploads/" in result.stderr


def test_chapter7_scripts_say_what_to_run_when_the_stack_is_down():
    import tempfile

    for name in CH7_AGENT_SCRIPTS:
        with tempfile.TemporaryDirectory() as tmp:
            result = _run_chapter7_script(Path(tmp), name, mode="down", prior_run=True)
        assert result.returncode != 0, name
        assert "The stack is not running. From chapter-07/ run: docker compose up -d --build --wait" in result.stderr, name


def test_chapter7_scripts_survive_a_tempo_that_does_not_return_the_trace():
    import tempfile

    for name in ("send-agent-to-tempo.sh", "capture-to-file.sh"):
        with tempfile.TemporaryDirectory() as tmp:
            result = _run_chapter7_script(Path(tmp), name, mode="tempo_empty")
        assert result.returncode != 0, name
        assert "Tempo has none of the trace" in result.stderr, name

    with tempfile.TemporaryDirectory() as tmp:
        result = _run_chapter7_script(Path(tmp), "send-agent-to-tempo.sh", tempo_json="not json")
    assert result.returncode != 0
    assert "Tempo has none of the trace" in result.stderr


def test_chapter7_scripts_survive_a_clickhouse_that_stops_answering():
    import tempfile

    for name in CH7_AGENT_SCRIPTS:
        with tempfile.TemporaryDirectory() as tmp:
            result = _run_chapter7_script(Path(tmp), name, ch_fails=True, prior_run=True)
        assert result.returncode != 0, name
        assert "ClickHouse is not answering" in result.stderr, name
