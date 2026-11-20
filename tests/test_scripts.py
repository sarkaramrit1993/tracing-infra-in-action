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
