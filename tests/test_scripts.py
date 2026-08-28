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
    assert out.count("## Store-then-stitch write cost") == 1
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
        data = json.loads((ROOT / "chapter-05/benchmarks/results"
                            f"/atomicity_audit-{mode}-2026-06-17.json").read_text())
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
