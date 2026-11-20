"""Offline tests for scripts/check_versions.py. No Docker required."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from check_versions import check, collect_pins, load_registry  # noqa: E402

REGISTRY = {
    "images": {
        "apache/kafka": {"version": "4.3.1"},
        "python": {"version": "3.12-slim"},
    },
    "packages": {
        "flask": {"version": "3.1.0"},
        "setuptools": {"range": ">=68.0,<80", "range_reason": "build tool"},
    },
    "local_images": {"tracing-in-action-agent:ch02": "built by chapter 2"},
    "open_drift": [],
}


def _tree(tmp_path, compose="image: apache/kafka:4.3.1\n", dockerfile="FROM python:3.12-slim\n",
          requirements="flask==3.1.0\nsetuptools>=68.0,<80\n"):
    ch = tmp_path / "chapter-04"
    (ch / "app").mkdir(parents=True)
    (ch / "docker-compose.yml").write_text(
        "services:\n  kafka:\n    " + compose + "  agent:\n    image: tracing-in-action-agent:ch02\n")
    (ch / "app" / "Dockerfile").write_text(dockerfile)
    (ch / "app" / "requirements.txt").write_text(requirements)
    return tmp_path


def _errors(root, registry=REGISTRY):
    return check(collect_pins(root), registry)[0]


def test_clean_tree_passes(tmp_path):
    assert _errors(_tree(tmp_path)) == []


def test_planted_mismatch_fails(tmp_path):
    errors = _errors(_tree(tmp_path, compose="image: apache/kafka:4.3.0\n"))
    assert len(errors) == 1 and "4.3.0" in errors[0] and "4.3.1" in errors[0]


def test_unregistered_pin_fails(tmp_path):
    errors = _errors(_tree(tmp_path, requirements="flask==3.1.0\nsetuptools>=68.0,<80\nrequests==2.32.3\n"))
    assert len(errors) == 1 and "requests" in errors[0] and "not in versions.yaml" in errors[0]


def test_unpinned_image_and_package_fail(tmp_path):
    root = _tree(tmp_path, compose="image: apache/kafka:latest\n", requirements="flask\nsetuptools>=68.0,<80\n")
    errors = _errors(root)
    assert len(errors) == 2 and all("unpinned" in e for e in errors)


def test_exception_needs_its_chapter(tmp_path):
    registry = {**REGISTRY, "images": {**REGISTRY["images"], "apache/kafka": {
        "version": "4.3.1",
        "exceptions": [{"chapter": "chapter-04", "version": "4.3.0", "reason": "Table 4.2"}]}}}
    assert _errors(_tree(tmp_path, compose="image: apache/kafka:4.3.0\n"), registry) == []


def test_open_drift_is_reported_not_failed_and_stale_drift_fails(tmp_path):
    drift = [{"component": "apache/kafka", "chapter": "chapter-04", "version": "4.3.0"}]
    registry = {**REGISTRY, "open_drift": drift}
    errors, open_drift = check(collect_pins(_tree(tmp_path, compose="image: apache/kafka:4.3.0\n")),
                               registry)
    assert errors == [] and len(open_drift) == 1
    errors, _ = check(collect_pins(_tree(tmp_path / "fixed")), registry)
    assert len(errors) == 1 and "stale open_drift" in errors[0]


def test_registry_entry_nobody_uses_fails(tmp_path):
    registry = {**REGISTRY, "packages": {**REGISTRY["packages"], "zstandard": {"version": "0.23.0"}}}
    errors = _errors(_tree(tmp_path), registry)
    assert len(errors) == 1 and "zstandard" in errors[0]


def test_build_stage_reference_is_not_a_pin(tmp_path):
    root = _tree(tmp_path, dockerfile="FROM python:3.12-slim AS base\nFROM base\n")
    assert _errors(root) == []


def test_companion_tree_agrees_with_versions_yaml():
    errors, _ = check(collect_pins(ROOT), load_registry(ROOT / "versions.yaml"))
    assert errors == [], "\n".join(errors)


def test_pip_install_in_a_dockerfile_is_checked(tmp_path):
    dockerfile = ("FROM python:3.12-slim\n"
                  "RUN apt-get update && \\\n    pip3 install --break-system-packages --no-cache-dir \\\n"
                  "        flask==3.0.0 \\\n        'setuptools>=68.0,<80' \\\n        -r requirements.txt\n")
    errors = _errors(_tree(tmp_path, dockerfile=dockerfile))
    assert len(errors) == 1 and "app/Dockerfile:2" in errors[0] and "flask==3.0.0" in errors[0]
