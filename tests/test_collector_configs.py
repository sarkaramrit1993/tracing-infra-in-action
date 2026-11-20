"""Runs `otelcol validate` from the pinned Collector image over every chapter 2 and 3
Collector config, the Kubernetes ConfigMaps included. Needs Docker."""
import re
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
CH2 = ROOT / "chapter-02"
CH3 = ROOT / "chapter-03"


def _image():
    tags = set(re.findall(r"otel/opentelemetry-collector-contrib:[\d.]+", (CH3 / "docker-compose.yml").read_text()))
    tags |= set(re.findall(r"otel/opentelemetry-collector-contrib:[\d.]+", (CH2 / "docker-compose.yml").read_text()))
    assert len(tags) == 1, f"chapters 2 and 3 pin different Collector images: {tags}"
    return tags.pop()


IMAGE = _image()


def _validate(directory, name, env=()):
    command = ["docker", "run", "--rm", "-v", f"{directory}:/cfg:ro"]
    for pair in env:
        command += ["-e", pair]
    command += [IMAGE, "validate", f"--config=/cfg/{name}"]
    return subprocess.run(command, capture_output=True, text=True, timeout=120)


def _write(directory, name, text):
    # The Collector image runs as a non-root user and pytest makes tmp_path 0700 on Linux.
    directory.chmod(0o755)
    path = directory / name
    path.write_text(text)
    path.chmod(0o644)


FILE_CONFIGS = sorted((CH2 / "collector").glob("*.yaml")) + sorted((CH3 / "collector").glob("*.yaml"))


@pytest.mark.parametrize("path", FILE_CONFIGS, ids=lambda p: f"{p.parent.parent.name}/{p.name}")
def test_collector_config_validates(path):
    result = _validate(path.parent, path.name)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("name", ["agent-configmap.yaml", "gateway-configmap.yaml", "consumer-configmap.yaml"])
def test_kubernetes_configmap_validates(tmp_path, name):
    configmap = yaml.safe_load((CH3 / "k8s" / name).read_text())
    _write(tmp_path, "config.yaml", configmap["data"]["config.yaml"])
    result = _validate(tmp_path, "config.yaml", env=["NODE_NAME=kind-worker"])
    assert result.returncode == 0, result.stderr


def test_validate_rejects_a_broken_config(tmp_path):
    good = (CH3 / "collector" / "agent-config.yaml").read_text()
    _write(tmp_path, "broken.yaml", good.replace("batch:", "batchh:", 1))
    result = _validate(tmp_path, "broken.yaml")
    assert result.returncode != 0
    assert 'unknown type: "batchh"' in result.stderr, result.stderr
