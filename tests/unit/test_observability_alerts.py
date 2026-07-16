"""Tests for backend Prometheus alert rule consistency."""

import os
import re
import subprocess
import sys

import pytest
import yaml
from prometheus_client.metrics import Counter, Gauge, Histogram, Info

from gulp.api.prometheus_api import GulpMetrics


def _declared_metric_names() -> set[str]:
    """Return metric series names exported by the backend metric definitions."""
    names: set[str] = set()
    for value in vars(GulpMetrics).values():
        if isinstance(value, Counter):
            names.add(value._name)
            names.add(f"{value._name}_total")
            names.add(f"{value._name}_created")
        elif isinstance(value, Gauge):
            names.add(value._name)
        elif isinstance(value, Histogram):
            names.add(value._name)
            names.add(f"{value._name}_bucket")
            names.add(f"{value._name}_count")
            names.add(f"{value._name}_sum")
            names.add(f"{value._name}_created")
        elif isinstance(value, Info):
            names.add(f"{value._name}_info")
    return names


@pytest.mark.unit
def test_prometheus_alerts_reference_declared_backend_metrics():
    """Alert rules should not drift from metrics exported by the backend."""
    alerts = yaml.safe_load(open("prometheus_alerts.yml", encoding="utf-8"))
    assert alerts["groups"][0]["name"] == "gulp-backend"
    declared_metrics = _declared_metric_names()
    allowed_external_metrics = {"up"}
    alert_names: set[str] = set()

    for rule in alerts["groups"][0]["rules"]:
        alert_name = rule.get("alert")
        assert alert_name
        assert alert_name not in alert_names
        alert_names.add(alert_name)
        assert rule.get("expr")
        assert rule.get("for")
        assert rule.get("labels", {}).get("severity") in {"warning", "critical"}

        referenced_metrics = {
            token
            for token in re.findall(r"\b(?:gulp_[a-zA-Z0-9_]+|up)\b", rule["expr"])
            if token not in {"on", "unless", "by", "sum", "max", "rate"}
        }
        missing = referenced_metrics - declared_metrics - allowed_external_metrics
        assert not missing, f"{alert_name} references unknown metrics: {missing}"


@pytest.mark.unit
def test_auto_created_prometheus_directory_is_removed_at_process_exit():
    env = os.environ.copy()
    env.pop("PROMETHEUS_MULTIPROC_DIR", None)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from gulp.api.prometheus_api import GulpMetrics; print(GulpMetrics._multiproc_dir)",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    directory = result.stdout.strip().splitlines()[-1]

    print(directory)
    assert directory.startswith("/tmp/gulp_prometheus_")
    assert not os.path.exists(directory)


@pytest.mark.unit
def test_configured_prometheus_directory_is_not_owned(monkeypatch, tmp_path):
    monkeypatch.setattr(GulpMetrics, "_multiproc_dir", str(tmp_path))
    monkeypatch.setattr(GulpMetrics, "_multiproc_dir_owner_pid", None)

    GulpMetrics.cleanup_multiproc_dir()

    assert tmp_path.exists()
