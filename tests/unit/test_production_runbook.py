"""Regression tests for backend production runbook examples."""

import json
import re
from pathlib import Path

import json5
import pytest

from gulp.config import GulpConfig


RUNBOOK_PATH = Path("docs/production_runbook.md")
TESTING_DOC_PATH = Path("docs/testing.md")
CONFIG_TEMPLATE_PATH = Path("src/gulp/gulp_cfg_template.json")


def _extract_config_profile_blocks() -> dict[str, dict]:
    runbook = RUNBOOK_PATH.read_text(encoding="utf-8")
    match = re.search(
        r"## config profiles(?P<section>.*?)## alert starting points",
        runbook,
        flags=re.DOTALL,
    )
    assert match, "production runbook must keep a config profiles section"

    blocks: dict[str, dict] = {}
    for block_match in re.finditer(
        r"### (?P<name>[^\n]+)\n.*?~~~json\n(?P<json>.*?)\n~~~",
        match.group("section"),
        flags=re.DOTALL,
    ):
        blocks[block_match.group("name").strip()] = json.loads(
            block_match.group("json")
        )
    return blocks


@pytest.mark.unit
def test_production_runbook_config_profiles_match_backend_config():
    """Documented production profiles should not drift from GulpConfig."""
    blocks = _extract_config_profile_blocks()
    template = json5.loads(CONFIG_TEMPLATE_PATH.read_text(encoding="utf-8"))
    expected_profiles = {
        "small controlled deployment",
        "medium multi-instance deployment",
        "high-volume deployment",
    }

    assert expected_profiles.issubset(blocks)

    for profile_name in expected_profiles:
        profile = blocks[profile_name]
        assert profile["prometheus_enabled"] is True
        for key in profile:
            assert key in template, f"{profile_name} documents unknown config key {key}"
            assert hasattr(GulpConfig, key), (
                f"{profile_name} documents {key}, but GulpConfig has no accessor"
            )


@pytest.mark.unit
def test_production_runbook_profiles_scale_capacity_monotonically():
    """Small/medium/high profile values should communicate increasing capacity."""
    blocks = _extract_config_profile_blocks()
    small = blocks["small controlled deployment"]
    medium = blocks["medium multi-instance deployment"]
    high = blocks["high-volume deployment"]

    increasing_keys = [
        "concurrency_opensearch_num_nodes",
        "concurrency_tasks_cap_per_process",
        "redis_stream_task_maxlen",
        "ws_queue_max_size",
    ]
    for key in increasing_keys:
        assert small[key] < medium[key] < high[key]

    assert small["redis_task_active_user_max"] == 0
    assert small["redis_task_active_operation_max"] == 0
    assert medium["redis_task_active_user_max"] < high["redis_task_active_user_max"]
    assert (
        medium["redis_task_active_operation_max"]
        < high["redis_task_active_operation_max"]
    )

    for profile in (small, medium, high):
        assert profile["ws_enqueue_timeout"] > 0
        assert profile["redis_stream_task_maxlen"] > 0
        if "redis_task_max_attempts" in profile:
            assert profile["redis_task_max_attempts"] >= 2
        if "redis_task_lease_refresh_interval_ms" in profile:
            assert (
                profile["redis_task_lease_refresh_interval_ms"]
                < profile["redis_task_autoclaim_idle_ms"]
            )
        if "redis_task_retry_backoff_base_ms" in profile:
            assert (
                profile["redis_task_retry_backoff_base_ms"]
                <= profile["redis_task_retry_backoff_max_ms"]
            )


@pytest.mark.unit
def test_testing_docs_include_multi_instance_soak_runner():
    """Testing docs should expose the backend multi-instance soak command path."""
    testing_doc = TESTING_DOC_PATH.read_text(encoding="utf-8")

    assert "./tests/integration/run_multi_instance_tests.sh" in testing_doc
    assert "GULP_MULTI_INSTANCE_SOAK=1" in testing_doc
    assert "GULP_MULTI_INSTANCE_POINTER_STRESS_COUNT" in testing_doc
    assert "GULP_MULTI_INSTANCE_POINTER_SOAK_SECONDS" in testing_doc


@pytest.mark.unit
def test_testing_docs_include_stress_metrics_coverage():
    """Testing docs should keep the Prometheus-enabled stress gate visible."""
    testing_doc = TESTING_DOC_PATH.read_text(encoding="utf-8")

    assert "GULP_STRESS_METRICS_TIMEOUT" in testing_doc
    assert "critical /metrics families" in testing_doc
    assert "test_concurrent_ingest_and_query" in testing_doc
