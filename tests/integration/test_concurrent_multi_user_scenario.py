"""High-concurrency core workflow across one or more real gULP instances."""

from __future__ import annotations

import asyncio
import os

import pytest

from tests.integration._test_multi_instance_base import _run_multi_user_scenario
from tests.integration.test_internal_events import _run_internal_event_stress


def _positive_int_env(name: str, default: int) -> int:
    """Read a positive integer environment variable."""
    return max(1, int(os.getenv(name, str(default))))


def _env_enabled(name: str) -> bool:
    """Return whether an opt-in environment variable is enabled."""
    return os.getenv(name, "").lower() in {"1", "true", "yes", "on"}


@pytest.mark.integration
@pytest.mark.stress
async def test_concurrent_multi_user_scenario() -> None:
    """Exercise core ingest, query, enrich, collab, and websocket paths concurrently."""
    first_url = os.getenv("GULP_BASE_URL", "http://localhost:8080")
    second_url = os.getenv("GULP_SECOND_BASE_URL")
    instance_urls = [first_url, *([second_url] if second_url else [])]
    users = _positive_int_env("GULP_CONCURRENT_USERS", 10)
    actor_urls = [instance_urls[idx % len(instance_urls)] for idx in range(users)]

    print(
        "concurrent multi-user scenario:",
        {
            "users": users,
            "instances": instance_urls,
            "internal_events": _env_enabled("GULP_SCENARIO_INTERNAL_EVENTS"),
        },
    )

    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(
            _run_multi_user_scenario(
                actor_urls,
                run_client_data=True,
                run_cross_instance_query=True,
                run_query_isolation=True,
                run_collab_lifecycle=True,
                run_ingest=True,
                run_enrich=True,
            )
        )
        if _env_enabled("GULP_SCENARIO_INTERNAL_EVENTS"):
            assert (
                second_url
            ), "internal-event stress requires two probe-enabled instances"
            tasks.create_task(_run_internal_event_stress(first_url, second_url))
