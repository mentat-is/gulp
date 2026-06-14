"""Cross-instance collaboration broadcast coverage."""

from __future__ import annotations

import os

import pytest

from tests.integration._test_multi_instance_base import _run_multi_user_scenario


@pytest.mark.integration
async def test_multi_instance_collab_broadcast() -> None:
    """Multiple users across two instances verify collab create/update/delete fanout."""
    first_url = os.getenv("GULP_BASE_URL", "http://localhost:8080")
    second_url = os.getenv("GULP_SECOND_BASE_URL", "http://localhost:8100")
    await _run_multi_user_scenario(
        [first_url, second_url],
        run_client_data=False,
        run_cross_instance_query=False,
        run_query_isolation=False,
        run_collab_lifecycle=True,
        run_ingest=False,
    )
