"""Single-instance WS_CLIENT_DATA routing coverage."""

from __future__ import annotations

import os

import pytest

from tests.integration._test_multi_instance_base import _run_multi_user_scenario


@pytest.mark.integration
async def test_single_instance_client_data() -> None:
    """Multiple users on one instance verify UI client-data routing on /ws_client_data."""
    base_url = os.getenv("GULP_BASE_URL", "http://localhost:8080")
    await _run_multi_user_scenario(
        [base_url, base_url, base_url],
        run_client_data=True,
        run_cross_instance_query=False,
        run_query_isolation=False,
        run_collab_lifecycle=False,
        run_ingest=False,
    )
