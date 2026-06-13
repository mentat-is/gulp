"""Single-instance multi-user websocket routing coverage."""

from __future__ import annotations

import os

import pytest

from tests.integration._test_multi_instance_base import _run_multi_user_scenario


@pytest.mark.integration
async def test_multi_user_single_instance() -> None:
    """Multiple users on one instance perform ingest/query/collab and verify WS routing semantics."""
    base_url = os.getenv("GULP_BASE_URL", "http://localhost:8080")
    await _run_multi_user_scenario(
        [base_url, base_url, base_url],
        run_client_data=False,
        run_cross_instance_query=False,
        run_query_isolation=True,
        run_collab_lifecycle=True,
        run_ingest=True,
    )
