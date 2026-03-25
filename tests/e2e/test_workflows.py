"""End-to-end tests for full SDK workflows."""

import asyncio
from pathlib import Path
from time import monotonic
import uuid

import pytest


def _unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _wait_for_request_done(client, req_id: str, timeout: float = 120.0) -> None:
    """Wait for an async request to complete through websocket stats updates."""
    ws = await client.ensure_websocket()
    deadline = monotonic() + timeout

    while True:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Timed out waiting for request completion: {req_id}")

        message = await asyncio.wait_for(ws.__anext__(), timeout=remaining)
        if message.type != "stats_update":
            continue

        obj = message.data.get("obj")
        if not isinstance(obj, dict):
            continue
        if (obj.get("id") or message.req_id) != req_id:
            continue

        status = obj.get("status")
        if status == "failed":
            raise AssertionError(f"Request failed: {req_id} -> {obj.get('errors', [])}")
        if status == "canceled":
            raise AssertionError(f"Request canceled: {req_id}")
        if status == "done":
            return


@pytest.mark.e2e
async def test_full_workflow(gulp_base_url, gulp_test_user, gulp_test_password):
    """Test login, operation creation, ingestion, query, and cleanup as one workflow."""
    from gulp_sdk import GulpClient

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        operation = await client.operations.create(
            name=_unique_name("sdk_e2e_workflow"),
            description="SDK end-to-end workflow test",
        )

        try:
            ingest_result = await client.ingest.file(
                operation_id=operation.id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_e2e_context",
            )
            assert ingest_result.req_id

            await _wait_for_request_done(client, ingest_result.req_id)

            query_result = await client.queries.query_raw(
                operation_id=operation.id,
                q=[{"query": {"match_all": {}}}],
                q_options={"preview_mode": True, "name": "sdk_e2e_preview"},
            )

            data = query_result.get("data", {})
            assert data.get("total_hits") == 7
            assert len(data.get("docs", [])) > 0
        finally:
            await client.operations.delete(operation.id)
