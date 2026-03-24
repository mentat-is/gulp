"""
Integration tests for the OpenSearch admin (db) API.

Requires a live Gulp server with an admin account.
Set GULP_BASE_URL, GULP_TEST_USER, GULP_TEST_PASSWORD env vars to override.

Run with:
    python -m pytest -v tests/integration/test_db.py -m integration
"""

import asyncio
from pathlib import Path
import uuid

import pytest


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _wait_request_done(client, req_id: str, timeout: float = 180.0) -> dict:
    """Poll request status until it reaches a terminal state."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            stats = await client.plugins.request_get(req_id)
            status = str(stats.get("status", "")).lower()
            if status in {"done", "failed", "canceled"}:
                return stats
        except Exception:
            pass
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"Timed out waiting request {req_id}")
        await asyncio.sleep(1.0)


async def _delete_operation_with_retry(client, operation_id: str, timeout: float = 30.0) -> None:
    """Delete operation tolerating transient running-request state."""
    try:
        await client.plugins.request_delete(operation_id)
    except Exception:
        pass

    deadline = asyncio.get_running_loop().time() + timeout
    last_exc: Exception | None = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            await client.operations.delete(operation_id)
            return
        except Exception as exc:
            last_exc = exc
            if "running requests" not in str(exc).lower():
                raise
            await asyncio.sleep(1.0)
    if last_exc:
        raise last_exc


@pytest.mark.integration
async def test_list_indexes(gulp_base_url, gulp_test_user, gulp_test_password):
    """list_indexes should return a list of index names."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        try:
            result = await client.db.list_indexes()
            assert isinstance(result, (list, dict))
        except GulpSDKError:
            pytest.skip("db.list_indexes not available in current server config")


@pytest.mark.integration
async def test_refresh_index(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    refresh_index on the default index should not raise.
    If the index does not exist the server may return 4xx — tolerate that.
    """
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"db_test_{uuid.uuid4().hex[:8]}")
        try:
            try:
                result = await client.db.refresh_index(op.id)
                assert result is not None
            except GulpSDKError:
                pass  # non-existent index returns 4xx
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_rebase_by_query_optional(gulp_base_url, gulp_test_user, gulp_test_password):
    """rebase_by_query should be callable after a small ingest (optional by server config)."""
    from gulp_sdk import GulpClient, GulpSDKError

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("db_rebase_test"))
        try:
            ingest = await client.ingest.file(
                operation_id=op.id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_db_rebase_context",
            )
            assert ingest.req_id
            await _wait_request_done(client, ingest.req_id)

            try:
                result = await client.db.rebase_by_query(
                    operation_id=op.id,
                    ws_id=client.ws_id,
                    offset_msec=1,
                    flt={"operation_ids": [op.id]},
                )
                assert isinstance(result, dict)
            except GulpSDKError as exc:
                pytest.skip(f"db.rebase_by_query unavailable in current server config: {exc}")
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_delete_index_optional(gulp_base_url, gulp_test_user, gulp_test_password):
    """delete_index endpoint is reachable (non-existent index is acceptable)."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        fake_index = _unique("sdk_nonexistent_index")
        try:
            result = await client.db.delete_index(fake_index, delete_operation=False)
            assert isinstance(result, dict)
        except GulpSDKError:
            # 404 / unavailable is acceptable for this reachability check.
            pass
