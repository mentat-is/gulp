"""Integration tests for websocket-driven wait behavior in the SDK wrappers."""

import asyncio
import uuid
from pathlib import Path

import pytest

from gulp_sdk import GulpClient


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _create_operation(client: GulpClient, prefix: str) -> str:
    op = await client.operations.create(name=_unique(prefix))
    op_id = getattr(op, "id", None) or (op.get("id") if isinstance(op, dict) else None)
    if not op_id:
        raise RuntimeError("failed to create operation id")
    return str(op_id)


async def _delete_operation_with_retry(client: GulpClient, operation_id: str, timeout: float = 30.0) -> None:
    """Delete operation tolerating transient running-request state."""
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


async def _fail_if_polling(*_args, **_kwargs):
    raise AssertionError(
        "wait_for_request_stats used /request_get_by_id polling instead of websocket notifications"
    )


@pytest.mark.integration
async def test_ws_wait_ingest_file_completion_uses_websocket(gulp_base_url, gulp_test_user, gulp_test_password):
    sample_path = Path("/gulp/samples/win_evtx/system.evtx")
    assert sample_path.exists(), f"EVTX sample not found: {sample_path}"

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        await client.ensure_websocket()

        op_id = await _create_operation(client, "ws_wait_ingest")
        try:
            original_request_get = client.plugins.request_get
            client.plugins.request_get = _fail_if_polling
            try:
                res = await client.ingest.file(
                    operation_id=op_id,
                    plugin_name="win_evtx",
                    file_path=str(sample_path),
                    wait=True,
                    timeout=180,
                )
            finally:
                client.plugins.request_get = original_request_get

            assert str(getattr(res, "status", "")).lower() == "done"
        finally:
            await _delete_operation_with_retry(client, op_id)


@pytest.mark.integration
async def test_ws_wait_query_gulp_completion_uses_websocket(gulp_base_url, gulp_test_user, gulp_test_password):
    sample_path = Path("/gulp/samples/win_evtx/system.evtx")
    assert sample_path.exists(), f"EVTX sample not found: {sample_path}"

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        await client.ensure_websocket()

        op_id = await _create_operation(client, "ws_wait_query")
        try:
            ingest_res = await client.ingest.file(
                operation_id=op_id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                wait=True,
                timeout=180,
            )
            assert str(getattr(ingest_res, "status", "")).lower() == "done"

            original_request_get = client.plugins.request_get
            client.plugins.request_get = _fail_if_polling
            try:
                qres = await client.queries.query_gulp(
                    operation_id=op_id,
                    flt={"operation_ids": [op_id]},
                    q_options={"limit": 20, "name": "ws_wait_query"},
                    wait=True,
                    timeout=180,
                )
            finally:
                client.plugins.request_get = original_request_get

            assert str((qres or {}).get("status", "")).lower() == "done"
        finally:
            await _delete_operation_with_retry(client, op_id)


@pytest.mark.integration
async def test_ws_wait_db_rebase_completion_uses_websocket(gulp_base_url, gulp_test_user, gulp_test_password):
    sample_path = Path("/gulp/samples/win_evtx/system.evtx")
    assert sample_path.exists(), f"EVTX sample not found: {sample_path}"

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        await client.ensure_websocket()

        op_id = await _create_operation(client, "ws_wait_rebase")
        try:
            ingest_res = await client.ingest.file(
                operation_id=op_id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                wait=True,
                timeout=180,
            )
            assert str(getattr(ingest_res, "status", "")).lower() == "done"

            original_request_get = client.plugins.request_get
            client.plugins.request_get = _fail_if_polling
            try:
                dres = await client.db.rebase_by_query(
                    operation_id=op_id,
                    ws_id=client.ws_id,
                    offset_msec=1,
                    flt={"operation_ids": [op_id]},
                    wait=True,
                    timeout=180,
                )
            finally:
                client.plugins.request_get = original_request_get

            assert str((dres or {}).get("status", "")).lower() == "done"
        finally:
            await _delete_operation_with_retry(client, op_id)
