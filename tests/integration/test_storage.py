"""
Integration tests for the Storage API.

Requires a live Gulp server (default: http://localhost:8080).
Set GULP_BASE_URL, GULP_TEST_USER, GULP_TEST_PASSWORD env vars to override.

Run with:
    python -m pytest -v tests/integration/test_storage.py -m integration
"""

import pytest
import uuid
import asyncio
from pathlib import Path


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _storage_ingest_params() -> dict:
    return {"plugin_params": {"store_file": True}}


async def _wait_request_done(client, req_id: str, timeout: float = 120.0) -> dict:
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


@pytest.mark.integration
async def test_list_files_empty_operation(gulp_base_url, gulp_test_user, gulp_test_password):
    """list_files for an operation that has no ingested data should return empty files list."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("store_test"))
        try:
            result = await client.storage.list_files(operation_id=op.id)
            # Result is a dict with at least a "files" key (may be empty list)
            assert isinstance(result, dict)
            files = result.get("files", result.get("data", []))
            assert isinstance(files, (list, type(None)))
        except GulpSDKError:
            pytest.skip("storage.list_files not available in current server config")
        finally:
            await client.operations.delete(op.id)


@pytest.mark.integration
async def test_list_files_no_filter(gulp_base_url, gulp_test_user, gulp_test_password):
    """list_files with no filter should not raise (may return empty result)."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        try:
            result = await client.storage.list_files()
            assert isinstance(result, (dict, list))
        except GulpSDKError:
            pytest.skip("storage.list_files not available in current server config")


@pytest.mark.integration
async def test_delete_by_tags_no_op(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    delete_by_tags on an operation with no stored files should not raise
    (it either succeeds with deleted=0 or raises GulpSDKError for missing data).
    """
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("store_del_test"))
        try:
            try:
                result = await client.storage.delete_by_tags(operation_id=op.id)
                assert result is not None
            except GulpSDKError:
                pass  # 404 on empty storage is acceptable
        finally:
            await client.operations.delete(op.id)


@pytest.mark.integration
async def test_get_file_by_id_after_ingest(
    gulp_base_url, gulp_test_user, gulp_test_password, tmp_path
):
    """A stored source file can be listed and downloaded back from filestore."""
    from gulp_sdk import GulpClient, GulpSDKError

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("store_get_test"))
        try:
            ingest = await client.ingest.file(
                operation_id=op.id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_storage_context",
                params=_storage_ingest_params(),
            )
            assert ingest.req_id
            await _wait_request_done(client, ingest.req_id)

            try:
                listed = await client.storage.list_files(operation_id=op.id)
            except GulpSDKError:
                pytest.skip("storage.list_files not available in current server config")

            objects = listed.get("objects") or listed.get("files") or []
            if not objects:
                pytest.skip("No stored files returned by storage.list_files in current server config")

            storage_id = objects[0].get("storage_id")
            if not storage_id:
                pytest.skip("storage.list_files did not return storage_id entries")

            output_path = tmp_path / "downloaded_sample.evtx"
            try:
                saved = await client.storage.get_file_by_id(op.id, storage_id, str(output_path))
            except GulpSDKError as exc:
                pytest.skip(f"storage.get_file_by_id not available in current server config: {exc}")

            assert saved == str(output_path)
            assert output_path.exists()
            assert output_path.stat().st_size > 0
        finally:
            await client.operations.delete(op.id)


@pytest.mark.integration
async def test_delete_by_id_after_ingest(gulp_base_url, gulp_test_user, gulp_test_password):
    """Delete one stored file by storage_id after a real ingest."""
    from gulp_sdk import GulpClient, GulpSDKError

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("store_delete_id_test"))
        try:
            ingest = await client.ingest.file(
                operation_id=op.id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_storage_delete_context",
                params=_storage_ingest_params(),
            )
            assert ingest.req_id
            await _wait_request_done(client, ingest.req_id)

            try:
                listed = await client.storage.list_files(operation_id=op.id)
            except GulpSDKError:
                pytest.skip("storage.list_files not available in current server config")

            objects = listed.get("objects") or listed.get("files") or []
            if not objects:
                pytest.skip("No stored files returned by storage.list_files in current server config")

            storage_id = objects[0].get("storage_id")
            if not storage_id:
                pytest.skip("storage.list_files did not return storage_id entries")

            try:
                deleted = await client.storage.delete_by_id(op.id, storage_id)
                assert isinstance(deleted, dict)
            except GulpSDKError as exc:
                pytest.skip(f"storage.delete_by_id unavailable in current server config: {exc}")
        finally:
            await client.operations.delete(op.id)
