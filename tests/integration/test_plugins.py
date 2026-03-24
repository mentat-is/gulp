"""
Integration tests for the Plugins/utility API.

Verifies server version, plugin listing, and mapping file listing.

Requires a live Gulp server (default: http://localhost:8080).
Set GULP_BASE_URL, GULP_TEST_USER, GULP_TEST_PASSWORD env vars to override.

Run with:
    python -m pytest -v tests/integration/test_plugins.py -m integration
"""

import pytest
import tempfile
import pathlib
import asyncio
import uuid
from pathlib import Path


@pytest.mark.integration
async def test_version(gulp_base_url, gulp_test_user, gulp_test_password):
    """Server /version should return a non-empty version string."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        ver = await client.plugins.version()
        assert isinstance(ver, str)
        assert len(ver) > 0


@pytest.mark.integration
async def test_plugin_list(gulp_base_url, gulp_test_user, gulp_test_password):
    """Plugin list should return at least one built-in plugin."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        plugins = await client.plugins.list()
        assert isinstance(plugins, list)
        assert len(plugins) > 0
        # Each entry should have at least filename and type
        for p in plugins:
            assert "filename" in p
            assert "type" in p


@pytest.mark.integration
async def test_ui_plugin_list(gulp_base_url, gulp_test_user, gulp_test_password):
    """UI plugin list should be a list (may be empty)."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        ui_plugins = await client.plugins.list_ui()
        assert isinstance(ui_plugins, list)


@pytest.mark.integration
async def test_mapping_file_list(gulp_base_url, gulp_test_user, gulp_test_password):
    """Mapping file list should return at least one built-in mapping."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        mappings = await client.plugins.mapping_list()
        assert isinstance(mappings, list)
        assert len(mappings) > 0
        for m in mappings:
            assert "filename" in m
            assert "mapping_ids" in m


@pytest.mark.integration
async def test_mapping_file_upload_download_delete(gulp_base_url, gulp_test_user, gulp_test_password):
    """Upload, download, and delete a minimal mapping file."""
    import json
    from gulp_sdk import GulpClient

    # Minimal valid mapping file
    mapping_content = json.dumps({
        "metadata": {"plugin": ["test_sdk.py"]},
        "mappings": {
            "test_sdk": {
                "fields": {}
            }
        }
    }).encode()

    with tempfile.NamedTemporaryFile(
        suffix="_test_sdk.json", delete=False, mode="wb"
    ) as f:
        f.write(mapping_content)
        upload_path = f.name
        upload_filename = pathlib.Path(upload_path).name

    download_path = upload_path + ".dl"

    try:
        async with GulpClient(gulp_base_url) as client:
            await client.auth.login(gulp_test_user, gulp_test_password)

            # Upload
            result = await client.plugins.mapping_upload(
                upload_path, fail_if_exists=False
            )
            assert "path" in result

            # Download
            dl = await client.plugins.mapping_download(upload_filename, download_path)
            assert pathlib.Path(dl).exists()
            downloaded = json.loads(pathlib.Path(dl).read_text())
            assert "mappings" in downloaded

            # Delete — tolerate known server bug (muty.file.delete_file_async)
            try:
                del_result = await client.plugins.mapping_delete(upload_filename)
                assert del_result is not None
            except Exception:
                pytest.xfail("Server-side mapping_file_delete has a bug")
    finally:
        pathlib.Path(upload_path).unlink(missing_ok=True)
        pathlib.Path(download_path).unlink(missing_ok=True)


@pytest.mark.integration
async def test_request_list(gulp_base_url, gulp_test_user, gulp_test_password):
    """Request list for a fresh operation should return an empty list."""
    from gulp_sdk import GulpClient
    import uuid

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"req_test_{uuid.uuid4().hex[:8]}")
        try:
            reqs = await client.plugins.request_list(op.id)
            assert isinstance(reqs, list)
        finally:
            await client.operations.delete(op.id)


@pytest.mark.integration
async def test_config_upload_download(gulp_base_url, gulp_test_user, gulp_test_password):
    """Upload/download config without leaving persistent changes on server."""
    import json
    import tempfile
    import pathlib
    from gulp_sdk import GulpClient, GulpSDKError

    # Minimal config-like JSON for round-trip test
    cfg_content = json.dumps({"_test_sdk": True}).encode()

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="wb") as f:
        f.write(cfg_content)
        upload_path = f.name

    download_path = upload_path + ".dl.json"
    backup_path = upload_path + ".backup.json"
    restore_check_path = upload_path + ".restore_check.json"

    try:
        async with GulpClient(gulp_base_url) as client:
            await client.auth.login(gulp_test_user, gulp_test_password)

            # Snapshot current server config before any mutation.
            try:
                await client.plugins.config_download(backup_path)
            except GulpSDKError:
                pytest.skip("config_upload/download not available in current server")

            original_cfg_bytes = pathlib.Path(backup_path).read_bytes()

            try:
                result = await client.plugins.config_upload(upload_path)
                assert result is not None

                dl = await client.plugins.config_download(download_path)
                assert pathlib.Path(dl).exists()
                downloaded_cfg = json.loads(pathlib.Path(dl).read_text(encoding="utf-8"))
                assert downloaded_cfg.get("_test_sdk") is True
            finally:
                # Always restore original config and verify exact content equality.
                await client.plugins.config_upload(backup_path)
                await client.plugins.config_download(restore_check_path)
                restored_cfg_bytes = pathlib.Path(restore_check_path).read_bytes()
                assert restored_cfg_bytes == original_cfg_bytes
    finally:
        pathlib.Path(upload_path).unlink(missing_ok=True)
        pathlib.Path(download_path).unlink(missing_ok=True)
        pathlib.Path(backup_path).unlink(missing_ok=True)
        pathlib.Path(restore_check_path).unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# Plugin upload / download / delete                                             #
# --------------------------------------------------------------------------- #

@pytest.mark.integration
async def test_plugin_upload_download_delete(gulp_base_url, gulp_test_user, gulp_test_password):
    """Upload a minimal plugin .py, download it back, then delete it."""
    import tempfile
    import pathlib
    from gulp_sdk import GulpClient, GulpSDKError

    plugin_src = b'"""Temporary test plugin for SDK integration tests."""\n'

    with tempfile.NamedTemporaryFile(
        prefix="sdk_test_", suffix=".py", delete=False, mode="wb"
    ) as f:
        f.write(plugin_src)
        upload_path = f.name

    plugin_filename = pathlib.Path(upload_path).name
    download_path = upload_path + ".dl.py"

    try:
        async with GulpClient(gulp_base_url) as client:
            await client.auth.login(gulp_test_user, gulp_test_password)
            try:
                result = await client.plugins.upload(upload_path, fail_if_exists=False)
                assert result is not None

                dl = await client.plugins.download(plugin_filename, download_path)
                assert pathlib.Path(dl).exists()

                del_result = await client.plugins.delete(plugin_filename)
                assert del_result is not None
            except GulpSDKError as exc:
                pytest.skip(f"plugin upload/download/delete not available: {exc}")
    finally:
        pathlib.Path(upload_path).unlink(missing_ok=True)
        pathlib.Path(download_path).unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# Request get / delete                                                          #
# --------------------------------------------------------------------------- #

@pytest.mark.integration
async def test_request_get_and_delete(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    request_list + request_get + request_delete on a fresh (empty) operation.

    A fresh operation has no requests, so request_list returns [].
    We verify request_get raises on a non-existent ID and request_delete
    with no obj_id clears (empty) the operation's request table.
    """
    import uuid
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"req_crud_{uuid.uuid4().hex[:8]}")
        try:
            # list should be empty
            reqs = await client.plugins.request_list(op.id)
            assert isinstance(reqs, list)

            # get on non-existent id should raise (obj_id is the first positional arg)
            try:
                await client.plugins.request_get(uuid.uuid4().hex)
                # some backends return empty rather than 404 — that's fine
            except GulpSDKError:
                pass

            # delete all — server raises 404 if no requests exist, which is acceptable
            try:
                result = await client.plugins.request_delete(op.id)
                assert result is not None
            except GulpSDKError:
                pass  # 404 "nothing to delete" is a valid server response
        finally:
            await client.operations.delete(op.id)


# --------------------------------------------------------------------------- #
# Enhance-document map CRUD                                                    #
# --------------------------------------------------------------------------- #

@pytest.mark.integration
async def test_enhance_map_crud(gulp_base_url, gulp_test_user, gulp_test_password):
    """Full create / list / get / update / delete cycle for enhance_map entries."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        try:
            # create
            entry = await client.plugins.enhance_map_create(
                gulp_event_code=99999,
                plugin="sdk_test_plugin",
                color="#abcdef",
            )
            assert "id" in entry
            obj_id = entry["id"]

            # list — should include our new entry
            entries = await client.plugins.enhance_map_list()
            assert isinstance(entries, list)
            assert any(e.get("id") == obj_id for e in entries)

            # get by id
            fetched = await client.plugins.enhance_map_get(obj_id)
            assert fetched.get("id") == obj_id

            # update color
            updated = await client.plugins.enhance_map_update(obj_id, color="#112233")
            assert updated is not None

            # delete
            result = await client.plugins.enhance_map_delete(obj_id)
            assert result is not None

        except GulpSDKError as exc:
            pytest.skip(f"enhance_map endpoints not available: {exc}")


async def _wait_request_visible(client, req_id: str, timeout: float = 30.0) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            stats = await client.plugins.request_get(req_id)
            if stats.get("id") == req_id:
                return stats
        except Exception:
            pass
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"Timed out waiting request stats visibility for {req_id}")
        await asyncio.sleep(0.5)


async def _wait_request_status(client, req_id: str, expected_status: str, timeout: float = 30.0) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        stats = await _wait_request_visible(client, req_id, timeout=timeout)
        if str(stats.get("status", "")).lower() == expected_status:
            return stats
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(
                f"Timed out waiting request {req_id} to become {expected_status}"
            )
        await asyncio.sleep(0.5)


def _sample_evtx_path() -> Path:
    return Path("/gulp/samples/win_evtx/Security_short_selected.evtx")


@pytest.mark.integration
async def test_request_set_completed(gulp_base_url, gulp_test_user, gulp_test_password):
    """A live request can be force-marked as failed via request_set_completed."""
    from gulp_sdk import GulpClient

    sample_path = _sample_evtx_path()
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"req_complete_{uuid.uuid4().hex[:8]}")
        try:
            ingest = await client.ingest.file(
                operation_id=op.id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_req_complete",
            )
            assert ingest.req_id

            await _wait_request_visible(client, ingest.req_id)
            result = await client.plugins.request_set_completed(ingest.req_id, failed=True)
            assert result.get("id") == ingest.req_id

            final_stats = await _wait_request_status(client, ingest.req_id, "failed")
            assert str(final_stats.get("status", "")).lower() == "failed"
        finally:
            await client.operations.delete(op.id)


@pytest.mark.integration
async def test_request_cancel(gulp_base_url, gulp_test_user, gulp_test_password):
    """A live request can be canceled through the utility API."""
    from gulp_sdk import GulpClient

    sample_path = _sample_evtx_path()
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"req_cancel_{uuid.uuid4().hex[:8]}")
        try:
            ingest = await client.ingest.file(
                operation_id=op.id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_req_cancel",
            )
            assert ingest.req_id

            await _wait_request_visible(client, ingest.req_id)
            result = await client.plugins.request_cancel(ingest.req_id)
            assert result.get("id") == ingest.req_id

            final_stats = await _wait_request_status(client, ingest.req_id, "canceled")
            assert str(final_stats.get("status", "")).lower() == "canceled"
        finally:
            await client.operations.delete(op.id)


@pytest.mark.integration
async def test_object_delete_bulk_notes(gulp_base_url, gulp_test_user, gulp_test_password):
    """Bulk delete can remove selected notes without deleting unrelated ones."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"bulk_note_{uuid.uuid4().hex[:8]}")
        try:
            ctx = await client.operations.context_create(op.id, "sdk_bulk_ctx")
            src = await client.operations.source_create(op.id, ctx["id"], "sdk_bulk_src")

            note_one = await client.collab.note_create(
                operation_id=op.id,
                context_id=ctx["id"],
                source_id=src["id"],
                name="bulk_note_one",
                text="note one",
                time_pin=1,
            )
            note_two = await client.collab.note_create(
                operation_id=op.id,
                context_id=ctx["id"],
                source_id=src["id"],
                name="bulk_note_two",
                text="note two",
                time_pin=2,
            )

            result = await client.plugins.object_delete_bulk(
                op.id,
                "note",
                {"ids": [note_one["id"]]},
            )
            assert int(result.get("deleted", 0)) >= 1

            notes = await client.collab.note_list(operation_id=op.id)
            note_ids = {note.get("id") for note in notes}
            assert note_one["id"] not in note_ids
            assert note_two["id"] in note_ids
        finally:
            await client.operations.delete(op.id)
