"""Live proof that deleting an operation removes its MinIO objects."""

import uuid
from pathlib import Path

import pytest


@pytest.mark.integration
async def test_operation_delete_removes_minio_objects(
    gulp_base_url, gulp_test_user, gulp_test_password
) -> None:
    from gulp_sdk import GulpClient

    sample = (
        Path(__file__).resolve().parents[2]
        / "samples/win_evtx/Security_short_selected.evtx"
    )
    assert sample.exists(), sample
    operation_id: str | None = None
    context_id: str | None = None
    operation_deleted = False
    cleanup_errors: list[str] = []

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        try:
            operation = await client.operations.create(
                f"storage_delete_{uuid.uuid4().hex[:8]}"
            )
            operation_id = operation.id
            ingest = await client.ingest.file(
                operation_id=operation_id,
                plugin_name="win_evtx",
                file_path=str(sample),
                context_name=f"storage_delete_ctx_{uuid.uuid4().hex[:8]}",
                params={"plugin_params": {"store_file": True}},
                wait=True,
                timeout=120,
            )
            print("stored-file ingest result:", ingest)
            assert ingest.req_id
            assert str(ingest.status).lower() == "done"

            before = await client.storage.list_files(
                operation_id=operation_id, max_results=1000
            )
            print("storage before operation delete:", before)
            objects = before["objects"]
            assert objects
            storage_ids = {obj["storage_id"] for obj in objects}
            context_id = objects[0]["tags"]["context_id"]

            deleted = await client.operations.delete(operation_id)
            operation_deleted = True
            print("operation delete response:", deleted)
            assert deleted is True

            after = await client.storage.list_files(
                context_id=context_id, max_results=1000
            )
            print("storage after operation delete:", after)
            assert storage_ids.isdisjoint(obj["storage_id"] for obj in after["objects"])
        finally:
            if context_id:
                try:
                    await client.storage.delete_by_tags(context_id=context_id)
                except Exception as ex:
                    cleanup_errors.append(f"storage cleanup failed: {ex}")
            if operation_id and not operation_deleted:
                try:
                    await client.operations.delete(operation_id, force=True)
                except Exception as ex:
                    cleanup_errors.append(f"operation cleanup failed: {ex}")
            if cleanup_errors:
                raise AssertionError("; ".join(cleanup_errors))
