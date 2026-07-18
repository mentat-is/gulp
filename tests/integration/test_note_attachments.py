"""Live REST coverage for normalized note attachments."""

import asyncio
import uuid

import pytest


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _capture_note_update(client, note_id: str, request):
    from gulp_sdk import WSMessageType

    event = asyncio.Event()
    messages = []

    async def _handler(message):
        obj = message.data.get("obj", {}) if isinstance(message.data, dict) else {}
        if obj.get("id") == note_id:
            messages.append(message)
            event.set()

    await client.register_ws_message_handler(WSMessageType.COLLAB_UPDATE, _handler)
    try:
        result = await request
        await asyncio.wait_for(event.wait(), timeout=15)
        return result, messages[0]
    finally:
        client.unregister_ws_message_handler(WSMessageType.COLLAB_UPDATE, _handler)


@pytest.mark.integration
async def test_note_attachment_lifecycle_and_acl(
    gulp_base_url, gulp_test_user, gulp_test_password, tmp_path
):
    from gulp_sdk import GulpClient, PermissionError as GulpPermissionError

    payload = b"\x00gULP note attachment\xff\n"
    low_user_id = _unique("attusr")
    low_password = "TestPass!123"
    operation_id = None
    note_id = None
    low_user_created = False
    cleanup_errors: list[str] = []

    async with (
        GulpClient(gulp_base_url) as admin,
        GulpClient(gulp_base_url) as reader,
    ):
        await admin.auth.login(gulp_test_user, gulp_test_password)
        try:
            operation = await admin.operations.create(_unique("attachment_op"))
            operation_id = operation.id
            context = await admin.operations.context_create(
                operation_id, _unique("attachment_ctx")
            )
            source = await admin.operations.source_create(
                operation_id, context["id"], _unique("attachment_src")
            )
            note = await admin.collab.note_create(
                operation_id=operation_id,
                context_id=context["id"],
                source_id=source["id"],
                name="Attachment test note",
                text="Attachment lifecycle",
                time_pin=1_000_000_000,
            )
            note_id = note["id"]
            print("created note:", note)
            assert note["attachment_ids"] == []

            await admin.users.create(
                user_id=low_user_id,
                password=low_password,
                permission=["read"],
            )
            low_user_created = True
            await admin.acl.add_granted_user(operation_id, "operation", low_user_id)
            await reader.auth.login(low_user_id, low_password)
            await reader.ensure_websocket()

            upload = tmp_path / "evidence.bin"
            upload.write_bytes(payload)
            metadata, add_event = await _capture_note_update(
                reader,
                note_id,
                admin.collab.note_add_attachment(
                    note_id,
                    str(upload),
                    title="Evidence sample.bin",
                    mime_type="application/x-gulp-evidence",
                ),
            )
            attachment_id = metadata["id"]
            print("attachment add response:", metadata)
            print("attachment add broadcast:", add_event)
            assert metadata == {
                "id": attachment_id,
                "note_id": note_id,
                "user_id": gulp_test_user,
                "operation_id": operation_id,
                "title": "Evidence sample.bin",
                "description": "",
                "mime_type": "application/x-gulp-evidence",
            }
            assert add_event.data["obj"]["attachment_ids"] == [attachment_id]

            listed = await reader.collab.note_list_attachments(note_id)
            print("reader attachment list response:", listed)
            assert listed == [metadata]

            fetched_note = await reader.collab.note_get_by_id(note_id)
            print("note with attachment IDs:", fetched_note)
            assert fetched_note["attachment_ids"] == [attachment_id]

            output = tmp_path / "downloaded.bin"
            await reader.collab.note_get_attachment(note_id, attachment_id, str(output))
            assert output.read_bytes() == payload

            raw = await reader._client.get(
                "/note_get_attachment",
                params={"obj_id": note_id, "attachment_id": attachment_id},
                headers={"token": reader.token},
            )
            print("attachment download headers:", dict(raw.headers))
            assert raw.status_code == 200
            assert raw.content == payload
            assert raw.headers["content-type"] == "application/x-gulp-evidence"
            assert "Evidence%20sample.bin" in raw.headers["content-disposition"]

            with pytest.raises(GulpPermissionError):
                await reader.collab.note_add_attachment(note_id, str(upload))
            with pytest.raises(GulpPermissionError):
                await reader.collab.note_delete_attachment(note_id, attachment_id)

            deleted, delete_event = await _capture_note_update(
                reader,
                note_id,
                admin.collab.note_delete_attachment(note_id, attachment_id),
            )
            print("attachment delete response:", deleted)
            print("attachment delete broadcast:", delete_event)
            assert deleted == {"id": attachment_id, "note_id": note_id}
            assert delete_event.data["obj"]["attachment_ids"] == []
            assert await admin.collab.note_list_attachments(note_id) == []
            assert (await admin.collab.note_get_by_id(note_id))["attachment_ids"] == []

            cascade_metadata = await admin.collab.note_add_attachment(
                note_id, str(upload)
            )
            cascade_id = cascade_metadata["id"]
            before_cascade = await admin.storage.list_files(
                operation_id=operation_id,
                context_id=context["id"],
                max_results=1000,
            )
            assert cascade_id in {
                obj["storage_id"] for obj in before_cascade["objects"]
            }

            await admin.collab.note_delete(note_id)
            note_id = None
            after_cascade = await admin.storage.list_files(
                operation_id=operation_id,
                context_id=context["id"],
                max_results=1000,
            )
            print("storage after note cascade:", after_cascade)
            assert cascade_id not in {
                obj["storage_id"] for obj in after_cascade["objects"]
            }
        finally:
            if note_id:
                try:
                    await admin.collab.note_delete(note_id)
                except Exception as ex:
                    cleanup_errors.append(f"note cleanup failed: {ex}")
            if low_user_created:
                try:
                    await admin.users.delete(low_user_id)
                except Exception as ex:
                    cleanup_errors.append(f"user cleanup failed: {ex}")
            if operation_id:
                try:
                    await admin.operations.delete(operation_id)
                except Exception as ex:
                    cleanup_errors.append(f"operation cleanup failed: {ex}")
            if cleanup_errors:
                raise AssertionError("; ".join(cleanup_errors))
