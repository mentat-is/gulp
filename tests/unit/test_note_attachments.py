"""Unit coverage for normalized note attachments."""

from contextlib import asynccontextmanager
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import UploadFile
from muty.jsend import JSendException
from starlette.datastructures import Headers


def _upload(data: bytes, filename: str = "evidence.bin", mime_type: str = "x/test"):
    return UploadFile(
        BytesIO(data),
        filename=filename,
        headers=Headers({"content-type": mime_type}),
    )


@pytest.mark.unit
def test_attachment_metadata_defaults_and_overrides(monkeypatch):
    from gulp.api.collab.note import GulpNoteAttachment

    ids = iter(["attachment-default", "attachment-override"])
    monkeypatch.setattr("muty.string.generate_unique", lambda: next(ids))
    note = SimpleNamespace(id="note-a", operation_id="op-a")

    default = GulpNoteAttachment.from_upload(
        note, "user-a", "evidence.txt", "text/plain"
    )
    override = GulpNoteAttachment.from_upload(
        note,
        "user-a",
        "evidence.txt",
        "text/plain",
        title="Analyst evidence",
        mime_type="application/x-evidence",
    )

    print("default attachment metadata:", default.to_dict())
    print("overridden attachment metadata:", override.to_dict())
    assert default.title == "evidence.txt"
    assert default.mime_type == "text/plain"
    assert default.description == ""
    assert override.title == "Analyst evidence"
    assert override.mime_type == "application/x-evidence"


@pytest.mark.unit
def test_note_serializes_derived_attachment_ids():
    from gulp.api.collab.note import GulpNote, GulpNoteAttachment
    from gulp.api.collab_api import GulpCollab

    GulpCollab.init_mappers()
    note = GulpNote(
        id="note-a",
        type="note",
        user_id="user-a",
        name="note",
        time_created=1,
        time_updated=1,
        operation_id="op-a",
        glyph_id=None,
        description=None,
        tags=[],
        color=None,
        granted_user_ids=[],
        granted_user_group_ids=[],
        context_id="ctx-a",
        source_id="src-a",
        text="text",
        text_id=None,
        doc=None,
        doc_id=None,
        time_pin=1,
        last_editor_id=None,
    )

    assert note.to_dict()["attachment_ids"] == []
    note.attachments.append(
        GulpNoteAttachment(
            id="attachment-a",
            note_id=note.id,
            user_id="user-a",
            operation_id="op-a",
            title="evidence.bin",
            description="",
            mime_type="application/octet-stream",
        )
    )
    assert note.to_dict()["attachment_ids"] == ["attachment-a"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_attachment_size_limit_accepts_exactly_32_mib():
    from gulp.api.server.note import MAX_NOTE_ATTACHMENT_SIZE, _read_note_attachment

    data = b"x" * MAX_NOTE_ATTACHMENT_SIZE
    assert await _read_note_attachment(_upload(data)) == data


@pytest.mark.unit
@pytest.mark.asyncio
async def test_attachment_size_limit_rejects_before_storage_or_database(monkeypatch):
    from gulp.api.collab.note import GulpNote, GulpNoteAttachment
    from gulp.api.collab_api import GulpCollab
    from gulp.api.server.note import (
        MAX_NOTE_ATTACHMENT_SIZE,
        note_add_attachment_handler,
    )

    session = SimpleNamespace()

    @asynccontextmanager
    async def _session():
        yield session

    monkeypatch.setattr(
        GulpCollab,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(session=_session)),
    )
    monkeypatch.setattr(
        GulpNote,
        "get_by_id_wrapper",
        AsyncMock(
            return_value=(
                SimpleNamespace(user=SimpleNamespace(id="user-a")),
                SimpleNamespace(id="note-a"),
                None,
            )
        ),
    )
    create = AsyncMock()
    monkeypatch.setattr(GulpNoteAttachment, "create_from_upload", create)

    with pytest.raises(JSendException) as exc:
        await note_add_attachment_handler(
            token="token-a",
            obj_id="note-a",
            ws_id="ws-a",
            attachment=_upload(b"x" * (MAX_NOTE_ATTACHMENT_SIZE + 1)),
            title=None,
            mime_type=None,
            req_id="req-a",
        )

    print("oversize response status:", exc.value.status_code)
    assert exc.value.status_code == 413
    create.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_database_insert_failure_deletes_uploaded_object(monkeypatch):
    from gulp.api.collab.note import GulpNoteAttachment
    from gulp.api.s3_api import GulpS3

    s3 = SimpleNamespace(upload_data=AsyncMock(), delete_file=AsyncMock())
    monkeypatch.setattr(GulpS3, "get_instance", staticmethod(lambda: s3))
    note = SimpleNamespace(
        id="note-a",
        operation_id="op-a",
        context_id="ctx-a",
        source_id="src-a",
        attachments=[],
    )
    session = SimpleNamespace(
        commit=AsyncMock(side_effect=RuntimeError("insert failed")),
        rollback=AsyncMock(),
    )

    with pytest.raises(RuntimeError, match="insert failed"):
        await GulpNoteAttachment.create_from_upload(
            session,
            note,
            "user-a",
            b"evidence",
            "evidence.bin",
            "application/octet-stream",
        )

    attachment_id = note.attachments[0].id
    s3.upload_data.assert_awaited_once_with(
        b"evidence",
        attachment_id,
        tags={
            "note_id": "note-a",
            "user_id": "user-a",
            "operation_id": "op-a",
            "context_id": "ctx-a",
            "source_id": "src-a",
            "kind": "note_attachment",
        },
    )
    session.rollback.assert_awaited_once()
    s3.delete_file.assert_awaited_once_with(attachment_id)


@pytest.mark.unit
def test_attachment_cascade_foreign_keys_and_indexes():
    from gulp.api.collab.note import GulpNoteAttachment

    foreign_keys = {
        fk.parent.name: (fk.target_fullname, fk.ondelete)
        for fk in GulpNoteAttachment.__table__.foreign_keys
    }
    indexes = {index.name for index in GulpNoteAttachment.__table__.indexes}

    print("attachment foreign keys:", foreign_keys)
    assert foreign_keys == {
        "note_id": ("note.id", "CASCADE"),
        "user_id": ("user.id", "CASCADE"),
        "operation_id": ("operation.id", "CASCADE"),
    }
    assert indexes == {
        "idx_note_attachment_note",
        "idx_note_attachment_user",
        "idx_note_attachment_operation",
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_note_explicit_and_bulk_delete_cleanup_objects(monkeypatch):
    from gulp.api.collab.note import GulpNote, GulpNoteAttachment
    from gulp.api.collab.structs import GulpCollabBase

    delete_objects = AsyncMock()
    base_delete = AsyncMock()
    base_bulk_delete = AsyncMock(return_value=2)
    monkeypatch.setattr(GulpNoteAttachment, "delete_objects", delete_objects)
    monkeypatch.setattr(GulpCollabBase, "delete", base_delete)
    monkeypatch.setattr(GulpCollabBase, "delete_by_filter", base_bulk_delete)

    note = GulpNote(
        id="note-a",
        type="note",
        user_id="user-a",
        name="note",
        time_created=1,
        time_updated=1,
        operation_id="op-a",
        glyph_id=None,
        description=None,
        tags=[],
        color=None,
        granted_user_ids=[],
        granted_user_group_ids=[],
        context_id="ctx-a",
        source_id="src-a",
        text="text",
        text_id=None,
        doc=None,
        doc_id=None,
        time_pin=1,
        last_editor_id=None,
    )
    note.attachments.append(
        GulpNoteAttachment(
            id="attachment-a",
            note_id="note-a",
            user_id="user-a",
            operation_id="op-a",
            title="evidence.bin",
            description="",
            mime_type="application/octet-stream",
        )
    )
    await GulpNote.delete(note, SimpleNamespace())
    delete_objects.assert_awaited_once_with(["attachment-a"])
    base_delete.assert_awaited_once()

    delete_objects.reset_mock()
    monkeypatch.setattr(
        GulpNote,
        "get_by_filter",
        AsyncMock(
            return_value=[
                SimpleNamespace(attachments=[SimpleNamespace(id="attachment-a")]),
                SimpleNamespace(attachments=[SimpleNamespace(id="attachment-b")]),
            ]
        ),
    )
    assert await GulpNote.delete_by_filter(SimpleNamespace()) == 2
    delete_objects.assert_awaited_once_with(["attachment-a", "attachment-b"])


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parent_cascades_cleanup_attachment_tags(monkeypatch):
    from gulp.api.collab.context import GulpContext
    from gulp.api.collab.mapping_parameters import GulpMappingParametersEntry
    from gulp.api.collab.note import GulpNoteAttachment
    from gulp.api.collab.operation import GulpOperation
    from gulp.api.collab.source import GulpSource
    from gulp.api.collab.structs import GulpCollabBase
    from gulp.api.collab.user import GulpUser

    common = {
        "user_id": "user-a",
        "name": "name",
        "time_created": 1,
        "time_updated": 1,
        "glyph_id": None,
        "description": None,
        "tags": [],
        "color": None,
        "granted_user_ids": [],
        "granted_user_group_ids": [],
    }
    operation = GulpOperation(
        id="op-a",
        type="operation",
        operation_id=None,
        index="op-a",
        **common,
    )
    objects = [
        (
            GulpContext(id="ctx-a", type="context", operation_id="op-a", **common),
            {"operation_id": "op-a", "context_id": "ctx-a"},
        ),
        (
            GulpSource(
                id="src-a",
                type="source",
                operation_id="op-a",
                context_id="ctx-a",
                mapping_parameters=None,
                **common,
            ),
            {
                "operation_id": "op-a",
                "context_id": "ctx-a",
                "source_id": "src-a",
            },
        ),
        (
            GulpUser(
                id="user-a",
                type="user",
                operation_id=None,
                pwd_hash="hash",
                **common,
            ),
            {"user_id": "user-a"},
        ),
    ]
    cleanup = AsyncMock()
    monkeypatch.setattr(GulpNoteAttachment, "delete_objects_by_tags", cleanup)
    monkeypatch.setattr(GulpCollabBase, "delete", AsyncMock())
    monkeypatch.setattr(GulpMappingParametersEntry, "delete_orphaned", AsyncMock())

    await operation.delete(SimpleNamespace(), delete_data=False)
    assert cleanup.await_args.args == ({"operation_id": "op-a"},)

    for obj, expected_tags in objects:
        await obj.delete(SimpleNamespace())
        assert cleanup.await_args.args == (expected_tags,)

    print("parent cascade cleanup tags:", cleanup.await_args_list)
    assert cleanup.await_count == 4
