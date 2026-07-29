"""Integration tests for collaboration websocket notifications."""

import asyncio
import uuid

import pytest

from gulp_sdk import GulpClient, WSMessageType


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _create_operation(client: GulpClient, prefix: str) -> str:
    op = await client.operations.create(name=_unique(prefix))
    op_id = getattr(op, "id", None) or (op.get("id") if isinstance(op, dict) else None)
    if not op_id:
        raise RuntimeError("failed to create operation id")
    return str(op_id)


async def _create_context(client: GulpClient, operation_id: str) -> str:
    ctx = await client.operations.context_create(operation_id=operation_id, context_name=_unique("ctx"))
    ctx_id = getattr(ctx, "id", None) or (ctx.get("id") if isinstance(ctx, dict) else None)
    if not ctx_id:
        raise RuntimeError("failed to create context id")
    return str(ctx_id)


async def _create_source(client: GulpClient, operation_id: str, context_id: str) -> str:
    src = await client.operations.source_create(
        operation_id=operation_id,
        context_id=context_id,
        source_name=_unique("src"),
    )
    src_id = getattr(src, "id", None) or (src.get("id") if isinstance(src, dict) else None)
    if not src_id:
        raise RuntimeError("failed to create source id")
    return str(src_id)


async def _wait_for_event(
    client: GulpClient,
    message_type: WSMessageType,
    predicate,
    action,
    timeout: float = 15.0,
):
    event = asyncio.Event()
    received = []

    async def _handler(msg):
        if predicate(msg):
            received.append(msg)
            event.set()

    await client.register_ws_message_handler(message_type, _handler)
    try:
        await action()
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return received[0]
    finally:
        client.unregister_ws_message_handler(message_type, _handler)


@pytest.mark.integration
async def test_ws_collab_note_create_notification(gulp_base_url, gulp_test_user, gulp_test_password):
    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        await client.ensure_websocket()

        operation_id = await _create_operation(client, "ws_collab_create")
        context_id = await _create_context(client, operation_id)
        source_id = await _create_source(client, operation_id, context_id)
        note_name = "ws_note_create"
        created_note_id = None

        async def _do_create():
            nonlocal created_note_id
            note = await client.collab.note_create(
                operation_id=operation_id,
                context_id=context_id,
                source_id=source_id,
                name=note_name,
                text="note create event",
                time_pin=1_000_000_000,
                tags=["ws"],
            )
            created_note_id = note.get("id")

        try:
            msg = await _wait_for_event(
                client,
                WSMessageType.COLLAB_CREATE,
                lambda m: isinstance(m.data, dict)
                and isinstance(m.data.get("obj"), dict)
                and m.data["obj"].get("operation_id") == operation_id
                and m.data["obj"].get("type") == "note"
                and m.data["obj"].get("name") == note_name,
                _do_create,
            )
            print("note_create notification:", msg)
            assert msg.type == WSMessageType.COLLAB_CREATE.value
            assert created_note_id is not None
        finally:
            if created_note_id:
                await client.collab.note_delete(created_note_id)
            await client.operations.delete(operation_id)


@pytest.mark.integration
async def test_ws_collab_note_update_notification(gulp_base_url, gulp_test_user, gulp_test_password):
    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        await client.ensure_websocket()

        operation_id = await _create_operation(client, "ws_collab_update")
        context_id = await _create_context(client, operation_id)
        source_id = await _create_source(client, operation_id, context_id)
        note = await client.collab.note_create(
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            name="ws_note_update",
            text="before update",
            time_pin=1_000_000_000,
            tags=["ws"],
        )
        note_id = note.get("id")
        assert note_id

        async def _do_update():
            await client.collab.note_update(note_id, text="after update", color="#00ff00")

        try:
            msg = await _wait_for_event(
                client,
                WSMessageType.COLLAB_UPDATE,
                lambda m: isinstance(m.data, dict)
                and isinstance(m.data.get("obj"), dict)
                and m.data["obj"].get("id") == note_id,
                _do_update,
            )
            print("note_update notification:", msg)
            assert msg.type == WSMessageType.COLLAB_UPDATE.value
        finally:
            await client.collab.note_delete(note_id)
            await client.operations.delete(operation_id)


@pytest.mark.integration
async def test_ws_collab_note_delete_notification(gulp_base_url, gulp_test_user, gulp_test_password):
    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        await client.ensure_websocket()

        operation_id = await _create_operation(client, "ws_collab_delete")
        context_id = await _create_context(client, operation_id)
        source_id = await _create_source(client, operation_id, context_id)
        note = await client.collab.note_create(
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            name="ws_note_delete",
            text="to delete",
            time_pin=1_000_000_000,
            tags=["ws"],
        )
        note_id = note.get("id")
        assert note_id

        async def _do_delete():
            await client.collab.note_delete(note_id)

        try:
            msg = await _wait_for_event(
                client,
                WSMessageType.COLLAB_DELETE,
                lambda m: isinstance(m.data, dict) and m.data.get("id") == note_id,
                _do_delete,
            )
            print("note_delete notification:", msg)
            assert msg.type == WSMessageType.COLLAB_DELETE.value
        finally:
            await client.operations.delete(operation_id)


@pytest.mark.integration
async def test_ws_collab_note_bulk_delete_notification(
    gulp_base_url,
    gulp_test_user,
    gulp_test_password,
):
    """Bulk note deletion emits one coalesced notification with exact IDs."""
    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        await client.ensure_websocket()

        operation_id = await _create_operation(client, "ws_collab_bulk_delete")
        context_id = await _create_context(client, operation_id)
        source_id = await _create_source(client, operation_id, context_id)
        notes = []
        for ordinal in range(3):
            notes.append(
                await client.collab.note_create(
                    operation_id=operation_id,
                    context_id=context_id,
                    source_id=source_id,
                    name=f"ws_note_bulk_delete_{ordinal}",
                    text="to delete in bulk",
                    time_pin=1_000_000_000 + ordinal,
                    tags=["ws", "bulk"],
                )
            )

        deleted_ids = {notes[0]["id"], notes[1]["id"]}
        received = []
        event = asyncio.Event()

        async def _handler(msg):
            if (
                isinstance(msg.data, dict)
                and msg.data.get("type") == "note"
                and isinstance(msg.data.get("ids"), list)
            ):
                received.append(msg)
                event.set()

        await client.register_ws_message_handler(WSMessageType.COLLAB_DELETE, _handler)
        try:
            result = await client.plugins.object_delete_bulk(
                operation_id,
                "note",
                {
                    "ids": sorted(deleted_ids),
                    # The endpoint must ignore a conflicting body scope.
                    "operation_ids": ["different-operation"],
                },
            )
            assert result == {"deleted": 2}
            await asyncio.wait_for(event.wait(), timeout=15.0)
            await asyncio.sleep(0.2)

            assert len(received) == 1
            assert set(received[0].data["ids"]) == deleted_ids

            remaining_notes = await client.collab.note_list(
                operation_id=operation_id
            )
            remaining_ids = {note["id"] for note in remaining_notes}
            assert notes[2]["id"] in remaining_ids
            assert deleted_ids.isdisjoint(remaining_ids)
        finally:
            client.unregister_ws_message_handler(
                WSMessageType.COLLAB_DELETE,
                _handler,
            )
            await client.operations.delete(operation_id)


@pytest.mark.integration
async def test_ws_collab_highlight_create_notification(gulp_base_url, gulp_test_user, gulp_test_password):
    """Validate COLLAB_CREATE for a non-note collab object (highlight)."""
    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        await client.ensure_websocket()

        operation_id = await _create_operation(client, "ws_collab_highlight")
        highlight_id = None

        async def _do_create_highlight():
            nonlocal highlight_id
            highlight = await client.collab.highlight_create(
                operation_id=operation_id,
                time_range=[1_000_000_000, 1_000_000_001],
                tags=["ws"],
            )
            highlight_id = highlight.get("id")

        try:
            msg = await _wait_for_event(
                client,
                WSMessageType.COLLAB_CREATE,
                lambda m: isinstance(m.data, dict)
                and (
                    (
                        isinstance(m.data.get("obj"), dict)
                        and m.data["obj"].get("operation_id") == operation_id
                        and m.data["obj"].get("type") == "highlight"
                    )
                    or (
                        isinstance(m.data.get("obj"), list)
                        and any(
                            isinstance(item, dict)
                            and item.get("operation_id") == operation_id
                            and item.get("type") == "highlight"
                            for item in m.data["obj"]
                        )
                    )
                ),
                _do_create_highlight,
            )
            print("highlight_create notification:", msg)
            assert msg.type == WSMessageType.COLLAB_CREATE.value
            assert highlight_id is not None
        finally:
            if highlight_id:
                await client.collab.highlight_delete(highlight_id)
            await client.operations.delete(operation_id)
