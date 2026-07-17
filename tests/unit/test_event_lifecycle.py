"""Regression tests for core lifecycle event timing and metadata."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.collab.stats import SourceCanceledError
from gulp.api.collab.structs import GulpRequestStatus
from gulp.events import GulpCoreEvent, events
from gulp.plugin import GulpPluginBase, GulpPluginType


class _IngestionPlugin(GulpPluginBase):
    def display_name(self) -> str:
        return "event_lifecycle"

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION


@pytest.mark.unit
@pytest.mark.asyncio
async def test_logout_emits_logout_type_with_user_identity(monkeypatch):
    from gulp.api.collab.user import GulpUser

    session = MagicMock()
    user_session = MagicMock()
    user_session.id = "session-id"
    user_session.user.id = "user-id"
    user_session.delete = AsyncMock()
    emit = AsyncMock(return_value=None)
    monkeypatch.setattr(events, "emit", emit)

    await GulpUser.logout(
        session,
        user_session,
        ws_id="websocket",
        req_id="request",
        user_ip="192.0.2.1",
    )

    emit.assert_awaited_once()
    assert emit.await_args.args[0] == GulpCoreEvent.USER_LOGOUT
    assert emit.await_args.kwargs["user_id"] == "user-id"
    assert emit.await_args.kwargs["data"]["user_id"] == "user-id"
    assert emit.await_args.kwargs["data"]["login"] is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_operation_deleted_event_follows_metadata_delete_without_data_delete(
    monkeypatch,
):
    from gulp.api.collab.operation import GulpOperation
    from gulp.api.collab.stats import GulpRequestStats
    from gulp.api.collab_api import GulpCollab
    from gulp.api.server.operation import operation_delete_handler

    order: list[str] = []
    session = MagicMock()
    session.commit = AsyncMock()

    @asynccontextmanager
    async def session_context():
        yield session

    collab = MagicMock()
    collab.session = session_context
    monkeypatch.setattr(GulpCollab, "get_instance", staticmethod(lambda: collab))

    operation = MagicMock()
    operation.id = "operation-id"
    operation.index = "index-name"

    async def delete(*args, **kwargs):
        order.append("metadata_deleted")

    operation.delete = AsyncMock(side_effect=delete)
    caller = MagicMock()
    caller.user.id = "user-id"
    monkeypatch.setattr(
        GulpOperation,
        "get_by_id_wrapper",
        AsyncMock(return_value=(caller, operation, None)),
    )
    monkeypatch.setattr(GulpRequestStats, "get_by_filter", AsyncMock(return_value=[]))

    async def emit(*args, **kwargs):
        order.append("event_emitted")

    monkeypatch.setattr(events, "emit", AsyncMock(side_effect=emit))

    response = await operation_delete_handler(
        token="token",
        operation_id="operation-id",
        ws_id=None,
        delete_data=False,
        force_delete=True,
        req_id="request",
    )

    assert response["data"]["id"] == "operation-id"
    assert order == ["metadata_deleted", "event_emitted"]
    assert events.emit.await_args.args[0] == GulpCoreEvent.OPERATION_DELETED
    assert events.emit.await_args.kwargs["operation_id"] == "operation-id"
    assert events.emit.await_args.kwargs["data"] == {"index": "index-name"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_standalone_datastream_delete_emits_no_operation_event(monkeypatch):
    from gulp.api.collab.user_session import GulpUserSession
    from gulp.api.collab_api import GulpCollab
    from gulp.api.opensearch_api import GulpOpenSearch
    from gulp.api.server.db import opensearch_delete_index_handler

    session = MagicMock(commit=AsyncMock())

    @asynccontextmanager
    async def session_context():
        yield session

    collab = MagicMock()
    collab.session = session_context
    monkeypatch.setattr(GulpCollab, "get_instance", staticmethod(lambda: collab))
    caller = MagicMock()
    caller.user.id = "admin"
    monkeypatch.setattr(GulpUserSession, "check_token", AsyncMock(return_value=caller))
    opensearch = MagicMock()
    opensearch.datastream_delete = AsyncMock()
    monkeypatch.setattr(
        GulpOpenSearch, "get_instance", staticmethod(lambda: opensearch)
    )
    emit = AsyncMock()
    monkeypatch.setattr(events, "emit", emit)

    response = await opensearch_delete_index_handler(
        token="token",
        index="standalone-index",
        delete_operation=False,
        req_id="request",
    )

    assert response.status_code == 200
    session.commit.assert_awaited_once()
    opensearch.datastream_delete.assert_awaited_once()
    emit.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception, expected_status",
    [
        (None, GulpRequestStatus.DONE),
        (RuntimeError("ingestion failed"), GulpRequestStatus.FAILED),
        (SourceCanceledError("source canceled"), GulpRequestStatus.CANCELED),
    ],
)
async def test_source_finished_emits_once_for_each_terminal_status(
    monkeypatch, exception, expected_status
):
    from gulp import plugin as plugin_module
    from gulp.api.server_api import GulpServer

    plugin = _IngestionPlugin("/tmp/event_lifecycle.py", "event_lifecycle")
    plugin._sess = MagicMock()
    plugin._stats = MagicMock()
    plugin._stats.errors = []
    plugin._stats.status = GulpRequestStatus.ONGOING
    plugin._stats.update_ingestion_stats = AsyncMock(
        return_value={"status": expected_status.value}
    )
    plugin._flush_buffer_and_send_to_ws = AsyncMock(return_value=(0, 0, 0))
    plugin._user_id = "user"
    plugin._operation_id = "operation"
    plugin._context_id = "context"
    plugin._source_id = "source"
    plugin._req_id = "request"
    plugin._ws_id = "websocket"
    plugin._index = "index"
    plugin._file_path = "/tmp/source"

    broker = MagicMock()
    broker.put = AsyncMock()
    monkeypatch.setattr(
        plugin_module.GulpRedisBroker, "get_instance", staticmethod(lambda: broker)
    )
    opensearch = MagicMock()

    async def update_types(*args, **kwargs):
        return None

    opensearch.datastream_update_source_field_types_by_src = update_types
    monkeypatch.setattr(
        plugin_module.GulpOpenSearch,
        "get_instance",
        staticmethod(lambda: opensearch),
    )

    def close_background(coro, _name):
        coro.close()

    monkeypatch.setattr(GulpServer, "spawn_bg_task", staticmethod(close_background))
    emit = AsyncMock(side_effect=RuntimeError("delivery failed"))
    monkeypatch.setattr(events, "emit", emit)

    result = await plugin.update_final_stats_and_flush(ex=exception)
    await plugin._emit_source_finished_event(expected_status, [])

    assert result == expected_status
    emit.assert_awaited_once()
    assert emit.await_args.args[0] == GulpCoreEvent.SOURCE_FINISHED
    assert emit.await_args.kwargs["data"]["status"] == expected_status.value
