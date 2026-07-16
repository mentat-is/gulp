"""Unit coverage for operation deletion websocket metadata."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab_api import GulpCollab
from gulp.api.ws_api import GulpRedisBroker, WSDATA_COLLAB_DELETE

GulpCollab.init_mappers()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_operation_delete_uses_deleted_id_as_envelope_operation_id(monkeypatch):
    operation = GulpOperation(
        id="operation-a",
        type="operation",
        user_id="admin",
        name="operation-a",
        time_created=0,
        time_updated=0,
        operation_id=None,
        glyph_id=None,
        description=None,
        tags=[],
        color=None,
        granted_user_ids=[],
        granted_user_group_ids=[],
        index="operation-a",
        operation_data={},
    )
    session = MagicMock()
    session.delete = AsyncMock()
    session.commit = AsyncMock()
    broker = MagicMock()
    broker.put = AsyncMock()

    @asynccontextmanager
    async def _advisory_lock(_session, _object_id):
        yield

    monkeypatch.setattr(
        GulpOperation,
        "advisory_lock",
        staticmethod(_advisory_lock),
    )
    monkeypatch.setattr(
        GulpRedisBroker,
        "get_instance",
        staticmethod(lambda: broker),
    )

    await operation.delete(
        session,
        ws_id="request-ws",
        req_id="request-a",
        user_id="admin",
    )

    session.delete.assert_awaited_once_with(operation)
    session.commit.assert_awaited_once()
    broker.put.assert_awaited_once_with(
        t=WSDATA_COLLAB_DELETE,
        ws_id="request-ws",
        user_id="admin",
        operation_id="operation-a",
        req_id="request-a",
        d={"type": "operation", "id": "operation-a"},
    )
