"""Unit coverage for coalesced collaboration bulk-delete notifications."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.collab.highlight import GulpHighlight
from gulp.api.collab.structs import GulpCollabBase, GulpCollabFilter
from gulp.api.collab_api import GulpCollab
from gulp.api.ws_api import GulpRedisBroker, WSDATA_COLLAB_DELETE

GulpCollab.init_mappers()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bulk_delete_publishes_one_packet_for_all_returned_ids(monkeypatch):
    """Publish one canonical-type packet containing every deleted row ID."""
    flt = GulpCollabFilter(operation_ids=["operation-a"])
    session = MagicMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    result = MagicMock()
    result.all.return_value = [
        ("highlight-a", "highlight", "operation-a"),
        ("highlight-b", "highlight", "operation-a"),
    ]
    session.execute.return_value = result
    broker = MagicMock()
    broker.put = AsyncMock()

    monkeypatch.setattr(
        GulpCollabBase,
        "_restrict_flt_to_user",
        AsyncMock(return_value=flt),
    )
    monkeypatch.setattr(
        GulpRedisBroker,
        "get_instance",
        staticmethod(lambda: broker),
    )

    deleted = await GulpHighlight.delete_by_filter(
        session,
        flt,
        user_id="user-a",
        ws_id="ws-a",
        req_id="request-a",
        operation_id="operation-a",
    )

    assert deleted == 2
    session.commit.assert_awaited_once()
    session.rollback.assert_not_awaited()
    broker.put.assert_awaited_once_with(
        t=WSDATA_COLLAB_DELETE,
        ws_id="ws-a",
        user_id="user-a",
        operation_id="operation-a",
        req_id="request-a",
        d={
            "type": "highlight",
            "ids": ["highlight-a", "highlight-b"],
        },
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bulk_delete_without_websocket_keeps_count_only_path(monkeypatch):
    """Avoid materializing IDs or publishing for internal cleanup callers."""
    flt = GulpCollabFilter(operation_ids=["operation-a"])
    session = MagicMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    result = MagicMock()
    result.rowcount = 3
    session.execute.return_value = result
    broker = MagicMock()
    broker.put = AsyncMock()

    monkeypatch.setattr(
        GulpCollabBase,
        "_restrict_flt_to_user",
        AsyncMock(return_value=flt),
    )
    monkeypatch.setattr(
        GulpRedisBroker,
        "get_instance",
        staticmethod(lambda: broker),
    )

    deleted = await GulpHighlight.delete_by_filter(
        session,
        flt,
        user_id="user-a",
    )

    assert deleted == 3
    session.commit.assert_awaited_once()
    result.all.assert_not_called()
    broker.put.assert_not_awaited()
