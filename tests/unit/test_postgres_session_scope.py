"""PostgreSQL sessions are released before slow external service calls."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gulp.api.server import db, operation, storage


class _TrackedCollab:
    def __init__(self):
        self.active = False
        self.sessions = 0
        self.init = AsyncMock()

    @asynccontextmanager
    async def session(self):
        assert self.active is False
        self.active = True
        self.sessions += 1
        try:
            yield _TrackedSession(self)
        finally:
            self.active = False


class _TrackedSession:
    def __init__(self, collab: _TrackedCollab):
        self.collab = collab

    async def commit(self) -> None:
        assert self.collab.active is True
        self.collab.active = False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_storage_listing_releases_session_before_minio(monkeypatch):
    collab = _TrackedCollab()

    async def _list_by_tags(**_kwargs):
        assert collab.active is False
        return {"objects": []}

    s3 = SimpleNamespace(list_by_tags=AsyncMock(side_effect=_list_by_tags))
    monkeypatch.setattr(storage.GulpCollab, "get_instance", lambda: collab)
    monkeypatch.setattr(storage.GulpS3, "get_instance", lambda: s3)
    monkeypatch.setattr(
        storage.GulpOperation,
        "get_by_id_wrapper",
        AsyncMock(return_value=(None, None, None)),
    )

    response = await storage.storage_list_files_handler(
        "token", "operation", "context", None, 100, "request"
    )

    print(response)
    assert collab.sessions == 1
    s3.list_by_tags.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_db_reset_releases_session_before_external_cleanup(monkeypatch):
    collab = _TrackedCollab()

    async def _delete_external(*_args, **_kwargs):
        assert collab.active is False

    opensearch = SimpleNamespace(
        datastream_delete=AsyncMock(side_effect=_delete_external)
    )
    s3 = SimpleNamespace(delete_by_tags=AsyncMock(side_effect=_delete_external))
    monkeypatch.setattr(db.GulpCollab, "get_instance", lambda: collab)
    monkeypatch.setattr(
        db.GulpOperation,
        "get_by_filter",
        AsyncMock(return_value=[SimpleNamespace(id="operation", index="index")]),
    )
    monkeypatch.setattr(db.GulpOpenSearch, "get_instance", lambda: opensearch)
    monkeypatch.setattr(db.GulpS3, "get_instance", lambda: s3)

    await db.db_reset()

    assert collab.sessions == 1
    collab.init.assert_awaited_once_with(main_process=True, force_recreate=True)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_operation_delete_releases_session_before_external_cleanup(monkeypatch):
    collab = _TrackedCollab()
    authorized_operation = SimpleNamespace(index="index", delete=AsyncMock())
    user_session = SimpleNamespace(user=SimpleNamespace(id="user"))

    async def _delete_external(*_args, **_kwargs):
        assert collab.active is False

    monkeypatch.setattr(operation.GulpCollab, "get_instance", lambda: collab)
    monkeypatch.setattr(
        operation.GulpOperation,
        "get_by_id_wrapper",
        AsyncMock(return_value=(user_session, authorized_operation, None)),
    )
    monkeypatch.setattr(
        operation.GulpRequestStats,
        "get_by_filter",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        operation.GulpOpenSearch,
        "get_instance",
        lambda: SimpleNamespace(
            datastream_delete=AsyncMock(side_effect=_delete_external)
        ),
    )
    monkeypatch.setattr(
        operation.GulpS3,
        "get_instance",
        lambda: SimpleNamespace(delete_by_tags=AsyncMock(side_effect=_delete_external)),
    )
    monkeypatch.setattr(operation.events, "emit", AsyncMock())

    response = await operation.operation_delete_handler(
        "token", "operation", delete_data=True, force_delete=False, req_id="request"
    )

    print(response)
    assert collab.sessions == 1
    authorized_operation.delete.assert_awaited_once()
