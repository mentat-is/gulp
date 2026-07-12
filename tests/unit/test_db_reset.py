from unittest.mock import AsyncMock, MagicMock

import pytest


class _FakeSession:
    async def __aenter__(self):
        return "session"

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeCollab:
    def __init__(self):
        self.init = AsyncMock()

    def session(self):
        return _FakeSession()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_db_reset_skips_row_delete_before_schema_recreate(monkeypatch):
    from gulp.api.server import db

    op = MagicMock(id="op-a", index="idx-a")
    op.delete = AsyncMock()
    collab = _FakeCollab()
    opensearch = MagicMock(datastream_delete=AsyncMock())
    s3 = MagicMock(delete_by_tags=AsyncMock())

    monkeypatch.setattr(db.GulpCollab, "get_instance", lambda: collab)
    monkeypatch.setattr(db.GulpOperation, "get_by_filter", AsyncMock(return_value=[op]))
    monkeypatch.setattr(db.GulpOpenSearch, "get_instance", lambda: opensearch)
    monkeypatch.setattr(db.GulpS3, "get_instance", lambda: s3)

    await db.db_reset()

    opensearch.datastream_delete.assert_awaited_once_with("idx-a")
    s3.delete_by_tags.assert_awaited_once_with({"operation_id": "op-a"})
    op.delete.assert_not_awaited()
    collab.init.assert_awaited_once_with(main_process=True, force_recreate=True)
