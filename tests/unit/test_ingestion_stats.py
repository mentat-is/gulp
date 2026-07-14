from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gulp.api.collab.stats import (
    GulpIngestionStats,
    GulpRequestStats,
    RequestStatsType,
)
from gulp.api.collab_api import GulpCollab


@asynccontextmanager
async def _noop_advisory_lock(_sess, _obj_id):
    yield


@pytest.mark.unit
@pytest.mark.asyncio
async def test_duplicate_ingestion_source_commits_without_expiring_stats(monkeypatch):
    GulpCollab.init_mappers()
    source = ("ctx-1", "context-1", "src-1", "source-1")
    stats = GulpRequestStats(
        id="req-1",
        type="request_stats",
        user_id="user-1",
        name="req-1",
        time_created=0,
        time_updated=0,
        operation_id="op-1",
        glyph_id=None,
        description=None,
        tags=[],
        color=None,
        granted_user_ids=[],
        granted_user_group_ids=[],
        server_id="server-1",
        req_type=RequestStatsType.REQUEST_TYPE_INGESTION.value,
        data=GulpIngestionStats(sources=[source]).model_dump(mode="json"),
    )
    sess = SimpleNamespace(
        refresh=AsyncMock(), commit=AsyncMock(), rollback=AsyncMock()
    )
    monkeypatch.setattr(
        GulpRequestStats,
        "advisory_lock",
        classmethod(lambda cls, sess, obj_id: _noop_advisory_lock(sess, obj_id)),
    )

    result = await stats.add_ingestion_source(
        sess,
        source[0],
        source[2],
        context_name=source[1],
        source_name=source[3],
    )

    assert result["data"]["sources"] == [list(source)]
    sess.commit.assert_awaited_once()
    sess.rollback.assert_not_awaited()
