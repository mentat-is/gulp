"""Redis event-listener mask lifecycle regressions."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gulp.api.redis_api import GulpRedis
from gulp.events import events


@pytest.mark.unit
@pytest.mark.asyncio
async def test_event_listener_mask_expires_and_tracks_heartbeat(monkeypatch):
    store = SimpleNamespace(set=AsyncMock())
    redis = SimpleNamespace(
        server_id="server",
        HEARTBEAT_TTL=30,
        client=lambda: store,
    )
    monkeypatch.setattr(GulpRedis, "get_instance", lambda: redis)

    await events._store_chunk_mask(events.CHUNK_PRE_INGEST)

    key = events._mask_key("server")
    print("event listener mask key:", key)
    store.set.assert_awaited_once_with(
        key,
        events.CHUNK_PRE_INGEST,
        ex=30,
    )

    redis_client = object.__new__(GulpRedis)
    GulpRedis.__init__(redis_client)
    redis_client.server_id = "server"
    redis_client._redis = SimpleNamespace(set=AsyncMock(), expire=AsyncMock())

    await redis_client._update_heartbeat()

    redis_client._redis.expire.assert_awaited_once_with(key, redis_client.HEARTBEAT_TTL)
