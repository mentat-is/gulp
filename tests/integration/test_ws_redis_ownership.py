"""Integration coverage for Redis-backed websocket ownership metadata."""

from uuid import uuid4

import pytest

from gulp.api.redis_api import GulpRedis
from gulp.api.ws_api import GulpWsType


@pytest.mark.integration
async def test_ws_unregister_preserves_reconnected_owner_with_real_redis(
    monkeypatch: pytest.MonkeyPatch,
):
    """A stale old-owner unregister must not erase a newer same-ws_id owner."""
    suffix = uuid4().hex
    ws_id = f"ws-reconnect-{suffix}"
    old_server_id = f"ws-owner-old-{suffix}"
    new_server_id = f"ws-owner-new-{suffix}"

    monkeypatch.setattr(GulpRedis, "_instance", None)
    old_owner = GulpRedis.get_instance()
    try:
        old_owner.initialize(server_id=old_server_id, main_process=False)
        raw_redis = old_owner.client()
        await raw_redis.ping()
    except Exception as exc:
        try:
            if old_owner._redis:
                await old_owner.shutdown()
        except Exception:
            pass
        GulpRedis._instance = None
        pytest.skip(f"Redis is not available through current gULP config: {exc}")

    new_owner = object.__new__(GulpRedis)
    GulpRedis.__init__(new_owner)
    new_owner._redis = raw_redis
    new_owner.server_id = new_server_id

    old_metadata_key = old_owner._get_ws_metadata_key_for_server(old_server_id, ws_id)
    new_metadata_key = old_owner._get_ws_metadata_key_for_server(new_server_id, ws_id)
    lookup_key = old_owner._get_ws_lookup_key(ws_id)

    try:
        await old_owner.ws_register(
            ws_id=ws_id,
            types=["stats_update"],
            operation_ids=["op-old"],
            socket_type=GulpWsType.WS_DEFAULT.value,
        )
        assert await old_owner.ws_get_server(ws_id) == old_server_id

        await new_owner.ws_register(
            ws_id=ws_id,
            types=["stats_update"],
            operation_ids=["op-new"],
            socket_type=GulpWsType.WS_DEFAULT.value,
        )
        assert await old_owner.ws_get_server(ws_id) == new_server_id

        assert await old_owner.ws_unregister(ws_id) is True
        assert await raw_redis.get(old_metadata_key) is None
        assert await raw_redis.get(new_metadata_key) is not None
        assert (await raw_redis.get(lookup_key)).decode() == new_server_id

        assert await new_owner.ws_unregister(ws_id) is True
        assert await raw_redis.get(new_metadata_key) is None
        assert await raw_redis.get(lookup_key) is None
    finally:
        await raw_redis.delete(old_metadata_key, new_metadata_key, lookup_key)
        await old_owner.shutdown()
        GulpRedis._instance = None
