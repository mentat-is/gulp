import asyncio
import time

import orjson
import pytest

from gulp.api.ws_api import (
    GulpConnectedSockets,
    GulpRedisBroker,
    GulpWsData,
    GulpWsType,
)


class _FakeRedisRaw:
    def __init__(self):
        self._data: dict[str, bytes] = {}

    async def set(self, key: str, value: bytes, ex: int | None = None):
        self._data[key] = value
        return True

    async def get(self, key: str):
        return self._data.get(key)

    async def delete(self, key: str):
        existed = 1 if key in self._data else 0
        self._data.pop(key, None)
        return existed


class _FakeRedisClient:
    def __init__(self):
        self._redis = _FakeRedisRaw()


@pytest.mark.unit
def test_redis_broker_broadcast_types_add_remove():
    broker = GulpRedisBroker.get_instance()
    original_types = set(broker.broadcast_types)

    try:
        broker.add_broadcast_type("unit_test_type")
        assert "unit_test_type" in broker.broadcast_types

        broker.remove_broadcast_type("unit_test_type")
        assert "unit_test_type" not in broker.broadcast_types

    finally:
        broker.broadcast_types = original_types


@pytest.mark.unit
def test_wsdata_check_type_in_broadcast_types():
    broker = GulpRedisBroker.get_instance()
    original_types = set(broker.broadcast_types)
    broker.broadcast_types = {"note", "unit_test_object"}

    try:
        # direct type broadcast
        data_one = GulpWsData(timestamp=123, type="note")
        assert data_one.check_type_in_broadcast_types()

        # payload object type broadcast
        data_two = GulpWsData(
            timestamp=124,
            type="other",
            payload={"obj": {"type": "unit_test_object"}},
        )
        assert data_two.check_type_in_broadcast_types()

        # non-broadcast type
        data_three = GulpWsData(timestamp=125, type="other")
        assert not data_three.check_type_in_broadcast_types()

    finally:
        broker.broadcast_types = original_types


@pytest.mark.unit
def test_gulp_connected_sockets_cache_and_queue_utilization():
    # reset singleton to avoid pollution
    GulpConnectedSockets._instance = None
    sockets = GulpConnectedSockets.get_instance()

    sockets._sockets = {}
    sockets._ws_server_cache = {}

    sockets.cache_server("ws-id-123", "server-a")
    assert sockets.get_cached_server("ws-id-123") == "server-a"

    sockets.invalidate_cached_server("ws-id-123")
    assert sockets.get_cached_server("ws-id-123") is None

    # negative cached server
    sockets.cache_server("ws-id-missing", None)
    assert sockets.get_cached_server("ws-id-missing") is None

    # a small fake socket supporting expected attributes
    class FakeSocket:
        def __init__(self, ws_id: str, use_capacity: int, capacity: int):
            self.ws_id = ws_id
            self.socket_type = GulpWsType.WS_DEFAULT
            self._queue_capacity = capacity
            self.q = asyncio.Queue(maxsize=capacity)
            for _ in range(use_capacity):
                self.q.put_nowait(1)

    sockets._sockets = {
        "a": FakeSocket("a", use_capacity=1, capacity=4),
        "b": FakeSocket("b", use_capacity=2, capacity=8),
    }

    # utilization = (1 + 2) / (4 + 8) = 0.25
    utilization = sockets.aggregate_queue_utilization()
    assert pytest.approx(utilization, rel=1e-6) == 0.25
    assert sockets.num_connected_sockets() == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_put_internal_event_wait_returns_response(monkeypatch):
    broker = GulpRedisBroker.get_instance()
    fake_redis = _FakeRedisClient()

    from gulp.api import ws_api as ws_api_module

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)

    async def _fake_put_common(wsd: GulpWsData) -> None:
        await fake_redis._redis.set(
            wsd.response_key,
            orjson.dumps({"ok": True, "event_type": wsd.type}),
            ex=300,
        )

    monkeypatch.setattr(broker, "_put_common", _fake_put_common)

    result = await broker.put_internal_event_wait(
        t="unit_wait_event",
        user_id="tester",
        timeout=0.5,
    )

    assert result == {"ok": True, "event_type": "unit_wait_event"}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_put_internal_event_wait_timeout(monkeypatch):
    broker = GulpRedisBroker.get_instance()
    fake_redis = _FakeRedisClient()

    from gulp.api import ws_api as ws_api_module

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)

    async def _fake_put_common(_wsd: GulpWsData) -> None:
        return

    monkeypatch.setattr(broker, "_put_common", _fake_put_common)

    with pytest.raises(TimeoutError):
        await broker.put_internal_event_wait(
            t="unit_wait_event_timeout",
            user_id="tester",
            timeout=0.05,
        )
