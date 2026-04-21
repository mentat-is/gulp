import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest

from gulp.api.ws_api import (
    GulpConnectedSockets,
    GulpMessageRoutingTarget,
    GulpRedis,
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


class _FakeConnectedSocket:
    def __init__(self, ws_id: str, socket_type: GulpWsType = GulpWsType.WS_DEFAULT):
        self.ws_id = ws_id
        self.socket_type = socket_type
        self.types = None
        self.operation_ids = None
        self.enqueue_message = AsyncMock(return_value=True)


class _FakePubSub:
    def __init__(self):
        self.channels: dict[str, bool] = {}
        self.patterns: dict[str, bool] = {}
        self.subscribe_calls = 0
        self.unsubscribe_calls = 0
        self.closed = False

    async def subscribe(self, *channels: str):
        self.subscribe_calls += 1
        for channel in channels:
            self.channels[channel] = True

    async def unsubscribe(self, *channels: str):
        self.unsubscribe_calls += 1
        for channel in channels:
            self.channels.pop(channel, None)

    async def get_message(self, *args, **kwargs):
        await asyncio.Future()

    async def close(self):
        self.closed = True
        self.channels.clear()
        self.patterns.clear()


class _FakeRedisPubSubClient:
    def __init__(self):
        self.pubsubs: list[_FakePubSub] = []

    def pubsub(self):
        pubsub = _FakePubSub()
        self.pubsubs.append(pubsub)
        return pubsub


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
@pytest.mark.asyncio
async def test_gulp_redis_subscribe_and_broker_lifecycle_are_idempotent(monkeypatch):
    GulpRedis._instance = None
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeRedisPubSubClient()
    redis_client._redis = fake_redis
    redis_client._pubsub = fake_redis.pubsub()
    redis_client.server_id = "server-a"

    async def _callback(_message: dict) -> None:
        return

    await redis_client.subscribe(_callback)
    first_task = redis_client._subscriber_task
    first_pubsub = redis_client._pubsub
    await asyncio.sleep(0)

    await redis_client.subscribe(_callback)

    assert redis_client._subscriber_task is first_task
    assert redis_client._pubsub is first_pubsub
    assert first_pubsub.subscribe_calls == 1

    await redis_client.unsubscribe()
    await redis_client.unsubscribe()

    assert first_task.done()
    assert first_task.exception() is None
    assert first_pubsub.unsubscribe_calls == 1
    assert first_pubsub.closed is True
    assert redis_client._subscriber_task is None
    assert redis_client._pubsub is None

    GulpRedisBroker._instance = None
    broker = GulpRedisBroker.get_instance()
    subscribe_mock = AsyncMock()
    unsubscribe_mock = AsyncMock()
    monkeypatch.setattr(redis_client, "subscribe", subscribe_mock)
    monkeypatch.setattr(redis_client, "unsubscribe", unsubscribe_mock)
    monkeypatch.setattr(
        "gulp.api.ws_api.GulpRedis.get_instance",
        lambda: redis_client,
    )

    await broker.initialize()
    await broker.initialize()
    await broker.shutdown()
    await broker.shutdown()

    subscribe_mock.assert_awaited_once()
    unsubscribe_mock.assert_awaited_once()


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
            orjson.dumps(
                {
                    "plugins": [],
                    "event": wsd.type,
                    "result": {"ok": True, "event_type": wsd.type},
                    "stop": False,
                }
            ),
            ex=300,
        )

    monkeypatch.setattr(broker, "_put_common", _fake_put_common)

    result = await broker.put_internal_event_wait(
        t="unit_wait_event",
        user_id="tester",
        timeout=0.5,
    )

    assert result.plugins == []
    assert result.event == "unit_wait_event"
    assert result.result == {"ok": True, "event_type": "unit_wait_event"}
    assert result.stop is False


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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_route_message_to_local_websockets_targets_matching_ws_only():
    GulpConnectedSockets._instance = None
    sockets = GulpConnectedSockets.get_instance()

    target_socket = _FakeConnectedSocket("target-ws")
    other_socket = _FakeConnectedSocket("other-ws")
    sockets._sockets = {
        target_socket.ws_id: target_socket,
        other_socket.ws_id: other_socket,
    }

    wsd = GulpWsData(timestamp=1, type="docs_chunk", ws_id="target-ws")
    await sockets.route_message_to_local_websockets(
        wsd,
        wsd.model_dump(exclude_none=True),
    )

    target_socket.enqueue_message.assert_awaited_once()
    other_socket.enqueue_message.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_route_message_to_local_websockets_keeps_broadcast_fanout_with_ws_id():
    GulpConnectedSockets._instance = None
    sockets = GulpConnectedSockets.get_instance()

    first_socket = _FakeConnectedSocket("target-ws")
    second_socket = _FakeConnectedSocket("other-ws")
    sockets._sockets = {
        first_socket.ws_id: first_socket,
        second_socket.ws_id: second_socket,
    }

    wsd = GulpWsData(timestamp=1, type="note", ws_id="target-ws")
    await sockets.route_message_to_local_websockets(
        wsd,
        wsd.model_dump(exclude_none=True),
    )

    first_socket.enqueue_message.assert_awaited_once()
    second_socket.enqueue_message.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_propagated_internal_event_rebroadcasts_once_and_skips_origin_echo(
    monkeypatch,
):
    from gulp.api import ws_api as ws_api_module
    import gulp.structs as plugin_module

    dispatch = AsyncMock(return_value={"ok": True})
    mock_mgr = MagicMock()
    mock_mgr.dispatch_internal_event = dispatch

    fake_redis = MagicMock()
    fake_redis.server_id = "instance-a"
    fake_redis.publish = AsyncMock()
    fake_redis.worker_to_main_channel = (
        lambda server_id=None: "gulpredis:server:instance-a"
    )
    fake_redis._redis = MagicMock()
    fake_redis._redis.set = AsyncMock(return_value=True)

    mock_sockets = MagicMock()
    mock_sockets.route_message_to_local_websockets = AsyncMock()

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        ws_api_module.GulpConnectedSockets, "get_instance", lambda: mock_sockets
    )
    monkeypatch.setattr(
        plugin_module.GulpInternalEventsManager,
        "get_instance",
        staticmethod(lambda: mock_mgr),
    )

    GulpRedisBroker._instance = None
    broker = GulpRedisBroker.get_instance()

    local_delivery = {
        "@timestamp": 1,
        "type": "propagated_event",
        "internal": True,
        "propagate_internal": True,
        "route_target_type": GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
        "origin_server_id": "instance-a",
        "__redis_channel__": fake_redis.worker_to_main_channel(),
        "payload": {"value": 1},
    }
    await broker._handle_pubsub_message(dict(local_delivery))

    dispatch.assert_awaited_once()
    fake_redis.publish.assert_awaited_once()
    publish_args = fake_redis.publish.await_args
    assert (
        publish_args.kwargs["channel"]
        == GulpMessageRoutingTarget.WORKER_TO_MAIN.redis_channel_name()
    )
    assert (
        publish_args.args[0]["route_target_type"]
        == GulpMessageRoutingTarget.WORKER_TO_MAIN.value
    )
    assert publish_args.args[0]["origin_server_id"] == "instance-a"
    assert publish_args.args[0]["origin_already_processed"] is True

    cluster_echo = dict(publish_args.args[0])
    cluster_echo["__redis_channel__"] = (
        GulpMessageRoutingTarget.WORKER_TO_MAIN.redis_channel_name()
    )
    await broker._handle_pubsub_message(dict(cluster_echo))

    dispatch.assert_awaited_once()
    mock_sockets.route_message_to_local_websockets.assert_not_awaited()
