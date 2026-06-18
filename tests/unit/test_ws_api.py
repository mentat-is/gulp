import asyncio
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest

from gulp.api.ws_api import (
    GulpConnectedSocket,
    GulpConnectedSockets,
    GulpMessageRoutingTarget,
    GulpRedis,
    GulpRedisBroker,
    GulpWsData,
    GulpWsType,
)
from gulp.api.redis_api import GulpWsMetadata


class _FakeRedisRaw:
    def __init__(self):
        self._data: dict[str, bytes] = {}
        self.published: list[tuple[str, bytes]] = []
        self.set_calls: list[tuple[str, bytes, int | None]] = []
        self.pipeline_transactions: list[bool] = []
        self.eval_error: Exception | None = None

    async def set(self, key: str, value: bytes, ex: int | None = None):
        self.set_calls.append((key, value, ex))
        self._data[key] = value
        return True

    async def get(self, key: str):
        return self._data.get(key)

    async def delete(self, key: str):
        existed = 1 if key in self._data else 0
        self._data.pop(key, None)
        return existed

    async def publish(self, channel: str, payload: bytes):
        self.published.append((channel, payload))
        return 1

    def pipeline(self, transaction: bool = True):
        self.pipeline_transactions.append(transaction)
        return _FakeRedisPipeline(self)

    async def eval(self, script: str, numkeys: int, *keys: str):
        if self.eval_error:
            raise self.eval_error

        values = []
        for key in keys[:numkeys]:
            value = self._data.get(key)
            if value is None:
                return None
            values.append(value)

        if "DEL" in script:
            for key in keys[:numkeys]:
                self._data.pop(key, None)

        return b"".join(values)


class _FakeRedisPipeline:
    def __init__(self, redis_raw: _FakeRedisRaw):
        self._redis_raw = redis_raw
        self._commands: list[tuple[str, tuple]] = []

    def set(self, key: str, value: bytes, ex: int | None = None):
        self._commands.append(("set", (key, value, ex)))
        return self

    def publish(self, channel: str, payload: bytes):
        self._commands.append(("publish", (channel, payload)))
        return self

    async def execute(self):
        results = []
        for command, args in self._commands:
            if command == "set":
                results.append(await self._redis_raw.set(*args))
            elif command == "publish":
                results.append(await self._redis_raw.publish(*args))
        return results


class _FakeRedisClient:
    def __init__(self):
        self._redis = _FakeRedisRaw()
        self.server_id = "server-a"

    async def ws_get_server(self, _ws_id: str):
        return None

    def worker_to_main_channel(self, server_id: str | None = None) -> str:
        return f"gulpredis:server:{server_id or self.server_id}"


class _FakeConnectedSocket:
    def __init__(
        self,
        ws_id: str,
        socket_type: GulpWsType = GulpWsType.WS_DEFAULT,
        user_id: str = "user-a",
    ):
        self.ws_id = ws_id
        self.socket_type = socket_type
        self.user_id = user_id
        self.types = None
        self.operation_ids = None
        self.enqueue_message = AsyncMock(return_value=True)


class _FakeConfig:
    def __init__(
        self,
        ws_queue_max_size: int = 1,
        ws_enqueue_timeout: float = 0.001,
        prometheus_enabled: bool = True,
        redis_compression_threshold: int = 1024,
        redis_pubsub_max_chunk_size: int = 128,
        redis_compression_enabled: bool = True,
    ):
        self._ws_queue_max_size = ws_queue_max_size
        self._ws_enqueue_timeout = ws_enqueue_timeout
        self._prometheus_enabled = prometheus_enabled
        self._redis_compression_threshold = redis_compression_threshold
        self._redis_pubsub_max_chunk_size = redis_pubsub_max_chunk_size
        self._redis_compression_enabled = redis_compression_enabled

    def ws_queue_max_size(self) -> int:
        return self._ws_queue_max_size

    def ws_enqueue_timeout(self) -> float:
        return self._ws_enqueue_timeout

    def ws_throttle_threshold(self) -> float:
        return 2.0

    def ws_throttle_max_delay(self) -> float:
        return 0.0

    def prometheus_enabled(self) -> bool:
        return self._prometheus_enabled

    def redis_compression_threshold(self) -> int:
        return self._redis_compression_threshold

    def redis_pubsub_max_chunk_size(self) -> int:
        return self._redis_pubsub_max_chunk_size

    def redis_compression_enabled(self) -> bool:
        return self._redis_compression_enabled


class _FakeMetricChild:
    def __init__(self):
        self.value = 0

    def inc(self, amount: int = 1):
        self.value += amount


class _FakeCounter:
    def __init__(self):
        self.value = 0
        self.children: dict[tuple[tuple[str, str], ...], _FakeMetricChild] = {}

    def inc(self, amount: int = 1):
        self.value += amount

    def labels(self, **labels: str) -> _FakeMetricChild:
        key = tuple(sorted(labels.items()))
        if key not in self.children:
            self.children[key] = _FakeMetricChild()
        return self.children[key]


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

    # missing owners are not cached; a future reconnect should be visible immediately
    sockets.cache_server("ws-id-missing", None)
    assert sockets.get_cached_server("ws-id-missing") is None
    assert "ws-id-missing" not in sockets._ws_server_cache

    # a small fake socket supporting expected attributes
    class FakeSocket:
        def __init__(
            self,
            ws_id: str,
            use_capacity: int,
            capacity: int,
            socket_type: GulpWsType,
            high_watermark: int,
        ):
            self.ws_id = ws_id
            self.socket_type = socket_type
            self._queue_capacity = capacity
            self._queue_high_watermark = high_watermark
            self.q = asyncio.Queue(maxsize=capacity)
            for _ in range(use_capacity):
                self.q.put_nowait(1)

    sockets._sockets = {
        "a": FakeSocket(
            "a",
            use_capacity=1,
            capacity=4,
            socket_type=GulpWsType.WS_DEFAULT,
            high_watermark=3,
        ),
        "b": FakeSocket(
            "b",
            use_capacity=2,
            capacity=8,
            socket_type=GulpWsType.WS_INGEST,
            high_watermark=5,
        ),
    }

    # utilization = (1 + 2) / (4 + 8) = 0.25
    utilization = sockets.aggregate_queue_utilization()
    assert pytest.approx(utilization, rel=1e-6) == 0.25
    assert sockets.num_connected_sockets() == 1
    assert sockets.num_connected_sockets(default_sockets_only=False) == 2
    assert sockets.socket_counts_by_type() == {
        GulpWsType.WS_DEFAULT.value: 1,
        GulpWsType.WS_INGEST.value: 1,
    }
    assert sockets.queue_high_watermarks_by_type() == {
        GulpWsType.WS_DEFAULT.value: 3,
        GulpWsType.WS_INGEST.value: 5,
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connected_socket_enqueue_timeout_records_metric(monkeypatch):
    from gulp.api import ws_api as ws_api_module
    import gulp.process as process_module

    fake_counter = _FakeCounter()
    fake_process = MagicMock()
    fake_process.server_id = "server-a"

    monkeypatch.setattr(
        ws_api_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: _FakeConfig()),
    )
    monkeypatch.setattr(
        process_module.GulpProcess,
        "get_instance",
        staticmethod(lambda: fake_process),
    )
    monkeypatch.setattr(
        ws_api_module.GulpMetrics,
        "ws_enqueue_timeout_total",
        fake_counter,
    )

    connected_socket = GulpConnectedSocket(
        ws=MagicMock(),
        ws_id="ws-timeout",
        socket_type=GulpWsType.WS_INGEST,
    )
    connected_socket.q.put_nowait({"already": "queued"})

    assert await connected_socket.enqueue_message({"next": "message"}) is False

    labels = (
        ("server_id", "server-a"),
        ("socket_type", GulpWsType.WS_INGEST.value),
    )
    assert fake_counter.children[labels].value == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_gulp_redis_ws_get_metadata_uses_owner_server_lookup():
    GulpRedis._instance = None
    redis_client = GulpRedis.get_instance()
    redis_client._redis = _FakeRedisRaw()
    redis_client.server_id = "server-local"

    metadata = GulpWsMetadata(
        ws_id="ws-remote",
        server_id="server-remote",
        types=["stats_update"],
        operation_ids=["op-1"],
        socket_type=GulpWsType.WS_DEFAULT.value,
    )

    await redis_client._redis.set(
        redis_client._get_ws_lookup_key("ws-remote"),
        b"server-remote",
    )
    await redis_client._redis.set(
        redis_client._get_ws_metadata_key_for_server("server-remote", "ws-remote"),
        orjson.dumps(metadata.model_dump()),
    )

    result = await redis_client.ws_get_metadata("ws-remote")

    assert result == metadata


@pytest.mark.unit
@pytest.mark.asyncio
async def test_gulp_redis_ws_unregister_preserves_new_remote_owner():
    GulpRedis._instance = None
    redis_client = GulpRedis.get_instance()
    redis_client._redis = _FakeRedisRaw()
    redis_client.server_id = "server-old"
    ws_id = "ws-reconnected"
    old_metadata_key = redis_client._get_ws_metadata_key_for_server(
        "server-old", ws_id
    )
    new_metadata_key = redis_client._get_ws_metadata_key_for_server(
        "server-new", ws_id
    )
    lookup_key = redis_client._get_ws_lookup_key(ws_id)

    await redis_client._redis.set(old_metadata_key, b"old")
    await redis_client._redis.set(new_metadata_key, b"new")
    await redis_client._redis.set(lookup_key, b"server-new")

    assert await redis_client.ws_unregister(ws_id) is True

    assert await redis_client._redis.get(old_metadata_key) is None
    assert await redis_client._redis.get(new_metadata_key) == b"new"
    assert await redis_client._redis.get(lookup_key) == b"server-new"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_gulp_redis_ws_unregister_removes_lookup_when_still_owner():
    GulpRedis._instance = None
    redis_client = GulpRedis.get_instance()
    redis_client._redis = _FakeRedisRaw()
    redis_client.server_id = "server-owner"
    ws_id = "ws-owned"
    metadata_key = redis_client._get_ws_metadata_key(ws_id)
    lookup_key = redis_client._get_ws_lookup_key(ws_id)

    await redis_client._redis.set(metadata_key, b"owned")
    await redis_client._redis.set(lookup_key, b"server-owner")

    assert await redis_client.ws_unregister(ws_id) is True

    assert await redis_client._redis.get(metadata_key) is None
    assert await redis_client._redis.get(lookup_key) is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_connected_sockets_remove_ignores_stale_same_id_cleanup(monkeypatch):
    from gulp.api import ws_api as ws_api_module

    sockets = GulpConnectedSockets()
    old_socket = _FakeConnectedSocket("ws-reconnected")
    old_socket.ws = MagicMock(name="old-websocket")
    new_socket = _FakeConnectedSocket("ws-reconnected")
    new_socket.ws = MagicMock(name="new-websocket")
    fake_redis = MagicMock()
    fake_redis.ws_unregister = AsyncMock()

    sockets._sockets = {"ws-reconnected": new_socket}

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        ws_api_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: _FakeConfig(prometheus_enabled=False)),
    )

    await sockets.remove("ws-reconnected", expected_socket=old_socket)

    assert sockets.get("ws-reconnected") is new_socket
    fake_redis.ws_unregister.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_gulp_redis_publish_records_route_target_metrics(monkeypatch):
    from gulp.api import redis_api as redis_api_module

    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeRedisRaw()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"

    publish_total = _FakeCounter()
    publish_bytes_total = _FakeCounter()
    publish_by_route = _FakeCounter()
    publish_bytes_by_route = _FakeCounter()
    targeted_total = _FakeCounter()

    async def _no_backpressure() -> None:
        return

    monkeypatch.setattr(
        redis_api_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: _FakeConfig()),
    )
    monkeypatch.setattr(redis_client, "_apply_publish_backpressure", _no_backpressure)
    monkeypatch.setattr(redis_api_module.GulpMetrics, "redis_publish_total", publish_total)
    monkeypatch.setattr(
        redis_api_module.GulpMetrics,
        "redis_publish_bytes_total",
        publish_bytes_total,
    )
    monkeypatch.setattr(
        redis_api_module.GulpMetrics,
        "redis_publish_by_route_total",
        publish_by_route,
    )
    monkeypatch.setattr(
        redis_api_module.GulpMetrics,
        "redis_publish_bytes_by_route_total",
        publish_bytes_by_route,
    )
    monkeypatch.setattr(
        redis_api_module.GulpMetrics,
        "redis_targeted_publish_total",
        targeted_total,
    )

    broadcast_message = {
        "@timestamp": 1,
        "type": "note",
        "route_target_type": GulpMessageRoutingTarget.BROADCAST.value,
    }
    targeted_message = {
        "@timestamp": 2,
        "type": "docs_chunk",
        "route_target_type": GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
    }

    await redis_client.publish(
        broadcast_message,
        channel=GulpMessageRoutingTarget.BROADCAST.redis_channel_name(),
    )
    await redis_client.publish(
        targeted_message,
        channel=redis_client.worker_to_main_channel("server-b"),
    )

    broadcast_len = len(orjson.dumps(broadcast_message))
    targeted_len = len(orjson.dumps(targeted_message))
    assert publish_total.value == 2
    assert publish_bytes_total.value == broadcast_len + targeted_len
    assert targeted_total.value == 1
    assert fake_redis.published == [
        ("gulpredis", orjson.dumps(broadcast_message)),
        ("gulpredis:server:server-b", orjson.dumps(targeted_message)),
    ]

    assert publish_by_route.children[
        (("channel_scope", "main"), ("route_target", "broadcast"))
    ].value == 1
    assert publish_bytes_by_route.children[
        (("channel_scope", "main"), ("route_target", "broadcast"))
    ].value == broadcast_len
    assert publish_by_route.children[
        (("channel_scope", "server"), ("route_target", "worker_to_main"))
    ].value == 1
    assert publish_bytes_by_route.children[
        (("channel_scope", "server"), ("route_target", "worker_to_main"))
    ].value == targeted_len


@pytest.mark.unit
@pytest.mark.asyncio
async def test_gulp_redis_publish_large_payload_stores_ttl_chunks_and_pointer(
    monkeypatch,
):
    from gulp.api import redis_api as redis_api_module

    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeRedisRaw()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-large-publisher"
    redis_client.PUBLISH_LARGE_PAYLOAD_TTL = 37
    chunked_publish_total = _FakeCounter()

    async def _no_backpressure() -> None:
        return

    fake_config = _FakeConfig(
        redis_compression_threshold=1,
        redis_pubsub_max_chunk_size=1,
        redis_compression_enabled=False,
    )
    monkeypatch.setattr(
        redis_api_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: fake_config),
    )
    monkeypatch.setattr(redis_client, "_apply_publish_backpressure", _no_backpressure)
    monkeypatch.setattr(
        redis_api_module.GulpMetrics,
        "redis_publish_total",
        _FakeCounter(),
    )
    monkeypatch.setattr(
        redis_api_module.GulpMetrics,
        "redis_publish_bytes_total",
        _FakeCounter(),
    )
    monkeypatch.setattr(
        redis_api_module.GulpMetrics,
        "redis_publish_by_route_total",
        _FakeCounter(),
    )
    monkeypatch.setattr(
        redis_api_module.GulpMetrics,
        "redis_publish_bytes_by_route_total",
        _FakeCounter(),
    )
    monkeypatch.setattr(
        redis_api_module.GulpMetrics,
        "redis_targeted_publish_total",
        _FakeCounter(),
    )
    monkeypatch.setattr(
        redis_api_module.GulpMetrics,
        "redis_chunked_publish_total",
        chunked_publish_total,
    )

    message = {
        "@timestamp": 123,
        "type": "docs_chunk",
        "route_target_type": GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
        "ws_id": "ws-large-target",
        "payload": {"blob": "x" * 5000},
    }
    original_payload = orjson.dumps(message)

    await redis_client.publish(
        message,
        channel=redis_client.worker_to_main_channel("server-owner"),
    )

    assert fake_redis.pipeline_transactions == [True]
    assert len(fake_redis.published) == 1
    published_channel, published_payload = fake_redis.published[0]
    assert published_channel == "gulpredis:server:server-owner"
    pointer_message = orjson.loads(published_payload)
    pointer_payload = pointer_message["payload"]
    base_key = pointer_payload["__pointer_key"]
    chunk_count = pointer_payload["__chunks"]

    assert pointer_message["type"] == "docs_chunk"
    assert pointer_message["ws_id"] == "ws-large-target"
    assert (
        pointer_message["route_target_type"]
        == GulpMessageRoutingTarget.WORKER_TO_MAIN.value
    )
    assert base_key.startswith("gulp:pub:payload:server-large-publisher:")
    assert chunk_count > 1
    assert pointer_payload["__compressed"] is False
    assert pointer_payload["__orig_len"] == len(original_payload)
    assert pointer_payload["__server_id"] == "server-large-publisher"
    assert chunked_publish_total.value == 1

    chunk_set_calls = [
        (key, value, ex)
        for key, value, ex in fake_redis.set_calls
        if key.startswith(f"{base_key}:")
    ]
    assert len(chunk_set_calls) == chunk_count
    assert all(ex == 37 for _, _, ex in chunk_set_calls)
    assert [key for key, _, _ in chunk_set_calls] == [
        f"{base_key}:{idx}" for idx in range(chunk_count)
    ]
    assert b"".join(value for _, value, _ in chunk_set_calls) == original_payload


@pytest.mark.unit
@pytest.mark.asyncio
async def test_redis_broker_drops_pubsub_message_missing_routing_metadata():
    broker = GulpRedisBroker.get_instance()

    # This should be logged and dropped, not raise KeyError from dict.pop().
    await broker._handle_pubsub_message(
        {
            "__redis_channel__": "gulpredis",
            "timestamp": 123,
            "type": "stats_update",
        }
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_redis_broker_payload_pointer_resolution_records_resolved(monkeypatch):
    from gulp.api import ws_api as ws_api_module

    fake_redis = _FakeRedisClient()
    resolved_message = {
        "@timestamp": 123,
        "type": "note",
        "payload": {"resolved": True},
    }
    await fake_redis._redis.set("payload-key", orjson.dumps(resolved_message))

    fake_counter = _FakeCounter()
    fake_sockets = MagicMock()
    fake_sockets.route_message_to_local_websockets = AsyncMock()

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        ws_api_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: _FakeConfig()),
    )
    monkeypatch.setattr(
        ws_api_module.GulpMetrics,
        "ws_payload_pointer_resolve_total",
        fake_counter,
    )
    monkeypatch.setattr(
        ws_api_module.GulpConnectedSockets,
        "get_instance",
        lambda: fake_sockets,
    )

    await GulpRedisBroker.get_instance()._handle_pubsub_message(
        {
            "__redis_channel__": GulpMessageRoutingTarget.BROADCAST.redis_channel_name(),
            "route_target_type": GulpMessageRoutingTarget.BROADCAST.value,
            "origin_server_id": "server-b",
            "@timestamp": 122,
            "type": "note",
            "payload": {
                "__pointer_key": "payload-key",
                "__server_id": fake_redis.server_id,
            },
        }
    )

    assert fake_counter.children[(("outcome", "resolved"),)].value == 1
    routed_wsd, routed_message = fake_sockets.route_message_to_local_websockets.await_args.args
    assert routed_wsd.payload == {"resolved": True}
    assert routed_message == resolved_message
    assert "payload-key" in fake_redis._redis._data


@pytest.mark.unit
@pytest.mark.asyncio
async def test_redis_broker_local_broadcast_pointer_keeps_chunks(monkeypatch):
    from gulp.api import ws_api as ws_api_module

    fake_redis = _FakeRedisClient()
    fake_redis.server_id = "server-a"
    resolved_message = {
        "@timestamp": 123,
        "type": "note",
        "payload": {
            "id": "note-local-pointer",
            "text": "x" * 1024,
        },
    }
    raw_payload = orjson.dumps(resolved_message)
    await fake_redis._redis.set("broadcast-payload:0", raw_payload[:500])
    await fake_redis._redis.set("broadcast-payload:1", raw_payload[500:])

    fake_counter = _FakeCounter()
    fake_sockets = MagicMock()
    fake_sockets.route_message_to_local_websockets = AsyncMock()

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        ws_api_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: _FakeConfig()),
    )
    monkeypatch.setattr(
        ws_api_module.GulpMetrics,
        "ws_payload_pointer_resolve_total",
        fake_counter,
    )
    monkeypatch.setattr(
        ws_api_module.GulpConnectedSockets,
        "get_instance",
        lambda: fake_sockets,
    )

    await GulpRedisBroker.get_instance()._handle_pubsub_message(
        {
            "__redis_channel__": GulpMessageRoutingTarget.BROADCAST.redis_channel_name(),
            "route_target_type": GulpMessageRoutingTarget.BROADCAST.value,
            "origin_server_id": "server-b",
            "@timestamp": 122,
            "type": "note",
            "payload": {
                "__pointer_key": "broadcast-payload",
                "__chunks": 2,
                "__compressed": False,
                "__server_id": fake_redis.server_id,
            },
        }
    )

    assert fake_counter.children[(("outcome", "resolved"),)].value == 1
    routed_wsd, routed_message = fake_sockets.route_message_to_local_websockets.await_args.args
    assert routed_wsd.payload["text"] == "x" * 1024
    assert routed_message == resolved_message
    assert "broadcast-payload:0" in fake_redis._redis._data
    assert "broadcast-payload:1" in fake_redis._redis._data


@pytest.mark.unit
@pytest.mark.asyncio
async def test_redis_broker_payload_pointer_resolution_records_missing(monkeypatch):
    from gulp.api import ws_api as ws_api_module

    fake_redis = _FakeRedisClient()
    fake_counter = _FakeCounter()
    fake_sockets = MagicMock()
    fake_sockets.route_message_to_local_websockets = AsyncMock()

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        ws_api_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: _FakeConfig()),
    )
    monkeypatch.setattr(
        ws_api_module.GulpMetrics,
        "ws_payload_pointer_resolve_total",
        fake_counter,
    )
    monkeypatch.setattr(
        ws_api_module.GulpConnectedSockets,
        "get_instance",
        lambda: fake_sockets,
    )

    await GulpRedisBroker.get_instance()._handle_pubsub_message(
        {
            "__redis_channel__": GulpMessageRoutingTarget.BROADCAST.redis_channel_name(),
            "route_target_type": GulpMessageRoutingTarget.BROADCAST.value,
            "origin_server_id": "server-b",
            "@timestamp": 122,
            "type": "note",
            "payload": {
                "__pointer_key": "missing-key",
                "__server_id": fake_redis.server_id,
            },
        }
    )

    assert fake_counter.children[(("outcome", "missing"),)].value == 1
    fake_sockets.route_message_to_local_websockets.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_redis_broker_payload_pointer_resolution_records_error(monkeypatch):
    from gulp.api import ws_api as ws_api_module

    fake_redis = _FakeRedisClient()
    fake_redis._redis.eval_error = RuntimeError("redis eval failed")
    fake_counter = _FakeCounter()
    fake_sockets = MagicMock()
    fake_sockets.route_message_to_local_websockets = AsyncMock()

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        ws_api_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: _FakeConfig()),
    )
    monkeypatch.setattr(
        ws_api_module.GulpMetrics,
        "ws_payload_pointer_resolve_total",
        fake_counter,
    )
    monkeypatch.setattr(
        ws_api_module.GulpConnectedSockets,
        "get_instance",
        lambda: fake_sockets,
    )
    monkeypatch.setattr(ws_api_module.asyncio, "sleep", AsyncMock())

    await GulpRedisBroker.get_instance()._handle_pubsub_message(
        {
            "__redis_channel__": GulpMessageRoutingTarget.BROADCAST.redis_channel_name(),
            "route_target_type": GulpMessageRoutingTarget.BROADCAST.value,
            "origin_server_id": "server-b",
            "@timestamp": 122,
            "type": "note",
            "payload": {
                "__pointer_key": "error-key",
                "__server_id": fake_redis.server_id,
            },
        }
    )

    assert fake_counter.children[(("outcome", "error"),)].value == 1
    fake_sockets.route_message_to_local_websockets.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_redis_broker_payload_pointer_resolution_records_skipped_not_owner(
    monkeypatch,
):
    from gulp.api import ws_api as ws_api_module

    fake_redis = _FakeRedisClient()
    fake_redis.ws_get_server = AsyncMock(return_value="server-b")
    fake_counter = _FakeCounter()
    fake_sockets = MagicMock()
    fake_sockets.route_message_to_local_websockets = AsyncMock()

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        ws_api_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: _FakeConfig()),
    )
    monkeypatch.setattr(
        ws_api_module.GulpMetrics,
        "ws_payload_pointer_resolve_total",
        fake_counter,
    )
    monkeypatch.setattr(
        ws_api_module.GulpConnectedSockets,
        "get_instance",
        lambda: fake_sockets,
    )

    await GulpRedisBroker.get_instance()._handle_pubsub_message(
        {
            "__redis_channel__": fake_redis.worker_to_main_channel(),
            "route_target_type": GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
            "origin_server_id": "server-b",
            "@timestamp": 122,
            "type": "docs_chunk",
            "ws_id": "remote-ws",
            "payload": {
                "__pointer_key": "remote-key",
                "__server_id": "server-b",
            },
        }
    )

    assert fake_counter.children[(("outcome", "skipped_not_owner"),)].value == 1
    fake_redis.ws_get_server.assert_awaited_once_with("remote-ws")
    fake_sockets.route_message_to_local_websockets.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_redis_broker_client_data_pointer_resolves_without_deleting_chunks(
    monkeypatch,
):
    from gulp.api import ws_api as ws_api_module

    fake_redis = _FakeRedisClient()
    fake_redis.server_id = "server-b"
    resolved_message = {
        "@timestamp": 123,
        "type": "client_data",
        "operation_id": "op-client-data",
        "payload": {
            "operation_id": "op-client-data",
            "target_user_ids": ["user-target"],
            "data": {
                "kind": "large_client_data",
                "blob": "x" * 1024,
            },
        },
    }
    raw_payload = orjson.dumps(resolved_message)
    await fake_redis._redis.set("client-data-payload:0", raw_payload[:500])
    await fake_redis._redis.set("client-data-payload:1", raw_payload[500:])

    target_socket = _FakeConnectedSocket(
        "client-data-target",
        socket_type=GulpWsType.WS_CLIENT_DATA,
        user_id="user-target",
    )
    other_socket = _FakeConnectedSocket(
        "client-data-other",
        socket_type=GulpWsType.WS_CLIENT_DATA,
        user_id="user-other",
    )
    default_socket = _FakeConnectedSocket(
        "default-target",
        socket_type=GulpWsType.WS_DEFAULT,
        user_id="user-target",
    )
    fake_sockets = MagicMock()
    fake_sockets.sockets.return_value = {
        target_socket.ws_id: target_socket,
        other_socket.ws_id: other_socket,
        default_socket.ws_id: default_socket,
    }

    fake_counter = _FakeCounter()
    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        ws_api_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: _FakeConfig()),
    )
    monkeypatch.setattr(
        ws_api_module.GulpMetrics,
        "ws_payload_pointer_resolve_total",
        fake_counter,
    )
    monkeypatch.setattr(
        ws_api_module.GulpConnectedSockets,
        "get_instance",
        lambda: fake_sockets,
    )

    await GulpRedisBroker.get_instance()._handle_pubsub_message(
        {
            "__redis_channel__": GulpMessageRoutingTarget.CLIENT_DATA.redis_channel_name(),
            "route_target_type": GulpMessageRoutingTarget.CLIENT_DATA.value,
            "origin_server_id": "server-a",
            "@timestamp": 122,
            "type": "client_data",
            "payload": {
                "__pointer_key": "client-data-payload",
                "__chunks": 2,
                "__compressed": False,
                "__server_id": "server-a",
            },
        }
    )

    assert fake_counter.children[(("outcome", "resolved"),)].value == 1
    target_socket.enqueue_message.assert_awaited_once()
    routed_message = target_socket.enqueue_message.await_args.args[0]
    assert routed_message["payload"]["data"]["blob"] == "x" * 1024
    other_socket.enqueue_message.assert_not_awaited()
    default_socket.enqueue_message.assert_not_awaited()
    assert "client-data-payload:0" in fake_redis._redis._data
    assert "client-data-payload:1" in fake_redis._redis._data


@pytest.mark.unit
@pytest.mark.asyncio
async def test_redis_broker_local_client_data_pointer_keeps_chunks(
    monkeypatch,
):
    from gulp.api import ws_api as ws_api_module

    fake_redis = _FakeRedisClient()
    fake_redis.server_id = "server-a"
    resolved_message = {
        "@timestamp": 123,
        "type": "client_data",
        "operation_id": "op-client-data-local",
        "payload": {
            "operation_id": "op-client-data-local",
            "target_user_ids": ["user-target"],
            "data": {
                "kind": "large_client_data_local",
                "blob": "y" * 1024,
            },
        },
    }
    raw_payload = orjson.dumps(resolved_message)
    await fake_redis._redis.set("client-data-local-payload:0", raw_payload[:500])
    await fake_redis._redis.set("client-data-local-payload:1", raw_payload[500:])

    target_socket = _FakeConnectedSocket(
        "client-data-target",
        socket_type=GulpWsType.WS_CLIENT_DATA,
        user_id="user-target",
    )
    other_socket = _FakeConnectedSocket(
        "client-data-other",
        socket_type=GulpWsType.WS_CLIENT_DATA,
        user_id="user-other",
    )
    fake_sockets = MagicMock()
    fake_sockets.sockets.return_value = {
        target_socket.ws_id: target_socket,
        other_socket.ws_id: other_socket,
    }

    fake_counter = _FakeCounter()
    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        ws_api_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: _FakeConfig()),
    )
    monkeypatch.setattr(
        ws_api_module.GulpMetrics,
        "ws_payload_pointer_resolve_total",
        fake_counter,
    )
    monkeypatch.setattr(
        ws_api_module.GulpConnectedSockets,
        "get_instance",
        lambda: fake_sockets,
    )

    await GulpRedisBroker.get_instance()._handle_pubsub_message(
        {
            "__redis_channel__": GulpMessageRoutingTarget.CLIENT_DATA.redis_channel_name(),
            "route_target_type": GulpMessageRoutingTarget.CLIENT_DATA.value,
            "origin_server_id": "server-b",
            "@timestamp": 122,
            "type": "client_data",
            "payload": {
                "__pointer_key": "client-data-local-payload",
                "__chunks": 2,
                "__compressed": False,
                "__server_id": fake_redis.server_id,
            },
        }
    )

    assert fake_counter.children[(("outcome", "resolved"),)].value == 1
    target_socket.enqueue_message.assert_awaited_once()
    routed_message = target_socket.enqueue_message.await_args.args[0]
    assert routed_message["payload"]["data"]["blob"] == "y" * 1024
    other_socket.enqueue_message.assert_not_awaited()
    assert "client-data-local-payload:0" in fake_redis._redis._data
    assert "client-data-local-payload:1" in fake_redis._redis._data


@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_to_main_targeted_ws_owned_by_remote_instance_forwards_to_owner(
    monkeypatch,
):
    from gulp.api import ws_api as ws_api_module

    fake_redis = MagicMock()
    fake_redis.server_id = "instance-a"
    fake_redis.ws_get_server = AsyncMock(return_value="instance-b")
    fake_redis.worker_to_main_channel = (
        lambda server_id=None: f"gulpredis:server:{server_id or 'instance-a'}"
    )
    fake_redis.publish = AsyncMock()

    fake_sockets = MagicMock()
    fake_sockets.get.return_value = None
    fake_sockets.route_message_to_local_websockets = AsyncMock()

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        ws_api_module.GulpConnectedSockets,
        "get_instance",
        lambda: fake_sockets,
    )

    broker = GulpRedisBroker.get_instance()
    await broker._handle_pubsub_message(
        {
            "__redis_channel__": fake_redis.worker_to_main_channel(),
            "route_target_type": GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
            "origin_server_id": "instance-a",
            "@timestamp": 123,
            "type": "docs_chunk",
            "ws_id": "remote-ws",
        }
    )

    fake_sockets.route_message_to_local_websockets.assert_not_awaited()
    fake_redis.ws_get_server.assert_awaited_once_with("remote-ws")
    fake_redis.publish.assert_awaited_once()
    assert fake_redis.publish.await_args.kwargs["channel"] == "gulpredis:server:instance-b"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_put_common_targeted_ws_owned_by_remote_instance_publishes_to_owner(
    monkeypatch,
):
    from gulp.api import ws_api as ws_api_module
    import gulp.process as process_module

    fake_process = MagicMock()
    fake_process.is_main_process.return_value = True

    fake_redis = MagicMock()
    fake_redis.server_id = "instance-a"
    fake_redis.ws_get_server = AsyncMock(return_value="instance-b")
    fake_redis.worker_to_main_channel = (
        lambda server_id=None: f"gulpredis:server:{server_id or 'instance-a'}"
    )
    fake_redis.publish = AsyncMock()

    fake_sockets = MagicMock()
    fake_sockets.get.return_value = None
    fake_sockets.route_message_to_local_websockets = AsyncMock()

    monkeypatch.setattr(
        process_module.GulpProcess,
        "get_instance",
        staticmethod(lambda: fake_process),
    )
    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        ws_api_module.GulpConnectedSockets,
        "get_instance",
        lambda: fake_sockets,
    )

    broker = GulpRedisBroker.get_instance()
    wsd = GulpWsData(timestamp=123, type="docs_chunk", ws_id="remote-ws")

    await broker._put_common(wsd)

    fake_sockets.route_message_to_local_websockets.assert_not_awaited()
    fake_redis.ws_get_server.assert_awaited_once_with("remote-ws")
    fake_redis.publish.assert_awaited_once()

    published_msg = fake_redis.publish.await_args.args[0]
    assert (
        published_msg["route_target_type"]
        == GulpMessageRoutingTarget.WORKER_TO_MAIN.value
    )
    assert published_msg["origin_server_id"] == "instance-a"
    assert fake_redis.publish.await_args.kwargs["channel"] == "gulpredis:server:instance-b"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_put_common_targeted_ws_owned_locally_routes_without_publish(monkeypatch):
    from gulp.api import ws_api as ws_api_module
    import gulp.process as process_module

    fake_process = MagicMock()
    fake_process.is_main_process.return_value = True

    fake_redis = MagicMock()
    fake_redis.server_id = "instance-a"
    fake_redis.ws_get_server = AsyncMock()
    fake_redis.publish = AsyncMock()

    fake_sockets = MagicMock()
    fake_sockets.get.return_value = object()
    fake_sockets.route_message_to_local_websockets = AsyncMock()

    monkeypatch.setattr(
        process_module.GulpProcess,
        "get_instance",
        staticmethod(lambda: fake_process),
    )
    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        ws_api_module.GulpConnectedSockets,
        "get_instance",
        lambda: fake_sockets,
    )

    broker = GulpRedisBroker.get_instance()
    wsd = GulpWsData(timestamp=123, type="docs_chunk", ws_id="local-ws")

    await broker._put_common(wsd)

    fake_sockets.route_message_to_local_websockets.assert_awaited_once()
    fake_redis.ws_get_server.assert_not_awaited()
    fake_redis.publish.assert_not_awaited()


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
