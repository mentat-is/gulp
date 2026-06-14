import fnmatch
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gulp.api.redis_api import GulpRedis
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.server.ws import (
    GulpAPIWebsocket,
    InternalWsIngestPacket,
    STREAM_TTL,
    WsIngestBackpressureError,
    WsIngestRawWorker,
)
from gulp.api.ws_api import GulpWsIngestPacket


class _FakeRedisClient:
    def __init__(self):
        self.xgroup_create_calls: list[tuple] = []
        self.expire_calls: list[tuple[str, int]] = []
        self.xadd_calls: list[tuple[str, dict]] = []
        self.exists_calls: list[str] = []
        self.xgroup_destroy_calls: list[tuple[str, str]] = []
        self.delete_calls: list[str] = []
        self.stream_lengths: dict[str, int] = {}
        self.values: dict[str, int] = {}
        self.existing_keys: set[str] = set()

    async def xgroup_create(self, *args, **kwargs):
        self.xgroup_create_calls.append((args, kwargs))
        return True

    async def expire(self, key: str, ttl: int):
        self.expire_calls.append((key, ttl))
        return True

    async def xadd(self, stream_key: str, fields: dict):
        self.xadd_calls.append((stream_key, fields))
        self.stream_lengths[stream_key] = self.stream_lengths.get(stream_key, 0) + 1
        return "1-0"

    async def xlen(self, stream_key: str):
        return self.stream_lengths.get(stream_key, 0)

    async def get(self, key: str):
        return self.values.get(key)

    async def incrby(self, key: str, amount: int):
        self.values[key] = self.values.get(key, 0) + amount
        return self.values[key]

    async def exists(self, key: str):
        self.exists_calls.append(key)
        return key in self.existing_keys

    async def xgroup_destroy(self, stream_key: str, group_name: str):
        self.xgroup_destroy_calls.append((stream_key, group_name))
        return True

    async def delete(self, *keys: str):
        self.delete_calls.extend(keys)
        for key in keys:
            self.stream_lengths.pop(key, None)
            self.values.pop(key, None)
            self.existing_keys.discard(key)
        return len(keys)

    async def scan_iter(self, match: str, count: int = 200):
        keys = set(self.stream_lengths) | set(self.values) | self.existing_keys
        for key in sorted(keys):
            if fnmatch.fnmatch(key, match):
                yield key


class _FakeCollabSession:
    async def __aenter__(self):
        return "fake-session"

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeCollab:
    def session(self):
        return _FakeCollabSession()


@pytest.fixture(autouse=True)
def _reset_redis_singleton():
    GulpRedis._instance = None
    yield
    GulpRedis._instance = None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ws_ingest_worker_stream_ttl_and_cleanup(monkeypatch):
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"

    fake_server = SimpleNamespace(spawn_worker_task=AsyncMock(return_value=True))
    monkeypatch.setattr("gulp.api.server.ws.GulpServer.get_instance", lambda: fake_server)
    monkeypatch.setattr(WsIngestRawWorker, "STOP_WAIT_TIMEOUT", 0.0)

    ws = SimpleNamespace(ws_id="ws-1")
    worker = WsIngestRawWorker(ws)

    await worker.start()
    assert fake_redis.xgroup_create_calls
    assert fake_redis.expire_calls == [(worker._stream_key, STREAM_TTL)]

    ingest_packet = GulpWsIngestPacket(
        index="index-1",
        operation_id="op-1",
        ws_id="ws-1",
        req_id="req-1",
    )
    packet = InternalWsIngestPacket(
        user_id="user-1",
        index="index-1",
        data=ingest_packet,
        raw_data=b"raw-bytes",
    )

    await worker.put(packet)
    assert fake_redis.xadd_calls
    assert "queued_bytes" in fake_redis.xadd_calls[-1][1]
    queued_bytes = int(fake_redis.xadd_calls[-1][1]["queued_bytes"])
    assert fake_redis.values[worker._bytes_key] == queued_bytes
    assert fake_redis.expire_calls[-2:] == [
        (worker._stream_key, STREAM_TTL),
        (worker._bytes_key, STREAM_TTL),
    ]

    await worker.stop()
    assert fake_redis.xadd_calls[-1][0] == worker._stream_key
    assert fake_redis.xgroup_destroy_calls == [(worker._stream_key, worker._consumer_group)]
    assert fake_redis.delete_calls.count(worker._stream_key) == 1
    assert fake_redis.delete_calls.count(worker._done_key) == 1
    assert fake_redis.delete_calls.count(worker._bytes_key) == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ws_ingest_worker_rejects_when_stream_entry_limit_hit():
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"

    ws = SimpleNamespace(ws_id="ws-limit")
    worker = WsIngestRawWorker(ws)
    worker._stream_max_entries = 1
    fake_redis.stream_lengths[worker._stream_key] = 1
    packet = InternalWsIngestPacket(
        user_id="user-1",
        index="index-1",
        data=GulpWsIngestPacket(
            index="index-1",
            operation_id="op-1",
            ws_id="ws-limit",
            req_id="req-limit",
        ),
        raw_data=b"raw-bytes",
    )

    with pytest.raises(WsIngestBackpressureError) as exc_info:
        await worker.put(packet)

    assert exc_info.value.stream_len == 1
    assert exc_info.value.stream_limit == 1
    assert fake_redis.xadd_calls == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ws_ingest_worker_rejects_when_buffered_byte_limit_hit():
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"

    ws = SimpleNamespace(ws_id="ws-bytes")
    worker = WsIngestRawWorker(ws)
    worker._stream_max_entries = 0
    worker._stream_max_buffered_bytes = 10
    packet = InternalWsIngestPacket(
        user_id="user-1",
        index="index-1",
        data=GulpWsIngestPacket(
            index="index-1",
            operation_id="op-1",
            ws_id="ws-bytes",
            req_id="req-bytes",
        ),
        raw_data=b"more-than-ten-bytes",
    )

    with pytest.raises(WsIngestBackpressureError) as exc_info:
        await worker.put(packet)

    assert exc_info.value.buffered_bytes > worker._stream_max_buffered_bytes
    assert exc_info.value.buffered_bytes_limit == 10
    assert fake_redis.xadd_calls == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ws_ingest_disconnect_before_last_marks_request_failed(monkeypatch):
    set_finished = AsyncMock()
    stats = SimpleNamespace(status=GulpRequestStatus.ONGOING.value, set_finished=set_finished)
    create_or_get = AsyncMock(return_value=(stats, False))
    monkeypatch.setattr(
        "gulp.api.server.ws.GulpCollab.get_instance",
        lambda: _FakeCollab(),
    )
    monkeypatch.setattr(
        "gulp.api.server.ws.GulpRequestStats.create_or_get_existing",
        create_or_get,
    )
    packet = GulpWsIngestPacket(
        index="index-disconnect",
        operation_id="op-disconnect",
        ws_id="ws-disconnect",
        req_id="req-disconnect",
        last=False,
    )

    await GulpAPIWebsocket._mark_ws_ingest_disconnect_failed(
        packet,
        user_id="user-disconnect",
    )

    create_or_get.assert_awaited_once()
    set_finished.assert_awaited_once()
    _, kwargs = set_finished.await_args
    assert kwargs["status"] == GulpRequestStatus.FAILED
    assert kwargs["user_id"] == "user-disconnect"
    assert kwargs["ws_id"] == "ws-disconnect"
    assert kwargs["errors"] == [
        "raw websocket ingest disconnected before final packet"
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ws_ingest_disconnect_does_not_rewrite_terminal_request(monkeypatch):
    set_finished = AsyncMock()
    stats = SimpleNamespace(status=GulpRequestStatus.DONE.value, set_finished=set_finished)
    monkeypatch.setattr(
        "gulp.api.server.ws.GulpCollab.get_instance",
        lambda: _FakeCollab(),
    )
    monkeypatch.setattr(
        "gulp.api.server.ws.GulpRequestStats.create_or_get_existing",
        AsyncMock(return_value=(stats, False)),
    )
    packet = GulpWsIngestPacket(
        index="index-terminal-disconnect",
        operation_id="op-terminal-disconnect",
        ws_id="ws-terminal-disconnect",
        req_id="req-terminal-disconnect",
        last=False,
    )

    await GulpAPIWebsocket._mark_ws_ingest_disconnect_failed(
        packet,
        user_id="user-terminal-disconnect",
    )

    set_finished.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ws_ingest_orphan_cleanup_deletes_dead_owner_stream_only():
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "cleanup-server"

    dead_stream = "gulp:stream:ws_ingest:dead-server:ws-dead"
    dead_bytes = "gulp:stream:ws_ingest:bytes:dead-server:ws-dead"
    dead_done = "gulp:ws_ingest:done:dead-server:ws-dead"
    live_stream = "gulp:stream:ws_ingest:live-server:ws-live"
    live_bytes = "gulp:stream:ws_ingest:bytes:live-server:ws-live"
    fake_redis.stream_lengths[dead_stream] = 1
    fake_redis.values[dead_bytes] = 10
    fake_redis.values[dead_done] = 1
    fake_redis.stream_lengths[live_stream] = 1
    fake_redis.values[live_bytes] = 10
    fake_redis.existing_keys.add("gulp:heartbeat:live-server")

    cleaned = await redis_client.cleanup_orphaned_ws_ingest_streams()

    assert cleaned == 1
    assert fake_redis.delete_calls == [dead_stream, dead_bytes, dead_done]
    assert live_stream in fake_redis.stream_lengths
    assert live_bytes in fake_redis.values
