"""Unit tests for the unified core and extension event bus."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest

from gulp.events import (
    GulpCoreEvent,
    GulpEvent,
    GulpEventError,
    GulpEventReply,
    events,
)


class _Listener:
    def __init__(self, name: str, callback=None) -> None:
        self.name = name
        self._callback = callback
        self.received: list[GulpEvent] = []

    async def on_event(self, event: GulpEvent):
        self.received.append(event)
        if self._callback:
            return await self._callback(event)
        return None


class _RedisStore:
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.deleted: list[str] = []

    async def get(self, key: str):
        return self.values.get(key)

    async def set(self, key: str, value, ex: int | None = None):
        if isinstance(value, int):
            value = str(value).encode()
        self.values[key] = value
        return True

    async def delete(self, key: str):
        self.deleted.append(key)
        self.values.pop(key, None)
        return 1


class _Redis:
    def __init__(self) -> None:
        self.server_id = "server-a"
        self.store = _RedisStore()
        self.publish = AsyncMock(return_value=1)

    def client(self):
        return self.store

    def worker_to_main_channel(self, server_id: str | None = None) -> str:
        return f"gulpredis:server:{server_id or self.server_id}"


@pytest.fixture(autouse=True)
def _reset_events(monkeypatch):
    from gulp.process import GulpProcess

    process = MagicMock()
    process.is_main_process.return_value = True
    monkeypatch.setattr(GulpProcess, "get_instance", staticmethod(lambda: process))
    events._reset_for_tests()
    monkeypatch.setattr(events, "_store_chunk_mask", AsyncMock())
    yield
    events._reset_for_tests()


@pytest.mark.unit
def test_subscription_validation_additive_order_and_masks():
    first = _Listener("first")
    second = _Listener("second")

    with pytest.raises(ValueError, match="at least one"):
        events.subscribe(first, [])
    with pytest.raises(ValueError, match="namespaced"):
        events.subscribe(first, ["not_namespaced"])

    events.subscribe(first, [GulpCoreEvent.CHUNK_PRE_INGEST])
    events.subscribe(first, [GulpCoreEvent.CHUNK_POST_INGEST])
    events.subscribe(second, [GulpCoreEvent.CHUNK_PRE_INGEST])

    assert list(events._listeners) == ["first", "second"]
    assert events._chunk_mask == (events.CHUNK_PRE_INGEST | events.CHUNK_POST_INGEST)

    duplicate = _Listener("first")
    with pytest.raises(ValueError, match="already registered"):
        events.subscribe(duplicate, [GulpCoreEvent.USER_LOGIN])

    events.unsubscribe(first)
    assert events._chunk_mask == events.CHUNK_PRE_INGEST
    events.unsubscribe(second)
    events.unsubscribe(second)
    assert events._chunk_mask == 0


@pytest.mark.unit
def test_subscription_is_main_process_only(monkeypatch):
    from gulp.process import GulpProcess

    process = MagicMock()
    process.is_main_process.return_value = False
    monkeypatch.setattr(GulpProcess, "get_instance", staticmethod(lambda: process))

    with pytest.raises(RuntimeError, match="main-process-only"):
        events.subscribe(_Listener("worker"), [GulpCoreEvent.USER_LOGIN])


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dispatch_chains_in_order_stops_and_preserves_metadata():
    seen: list[tuple[str, dict]] = []

    async def first_callback(event: GulpEvent):
        seen.append(("first", event.data))
        return {"value": event.data["value"] + 1}

    async def second_callback(event: GulpEvent):
        seen.append(("second", event.data))
        return GulpEventReply(result={"value": 99}, stop=True)

    third = _Listener("third")
    events.subscribe(_Listener("first", first_callback), ["example.changed"])
    events.subscribe(_Listener("second", second_callback), ["example.changed"])
    events.subscribe(third, ["example.changed"])

    event = GulpEvent(
        event_id="event-id",
        event="example.changed",
        timestamp_msec=123,
        data={"value": 1},
        user_id="user",
        operation_id="operation",
        req_id="request",
    )
    result = await events._dispatch(event, wait=True)

    assert seen == [("first", {"value": 1}), ("second", {"value": 2})]
    assert result.model_dump() == {
        "event_id": "event-id",
        "event": "example.changed",
        "handlers": ["first", "second"],
        "result": {"value": 99},
        "stopped": True,
    }
    assert not third.received


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dispatch_snapshots_listeners_and_empty_result():
    late = _Listener("late")

    async def add_late(_event: GulpEvent):
        events.subscribe(late, ["example.snapshot"])

    events.subscribe(_Listener("first", add_late), ["example.snapshot"])
    event = GulpEvent(
        event_id="one",
        event="example.snapshot",
        timestamp_msec=1,
        data={"unchanged": True},
    )

    first = await events._dispatch(event, wait=True)
    second = await events._dispatch(
        event.model_copy(update={"event_id": "two"}), wait=True
    )
    empty = await events._dispatch(
        event.model_copy(update={"event": "example.none"}), wait=True
    )

    assert first.handlers == ["first"]
    assert second.handlers == ["first", "late"]
    assert len(late.received) == 1
    assert empty.handlers == []
    assert empty.result == {"unchanged": True}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_listener_errors_raise_when_waited_and_continue_for_notifications():
    async def fail(_event: GulpEvent):
        raise RuntimeError("boom")

    later = _Listener("later")
    events.subscribe(_Listener("failing", fail), ["example.error"])
    events.subscribe(later, ["example.error"])
    event = GulpEvent(
        event_id="error",
        event="example.error",
        timestamp_msec=1,
    )

    with pytest.raises(GulpEventError, match="failing.*boom"):
        await events._dispatch(event, wait=True)
    assert not later.received

    result = await events._dispatch(event, wait=False)
    assert result.handlers == ["failing", "later"]
    assert len(later.received) == 1


def _patch_redis(monkeypatch, redis: _Redis) -> None:
    from gulp.api.redis_api import GulpRedis

    monkeypatch.setattr(GulpRedis, "get_instance", staticmethod(lambda: redis))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_waited_emit_round_trip_preserves_metadata_and_cleans_response(
    monkeypatch,
):
    redis = _Redis()
    _patch_redis(monkeypatch, redis)
    listener = _Listener("listener")
    events.subscribe(listener, ["example.round_trip"])

    async def publish(message: dict, channel: str):
        asyncio.create_task(
            events._handle_redis_message(
                dict(message),
                redis_channel=channel,
                origin_server_id=message["origin_server_id"],
            )
        )
        return 1

    redis.publish = AsyncMock(side_effect=publish)
    result = await events.emit(
        "example.round_trip",
        {"value": 1},
        wait=True,
        user_id="user",
        operation_id="operation",
        req_id="request",
    )

    assert result.handlers == ["listener"]
    received = listener.received[0]
    assert received.event_id == result.event_id
    assert received.timestamp_msec > 0
    assert received.user_id == "user"
    assert received.operation_id == "operation"
    assert received.req_id == "request"
    assert f"gulp:events:response:{result.event_id}" in redis.store.deleted


@pytest.mark.unit
@pytest.mark.asyncio
async def test_emit_zero_subscribers_malformed_response_timeout_and_validation(
    monkeypatch,
):
    redis = _Redis()
    _patch_redis(monkeypatch, redis)
    redis.publish = AsyncMock(return_value=0)

    with pytest.raises(GulpEventError, match="no main-process subscriber"):
        await events.emit("example.missing", wait=True)

    async def malformed(message: dict, channel: str):
        redis.store.values[message["response_key"]] = b"not-json"
        return 1

    redis.publish = AsyncMock(side_effect=malformed)
    with pytest.raises(GulpEventError, match="malformed"):
        await events.emit("example.malformed", wait=True)

    redis.publish = AsyncMock(return_value=1)
    with pytest.raises(GulpEventError, match="timed out"):
        await events.emit("example.timeout", wait=True, timeout=0.02)

    with pytest.raises(ValueError, match="cannot be broadcast"):
        await events.emit("example.invalid", wait=True, broadcast=True)
    with pytest.raises(TypeError, match="dictionary"):
        await events.emit("example.invalid", data=[])


@pytest.mark.unit
@pytest.mark.asyncio
async def test_notification_publish_failure_does_not_fail_producer(monkeypatch):
    redis = _Redis()
    _patch_redis(monkeypatch, redis)
    redis.publish = AsyncMock(side_effect=RuntimeError("redis unavailable"))

    assert await events.emit(GulpCoreEvent.USER_LOGIN, {"user_id": "user"}) is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_listener_mask_reads_current_redis_value(monkeypatch):
    from gulp.process import GulpProcess

    redis = _Redis()
    _patch_redis(monkeypatch, redis)
    await redis.store.set(
        events._mask_key(redis.server_id),
        events.CHUNK_PRE_INGEST | events.CHUNK_POST_INGEST,
    )
    process = MagicMock()
    process.is_main_process.return_value = False
    monkeypatch.setattr(GulpProcess, "get_instance", staticmethod(lambda: process))

    assert await events.listener_mask() == 3


@pytest.mark.unit
@pytest.mark.asyncio
async def test_redis_handler_writes_explicit_error_envelope(monkeypatch):
    redis = _Redis()
    _patch_redis(monkeypatch, redis)

    async def fail(_event: GulpEvent):
        raise RuntimeError("listener failed")

    events.subscribe(_Listener("failure", fail), ["example.failure"])
    event = GulpEvent(
        event_id="failure-id",
        event="example.failure",
        timestamp_msec=1,
    )
    response_key = "gulp:events:response:failure-id"
    await events._handle_redis_message(
        {
            "payload": event.model_dump(),
            "wait": True,
            "broadcast": False,
            "response_key": response_key,
        },
        redis_channel=redis.worker_to_main_channel(),
        origin_server_id=redis.server_id,
    )

    envelope = orjson.loads(redis.store.values[response_key])
    assert envelope["ok"] is False
    assert "listener failed" in envelope["error"]
