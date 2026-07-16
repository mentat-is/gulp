"""Redis transport tests specific to internal event delivery."""

import asyncio

from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest

from gulp.api.redis_api import GulpRedis, _MAIN_REDIS_CHANNEL
from gulp.api.ws_api import GulpMessageRoutingTarget, GulpRedisBroker
from gulp.events import GulpEvent, events


def _redis_client(monkeypatch) -> GulpRedis:
    from gulp.api import redis_api

    redis_client = GulpRedis()
    redis_client.server_id = "server-a"
    redis_client._redis = MagicMock()
    redis_client._apply_publish_backpressure = AsyncMock()
    config = MagicMock()
    config.prometheus_enabled.return_value = False
    monkeypatch.setattr(
        redis_api.GulpConfig, "get_instance", staticmethod(lambda: config)
    )
    return redis_client


@pytest.mark.unit
@pytest.mark.asyncio
async def test_publish_returns_inline_subscriber_count(monkeypatch):
    redis_client = _redis_client(monkeypatch)
    redis_client._redis.publish = AsyncMock(return_value=4)

    count = await redis_client.publish(
        {"route_target_type": "event", "payload": {"value": 1}}
    )

    assert count == 4


@pytest.mark.unit
@pytest.mark.asyncio
async def test_publish_returns_chunked_subscriber_count(monkeypatch):
    redis_client = _redis_client(monkeypatch)
    redis_client.PUBSUB_COMPRESSION_THRESHOLD = 1
    pipeline = MagicMock()
    pipeline.execute = AsyncMock(return_value=[True, 7])
    redis_client._redis.pipeline.return_value = pipeline

    count = await redis_client.publish(
        {
            "route_target_type": "event",
            "origin_server_id": "server-a",
            "payload": {"value": "large"},
        }
    )

    assert count == 7
    pipeline.publish.assert_called_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_drain_pubsub_handlers_accepts_completed_tasks():
    async def _done() -> None:
        return None

    task = asyncio.create_task(_done())
    await task
    redis_client = GulpRedis()
    redis_client._handler_tasks.add(task)

    await asyncio.wait_for(redis_client.drain_pubsub_handlers(), timeout=0.1)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_two_peers_read_one_large_broadcast_event(monkeypatch):
    from gulp.api import ws_api

    event = GulpEvent(
        event_id="event-id",
        event="example.large",
        timestamp_msec=1,
        data={"blob": "x" * 300_000},
    )
    full_envelope = {
        "route_target_type": GulpMessageRoutingTarget.EVENT.value,
        "origin_server_id": "origin",
        "payload": event.model_dump(),
        "wait": False,
        "broadcast": True,
        "origin_processed": True,
    }
    pointer = {
        "route_target_type": GulpMessageRoutingTarget.EVENT.value,
        "origin_server_id": "origin",
        "payload": {
            "__pointer_key": "gulp:pub:payload:origin:event",
            "__server_id": "origin",
        },
        "broadcast": True,
        "__redis_channel__": _MAIN_REDIS_CHANNEL,
    }
    fake_redis = MagicMock()
    fake_redis.server_id = "peer-one"
    fake_redis.worker_to_main_channel.side_effect = (
        lambda server_id=None: f"gulpredis:server:{server_id or fake_redis.server_id}"
    )
    monkeypatch.setattr(
        ws_api.GulpRedis, "get_instance", staticmethod(lambda: fake_redis)
    )
    redis_eval = AsyncMock(return_value=orjson.dumps(full_envelope))
    monkeypatch.setattr(GulpRedisBroker, "_redis_eval", redis_eval)
    handle = AsyncMock()
    monkeypatch.setattr(events, "_handle_redis_message", handle)

    broker = GulpRedisBroker()
    await broker._handle_pubsub_message(dict(pointer))
    fake_redis.server_id = "peer-two"
    await broker._handle_pubsub_message(dict(pointer))

    assert handle.await_count == 2
    assert all("DEL" not in call.args[1] for call in redis_eval.await_args_list)
