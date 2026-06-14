"""Integration coverage for raw websocket ingest Redis backpressure."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from gulp.api.redis_api import GulpRedis
from gulp.api.server.ws import (
    InternalWsIngestPacket,
    WsIngestBackpressureError,
    WsIngestRawWorker,
)
from gulp.api.ws_api import GulpWsIngestPacket


@pytest.mark.integration
async def test_ws_ingest_worker_backpressure_limits_stream_entries_with_real_redis():
    """A raw websocket stream rejects new packets once its Redis stream cap is hit."""
    suffix = uuid4().hex
    redis_client = GulpRedis.get_instance()
    try:
        redis_client.initialize(server_id=f"integration-ws-ingest-{suffix}", main_process=False)
        raw_redis = redis_client.client()
        await raw_redis.ping()
    except Exception as exc:
        try:
            if redis_client._redis:
                await redis_client.shutdown()
        except Exception:
            pass
        GulpRedis._instance = None
        pytest.skip(f"Redis is not available through current gULP config: {exc}")

    ws = SimpleNamespace(ws_id=f"ws-{suffix}")
    worker = WsIngestRawWorker(ws)
    worker._stream_max_entries = 1
    worker._stream_max_buffered_bytes = 0
    packet = InternalWsIngestPacket(
        user_id="user-1",
        index="index-1",
        data=GulpWsIngestPacket(
            index="index-1",
            operation_id="op-1",
            ws_id=ws.ws_id,
            req_id=f"req-{suffix}",
        ),
        raw_data=b"raw-bytes",
    )

    try:
        await worker.put(packet)
        with pytest.raises(WsIngestBackpressureError) as exc_info:
            await worker.put(packet)

        print(
            "ws ingest backpressure:",
            {
                "stream_key": worker._stream_key,
                "stream_len": await raw_redis.xlen(worker._stream_key),
                "bytes": await raw_redis.get(worker._bytes_key),
                "error": str(exc_info.value),
            },
        )
        assert exc_info.value.stream_len == 1
        assert exc_info.value.stream_limit == 1
        assert await raw_redis.xlen(worker._stream_key) == 1
        assert int(await raw_redis.get(worker._bytes_key)) > 0
    finally:
        await raw_redis.delete(worker._stream_key, worker._done_key, worker._bytes_key)
        await redis_client.shutdown()
        GulpRedis._instance = None


@pytest.mark.integration
async def test_ws_ingest_orphan_cleanup_deletes_dead_owner_stream_with_real_redis(
    monkeypatch: pytest.MonkeyPatch,
):
    """Raw websocket streams owned by servers without heartbeat are reaped."""
    suffix = uuid4().hex
    stream_prefix = f"gulp:test:stream:ws_ingest:{suffix}"
    bytes_prefix = f"gulp:test:stream:ws_ingest:bytes:{suffix}"
    done_prefix = f"gulp:test:ws_ingest:done:{suffix}"
    heartbeat_prefix = f"gulp:test:heartbeat:{suffix}"
    redis_client = GulpRedis.get_instance()
    try:
        redis_client.initialize(
            server_id=f"integration-ws-ingest-cleanup-{suffix}",
            main_process=False,
        )
        raw_redis = redis_client.client()
        await raw_redis.ping()
    except Exception as exc:
        try:
            if redis_client._redis:
                await redis_client.shutdown()
        except Exception:
            pass
        GulpRedis._instance = None
        pytest.skip(f"Redis is not available through current gULP config: {exc}")

    monkeypatch.setattr(GulpRedis, "WS_INGEST_STREAM_PREFIX", stream_prefix)
    monkeypatch.setattr(GulpRedis, "WS_INGEST_BYTES_PREFIX", bytes_prefix)
    monkeypatch.setattr(GulpRedis, "WS_INGEST_DONE_PREFIX", done_prefix)
    monkeypatch.setattr(GulpRedis, "HEARTBEAT_KEY_PREFIX", heartbeat_prefix)

    dead_stream = f"{stream_prefix}:dead-owner:ws-dead"
    dead_bytes = f"{bytes_prefix}:dead-owner:ws-dead"
    dead_done = f"{done_prefix}:dead-owner:ws-dead"
    live_stream = f"{stream_prefix}:live-owner:ws-live"
    live_bytes = f"{bytes_prefix}:live-owner:ws-live"
    live_done = f"{done_prefix}:live-owner:ws-live"
    live_heartbeat = f"{heartbeat_prefix}:live-owner"

    try:
        await raw_redis.xadd(dead_stream, {"data": b"dead"})
        await raw_redis.set(dead_bytes, "4", ex=60)
        await raw_redis.set(dead_done, "1", ex=60)
        await raw_redis.xadd(live_stream, {"data": b"live"})
        await raw_redis.set(live_bytes, "4", ex=60)
        await raw_redis.set(live_done, "1", ex=60)
        await raw_redis.set(live_heartbeat, "1", ex=60)

        cleaned = await redis_client.cleanup_orphaned_ws_ingest_streams()
        print(
            "ws ingest orphan cleanup:",
            {
                "cleaned": cleaned,
                "dead_stream_exists": await raw_redis.exists(dead_stream),
                "live_stream_exists": await raw_redis.exists(live_stream),
            },
        )

        assert cleaned == 1
        assert await raw_redis.exists(dead_stream) == 0
        assert await raw_redis.exists(dead_bytes) == 0
        assert await raw_redis.exists(dead_done) == 0
        assert await raw_redis.exists(live_stream) == 1
        assert await raw_redis.exists(live_bytes) == 1
        assert await raw_redis.exists(live_done) == 1
    finally:
        keys_to_delete = [
            dead_stream,
            dead_bytes,
            dead_done,
            live_stream,
            live_bytes,
            live_done,
            live_heartbeat,
        ]
        await raw_redis.delete(*keys_to_delete)
        await redis_client.shutdown()
        GulpRedis._instance = None
