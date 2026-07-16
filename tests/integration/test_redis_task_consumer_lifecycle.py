"""Integration coverage for Redis task-consumer cleanup."""

from uuid import uuid4

import orjson
import pytest

from gulp.api.redis_api import GulpRedis


@pytest.mark.integration
async def test_pending_task_survives_consumer_cleanup_and_remains_claimable(
    monkeypatch: pytest.MonkeyPatch,
):
    suffix = uuid4().hex
    stream_prefix = f"gulp:test:stream:tasks:{suffix}"
    types_key = f"gulp:test:queue:types:{suffix}"
    group = f"gulp:test:stream:group:tasks:{suffix}"
    heartbeat_prefix = f"gulp:test:heartbeat:{suffix}"
    stream = f"{stream_prefix}:query"
    owner = f"dead-owner-{suffix}"
    recovery = f"live-recovery-{suffix}"

    redis_client = GulpRedis.get_instance()
    try:
        redis_client.initialize(server_id=recovery, main_process=False)
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

    monkeypatch.setattr(GulpRedis, "STREAM_TASK_PREFIX", stream_prefix)
    monkeypatch.setattr(GulpRedis, "TASK_TYPES_SET", types_key)
    monkeypatch.setattr(GulpRedis, "STREAM_CONSUMER_GROUP", group)
    monkeypatch.setattr(GulpRedis, "HEARTBEAT_KEY_PREFIX", heartbeat_prefix)

    try:
        await raw_redis.sadd(types_key, "query")
        await raw_redis.xadd(
            stream,
            {"data": orjson.dumps({"task_type": "query", "req_id": suffix})},
        )
        await raw_redis.xgroup_create(stream, group, id="0")
        claimed = await raw_redis.xreadgroup(
            group,
            owner,
            streams={stream: ">"},
            count=1,
        )
        message_id = claimed[0][1][0][0]

        await redis_client.cleanup_consumers_for_server(owner)
        consumers = await raw_redis.xinfo_consumers(stream, group)
        assert any(
            (item["name"].decode() if isinstance(item["name"], bytes) else item["name"])
            == owner
            and item["pending"] == 1
            for item in consumers
        )

        _, recovered, _ = await raw_redis.xautoclaim(
            stream,
            group,
            recovery,
            min_idle_time=0,
            start_id="0-0",
            count=1,
        )
        assert recovered[0][0] == message_id

        await raw_redis.xack(stream, group, message_id)
        await raw_redis.xdel(stream, message_id)
        await raw_redis.set(f"{heartbeat_prefix}:{recovery}", "1", ex=60)
        removed = await redis_client.cleanup_dead_task_consumers()
        remaining = await raw_redis.xinfo_consumers(stream, group)
        remaining_names = {
            item["name"].decode() if isinstance(item["name"], bytes) else item["name"]
            for item in remaining
        }
        print(
            "task consumer lifecycle:",
            {
                "message_id": message_id,
                "recovered": len(recovered),
                "removed": removed,
                "remaining": sorted(remaining_names),
            },
        )
        assert owner not in remaining_names
        assert recovery in remaining_names
    finally:
        try:
            await raw_redis.xgroup_destroy(stream, group)
        except Exception:
            pass
        await raw_redis.delete(
            stream,
            types_key,
            f"{heartbeat_prefix}:{recovery}",
        )
        await redis_client.shutdown()
        GulpRedis._instance = None
