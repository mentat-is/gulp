"""Integration coverage for Redis-backed task queue lifecycle semantics."""

import asyncio
from collections.abc import AsyncGenerator
from uuid import uuid4

import orjson
import pytest

from gulp.api.redis_api import GulpRedis, IpRateLimitError, TaskQueueFullError
from gulp.api.server.structs import (
    TASK_TYPE_ENRICH,
    TASK_TYPE_EXTERNAL_QUERY,
    TASK_TYPE_INGEST,
    TASK_TYPE_QUERY,
    TASK_TYPE_REBASE,
)


class _FakeCollabSession:
    async def __aenter__(self):
        return "fake-session"

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeCollab:
    def session(self):
        return _FakeCollabSession()


@pytest.fixture
async def isolated_task_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[tuple[GulpRedis, str, str], None]:
    """Provide a real Redis client using test-only task stream key prefixes."""
    suffix = uuid4().hex
    server_id = f"integration-task-queue-{suffix}"
    task_type = f"query-{suffix}"
    task_types_key = f"gulp:test:queue:types:{suffix}"
    stream_prefix = f"gulp:test:stream:tasks:{suffix}"
    dead_stream_prefix = f"gulp:test:stream:tasks:dead:{suffix}"
    lifecycle_prefix = f"gulp:test:task:lifecycle:{suffix}"
    lock_prefix = f"gulp:test:task:execution_lock:{suffix}"
    side_effect_lock_prefix = f"gulp:test:task:side_effect_lock:{suffix}"
    consumer_group = f"gulp:test:stream:group:tasks:{suffix}"
    stream_key = f"{stream_prefix}:{task_type}"
    dead_stream_key = f"{dead_stream_prefix}:{task_type}"
    req_id = f"req-{task_type}"
    duplicate_req_id = f"req-duplicate-{task_type}"

    monkeypatch.setattr(GulpRedis, "_instance", None)
    redis_client = GulpRedis.get_instance()
    try:
        redis_client.initialize(server_id=server_id, main_process=False)
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

    monkeypatch.setattr(GulpRedis, "TASK_TYPES_SET", task_types_key)
    monkeypatch.setattr(GulpRedis, "STREAM_TASK_PREFIX", stream_prefix)
    monkeypatch.setattr(GulpRedis, "STREAM_TASK_DLQ_PREFIX", dead_stream_prefix)
    monkeypatch.setattr(GulpRedis, "TASK_LIFECYCLE_PREFIX", lifecycle_prefix)
    monkeypatch.setattr(GulpRedis, "TASK_EXECUTION_LOCK_PREFIX", lock_prefix)
    monkeypatch.setattr(GulpRedis, "TASK_SIDE_EFFECT_LOCK_PREFIX", side_effect_lock_prefix)
    monkeypatch.setattr(GulpRedis, "STREAM_CONSUMER_GROUP", consumer_group)
    monkeypatch.setattr(GulpRedis, "STREAM_TASK_MAXLEN", 10)
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_USER_MAX", 0)
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_OPERATION_MAX", 0)
    redis_client._instance_roles = []

    try:
        yield redis_client, task_type, dead_stream_key
    finally:
        keys_to_delete = [
            task_types_key,
            GulpRedis.TASK_ACTIVE_USER_HASH,
            GulpRedis.TASK_ACTIVE_OPERATION_HASH,
        ]
        async for key in raw_redis.scan_iter(match=f"{stream_prefix}:*"):
            keys_to_delete.append(key)
        async for key in raw_redis.scan_iter(match=f"{dead_stream_prefix}:*"):
            keys_to_delete.append(key)
        async for key in raw_redis.scan_iter(match=f"{lifecycle_prefix}:*"):
            keys_to_delete.append(key)
        async for key in raw_redis.scan_iter(match=f"{lock_prefix}:*"):
            keys_to_delete.append(key)
        async for key in raw_redis.scan_iter(match=f"{side_effect_lock_prefix}:*"):
            keys_to_delete.append(key)
        await raw_redis.delete(*keys_to_delete)
        await redis_client.shutdown()
        GulpRedis._instance = None


@pytest.fixture
async def isolated_ip_rate_limit_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[tuple[GulpRedis, str], None]:
    """Provide a real Redis client using test-only per-IP rate limit keys."""
    suffix = uuid4().hex
    server_id = f"integration-ip-rate-limit-{suffix}"
    rate_limit_prefix = f"gulp:test:rate:ip:{suffix}"

    monkeypatch.setattr(GulpRedis, "_instance", None)
    redis_client = GulpRedis.get_instance()
    try:
        redis_client.initialize(server_id=server_id, main_process=False)
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

    monkeypatch.setattr(GulpRedis, "IP_RATE_LIMIT_PREFIX", rate_limit_prefix)

    try:
        yield redis_client, rate_limit_prefix
    finally:
        keys_to_delete = []
        async for key in raw_redis.scan_iter(match=f"{rate_limit_prefix}:*"):
            keys_to_delete.append(key)
        if keys_to_delete:
            await raw_redis.delete(*keys_to_delete)
        await redis_client.shutdown()
        GulpRedis._instance = None


@pytest.mark.integration
async def test_ip_rate_limit_rejects_same_ip_with_real_redis(
    isolated_ip_rate_limit_redis: tuple[GulpRedis, str],
):
    """Per-IP login throttle state is shared through Redis and isolated by IP."""
    redis_client, rate_limit_prefix = isolated_ip_rate_limit_redis
    raw_redis = redis_client.client()

    await redis_client.check_ip_rate_limit("login", "192.0.2.10", limit=2, window_sec=30)
    await redis_client.check_ip_rate_limit("login", "192.0.2.10", limit=2, window_sec=30)

    with pytest.raises(IpRateLimitError) as exc_info:
        await redis_client.check_ip_rate_limit(
            "login",
            "192.0.2.10",
            limit=2,
            window_sec=30,
        )

    assert exc_info.value.count == 3
    assert exc_info.value.limit == 2
    assert exc_info.value.retry_after_msec > 0

    await redis_client.check_ip_rate_limit("login", "198.51.100.20", limit=2, window_sec=30)

    keys = []
    async for key in raw_redis.scan_iter(match=f"{rate_limit_prefix}:login:*"):
        keys.append(key.decode() if isinstance(key, bytes) else key)
    print("ip rate limit keys:", keys)
    assert len(keys) == 2
    assert all(key.startswith(f"{rate_limit_prefix}:login:") for key in keys)
    assert all("192.0.2.10" not in key for key in keys)
    ttls = [await raw_redis.ttl(key) for key in keys]
    print("ip rate limit ttls:", ttls)
    assert all(ttl > 0 for ttl in ttls)


@pytest.mark.integration
async def test_task_queue_full_retry_hint_is_fixed(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
):
    """Queue-full retry hints should not require Redis-side drain tracking."""
    redis_client, task_type, _ = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
    monkeypatch.setattr(GulpRedis, "STREAM_TASK_MAXLEN", 2)

    for idx in range(2):
        await redis_client.task_enqueue(
            {
                "task_type": task_type,
                "operation_id": "op-adaptive-retry",
                "user_id": "admin",
                "ws_id": "ws-adaptive-retry",
                "req_id": f"req-adaptive-retry-{idx}-{task_type}",
                "params": {},
            }
        )

    with pytest.raises(TaskQueueFullError) as exc_info:
        await redis_client.task_enqueue(
            {
                "task_type": task_type,
                "operation_id": "op-adaptive-retry",
                "user_id": "admin",
                "ws_id": "ws-adaptive-retry",
                "req_id": f"req-adaptive-retry-full-{task_type}",
                "params": {},
            }
        )

    print(
        "task queue adaptive retry hint:",
        {
            "stream_key": stream_key,
            "stream_len": await raw_redis.xlen(stream_key),
            "retry_after_msec": exc_info.value.retry_after_msec,
        },
    )
    assert exc_info.value.queue_depth == 2
    assert exc_info.value.queue_limit == 2
    assert exc_info.value.work_units == 1
    assert exc_info.value.retry_after_msec == 1000


@pytest.mark.integration
async def test_task_queue_failure_dead_letters_with_real_redis(
    isolated_task_redis: tuple[GulpRedis, str, str],
):
    """A failed valid task is moved directly to the dead-letter stream."""
    redis_client, task_type, dead_stream_key = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
    task = {
        "task_type": task_type,
        "operation_id": "op-integration-task-queue",
        "user_id": "admin",
        "ws_id": "ws-integration-task-queue",
        "req_id": f"req-{task_type}",
        "params": {"query": "*"},
    }

    await redis_client.task_enqueue(task)
    queued_snapshot = await redis_client.task_metrics_snapshot()
    print("task queue queued metrics snapshot:", queued_snapshot)
    assert queued_snapshot["task_types"][task_type]["queued"] == 1
    assert queued_snapshot["task_types"][task_type]["oldest_queued_age_msec"] >= 0
    assert queued_snapshot["task_types"][task_type]["oldest_pending_age_msec"] == 0
    first_batch = await redis_client.task_dequeue_batch(1)

    print(
        "task queue first dequeue:",
        {
            "stream_key": stream_key,
            "batch": first_batch,
            "stream_len": await raw_redis.xlen(stream_key),
        },
    )
    assert len(first_batch) == 1
    assert first_batch[0]["req_id"] == task["req_id"]
    assert first_batch[0]["__redis_stream__"] == stream_key
    assert await raw_redis.xlen(stream_key) == 1
    pending_snapshot = await redis_client.task_metrics_snapshot()
    print("task queue pending metrics snapshot:", pending_snapshot)
    assert pending_snapshot["task_types"][task_type]["pending"] == 1
    assert pending_snapshot["task_types"][task_type]["oldest_pending_age_msec"] >= 0
    assert await redis_client.task_claim_execution(first_batch[0]) == "claimed"
    running_snapshot = await redis_client.task_metrics_snapshot()
    print("task queue running metrics snapshot:", running_snapshot)
    assert running_snapshot["running"][redis_client.server_id][task_type] == 1
    assert await redis_client.task_fail_dead_letter(
        first_batch[0],
        "integration failure",
    ) == "dead_letter"

    dead_entries = await raw_redis.xrange(dead_stream_key)

    print(
        "task queue dead letter:",
        {
            "stream_key": stream_key,
            "dead_stream_key": dead_stream_key,
            "stream_len": await raw_redis.xlen(stream_key),
            "dead_entries": dead_entries,
        },
    )
    assert await raw_redis.xlen(stream_key) == 0
    assert len(dead_entries) == 1
    dead_snapshot = await redis_client.task_metrics_snapshot()
    print("task queue dead-letter metrics snapshot:", dead_snapshot)
    assert dead_snapshot["task_types"][task_type]["dead_lettered"] == 1

    dead_payload = orjson.loads(dead_entries[0][1][b"data"])
    assert dead_payload["reason"] == "task failed: integration failure"
    assert dead_payload["stream_name"] == stream_key
    assert dead_payload["task"]["req_id"] == task["req_id"]
    assert dead_payload["task"]["__task_last_error__"] == "integration failure"
    assert "__task_attempts__" not in dead_payload["task"]


@pytest.mark.integration
async def test_task_lifecycle_suppresses_duplicate_stream_delivery_with_real_redis(
    isolated_task_redis: tuple[GulpRedis, str, str],
):
    """A replayed stream message for one unique req_id is ACKed/skipped."""
    redis_client, task_type, _ = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
    req_id = f"req-duplicate-{task_type}"
    base_task = {
        "task_type": task_type,
        "operation_id": "op-integration-task-lifecycle",
        "user_id": "admin",
        "ws_id": "ws-integration-task-lifecycle",
        "req_id": req_id,
        "params": {"query": "*"},
    }

    await redis_client.task_enqueue(base_task)
    await raw_redis.xadd(
        stream_key,
        {"data": orjson.dumps({**base_task, "params": {"query": "replayed"}})},
    )

    batch = await redis_client.task_dequeue_batch(2)

    print(
        "task lifecycle duplicate delivery batch:",
        {
            "stream_key": stream_key,
            "batch": batch,
            "stream_len": await raw_redis.xlen(stream_key),
        },
    )
    assert len(batch) == 2
    assert await redis_client.task_claim_execution(batch[0]) == "claimed"
    assert await redis_client.task_claim_execution(batch[1]) == "duplicate_running"

    await redis_client.task_ack_delete(batch[1])
    await redis_client.task_mark_succeeded(batch[0])
    await redis_client.task_ack_delete(batch[0])

    lifecycle = await raw_redis.hgetall(f"{GulpRedis.TASK_LIFECYCLE_PREFIX}:{req_id}")
    decoded = {
        key.decode() if isinstance(key, bytes) else key: value.decode()
        if isinstance(value, bytes)
        else value
        for key, value in lifecycle.items()
    }

    print("task lifecycle duplicate delivery final state:", decoded)
    assert decoded["status"] == "succeeded"
    assert decoded["req_id"] == req_id
    assert await raw_redis.xlen(stream_key) == 0


@pytest.mark.integration
async def test_task_lifecycle_allows_distinct_task_ids_for_same_req_id(
    isolated_task_redis: tuple[GulpRedis, str, str],
):
    """A fan-out request may execute multiple queued tasks under one req_id."""
    redis_client, task_type, _ = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
    req_id = f"req-multi-task-{task_type}"
    first_task_id = f"{req_id}:batch:1"
    second_task_id = f"{req_id}:batch:2"
    base_task = {
        "task_type": task_type,
        "operation_id": "op-integration-task-lifecycle",
        "user_id": "admin",
        "ws_id": "ws-integration-task-lifecycle",
        "req_id": req_id,
        "params": {"query": "*"},
    }

    await redis_client.task_enqueue({**base_task, "__task_id__": first_task_id})
    await redis_client.task_enqueue(
        {**base_task, "__task_id__": second_task_id, "params": {"query": "second"}}
    )

    batch = await redis_client.task_dequeue_batch(2)

    print(
        "task lifecycle same req_id distinct task_ids batch:",
        {
            "stream_key": stream_key,
            "batch": batch,
            "stream_len": await raw_redis.xlen(stream_key),
        },
    )
    assert len(batch) == 2
    assert {task["__task_id__"] for task in batch} == {first_task_id, second_task_id}
    assert await redis_client.task_claim_execution(batch[0]) == "claimed"
    assert await redis_client.task_claim_execution(batch[1]) == "claimed"

    for task in batch:
        await redis_client.task_mark_succeeded(task)
        await redis_client.task_ack_delete(task)

    first_lifecycle = await raw_redis.hgetall(
        f"{GulpRedis.TASK_LIFECYCLE_PREFIX}:{first_task_id}"
    )
    second_lifecycle = await raw_redis.hgetall(
        f"{GulpRedis.TASK_LIFECYCLE_PREFIX}:{second_task_id}"
    )
    decoded_lifecycles = []
    for lifecycle in (first_lifecycle, second_lifecycle):
        decoded_lifecycles.append(
            {
                key.decode() if isinstance(key, bytes) else key: value.decode()
                if isinstance(value, bytes)
                else value
                for key, value in lifecycle.items()
            }
        )

    print("task lifecycle same req_id distinct task_ids final state:", decoded_lifecycles)
    assert [item["status"] for item in decoded_lifecycles] == ["succeeded", "succeeded"]
    assert {item["req_id"] for item in decoded_lifecycles} == {req_id}
    assert {item["task_id"] for item in decoded_lifecycles} == {
        first_task_id,
        second_task_id,
    }
    assert await raw_redis.xlen(stream_key) == 0


@pytest.mark.integration
async def test_task_lifecycle_suppresses_duplicate_delivery_across_two_instances(
    isolated_task_redis: tuple[GulpRedis, str, str],
):
    """Two all-role consumers cannot both claim work for the same unique req_id."""
    redis_client, task_type, _ = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
    other_client = object.__new__(GulpRedis)
    GulpRedis.__init__(other_client)
    other_client._redis = raw_redis
    other_client.server_id = f"integration-task-queue-other-{task_type}"
    other_client._instance_roles = []
    req_id = f"req-two-instance-duplicate-{task_type}"
    base_task = {
        "task_type": task_type,
        "operation_id": "op-integration-task-lifecycle",
        "user_id": "admin",
        "ws_id": "ws-integration-task-lifecycle",
        "req_id": req_id,
        "params": {"query": "*"},
    }

    await redis_client.task_enqueue(base_task)
    await raw_redis.xadd(
        stream_key,
        {"data": orjson.dumps({**base_task, "params": {"query": "replayed"}})},
    )

    first_batch = await redis_client.task_dequeue_batch(1)
    second_batch = await other_client.task_dequeue_batch(1)

    print(
        "task lifecycle two-instance duplicate delivery:",
        {
            "first_server_id": redis_client.server_id,
            "second_server_id": other_client.server_id,
            "first_batch": first_batch,
            "second_batch": second_batch,
            "stream_len": await raw_redis.xlen(stream_key),
        },
    )
    assert len(first_batch) == 1
    assert len(second_batch) == 1
    assert first_batch[0]["req_id"] == req_id
    assert second_batch[0]["req_id"] == req_id
    assert first_batch[0]["__redis_consumer_name__"] == redis_client.server_id
    assert second_batch[0]["__redis_consumer_name__"] == other_client.server_id

    assert await redis_client.task_claim_execution(first_batch[0]) == "claimed"
    assert await other_client.task_claim_execution(second_batch[0]) == "duplicate_running"

    await other_client.task_ack_delete(second_batch[0])
    await redis_client.task_mark_succeeded(first_batch[0])
    await redis_client.task_ack_delete(first_batch[0])

    lifecycle = await raw_redis.hgetall(f"{GulpRedis.TASK_LIFECYCLE_PREFIX}:{req_id}")
    decoded = {
        key.decode() if isinstance(key, bytes) else key: value.decode()
        if isinstance(value, bytes)
        else value
        for key, value in lifecycle.items()
    }

    print("task lifecycle two-instance final state:", decoded)
    assert decoded["status"] == "succeeded"
    assert decoded["server_id"] == redis_client.server_id
    assert await raw_redis.xlen(stream_key) == 0


@pytest.mark.integration
async def test_task_autoclaim_reclaims_pending_after_crashed_dispatcher_lock_expires(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
):
    """A pending task is reclaimed only after execution and side-effect leases clear."""
    redis_client, task_type, _ = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
    original_server_id = redis_client.server_id
    reclaimer_server_id = f"reclaimer-{task_type}"
    original_heartbeat_key = f"{GulpRedis.HEARTBEAT_KEY_PREFIX}:{original_server_id}"
    req_id = f"req-autoclaim-crash-{task_type}"
    side_effect_lock_key = f"{GulpRedis.TASK_SIDE_EFFECT_LOCK_PREFIX}:{req_id}"
    task = {
        "task_type": task_type,
        "operation_id": "op-integration-task-autoclaim",
        "user_id": "admin",
        "ws_id": "ws-integration-task-autoclaim",
        "req_id": req_id,
        "params": {"query": "*"},
    }
    monkeypatch.setattr(GulpRedis, "TASK_AUTOCLAIM_IDLE_MS", 1)
    monkeypatch.setattr(GulpRedis, "TASK_LEASE_REFRESH_INTERVAL_MS", 1)

    try:
        await redis_client.task_enqueue(task)
        first_batch = await redis_client.task_dequeue_batch(1)
        assert len(first_batch) == 1
        assert await redis_client.task_claim_execution(first_batch[0]) == "claimed"

        redis_client.server_id = reclaimer_server_id
        await asyncio.sleep(0.02)
        reclaimed_while_locked = await redis_client.task_autoclaim_stale()

        print(
            "task autoclaim while original execution lock is alive:",
            {
                "original_server_id": original_server_id,
                "reclaimer_server_id": reclaimer_server_id,
                "reclaimed": reclaimed_while_locked,
                "stream_len": await raw_redis.xlen(stream_key),
            },
        )
        assert len(reclaimed_while_locked) == 1
        assert reclaimed_while_locked[0]["req_id"] == req_id
        assert reclaimed_while_locked[0]["__redis_autoclaimed__"] is True
        assert (
            await redis_client.task_claim_execution(reclaimed_while_locked[0])
            == "duplicate_running"
        )
        assert await raw_redis.xlen(stream_key) == 1

        await raw_redis.set(original_heartbeat_key, "1", ex=60)
        await asyncio.sleep(1.1)
        reclaimed_while_owner_alive = await redis_client.task_autoclaim_stale()
        assert len(reclaimed_while_owner_alive) == 1
        assert (
            await redis_client.task_claim_execution(reclaimed_while_owner_alive[0])
            == "duplicate_running"
        )
        assert await raw_redis.exists(
            f"{GulpRedis.TASK_EXECUTION_LOCK_PREFIX}:{req_id}"
        ) == 0
        assert await raw_redis.xlen(stream_key) == 1

        await raw_redis.delete(original_heartbeat_key)
        reclaimed_while_side_effect_busy = await redis_client.task_autoclaim_stale()
        assert len(reclaimed_while_side_effect_busy) == 1
        assert (
            await redis_client.task_claim_execution(reclaimed_while_side_effect_busy[0])
            == "duplicate_running"
        )
        assert await raw_redis.exists(side_effect_lock_key) == 1
        assert await raw_redis.xlen(stream_key) == 1

        await raw_redis.delete(side_effect_lock_key)
        reclaimed_after_side_effect_lease = await redis_client.task_autoclaim_stale()

        print(
            "task autoclaim after original owner heartbeat and side-effect lease expire:",
            {
                "reclaimed": reclaimed_after_side_effect_lease,
                "stream_len": await raw_redis.xlen(stream_key),
            },
        )
        assert len(reclaimed_after_side_effect_lease) == 1
        assert reclaimed_after_side_effect_lease[0]["req_id"] == req_id
        assert (
            await redis_client.task_claim_execution(reclaimed_after_side_effect_lease[0])
            == "claimed"
        )
        await redis_client.task_mark_succeeded(reclaimed_after_side_effect_lease[0])
        await redis_client.task_ack_delete(reclaimed_after_side_effect_lease[0])

        lifecycle = await raw_redis.hgetall(
            f"{GulpRedis.TASK_LIFECYCLE_PREFIX}:{req_id}"
        )
        decoded = {
            key.decode() if isinstance(key, bytes) else key: value.decode()
            if isinstance(value, bytes)
            else value
            for key, value in lifecycle.items()
        }

        print("task autoclaim final lifecycle:", decoded)
        assert decoded["status"] == "succeeded"
        assert decoded["server_id"] == reclaimer_server_id
        assert await raw_redis.xlen(stream_key) == 0
    finally:
        await raw_redis.delete(original_heartbeat_key, side_effect_lock_key)


@pytest.mark.integration
async def test_reclaimed_dispatcher_runs_handler_once_after_side_effect_lease_clears(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
):
    """A reclaimed stream entry should run through dispatcher finalization once."""
    from gulp.api.server_api import GulpServer

    redis_client, task_type, _ = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
    original_server_id = redis_client.server_id
    reclaimer_server_id = f"reclaimer-dispatch-{task_type}"
    req_id = f"req-reclaimed-dispatch-{task_type}"
    execution_lock_key = f"{GulpRedis.TASK_EXECUTION_LOCK_PREFIX}:{req_id}"
    side_effect_lock_key = f"{GulpRedis.TASK_SIDE_EFFECT_LOCK_PREFIX}:{req_id}"
    side_effect_key = f"gulp:test:handler_side_effect:{task_type}"
    task = {
        "task_type": task_type,
        "operation_id": "op-integration-reclaimed-dispatch",
        "user_id": "admin",
        "ws_id": "ws-integration-reclaimed-dispatch",
        "req_id": req_id,
        "params": {"query": "*"},
    }
    monkeypatch.setattr(GulpRedis, "TASK_AUTOCLAIM_IDLE_MS", 1)
    monkeypatch.setattr(GulpRedis, "TASK_LEASE_REFRESH_INTERVAL_MS", 1)
    GulpServer._instance = None
    server = GulpServer.get_instance()

    async def _side_effect_handler(_task: dict) -> bool:
        await raw_redis.incr(side_effect_key)
        return True

    monkeypatch.setattr(server, "_run_claimed_task", _side_effect_handler)

    try:
        await redis_client.task_enqueue(task)
        first_batch = await redis_client.task_dequeue_batch(1)
        assert len(first_batch) == 1
        assert await redis_client.task_claim_execution(first_batch[0]) == "claimed"

        redis_client.server_id = reclaimer_server_id
        await asyncio.sleep(0.02)
        reclaimed_while_busy = await redis_client.task_autoclaim_stale()
        assert len(reclaimed_while_busy) == 1

        await server._dispatch_claimed_tasks(
            redis_client,
            reclaimed_while_busy,
            source="reclaimed",
        )
        assert await raw_redis.get(side_effect_key) is None
        assert await raw_redis.xlen(stream_key) == 1

        await raw_redis.delete(execution_lock_key, side_effect_lock_key)
        await asyncio.sleep(0.02)
        reclaimed_after_lease = await redis_client.task_autoclaim_stale()
        assert len(reclaimed_after_lease) == 1
        assert reclaimed_after_lease[0]["req_id"] == req_id

        await server._dispatch_claimed_tasks(
            redis_client,
            reclaimed_after_lease,
            source="reclaimed",
        )

        lifecycle = await raw_redis.hgetall(
            f"{GulpRedis.TASK_LIFECYCLE_PREFIX}:{req_id}"
        )
        decoded = {
            key.decode() if isinstance(key, bytes) else key: value.decode()
            if isinstance(value, bytes)
            else value
            for key, value in lifecycle.items()
        }
        side_effect_count = await raw_redis.get(side_effect_key)

        print(
            "reclaimed dispatcher final state:",
            {
                "original_server_id": original_server_id,
                "reclaimer_server_id": reclaimer_server_id,
                "lifecycle": decoded,
                "side_effect_count": side_effect_count,
                "stream_len": await raw_redis.xlen(stream_key),
            },
        )
        assert side_effect_count == b"1"
        assert decoded["status"] == "succeeded"
        assert decoded["server_id"] == reclaimer_server_id
        assert await raw_redis.xlen(stream_key) == 0

        replay_id = await raw_redis.xadd(stream_key, {"data": orjson.dumps(task)})
        replay_envelope = redis_client._task_envelope(
            task,
            stream_key,
            replay_id,
            consumer_name=redis_client.server_id,
        )
        await server._dispatch_claimed_tasks(
            redis_client,
            [replay_envelope],
            source="replayed",
        )

        assert await raw_redis.get(side_effect_key) == b"1"
        assert await raw_redis.xlen(stream_key) == 0
    finally:
        await raw_redis.delete(
            execution_lock_key,
            side_effect_lock_key,
            side_effect_key,
        )
        GulpServer._instance = None


@pytest.mark.integration
@pytest.mark.parametrize(
    "handler_task_type",
    [
        TASK_TYPE_QUERY,
        TASK_TYPE_EXTERNAL_QUERY,
        TASK_TYPE_REBASE,
        TASK_TYPE_INGEST,
        TASK_TYPE_ENRICH,
    ],
)
async def test_reclaimed_dispatcher_uses_real_task_handler_entrypoint_once(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
    handler_task_type: str,
):
    """Reclaimed Redis task types should execute their real entrypoint only once."""
    from gulp.api.server import db as db_mod
    from gulp.api.server import enrich as enrich_mod
    from gulp.api.server import ingest as ingest_mod
    from gulp.api.server import query as query_mod
    from gulp.api.server_api import GulpServer

    redis_client, isolated_task_type, _ = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{handler_task_type}"
    original_server_id = redis_client.server_id
    reclaimer_server_id = f"reclaimer-{handler_task_type}-{isolated_task_type}"
    req_id = f"req-reclaimed-{handler_task_type}-{isolated_task_type}"
    execution_lock_key = f"{GulpRedis.TASK_EXECUTION_LOCK_PREFIX}:{req_id}"
    side_effect_lock_key = f"{GulpRedis.TASK_SIDE_EFFECT_LOCK_PREFIX}:{req_id}"
    side_effect_key = f"gulp:test:handler_entrypoint:{handler_task_type}:{isolated_task_type}"

    if handler_task_type in {TASK_TYPE_QUERY, TASK_TYPE_EXTERNAL_QUERY}:
        task = {
            "task_type": handler_task_type,
            "operation_id": f"op-reclaimed-{handler_task_type}-handler",
            "user_id": "admin",
            "ws_id": f"ws-reclaimed-{handler_task_type}-handler",
            "req_id": req_id,
            "params": {
                "queries": [
                    {
                        "q": {"query": {"match_all": {}}},
                        "q_name": "reclaimed_handler_query",
                    }
                ],
                "q_options": {"limit": 1},
                "total_num_queries": 1,
                "plugin": "query_elasticsearch"
                if handler_task_type == TASK_TYPE_EXTERNAL_QUERY
                else None,
                "plugin_params": {"uri": "http://127.0.0.1:9200"}
                if handler_task_type == TASK_TYPE_EXTERNAL_QUERY
                else None,
            },
        }

        async def _process_queries(*args, **kwargs):
            assert kwargs["req_id"] == req_id
            assert kwargs["operation_id"] == task["operation_id"]
            assert len(kwargs["queries"]) == 1
            await raw_redis.incr(side_effect_key)

        monkeypatch.setattr(
            query_mod.GulpCollab,
            "get_instance",
            staticmethod(lambda: _FakeCollab()),
        )
        monkeypatch.setattr(query_mod, "process_queries", _process_queries)
    elif handler_task_type == TASK_TYPE_REBASE:
        task = {
            "task_type": TASK_TYPE_REBASE,
            "operation_id": "op-reclaimed-rebase-handler",
            "user_id": "admin",
            "ws_id": "ws-reclaimed-rebase-handler",
            "req_id": req_id,
            "params": {
                "index": "idx-reclaimed-rebase-handler",
                "offset_msec": 1000,
                "flt": None,
                "fields": ["@timestamp"],
            },
        }
    elif handler_task_type == TASK_TYPE_INGEST:
        task = {
            "task_type": TASK_TYPE_INGEST,
            "operation_id": "op-reclaimed-ingest-handler",
            "user_id": "admin",
            "ws_id": "ws-reclaimed-ingest-handler",
            "req_id": req_id,
            "params": {
                "req_id": req_id,
                "ws_id": "ws-reclaimed-ingest-handler",
                "context_name": "ctx-reclaimed-ingest-handler",
                "source_name": "src-reclaimed-ingest-handler",
                "index": "idx-reclaimed-ingest-handler",
                "plugin": "raw",
                "file_path": "/tmp/reclaimed-ingest-handler.json",
                "payload": {},
                "delete_after": False,
            },
        }
    else:
        task = {
            "task_type": TASK_TYPE_ENRICH,
            "operation_id": "op-reclaimed-enrich-handler",
            "user_id": "admin",
            "ws_id": "ws-reclaimed-enrich-handler",
            "req_id": req_id,
            "params": {
                "action": "update_documents",
                "index": "idx-reclaimed-enrich-handler",
                "flt": {"operation_ids": ["op-reclaimed-enrich-handler"]},
                "data": {"field": "value"},
            },
        }

    monkeypatch.setattr(GulpRedis, "TASK_AUTOCLAIM_IDLE_MS", 1)
    monkeypatch.setattr(GulpRedis, "TASK_LEASE_REFRESH_INTERVAL_MS", 1)
    GulpServer._instance = None
    server = GulpServer.get_instance()

    if handler_task_type in {TASK_TYPE_REBASE, TASK_TYPE_INGEST, TASK_TYPE_ENRICH}:

        async def _spawn_worker_task(
            fn,
            *args,
            wait: bool = False,
            task_name: str = None,
        ):
            if handler_task_type == TASK_TYPE_REBASE:
                assert fn is db_mod._rebase_by_query_internal
                assert args[0] == req_id
                assert args[3] == task["operation_id"]
                assert wait is True
                assert task_name == f"rebase_{req_id}"
            elif handler_task_type == TASK_TYPE_INGEST:
                assert fn is ingest_mod.run_ingest_file_task
                assert len(args) == 1
                assert args[0]["req_id"] == req_id
                assert args[0]["operation_id"] == task["operation_id"]
                assert args[0]["__redis_autoclaimed__"] is True
                assert wait is True
                assert task_name is None
                monkeypatch.setattr(
                    ingest_mod,
                    "_ingest_file_internal",
                    _ingest_file_internal,
                )
                assert await fn(*args) is True
            else:
                assert fn is enrich_mod._update_documents_internal
                assert args[0] == task["user_id"]
                assert args[1] == task["ws_id"]
                assert args[2] == req_id
                assert args[3] == task["operation_id"]
                assert args[4] == task["params"]["index"]
                assert args[6] == task["params"]["data"]
                assert wait is True
                assert task_name == f"enrich_{req_id}"
            await raw_redis.incr(side_effect_key)
            return True

        async def _ingest_file_internal(**kwargs):
            assert kwargs["req_id"] == req_id
            assert kwargs["operation_id"] == task["operation_id"]
            assert kwargs["payload"].__class__.__name__ == "GulpIngestPayload"

        monkeypatch.setattr(server, "spawn_worker_task", _spawn_worker_task)

    try:
        await redis_client.task_enqueue(task)
        first_batch = await redis_client.task_dequeue_batch(1)
        assert len(first_batch) == 1
        assert await redis_client.task_claim_execution(first_batch[0]) == "claimed"

        redis_client.server_id = reclaimer_server_id
        await asyncio.sleep(0.02)
        reclaimed_while_busy = await redis_client.task_autoclaim_stale()
        assert len(reclaimed_while_busy) == 1

        await server._dispatch_claimed_tasks(
            redis_client,
            reclaimed_while_busy,
            source="reclaimed",
        )
        assert await raw_redis.get(side_effect_key) is None
        assert await raw_redis.xlen(stream_key) == 1

        await raw_redis.delete(execution_lock_key, side_effect_lock_key)
        await asyncio.sleep(0.02)
        reclaimed_after_lease = await redis_client.task_autoclaim_stale()
        assert len(reclaimed_after_lease) == 1
        assert reclaimed_after_lease[0]["req_id"] == req_id

        await server._dispatch_claimed_tasks(
            redis_client,
            reclaimed_after_lease,
            source="reclaimed",
        )

        lifecycle = await raw_redis.hgetall(
            f"{GulpRedis.TASK_LIFECYCLE_PREFIX}:{req_id}"
        )
        decoded = {
            key.decode() if isinstance(key, bytes) else key: value.decode()
            if isinstance(value, bytes)
            else value
            for key, value in lifecycle.items()
        }

        print(
            "reclaimed real handler final state:",
            {
                "handler_task_type": handler_task_type,
                "original_server_id": original_server_id,
                "reclaimer_server_id": reclaimer_server_id,
                "lifecycle": decoded,
                "side_effect_count": await raw_redis.get(side_effect_key),
                "stream_len": await raw_redis.xlen(stream_key),
            },
        )
        assert await raw_redis.get(side_effect_key) == b"1"
        assert decoded["status"] == "succeeded"
        assert decoded["server_id"] == reclaimer_server_id
        assert await raw_redis.xlen(stream_key) == 0

        replay_id = await raw_redis.xadd(stream_key, {"data": orjson.dumps(task)})
        replay_envelope = redis_client._task_envelope(
            task,
            stream_key,
            replay_id,
            consumer_name=redis_client.server_id,
        )
        await server._dispatch_claimed_tasks(
            redis_client,
            [replay_envelope],
            source="replayed",
        )

        assert await raw_redis.get(side_effect_key) == b"1"
        assert await raw_redis.xlen(stream_key) == 0
    finally:
        await raw_redis.delete(
            stream_key,
            execution_lock_key,
            side_effect_lock_key,
            side_effect_key,
        )
        GulpServer._instance = None


@pytest.mark.integration
@pytest.mark.parametrize(
    ("action", "params", "expected_worker"),
    [
        (
            "update_documents",
            {
                "data": {"sdk_worker_failure": "update"},
            },
            "_update_documents_internal",
        ),
        (
            "tag_documents",
            {
                "data": {"gulp.tags": ["sdk_worker_failure_tag"]},
            },
            "_update_documents_internal",
        ),
        (
            "untag_documents",
            {
                "tags": ["sdk_worker_failure_tag"],
            },
            "_untag_documents_internal",
        ),
        (
            "enrich_remove",
            {
                "remove_fields": ["gulp.enriched"],
            },
            "_enrich_remove_internal",
        ),
    ],
)
async def test_enrich_mutation_worker_failure_dead_letters(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    params: dict,
    expected_worker: str,
):
    """Redis dispatch dead-letters failed bulk enrich mutation workers."""
    from gulp.api.server import enrich as enrich_mod
    from gulp.api.server_api import GulpServer

    redis_client, _, _ = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{TASK_TYPE_ENRICH}"
    dead_stream_key = f"{GulpRedis.STREAM_TASK_DLQ_PREFIX}:{TASK_TYPE_ENRICH}"
    req_id = f"req-enrich-worker-failure-{action}-{uuid4().hex}"
    task = {
        "task_type": TASK_TYPE_ENRICH,
        "operation_id": f"op-enrich-worker-failure-{action}",
        "user_id": "admin",
        "ws_id": f"ws-enrich-worker-failure-{action}",
        "req_id": req_id,
        "params": {
            "action": action,
            "index": f"idx-enrich-worker-failure-{action}",
            "flt": {"operation_ids": [f"op-enrich-worker-failure-{action}"]},
            **params,
        },
    }
    worker_calls: list[dict] = []

    GulpServer._instance = None
    server = GulpServer.get_instance()

    async def _spawn_worker_task(fn, *args, wait: bool = False, task_name: str = None):
        worker_calls.append(
            {
                "fn": fn.__name__,
                "args": args,
                "wait": wait,
                "task_name": task_name,
            }
        )
        assert fn is getattr(enrich_mod, expected_worker)
        assert wait is True
        assert task_name == f"enrich_{req_id}"
        raise RuntimeError(f"simulated {action} worker future failure")

    async def _mark_dead_lettered_task_stats_failed(_task: dict, _reason: str) -> None:
        return None

    monkeypatch.setattr(server, "spawn_worker_task", _spawn_worker_task)
    monkeypatch.setattr(
        server,
        "_mark_dead_lettered_task_stats_failed",
        _mark_dead_lettered_task_stats_failed,
    )

    try:
        await redis_client.task_enqueue(task)
        first_batch = await redis_client.task_dequeue_batch(1)
        assert len(first_batch) == 1
        await server._dispatch_claimed_tasks(redis_client, first_batch, source="queued")

        failure_snapshot = await redis_client.task_metrics_snapshot()
        print(
            "enrich worker failure dead-letter state:",
            {
                "action": action,
                "worker_calls": worker_calls,
                "failure_snapshot": failure_snapshot,
                "stream_len": await raw_redis.xlen(stream_key),
            },
        )
        assert len(worker_calls) == 1
        assert failure_snapshot["task_types"][TASK_TYPE_ENRICH]["dead_lettered"] == 1

        lifecycle = await raw_redis.hgetall(
            f"{GulpRedis.TASK_LIFECYCLE_PREFIX}:{req_id}"
        )
        decoded = {
            key.decode() if isinstance(key, bytes) else key: value.decode()
            if isinstance(value, bytes)
            else value
            for key, value in lifecycle.items()
        }
        dead_entries = await raw_redis.xrange(dead_stream_key)

        print(
            "enrich worker failure dead-letter state:",
            {
                "action": action,
                "worker_calls": worker_calls,
                "lifecycle": decoded,
                "dead_entries": dead_entries,
                "stream_len": await raw_redis.xlen(stream_key),
            },
        )
        assert len(worker_calls) == 1
        expected_error = f"RuntimeError('simulated {action} worker future failure')"
        assert decoded["status"] == "dead_lettered"
        assert decoded["last_error"] == expected_error
        assert await raw_redis.xlen(stream_key) == 0
        assert len(dead_entries) == 1
        dead_payload = orjson.loads(dead_entries[0][1][b"data"])
        assert dead_payload["reason"].startswith("task failed:")
        assert expected_error in dead_payload["reason"]
        assert dead_payload["task"]["req_id"] == req_id
        assert "__task_attempts__" not in dead_payload["task"]
    finally:
        await raw_redis.delete(stream_key, dead_stream_key)
        GulpServer._instance = None


@pytest.mark.integration
async def test_task_cancel_purges_queued_task_and_releases_active_admission(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
):
    """Queued request cancellation releases active units and blocks replay by req_id."""
    redis_client, task_type, _ = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
    user_id = "user-cancel"
    operation_id = "op-cancel"
    req_id = f"req-cancel-{task_type}"
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_USER_MAX", 10)
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_OPERATION_MAX", 10)
    task = {
        "task_type": task_type,
        "operation_id": operation_id,
        "user_id": user_id,
        "ws_id": "ws-integration-task-cancel",
        "req_id": req_id,
        "params": {"query": "*"},
        "__task_work_units__": 3,
    }

    await redis_client.task_enqueue(task)
    assert int(await raw_redis.hget(GulpRedis.TASK_ACTIVE_USER_HASH, user_id)) == 3
    assert (
        int(await raw_redis.hget(GulpRedis.TASK_ACTIVE_OPERATION_HASH, operation_id))
        == 3
    )

    removed = await redis_client.task_purge_by_filter(
        operation_id=operation_id,
        req_id=req_id,
    )

    lifecycle = await raw_redis.hgetall(f"{GulpRedis.TASK_LIFECYCLE_PREFIX}:{req_id}")
    decoded = {
        key.decode() if isinstance(key, bytes) else key: value.decode()
        if isinstance(value, bytes)
        else value
        for key, value in lifecycle.items()
    }
    active_user_units = await raw_redis.hget(GulpRedis.TASK_ACTIVE_USER_HASH, user_id)
    active_operation_units = await raw_redis.hget(
        GulpRedis.TASK_ACTIVE_OPERATION_HASH,
        operation_id,
    )

    print(
        "task cancel purge state:",
        {
            "removed": removed,
            "lifecycle": decoded,
            "active_user_units": active_user_units,
            "active_operation_units": active_operation_units,
            "stream_len": await raw_redis.xlen(stream_key),
        },
    )
    assert removed == 1
    assert decoded["status"] == "canceled"
    assert await redis_client.task_record_queued(task) == "canceled"
    assert await raw_redis.xlen(stream_key) == 0
    assert active_user_units in {None, b"0"}
    assert active_operation_units in {None, b"0"}


@pytest.mark.integration
async def test_task_admission_fairness_limits_one_user_not_another_with_real_redis(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
):
    """Per-user active limits reject one saturated user without starving another."""
    redis_client, task_type, _ = isolated_task_redis
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_USER_MAX", 1)

    user_one_task = {
        "task_type": task_type,
        "operation_id": "op-user-one",
        "user_id": "user-one",
        "ws_id": "ws-user-one",
        "req_id": f"req-user-one-{task_type}",
        "params": {"query": "one"},
    }
    user_one_second_task = {
        **user_one_task,
        "operation_id": "op-user-one-second",
        "req_id": f"req-user-one-second-{task_type}",
        "params": {"query": "one-second"},
    }
    user_two_task = {
        **user_one_task,
        "operation_id": "op-user-two",
        "user_id": "user-two",
        "ws_id": "ws-user-two",
        "req_id": f"req-user-two-{task_type}",
        "params": {"query": "two"},
    }

    await redis_client.task_enqueue(user_one_task)
    with pytest.raises(TaskQueueFullError) as exc_info:
        await redis_client.task_enqueue(user_one_second_task)
    assert getattr(exc_info.value, "scope", None) == "user"

    await redis_client.task_enqueue(user_two_task)
    raw_redis = redis_client.client()
    active_users = await raw_redis.hgetall(GulpRedis.TASK_ACTIVE_USER_HASH)
    decoded = {
        key.decode() if isinstance(key, bytes) else key: value.decode()
        if isinstance(value, bytes)
        else value
        for key, value in active_users.items()
    }

    print("task admission active users:", decoded)
    assert decoded["user-one"] == "1"
    assert decoded["user-two"] == "1"


@pytest.mark.integration
async def test_task_admission_counts_query_batch_work_units_with_real_redis(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
):
    """Large query batches consume proportionally more active admission units."""
    redis_client, task_type, _ = isolated_task_redis
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_USER_MAX", 4)

    large_batch_task = {
        "task_type": task_type,
        "operation_id": "op-large-batch",
        "user_id": "user-large-batch",
        "ws_id": "ws-large-batch",
        "req_id": f"req-large-batch-{task_type}",
        "params": {
            "total_num_queries": 3,
            "queries": [{"query": {"match_all": {}}}],
        },
    }
    second_large_task = {
        **large_batch_task,
        "operation_id": "op-large-batch-second",
        "req_id": f"req-large-batch-second-{task_type}",
    }
    other_user_small_task = {
        **large_batch_task,
        "operation_id": "op-other-small",
        "user_id": "user-other-small",
        "ws_id": "ws-other-small",
        "req_id": f"req-other-small-{task_type}",
        "params": {"queries": [{"query": {"match_none": {}}}]},
    }

    await redis_client.task_enqueue(large_batch_task)
    with pytest.raises(TaskQueueFullError) as exc_info:
        await redis_client.task_enqueue(second_large_task)
    assert exc_info.value.scope == "user"
    assert exc_info.value.queue_depth == 3
    assert exc_info.value.queue_limit == 4
    assert exc_info.value.work_units == 3

    await redis_client.task_enqueue(other_user_small_task)

    raw_redis = redis_client.client()
    active_users = await raw_redis.hgetall(GulpRedis.TASK_ACTIVE_USER_HASH)
    decoded = {
        key.decode() if isinstance(key, bytes) else key: value.decode()
        if isinstance(value, bytes)
        else value
        for key, value in active_users.items()
    }

    print("task admission work-aware active users:", decoded)
    assert decoded["user-large-batch"] == "3"
    assert decoded["user-other-small"] == "1"


@pytest.mark.integration
async def test_task_admission_counts_large_ingest_work_units_with_real_redis(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
):
    """Large declared ingest sizes consume more active admission units."""
    redis_client, task_type, _ = isolated_task_redis
    raw_redis = redis_client.client()
    ingest_type = f"ingest-{task_type}"
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_USER_MAX", 4)

    large_ingest_task = {
        "task_type": ingest_type,
        "operation_id": "op-large-ingest",
        "user_id": "user-large-ingest",
        "ws_id": "ws-large-ingest",
        "req_id": f"req-large-ingest-{task_type}",
        "params": {
            "file_total": 2,
            "file_size": 300 * 1024 * 1024,
            "payload": {"plugin_params": {"store_file": True}},
        },
    }
    small_same_user_task = {
        **large_ingest_task,
        "operation_id": "op-large-ingest-second",
        "req_id": f"req-large-ingest-second-{task_type}",
        "params": {"file_total": 1, "file_size": 1024},
    }

    try:
        await redis_client.task_enqueue(large_ingest_task)
        with pytest.raises(TaskQueueFullError) as exc_info:
            await redis_client.task_enqueue(small_same_user_task)
        assert exc_info.value.scope == "user"
        assert exc_info.value.queue_depth == 4
        assert exc_info.value.queue_limit == 4
        assert exc_info.value.work_units == 1

        active_users = await raw_redis.hgetall(GulpRedis.TASK_ACTIVE_USER_HASH)
        decoded = {
            key.decode() if isinstance(key, bytes) else key: value.decode()
            if isinstance(value, bytes)
            else value
            for key, value in active_users.items()
        }

        print("large ingest active users:", decoded)
        assert decoded["user-large-ingest"] == "4"
    finally:
        await raw_redis.delete(f"{GulpRedis.STREAM_TASK_PREFIX}:{ingest_type}")


@pytest.mark.integration
async def test_task_admission_charges_broad_rebase_more_than_narrow_with_real_redis(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
):
    """Unfiltered rebase jobs consume more capacity than selective rebase jobs."""
    redis_client, task_type, _ = isolated_task_redis
    raw_redis = redis_client.client()
    rebase_type = f"rebase-{task_type}"
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_OPERATION_MAX", 4)

    narrow_rebase_task = {
        "task_type": rebase_type,
        "operation_id": "op-rebase-calibration",
        "user_id": "user-rebase-calibration",
        "ws_id": "ws-rebase-calibration",
        "req_id": f"req-narrow-rebase-{task_type}",
        "params": {
            "fields": ["event.code"],
            "flt": {"source_id": ["source-1"]},
        },
    }
    broad_rebase_task = {
        **narrow_rebase_task,
        "req_id": f"req-broad-rebase-{task_type}",
        "params": {
            "fields": ["event.code"],
            "flt": {},
        },
    }

    try:
        await redis_client.task_enqueue(narrow_rebase_task)
        with pytest.raises(TaskQueueFullError) as exc_info:
            await redis_client.task_enqueue(broad_rebase_task)
        assert exc_info.value.scope == "operation"
        assert exc_info.value.queue_depth == 2
        assert exc_info.value.queue_limit == 4
        assert exc_info.value.work_units == 5

        active_operations = await raw_redis.hgetall(GulpRedis.TASK_ACTIVE_OPERATION_HASH)
        decoded = {
            key.decode() if isinstance(key, bytes) else key: value.decode()
            if isinstance(value, bytes)
            else value
            for key, value in active_operations.items()
        }

        print("rebase calibrated active operations:", decoded)
        assert decoded["op-rebase-calibration"] == "2"
    finally:
        await raw_redis.delete(f"{GulpRedis.STREAM_TASK_PREFIX}:{rebase_type}")


@pytest.mark.integration
async def test_task_admission_fairness_limits_one_operation_not_another_with_real_redis(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
):
    """Per-operation active limits reject one saturated operation only."""
    redis_client, task_type, _ = isolated_task_redis
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_OPERATION_MAX", 1)

    operation_one_task = {
        "task_type": task_type,
        "operation_id": "op-one",
        "user_id": "user-one",
        "ws_id": "ws-operation-one",
        "req_id": f"req-operation-one-{task_type}",
        "params": {"query": "one"},
    }
    operation_one_second_task = {
        **operation_one_task,
        "user_id": "user-two",
        "ws_id": "ws-operation-one-second",
        "req_id": f"req-operation-one-second-{task_type}",
        "params": {"query": "one-second"},
    }
    operation_two_task = {
        **operation_one_task,
        "operation_id": "op-two",
        "user_id": "user-three",
        "ws_id": "ws-operation-two",
        "req_id": f"req-operation-two-{task_type}",
        "params": {"query": "two"},
    }

    await redis_client.task_enqueue(operation_one_task)
    with pytest.raises(TaskQueueFullError) as exc_info:
        await redis_client.task_enqueue(operation_one_second_task)
    assert exc_info.value.scope == "operation"

    await redis_client.task_enqueue(operation_two_task)
    raw_redis = redis_client.client()
    active_operations = await raw_redis.hgetall(GulpRedis.TASK_ACTIVE_OPERATION_HASH)
    decoded = {
        key.decode() if isinstance(key, bytes) else key: value.decode()
        if isinstance(value, bytes)
        else value
        for key, value in active_operations.items()
    }

    print("task admission active operations:", decoded)
    assert decoded["op-one"] == "1"
    assert decoded["op-two"] == "1"
