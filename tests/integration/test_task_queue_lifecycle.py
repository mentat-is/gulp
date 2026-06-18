"""Integration coverage for Redis-backed task queue transport semantics."""

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
    consumer_group = f"gulp:test:stream:group:tasks:{suffix}"
    stream_key = f"{stream_prefix}:{task_type}"

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
    monkeypatch.setattr(GulpRedis, "STREAM_CONSUMER_GROUP", consumer_group)
    monkeypatch.setattr(GulpRedis, "STREAM_TASK_MAXLEN", 10)
    redis_client._instance_roles = []

    try:
        yield redis_client, task_type, stream_key
    finally:
        keys_to_delete = [task_types_key]
        async for key in raw_redis.scan_iter(match=f"{stream_prefix}:*"):
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

    await redis_client.check_ip_rate_limit(
        "login", "192.0.2.10", limit=2, window_sec=30
    )
    await redis_client.check_ip_rate_limit(
        "login", "192.0.2.10", limit=2, window_sec=30
    )

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

    await redis_client.check_ip_rate_limit(
        "login", "198.51.100.20", limit=2, window_sec=30
    )

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
async def test_task_queue_full_wait_hint_is_fixed(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
):
    """Queue-full wait hints should not require Redis-side drain tracking."""
    redis_client, task_type, _ = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
    monkeypatch.setattr(GulpRedis, "STREAM_TASK_MAXLEN", 2)

    for idx in range(2):
        await redis_client.task_enqueue(
            {
                "task_type": task_type,
                "operation_id": "op-adaptive-wait",
                "user_id": "admin",
                "ws_id": "ws-adaptive-wait",
                "req_id": f"req-adaptive-wait-{idx}-{task_type}",
                "params": {},
            }
        )

    with pytest.raises(TaskQueueFullError) as exc_info:
        await redis_client.task_enqueue(
            {
                "task_type": task_type,
                "operation_id": "op-adaptive-wait",
                "user_id": "admin",
                "ws_id": "ws-adaptive-wait",
                "req_id": f"req-adaptive-wait-full-{task_type}",
                "params": {},
            }
        )

    print(
        "task queue adaptive wait hint:",
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
async def test_task_queue_failure_marks_failed_with_real_redis(
    isolated_task_redis: tuple[GulpRedis, str, str],
):
    """A failed valid task is marked failed and removed from the stream."""
    redis_client, task_type, _ = isolated_task_redis
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
    assert (
        await redis_client.task_mark_failed(
            first_batch[0],
            "integration failure",
        )
        == "failed"
    )

    print(
        "task queue failed task:",
        {
            "stream_key": stream_key,
            "stream_len": await raw_redis.xlen(stream_key),
        },
    )
    assert await raw_redis.xlen(stream_key) == 0


@pytest.mark.integration
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
async def test_enrich_mutation_worker_failure_marks_failed(
    isolated_task_redis: tuple[GulpRedis, str, str],
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    params: dict,
    expected_worker: str,
):
    """Redis dispatch marks failed bulk enrich mutation workers terminal."""
    from gulp.api.server import enrich as enrich_mod
    from gulp.api.server_api import GulpServer

    redis_client, _, _ = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{TASK_TYPE_ENRICH}"
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

    async def _mark_failed_task_stats_failed(_task: dict, _reason: str) -> None:
        return None

    monkeypatch.setattr(server, "spawn_worker_task", _spawn_worker_task)
    monkeypatch.setattr(
        server,
        "_mark_failed_task_stats_failed",
        _mark_failed_task_stats_failed,
    )

    try:
        await redis_client.task_enqueue(task)
        first_batch = await redis_client.task_dequeue_batch(1)
        assert len(first_batch) == 1
        await server._dispatch_claimed_tasks(redis_client, first_batch, source="queued")

        failure_snapshot = await redis_client.task_metrics_snapshot()
        print(
            "enrich worker failure state:",
            {
                "action": action,
                "worker_calls": worker_calls,
                "failure_snapshot": failure_snapshot,
                "stream_len": await raw_redis.xlen(stream_key),
            },
        )
        assert len(worker_calls) == 1
        assert failure_snapshot["task_types"][TASK_TYPE_ENRICH]["queued"] == 0

        print(
            "enrich worker failure state:",
            {
                "action": action,
                "worker_calls": worker_calls,
                "stream_len": await raw_redis.xlen(stream_key),
            },
        )
        assert len(worker_calls) == 1
        assert await raw_redis.xlen(stream_key) == 0
    finally:
        await raw_redis.delete(stream_key)
        GulpServer._instance = None


@pytest.mark.integration
async def test_task_cancel_purges_queued_task(
    isolated_task_redis: tuple[GulpRedis, str, str],
):
    """Queued request cancellation deletes matching stream entries."""
    redis_client, task_type, _ = isolated_task_redis
    raw_redis = redis_client.client()
    stream_key = f"{GulpRedis.STREAM_TASK_PREFIX}:{task_type}"
    operation_id = "op-cancel"
    req_id = f"req-cancel-{task_type}"
    task = {
        "task_type": task_type,
        "operation_id": operation_id,
        "user_id": "user-cancel",
        "ws_id": "ws-integration-task-cancel",
        "req_id": req_id,
        "params": {"query": "*"},
    }

    await redis_client.task_enqueue(task)

    removed = await redis_client.task_purge_by_filter(
        operation_id=operation_id,
        req_id=req_id,
    )

    print(
        "task cancel purge state:",
        {
            "removed": removed,
            "stream_len": await raw_redis.xlen(stream_key),
        },
    )
    assert removed == 1
    assert await raw_redis.xlen(stream_key) == 0
