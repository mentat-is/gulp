import asyncio
import fnmatch
from contextlib import suppress

import pytest
from unittest.mock import AsyncMock, MagicMock

import orjson

from gulp.api import redis_api as redis_api_module
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.redis_api import GulpRedis, IpRateLimitError, TaskQueueFullError
from gulp.api.server.db import run_rebase_task
from gulp.api.server_api import GulpServer


class _FakeTaskPipeline:
    def __init__(self, client):
        self._client = client

    def xack(self, stream_name: str, group_name: str, msg_id: str):
        self._client.xack_calls.append((stream_name, group_name, msg_id))
        return self

    def xdel(self, stream_name: str, msg_id: str):
        self._client.xdel_calls.append((stream_name, msg_id))
        return self

    async def execute(self):
        self._client.pipeline_exec_count += 1
        return []


class _FakeTaskRedisClient:
    def __init__(self):
        self.known_task_types: set[str] = set()
        self.stream_depths: dict[str, int] = {}
        self.stream_first_ids: dict[str, str] = {}
        self.read_entries: list[tuple[str, list[tuple[str, dict[bytes, bytes]]]]] = []
        self.xadd_calls: list[tuple[str, dict[str, bytes]]] = []
        self.zadd_calls: list[tuple[str, dict[bytes, int]]] = []
        self.zrem_calls: list[tuple[str, bytes]] = []
        self.zset_entries: dict[bytes, int] = {}
        self.sadd_calls: list[tuple[str, str]] = []
        self.xlen_calls: list[str] = []
        self.pending_counts: dict[str, int] = {}
        self.pending_min_ids: dict[str, str] = {}
        self.xgroup_create_calls: list[tuple[tuple, dict]] = []
        self.xreadgroup_calls: list[tuple[tuple, dict]] = []
        self.xclaim_calls: list[tuple[tuple, dict]] = []
        self.xautoclaim_calls: list[tuple[tuple, dict]] = []
        self.autoclaim_entries: dict[
            str, list[tuple[str, dict[bytes, bytes]]]
        ] = {}
        self.hset_calls: list[tuple[str, dict]] = []
        self.expire_calls: list[tuple[str, int]] = []
        self.pexpire_calls: list[tuple[str, int]] = []
        self.delete_calls: list[str] = []
        self.hashes: dict[str, dict[str, str]] = {}
        self.strings: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.pipeline_calls: list[bool] = []
        self.xack_calls: list[tuple[str, str, str]] = []
        self.xdel_calls: list[tuple[str, str]] = []
        self.pipeline_exec_count = 0

    async def smembers(self, key: str):
        return {member.encode() for member in self.known_task_types}

    async def xlen(self, key: str):
        self.xlen_calls.append(key)
        return self.stream_depths.get(key, 0)

    async def xpending(self, stream_name: str, group_name: str):
        return {
            "pending": self.pending_counts.get(stream_name, 0),
            "min": self.pending_min_ids.get(stream_name),
        }

    async def xrange(
        self,
        stream_name: str,
        min: str = "-",
        max: str = "+",
        count: int | None = None,
    ):
        stream_id = self.stream_first_ids.get(stream_name)
        if stream_id is None:
            return []
        return [(stream_id, {b"data": b"{}"})]

    async def xadd(self, stream_name: str, fields: dict[str, bytes]):
        self.xadd_calls.append((stream_name, fields))
        return "1-0"

    async def zadd(self, key: str, values: dict[bytes, int]):
        self.zadd_calls.append((key, values))
        self.zset_entries.update(values)
        return len(values)

    async def zrangebyscore(
        self,
        key: str,
        minimum: int,
        maximum: int,
        start: int = 0,
        num: int | None = None,
    ):
        matches = [
            member
            for member, score in self.zset_entries.items()
            if minimum <= score <= maximum
        ]
        if num is None:
            return matches[start:]
        return matches[start : start + num]

    async def zrem(self, key: str, member: bytes):
        self.zrem_calls.append((key, member))
        if member not in self.zset_entries:
            return 0
        del self.zset_entries[member]
        return 1

    async def zcount(self, key: str, minimum: int, maximum: int):
        return sum(
            1 for score in self.zset_entries.values() if minimum <= score <= maximum
        )

    async def zremrangebyscore(self, key: str, minimum: int, maximum: int):
        removed = [
            member
            for member, score in self.zset_entries.items()
            if minimum <= score <= maximum
        ]
        for member in removed:
            del self.zset_entries[member]
        return len(removed)

    async def zcard(self, key: str):
        return len(self.zset_entries)

    async def sadd(self, key: str, value: str):
        self.sadd_calls.append((key, value))
        self.known_task_types.add(value)
        return 1

    async def xgroup_create(self, *args, **kwargs):
        self.xgroup_create_calls.append((args, kwargs))
        return True

    async def xreadgroup(self, *args, **kwargs):
        self.xreadgroup_calls.append((args, kwargs))
        return self.read_entries

    async def xclaim(self, *args, **kwargs):
        self.xclaim_calls.append((args, kwargs))
        return [(kwargs["message_ids"][0], {})]

    async def xautoclaim(self, *args, **kwargs):
        self.xautoclaim_calls.append((args, kwargs))
        stream_name = args[0]
        return "0-0", self.autoclaim_entries.get(stream_name, []), []

    async def hgetall(self, key: str):
        return self.hashes.get(key, {})

    async def scan_iter(self, match: str):
        for key in list(self.hashes):
            if fnmatch.fnmatch(key, match):
                yield key

    async def hget(self, key: str, field: str):
        return self.hashes.get(key, {}).get(field)

    async def hset(self, key: str, mapping: dict):
        self.hset_calls.append((key, mapping))
        current = self.hashes.setdefault(key, {})
        current.update({str(k): str(v) for k, v in mapping.items()})
        return len(mapping)

    async def hincrby(self, key: str, field: str, amount: int):
        current = int(self.hashes.setdefault(key, {}).get(field, 0))
        current += amount
        self.hashes[key][field] = str(current)
        return current

    async def hdel(self, key: str, *fields: str):
        current = self.hashes.setdefault(key, {})
        removed = 0
        for field in fields:
            if field in current:
                del current[field]
                removed += 1
        return removed

    async def hvals(self, key: str):
        return list(self.hashes.get(key, {}).values())

    async def expire(self, key: str, seconds: int):
        self.expire_calls.append((key, seconds))
        self.ttls[key] = seconds
        return True

    async def incr(self, key: str):
        current = int(self.strings.get(key, "0")) + 1
        self.strings[key] = str(current)
        return current

    async def ttl(self, key: str):
        return self.ttls.get(key, -1 if key in self.strings else -2)

    async def set(self, key: str, value: str, nx: bool = False, px: int | None = None):
        if nx and key in self.strings:
            return False
        self.strings[key] = value
        return True

    async def get(self, key: str):
        return self.strings.get(key)

    async def exists(self, key: str):
        return 1 if key in self.strings else 0

    async def pexpire(self, key: str, milliseconds: int):
        self.pexpire_calls.append((key, milliseconds))
        return key in self.strings

    async def delete(self, *keys: str):
        for key in keys:
            self.delete_calls.append(key)
            self.hashes.pop(key, None)
            self.strings.pop(key, None)
        return len(keys)

    def pipeline(self, transaction: bool = False):
        self.pipeline_calls.append(transaction)
        return _FakeTaskPipeline(self)


class _FakeCollabSession:
    async def __aenter__(self):
        return "fake-session"

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePrometheusConfig:
    def prometheus_enabled(self) -> bool:
        return True


class _FakeLabeledCounter:
    def __init__(self):
        self.calls: list[tuple[dict[str, str], int]] = []

    def labels(self, **labels):
        parent = self

        class _Child:
            def inc(self, amount: int = 1):
                parent.calls.append((labels, amount))

        return _Child()


@pytest.fixture(autouse=True)
def _reset_redis_singleton():
    GulpRedis._instance = None
    yield
    GulpRedis._instance = None


@pytest.fixture
def task_transition_counter(monkeypatch):
    counter = _FakeLabeledCounter()
    monkeypatch.setattr(
        redis_api_module.GulpMetrics,
        "redis_task_transition_total",
        counter,
    )
    monkeypatch.setattr(
        redis_api_module.GulpConfig,
        "get_instance",
        lambda: _FakePrometheusConfig(),
    )
    return counter


@pytest.fixture
def api_rejection_counter(monkeypatch):
    counter = _FakeLabeledCounter()
    monkeypatch.setattr(
        "gulp.api.prometheus_api.GulpMetrics.api_request_rejected_total",
        counter,
    )
    monkeypatch.setattr(
        "gulp.api.prometheus_api.GulpConfig.get_instance",
        lambda: _FakePrometheusConfig(),
    )
    return counter


@pytest.mark.unit
@pytest.mark.asyncio
async def test_task_enqueue_rejects_when_queue_depth_hits_limit(monkeypatch):
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    fake_redis.stream_depths["gulp:stream:tasks:query"] = 3
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"

    monkeypatch.setattr(GulpRedis, "STREAM_TASK_MAXLEN", 3)

    task = {
        "task_type": "query",
        "operation_id": "op-1",
        "user_id": "user-1",
        "ws_id": "ws-1",
        "req_id": "req-1",
        "params": {"queries": [{"query": "one"}, {"query": "two"}]},
    }

    with pytest.raises(TaskQueueFullError) as exc_info:
        await redis_client.task_enqueue(task)

    assert exc_info.value.task_type == "query"
    assert exc_info.value.queue_depth == 3
    assert exc_info.value.queue_limit == 3
    assert exc_info.value.work_units == 1
    assert fake_redis.xlen_calls == ["gulp:stream:tasks:query"]
    assert fake_redis.xadd_calls == []
    assert fake_redis.sadd_calls == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_task_enqueue_queue_full_uses_fixed_wait_hint(monkeypatch):
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    fake_redis.stream_depths["gulp:stream:tasks:query"] = 12
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"

    monkeypatch.setattr(GulpRedis, "STREAM_TASK_MAXLEN", 10)

    task = {
        "task_type": "query",
        "operation_id": "op-1",
        "user_id": "user-1",
        "ws_id": "ws-1",
        "req_id": "req-adaptive-wait",
        "params": {},
    }

    with pytest.raises(TaskQueueFullError) as exc_info:
        await redis_client.task_enqueue(task)

    assert exc_info.value.queue_depth == 12
    assert exc_info.value.queue_limit == 10
    assert exc_info.value.work_units == 1
    assert exc_info.value.retry_after_msec == 1000
    assert fake_redis.xadd_calls == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_task_metrics_snapshot_reports_queue_operator_state():
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"
    fake_redis.known_task_types.add("query")
    fake_redis.stream_depths["gulp:stream:tasks:query"] = 7
    fake_redis.stream_first_ids["gulp:stream:tasks:query"] = "1000000-0"
    fake_redis.pending_counts["gulp:stream:tasks:query"] = 3
    fake_redis.pending_min_ids["gulp:stream:tasks:query"] = "1000500-0"

    original_time = redis_api_module.time.time
    redis_api_module.time.time = lambda: 1002.0
    try:
        snapshot = await redis_client.task_metrics_snapshot()
    finally:
        redis_api_module.time.time = original_time

    assert snapshot == {
        "task_types": {
            "query": {
                "queued": 7,
                "pending": 3,
                "oldest_queued_age_msec": 2000,
                "oldest_pending_age_msec": 1500,
            }
        },
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_task_drop_stale_pending_deletes_pending_messages(task_transition_counter):
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-b"
    fake_redis.known_task_types.add("query")
    fake_redis.autoclaim_entries["gulp:stream:tasks:query"] = [
        (
            "10-0",
            {
                b"data": orjson.dumps(
                    {
                        "task_type": "query",
                        "operation_id": "op-stale",
                        "req_id": "req-stale",
                    }
                )
            },
        )
    ]

    dropped = await redis_client.task_drop_stale_pending()

    assert dropped == [
        {
            "task_type": "query",
            "operation_id": "op-stale",
            "req_id": "req-stale",
            "__redis_stream__": "gulp:stream:tasks:query",
            "__redis_message_id__": "10-0",
            "__redis_consumer_group__": GulpRedis.STREAM_CONSUMER_GROUP,
            "__redis_consumer_name__": "server-b",
        }
    ]
    assert fake_redis.xack_calls == [
        ("gulp:stream:tasks:query", GulpRedis.STREAM_CONSUMER_GROUP, "10-0")
    ]
    assert fake_redis.xdel_calls == [("gulp:stream:tasks:query", "10-0")]
    args, kwargs = fake_redis.xautoclaim_calls[0]
    assert args[:3] == (
        "gulp:stream:tasks:query",
        GulpRedis.STREAM_CONSUMER_GROUP,
        "server-b",
    )
    assert kwargs["min_idle_time"] == GulpRedis.TASK_AUTOCLAIM_IDLE_MS
    counter = task_transition_counter
    assert (
        {
            "action": "stale_pending",
            "task_type": "query",
            "outcome": "dropped",
        },
        1,
    ) in counter.calls


@pytest.mark.unit
@pytest.mark.asyncio
async def test_task_transition_metrics_cover_failed_task(
    task_transition_counter,
):
    transition_counter = task_transition_counter
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"

    task = {
        "task_type": "query",
        "operation_id": "op-dead-metrics",
        "req_id": "req-dead-metrics",
        "__redis_stream__": "gulp:stream:tasks:query",
        "__redis_message_id__": "1-0",
        "__redis_consumer_name__": "server-a",
    }

    assert await redis_client.task_mark_failed(task, "terminal failure") == "failed"

    assert (
        {"action": "failure", "task_type": "query", "outcome": "failed"},
        1,
    ) in transition_counter.calls


@pytest.mark.unit
def test_task_queue_full_response_includes_wait_guidance(api_rejection_counter):
    from gulp.api.server.server_utils import ServerUtils

    response = ServerUtils.task_queue_full_response(
        "query",
        "req-queue-full",
        TaskQueueFullError(
            "query",
            3,
            2,
            scope="user",
            retry_after_msec=1500,
            work_units=4,
        ),
    )

    payload = orjson.loads(response.body)
    error = payload["data"]["__error"]
    assert response.status_code == 503
    assert error == {
        "error": "task_queue_full",
        "task_type": "query",
        "scope": "user",
        "queue_depth": 3,
        "queue_limit": 2,
        "work_units": 4,
        "retry_after_msec": 1500,
    }
    assert (
        {
            "endpoint": "query",
            "reason": "task_queue_full",
            "task_type": "query",
            "scope": "user",
        },
        1,
    ) in api_rejection_counter.calls

    ingest_full = ServerUtils.task_queue_full_response(
        "ingest",
        "req-ingest-full",
        TaskQueueFullError("ingest", 5, 4, scope="operation", work_units=2),
    )
    db_full = ServerUtils.task_queue_full_response(
        "db",
        "req-db-full",
        TaskQueueFullError("rebase", 9, 8, scope="task_type", work_units=3),
    )
    enrich_full = ServerUtils.task_queue_full_response(
        "enrich",
        "req-enrich-full",
        TaskQueueFullError("enrich", 7, 6, scope="task_type", work_units=5),
    )
    ingest_error = orjson.loads(ingest_full.body)["data"]["__error"]
    db_error = orjson.loads(db_full.body)["data"]["__error"]
    enrich_error = orjson.loads(enrich_full.body)["data"]["__error"]
    assert ingest_error["work_units"] == 2
    assert db_error["work_units"] == 3
    assert enrich_error["work_units"] == 5

    assert (
        {
            "endpoint": "ingest",
            "reason": "task_queue_full",
            "task_type": "ingest",
            "scope": "operation",
        },
        1,
    ) in api_rejection_counter.calls
    assert (
        {
            "endpoint": "db",
            "reason": "task_queue_full",
            "task_type": "rebase",
            "scope": "task_type",
        },
        1,
    ) in api_rejection_counter.calls
    assert (
        {
            "endpoint": "enrich",
            "reason": "task_queue_full",
            "task_type": "enrich",
            "scope": "task_type",
        },
        1,
    ) in api_rejection_counter.calls


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ip_rate_limit_rejects_same_ip_without_affecting_other_ips():
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    redis_client._redis = fake_redis

    await redis_client.check_ip_rate_limit(
        scope="login",
        ip="192.0.2.10",
        limit=2,
        window_sec=60,
    )
    await redis_client.check_ip_rate_limit(
        scope="login",
        ip="192.0.2.10",
        limit=2,
        window_sec=60,
    )

    with pytest.raises(IpRateLimitError) as exc_info:
        await redis_client.check_ip_rate_limit(
            scope="login",
            ip="192.0.2.10",
            limit=2,
            window_sec=60,
        )

    assert exc_info.value.scope == "login"
    assert exc_info.value.count == 3
    assert exc_info.value.limit == 2
    assert exc_info.value.window_sec == 60
    assert exc_info.value.retry_after_msec == 60000

    await redis_client.check_ip_rate_limit(
        scope="login",
        ip="198.51.100.20",
        limit=2,
        window_sec=60,
    )

    assert len(fake_redis.strings) == 2
    assert all(key.startswith("gulp:rate:ip:login:") for key in fake_redis.strings)
    assert "192.0.2.10" not in next(iter(fake_redis.strings))


@pytest.mark.unit
def test_login_ip_throttle_response_includes_wait_guidance(api_rejection_counter):
    from gulp.api.server.user import _login_ip_throttle_response

    response = _login_ip_throttle_response(
        "req-login-throttle",
        IpRateLimitError(
            scope="login",
            ip="192.0.2.10",
            count=6,
            limit=5,
            window_sec=60,
            retry_after_msec=30000,
        ),
    )

    payload = orjson.loads(response.body)
    error = payload["data"]["__error"]
    assert response.status_code == 429
    assert error == {
        "error": "ip_throttle",
        "scope": "login",
        "limit": 5,
        "window_sec": 60,
        "retry_after_msec": 30000,
    }
    assert (
        {
            "endpoint": "login",
            "reason": "ip_throttle",
            "task_type": "none",
            "scope": "ip",
        },
        1,
    ) in api_rejection_counter.calls


@pytest.mark.unit
@pytest.mark.asyncio
async def test_task_mark_failed_ack_deletes_message():
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"
    task = {
        "task_type": "query",
        "operation_id": "op-canceled-failure",
        "req_id": "req-canceled-failure",
        "__redis_stream__": "gulp:stream:tasks:query",
        "__redis_message_id__": "5-0",
    }

    assert (
        await redis_client.task_mark_failed(task, "worker returned false")
        == "failed"
    )
    assert fake_redis.xack_calls == [
        ("gulp:stream:tasks:query", GulpRedis.STREAM_CONSUMER_GROUP, "5-0")
    ]
    assert fake_redis.xdel_calls == [("gulp:stream:tasks:query", "5-0")]
    assert fake_redis.pipeline_exec_count == 1


async def _return_value(value):
    return value


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dispatch_claimed_queued_task_runs_handler_then_ack_deletes():
    GulpServer._instance = None
    server = GulpServer.get_instance()
    task = {
        "task_type": "query",
        "operation_id": "op-queued-success",
        "req_id": "req-queued-success",
        "__redis_stream__": "gulp:stream:tasks:query",
        "__redis_message_id__": "6-0",
    }
    fake_redis = MagicMock()
    fake_redis.task_ack_delete = AsyncMock()
    fake_redis.task_mark_failed = AsyncMock()
    server._run_claimed_task = MagicMock(return_value=_return_value(True))

    await server._dispatch_claimed_tasks(fake_redis, [task], source="queued")

    server._run_claimed_task.assert_called_once_with(task)
    fake_redis.task_ack_delete.assert_awaited_once_with(task)
    fake_redis.task_mark_failed.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dispatch_claimed_queued_failure_marks_failed():
    GulpServer._instance = None
    server = GulpServer.get_instance()
    task = {
        "task_type": "query",
        "operation_id": "op-queued-failure",
        "req_id": "req-queued-failure",
        "__redis_stream__": "gulp:stream:tasks:query",
        "__redis_message_id__": "8-0",
    }
    fake_redis = MagicMock()
    fake_redis.task_ack_delete = AsyncMock()
    fake_redis.task_mark_failed = AsyncMock(return_value="failed")
    server._mark_failed_task_stats_failed = AsyncMock()
    server._run_claimed_task = MagicMock(return_value=_return_value(False))

    await server._dispatch_claimed_tasks(fake_redis, [task], source="queued")

    server._run_claimed_task.assert_called_once_with(task)
    fake_redis.task_ack_delete.assert_not_awaited()
    fake_redis.task_mark_failed.assert_awaited_once_with(task, "False")
    server._mark_failed_task_stats_failed.assert_awaited_once_with(
        task,
        "False",
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dispatch_claimed_task_timeout_marks_failed():
    GulpServer._instance = None
    server = GulpServer.get_instance()
    task = {
        "task_type": "query",
        "operation_id": "op-timeout",
        "req_id": "req-timeout",
        "__task_timeout_sec__": 1,
        "__redis_stream__": "gulp:stream:tasks:query",
        "__redis_message_id__": "9-0",
    }
    fake_redis = MagicMock()
    fake_redis.task_ack_delete = AsyncMock()
    fake_redis.task_mark_failed = AsyncMock(return_value="failed")
    fake_redis.task_refresh_lease = AsyncMock()

    async def _never_finishes():
        await asyncio.sleep(60)

    server._run_claimed_task = MagicMock(return_value=_never_finishes())

    await server._dispatch_claimed_tasks(fake_redis, [task], source="batch")

    fake_redis.task_ack_delete.assert_not_awaited()
    fake_redis.task_mark_failed.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_claimed_task_routes_to_task_type_handlers(monkeypatch):
    from gulp.api.server import db as db_mod
    from gulp.api.server import enrich as enrich_mod
    from gulp.api.server import ingest as ingest_mod
    from gulp.api.server import query as query_mod

    GulpServer._instance = None
    server = GulpServer.get_instance()
    query = AsyncMock(return_value=True)
    rebase = AsyncMock(return_value=True)
    enrich = AsyncMock(return_value=True)
    monkeypatch.setattr(query_mod, "run_query_task", query)
    monkeypatch.setattr(db_mod, "run_rebase_task", rebase)
    monkeypatch.setattr(enrich_mod, "run_enrich_task", enrich)
    server.spawn_worker_task = AsyncMock(return_value=True)

    query_task = {"task_type": "query", "req_id": "req-query"}
    external_query_task = {"task_type": "external_query", "req_id": "req-external"}
    rebase_task = {"task_type": "rebase", "req_id": "req-rebase"}
    ingest_task = {"task_type": "ingest", "req_id": "req-ingest"}
    enrich_task = {"task_type": "enrich", "req_id": "req-enrich"}

    assert await server._run_claimed_task(query_task) is True
    assert await server._run_claimed_task(external_query_task) is True
    assert await server._run_claimed_task(rebase_task) is True
    assert await server._run_claimed_task(ingest_task) is True
    assert await server._run_claimed_task(enrich_task) is True

    query.assert_any_await(query_task)
    query.assert_any_await(external_query_task)
    rebase.assert_awaited_once_with(rebase_task)
    enrich.assert_awaited_once_with(enrich_task)
    server.spawn_worker_task.assert_awaited_once_with(
        ingest_mod.run_ingest_file_task,
        ingest_task,
        wait=True,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_task_dequeue_batch_preserves_envelope_without_ack_or_delete():
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"
    fake_redis.known_task_types.add("query")

    task = {
        "task_type": "query",
        "operation_id": "op-2",
        "user_id": "user-2",
        "ws_id": "ws-2",
        "req_id": "req-2",
        "params": {"foo": "bar"},
    }
    fake_redis.read_entries = [
        (
            "gulp:stream:tasks:query",
            [("1-0", {b"data": orjson.dumps(task)})],
        )
    ]

    batch = await redis_client.task_dequeue_batch(10)

    assert len(batch) == 1
    envelope = batch[0]
    assert envelope["task_type"] == "query"
    assert envelope["req_id"] == "req-2"
    assert envelope["__redis_stream__"] == "gulp:stream:tasks:query"
    assert envelope["__redis_message_id__"] == "1-0"
    assert envelope["__redis_consumer_group__"] == GulpRedis.STREAM_CONSUMER_GROUP
    assert envelope["__redis_consumer_name__"] == "server-a"
    assert fake_redis.xack_calls == []
    assert fake_redis.xdel_calls == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_task_dequeue_batch_drops_invalid_payload():
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"
    fake_redis.known_task_types.add("query")

    fake_redis.read_entries = [
        (
            "gulp:stream:tasks:query",
            [("1-0", {b"data": b"{not-json"})],
        )
    ]

    batch = await redis_client.task_dequeue_batch(10)

    assert batch == []
    assert fake_redis.xadd_calls == []
    assert fake_redis.pipeline_calls == [False]
    assert fake_redis.xack_calls == [
        ("gulp:stream:tasks:query", GulpRedis.STREAM_CONSUMER_GROUP, "1-0")
    ]
    assert fake_redis.xdel_calls == [("gulp:stream:tasks:query", "1-0")]
    assert fake_redis.pipeline_exec_count == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_task_refresh_lease_resets_pending_idle_timer():
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"

    task = {
        "task_type": "query",
        "req_id": "req-lease",
        "__redis_stream__": "gulp:stream:tasks:query",
        "__redis_message_id__": "1-0",
        "__redis_consumer_name__": "server-a",
    }

    assert await redis_client.task_refresh_lease(task) is True

    assert fake_redis.xclaim_calls == [
        (
            (
                "gulp:stream:tasks:query",
                GulpRedis.STREAM_CONSUMER_GROUP,
                "server-a",
            ),
            {
                "min_idle_time": 0,
                "message_ids": ["1-0"],
            },
        )
    ]
    assert fake_redis.pexpire_calls == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_task_metrics_snapshot_treats_missing_consumer_group_as_zero_pending():
    from redis.exceptions import ResponseError

    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    redis_client._redis = fake_redis
    fake_redis.known_task_types.add("query")
    fake_redis.stream_depths["gulp:stream:tasks:query"] = 1
    fake_redis.stream_first_ids["gulp:stream:tasks:query"] = "1000-0"
    fake_redis.xpending = AsyncMock(
        side_effect=ResponseError(
            "NOGROUP No such key 'gulp:stream:tasks:query' or consumer group"
        )
    )

    snapshot = await redis_client.task_metrics_snapshot()

    assert snapshot["task_types"]["query"]["queued"] == 1
    assert snapshot["task_types"]["query"]["pending"] == 0
    assert snapshot["task_types"]["query"]["oldest_pending_age_msec"] == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_task_mark_failed_acks_and_deletes_immediately():
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"

    task = {
        "task_type": "query",
        "operation_id": "op-dead",
        "req_id": "req-dead",
        "__redis_stream__": "gulp:stream:tasks:query",
        "__redis_message_id__": "1-0",
        "__redis_consumer_name__": "server-a",
    }

    result = await redis_client.task_mark_failed(
        task,
        "handler returned False",
    )

    assert result == "failed"
    assert fake_redis.xadd_calls == []
    assert fake_redis.hashes == {}
    assert fake_redis.xack_calls == [
        ("gulp:stream:tasks:query", GulpRedis.STREAM_CONSUMER_GROUP, "1-0")
    ]
    assert fake_redis.xdel_calls == [("gulp:stream:tasks:query", "1-0")]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_task_mark_failed_handles_unknown_task_type():
    redis_client = GulpRedis.get_instance()
    fake_redis = _FakeTaskRedisClient()
    redis_client._redis = fake_redis
    redis_client.server_id = "server-a"

    task = {
        "task_type": "unknown_task",
        "req_id": "req-unknown",
        "__redis_stream__": "gulp:stream:tasks:unknown_task",
        "__redis_message_id__": "1-0",
    }

    assert await redis_client.task_mark_failed(task, "unknown task_type") == "failed"

    assert fake_redis.xadd_calls == []
    assert fake_redis.hashes == {}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dispatcher_refreshes_task_lease_while_task_is_running(monkeypatch):
    GulpServer._instance = None
    server = GulpServer.get_instance()
    task = {
        "task_type": "query",
        "req_id": "req-running",
        "__redis_stream__": "gulp:stream:tasks:query",
        "__redis_message_id__": "1-0",
        "__redis_consumer_name__": "server-a",
    }
    fake_redis = MagicMock()
    fake_redis.task_refresh_lease = AsyncMock(return_value=True)
    monkeypatch.setattr(GulpRedis, "TASK_LEASE_REFRESH_INTERVAL_MS", 1)
    monkeypatch.setattr(
        "gulp.api.server_api.GulpRedis.get_instance",
        lambda: fake_redis,
    )

    async def _long_running_task():
        await asyncio.sleep(0.01)
        return True

    assert await server._await_task_with_lease(task, _long_running_task()) is True
    fake_redis.task_refresh_lease.assert_awaited()
    fake_redis.task_refresh_lease.assert_any_await(task)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dispatch_tasks_refills_free_slots_before_slow_task_finishes(monkeypatch):
    GulpServer._instance = None
    server = GulpServer.get_instance()
    tasks = [
        {"task_type": "ingest", "req_id": "slow"},
        {"task_type": "ingest", "req_id": "fast"},
        {"task_type": "ingest", "req_id": "third"},
    ]

    class _FakeDispatchRedis:
        async def task_pop_blocking(self, timeout: int):
            if tasks:
                return tasks.pop(0)
            await asyncio.sleep(0.01)
            return None

        async def task_dequeue_batch(self, max_items: int):
            batch = tasks[:max_items]
            del tasks[:max_items]
            return batch

    fake_redis = _FakeDispatchRedis()
    config = MagicMock()
    config.concurrency_num_tasks.return_value = 2
    config.parallel_processes_max.return_value = 1
    monkeypatch.setattr("gulp.api.server_api.GulpConfig.get_instance", lambda: config)
    monkeypatch.setattr(
        "gulp.api.server_api.GulpRedis.get_instance",
        lambda: fake_redis,
    )

    release_slow = asyncio.Event()
    third_started = asyncio.Event()
    started: list[str] = []

    async def _dispatch_one(_redis_inst, claimed_tasks, *, source: str):
        req_id = claimed_tasks[0]["req_id"]
        started.append(req_id)
        if req_id == "third":
            third_started.set()
        if req_id == "slow":
            await release_slow.wait()
        return None

    server._dispatch_claimed_tasks = AsyncMock(side_effect=_dispatch_one)

    dispatch_task = asyncio.create_task(server._dispatch_tasks())
    await asyncio.wait_for(third_started.wait(), timeout=1)

    assert started[:2] == ["slow", "fast"]
    assert "third" in started
    assert not release_slow.is_set()

    dispatch_task.cancel()
    with suppress(asyncio.CancelledError):
        await dispatch_task


@pytest.mark.unit
@pytest.mark.asyncio
async def test_failed_task_marks_existing_request_stats_failed(monkeypatch):
    from gulp.api import server_api as server_api_module

    GulpServer._instance = None
    server = GulpServer.get_instance()
    fake_stats = MagicMock()
    fake_stats.status = GulpRequestStatus.ONGOING.value
    fake_stats.set_finished = AsyncMock()
    fake_collab = MagicMock()
    fake_collab.session.return_value = _FakeCollabSession()
    monkeypatch.setattr(
        server_api_module.GulpCollab, "get_instance", lambda: fake_collab
    )
    get_by_id = AsyncMock(return_value=fake_stats)
    monkeypatch.setattr(server_api_module.GulpRequestStats, "get_by_id", get_by_id)

    task = {
        "task_type": "query",
        "req_id": "req-dead",
        "user_id": "user-dead",
        "ws_id": "ws-dead",
    }

    await server._mark_failed_task_stats_failed(task, "failure reason")

    get_by_id.assert_awaited_once_with(
        "fake-session",
        "req-dead",
        throw_if_not_found=False,
    )
    fake_stats.set_finished.assert_awaited_once_with(
        "fake-session",
        status=GulpRequestStatus.FAILED,
        user_id="user-dead",
        ws_id="ws-dead",
        errors=["failure reason"],
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_failed_task_does_not_rewrite_terminal_request_stats(monkeypatch):
    from gulp.api import server_api as server_api_module

    GulpServer._instance = None
    server = GulpServer.get_instance()
    fake_stats = MagicMock()
    fake_stats.status = GulpRequestStatus.DONE.value
    fake_stats.set_finished = AsyncMock()
    fake_collab = MagicMock()
    fake_collab.session.return_value = _FakeCollabSession()
    monkeypatch.setattr(
        server_api_module.GulpCollab, "get_instance", lambda: fake_collab
    )
    monkeypatch.setattr(
        server_api_module.GulpRequestStats,
        "get_by_id",
        AsyncMock(return_value=fake_stats),
    )

    await server._mark_failed_task_stats_failed(
        {"task_type": "query", "req_id": "req-done"},
        "failure reason",
    )

    fake_stats.set_finished.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_rebase_task_waits_for_worker_completion(monkeypatch):
    from gulp.api import server_api as server_api_module

    fake_server = MagicMock()
    fake_server.spawn_worker_task = AsyncMock(return_value=True)
    monkeypatch.setattr(
        server_api_module.GulpServer, "get_instance", lambda: fake_server
    )

    task = {
        "task_type": "rebase",
        "operation_id": "op-3",
        "user_id": "user-3",
        "ws_id": "ws-3",
        "req_id": "req-3",
        "params": {
            "index": "index-3",
            "offset_msec": 42,
            "flt": {"query": {"match_all": {}}},
            "fields": ["@timestamp"],
        },
    }

    assert await run_rebase_task(task) is True

    fake_server.spawn_worker_task.assert_awaited_once()
    _, kwargs = fake_server.spawn_worker_task.call_args
    assert kwargs["task_name"] == "rebase_req-3"
    assert kwargs["wait"] is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_enrich_task_waits_for_worker_completion(monkeypatch):
    from gulp.api.server import enrich as enrich_mod

    fake_server = MagicMock()
    fake_server.spawn_worker_task = AsyncMock(return_value=True)
    monkeypatch.setattr(enrich_mod.GulpServer, "get_instance", lambda: fake_server)

    base_task = {
        "task_type": "enrich",
        "operation_id": "op-enrich-task",
        "user_id": "user-enrich-task",
        "ws_id": "ws-enrich-task",
        "req_id": "req-enrich-task",
    }
    cases = [
        (
            "enrich_documents",
            enrich_mod._enrich_documents_internal,
            {
                "plugin": "enrich_update_marker",
                "fields": {"field": None},
                "plugin_params": {"custom_parameters": {"x": "y"}},
            },
        ),
        (
            "update_documents",
            enrich_mod._update_documents_internal,
            {"data": {"field": "value"}},
        ),
        (
            "tag_documents",
            enrich_mod._update_documents_internal,
            {"data": {"gulp.tags": ["tag-a"]}},
        ),
        (
            "untag_documents",
            enrich_mod._untag_documents_internal,
            {"tags": ["tag-a"]},
        ),
        (
            "enrich_remove",
            enrich_mod._enrich_remove_internal,
            {"remove_fields": ["gulp.enriched"]},
        ),
    ]

    for action, expected_fn, extra_params in cases:
        task = {
            **base_task,
            "params": {
                "action": action,
                "index": "idx-enrich-task",
                "flt": {"operation_ids": ["op-enrich-task"]},
                **extra_params,
            },
        }
        assert await enrich_mod.run_enrich_task(task) is True
        args, kwargs = fake_server.spawn_worker_task.await_args
        assert args[0] is expected_fn
        assert kwargs["task_name"] == "enrich_req-enrich-task"
        assert kwargs["wait"] is True
        fake_server.spawn_worker_task.reset_mock()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_enrich_task_propagates_worker_failure(monkeypatch):
    from gulp.api.server import enrich as enrich_mod

    fake_server = MagicMock()
    fake_server.spawn_worker_task = AsyncMock(
        side_effect=RuntimeError("worker exploded")
    )
    monkeypatch.setattr(enrich_mod.GulpServer, "get_instance", lambda: fake_server)

    task = {
        "task_type": "enrich",
        "operation_id": "op-enrich-failure",
        "user_id": "user-enrich-failure",
        "ws_id": "ws-enrich-failure",
        "req_id": "req-enrich-failure",
        "params": {
            "action": "update_documents",
            "index": "idx-enrich-failure",
            "flt": {"operation_ids": ["op-enrich-failure"]},
            "data": {"field": "value"},
        },
    }

    with pytest.raises(RuntimeError, match="worker exploded"):
        await enrich_mod.run_enrich_task(task)

    fake_server.spawn_worker_task.assert_awaited_once()
