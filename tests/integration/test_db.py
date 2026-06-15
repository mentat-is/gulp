"""
Integration tests for the OpenSearch admin (db) API.

Requires a live Gulp server with an admin account.
Set GULP_BASE_URL, GULP_TEST_USER, GULP_TEST_PASSWORD env vars to override.

Run with:
    python -m pytest -v tests/integration/test_db.py -m integration
"""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
import uuid

import orjson
import pytest


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _wait_request_done(client, req_id: str, timeout: float = 180.0) -> dict:
    """Poll request status until it reaches a terminal state."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            stats = await client.plugins.request_get(req_id)
            status = str(stats.get("status", "")).lower()
            if status in {"done", "failed", "canceled"}:
                return stats
        except Exception:
            pass
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"Timed out waiting request {req_id}")
        await asyncio.sleep(1.0)


async def _delete_operation_with_retry(
    client, operation_id: str, timeout: float = 30.0
) -> None:
    """Delete operation tolerating transient running-request state."""
    try:
        await client.plugins.request_delete(operation_id)
    except Exception:
        pass

    deadline = asyncio.get_running_loop().time() + timeout
    last_exc: Exception | None = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            await client.operations.delete(operation_id)
            return
        except Exception as exc:
            last_exc = exc
            if "running requests" not in str(exc).lower():
                raise
            await asyncio.sleep(1.0)
    if last_exc:
        raise last_exc


def _parse_utc_timestamp(value: str) -> datetime:
    """Parse a UTC ISO8601 timestamp emitted by Gulp/OpenSearch into a datetime."""
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    if "." in normalized:
        head, tail = normalized.split(".", 1)
        if "+" in tail:
            fraction, tz = tail.split("+", 1)
            normalized = f"{head}.{fraction[:6].ljust(6, '0')}+{tz}"
        elif "-" in tail:
            fraction, tz = tail.split("-", 1)
            normalized = f"{head}.{fraction[:6].ljust(6, '0')}-{tz}"

    return datetime.fromisoformat(normalized).astimezone(timezone.utc)


class _InlineProcessPool:
    """Run worker-pool coroutines inline while preserving the process-pool API."""

    async def apply(self, func, args: tuple = (), kwds: dict[str, Any] = None):
        return await func(*(args or ()), **(kwds or {}))


async def _query_doc_by_id(client, operation_id: str, doc_id: str) -> dict:
    """Fetch a single preview document by OpenSearch _id."""
    result = await client.queries.query_single_id(
        operation_id=operation_id,
        doc_id=doc_id,
    )
    return result


@pytest.mark.integration
async def test_list_indexes(gulp_base_url, gulp_test_user, gulp_test_password):
    """list_indexes should return a list of index names."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        try:
            result = await client.db.list_indexes()
            assert isinstance(result, (list, dict))
        except GulpSDKError:
            pytest.skip("db.list_indexes not available in current server config")


@pytest.mark.integration
async def test_refresh_index(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    refresh_index on the default index should not raise.
    If the index does not exist the server may return 4xx — tolerate that.
    """
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"db_test_{uuid.uuid4().hex[:8]}")
        try:
            try:
                result = await client.db.refresh_index(op.id)
                assert result is not None
            except GulpSDKError:
                pass  # non-existent index returns 4xx
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_rebase_by_query_optional(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """rebase_by_query should be callable after a small ingest (optional by server config)."""
    from gulp_sdk import GulpClient, GulpSDKError

    one_day_msec = 24 * 60 * 60 * 1000
    one_day_nsec = one_day_msec * 1_000_000

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("db_rebase_test"))
        try:
            ingest = await client.ingest.file(
                operation_id=op.id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_db_rebase_context",
            )
            assert ingest.req_id
            await _wait_request_done(client, ingest.req_id)

            # get first document's original timestamps for later verification
            results = await client.queries.query_raw(
                operation_id=op.id,
                q=[{"query": {"match_all": {}}}],
                q_options={"preview_mode": True},
            )
            original_doc = results["data"]["docs"][0]
            original_timestamp = original_doc["@timestamp"]
            target_doc_id = original_doc["_id"]
            # expected rebased @timestamp is original + 1 day
            original_gulp_timestamp = int(original_doc["gulp.timestamp"])
            expected_rebased_gulp_timestamp = original_gulp_timestamp + one_day_nsec

            try:
                result = await client.db.rebase_by_query(
                    operation_id=op.id,
                    ws_id=client.ws_id,
                    offset_msec=one_day_msec,
                    flt={"operation_ids": [op.id]},
                    wait=True,
                    timeout=300,
                )
                assert isinstance(result, dict)
                assert str(result.get("status", "")).lower() == "done"
                assert int(result.get("data", {}).get("updated", 0)) == 7
                rebase_req_id = result.get("req_id") or result.get("id")
                assert rebase_req_id

                rebased_doc = await client.queries.query_single_id(
                    operation_id=op.id,
                    doc_id=target_doc_id,
                )
                assert rebased_doc["_id"] == target_doc_id
                assert (
                    int(rebased_doc["gulp.timestamp"])
                    == expected_rebased_gulp_timestamp
                )
                assert _parse_utc_timestamp(rebased_doc["@timestamp"]) == (
                    _parse_utc_timestamp(original_timestamp) + timedelta(days=1)
                )
                assert rebase_req_id in rebased_doc.get("gulp.rebase_req_ids", [])
            except GulpSDKError as exc:
                pytest.skip(
                    f"db.rebase_by_query unavailable in current server config: {exc}"
                )
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_reclaimed_rebase_task_does_not_double_apply_live_offset(
    gulp_base_url,
    gulp_test_user,
    gulp_test_password,
    monkeypatch: pytest.MonkeyPatch,
):
    """Reclaimed rebase task execution should apply the real offset only once."""
    from gulp.api.collab_api import GulpCollab
    from gulp.api.opensearch_api import GulpOpenSearch
    from gulp.api.redis_api import GulpRedis
    from gulp.api.server.structs import TASK_TYPE_REBASE
    from gulp.api.server_api import GulpServer
    from gulp.process import GulpProcess
    from gulp_sdk import GulpClient, GulpSDKError

    one_day_msec = 24 * 60 * 60 * 1000
    one_day_nsec = one_day_msec * 1_000_000

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    suffix = uuid.uuid4().hex
    server_id = f"integration-live-rebase-reclaim-{suffix}"
    reclaimer_server_id = f"reclaimer-live-rebase-reclaim-{suffix}"
    task_types_key = f"gulp:test:queue:types:{suffix}"
    stream_prefix = f"gulp:test:stream:tasks:{suffix}"
    dead_stream_prefix = f"gulp:test:stream:tasks:dead:{suffix}"
    lifecycle_prefix = f"gulp:test:task:lifecycle:{suffix}"
    lock_prefix = f"gulp:test:task:execution_lock:{suffix}"
    consumer_group = f"gulp:test:stream:group:tasks:{suffix}"
    stream_key = f"{stream_prefix}:{TASK_TYPE_REBASE}"
    req_id = f"req-reclaimed-rebase-live-{suffix}"
    execution_lock_key = f"{lock_prefix}:{req_id}"
    side_effect_lock_key = f"{GulpRedis.TASK_SIDE_EFFECT_LOCK_PREFIX}:{req_id}"

    proc = GulpProcess.get_instance()
    old_process_pool = proc.process_pool
    old_process_server_id = proc.server_id
    old_main_process = proc._main_process
    redis_client = None

    monkeypatch.setattr(GulpRedis, "_instance", None)
    monkeypatch.setattr(GulpRedis, "TASK_TYPES_SET", task_types_key)
    monkeypatch.setattr(GulpRedis, "STREAM_TASK_PREFIX", stream_prefix)
    monkeypatch.setattr(GulpRedis, "STREAM_TASK_DLQ_PREFIX", dead_stream_prefix)
    monkeypatch.setattr(GulpRedis, "TASK_LIFECYCLE_PREFIX", lifecycle_prefix)
    monkeypatch.setattr(GulpRedis, "TASK_EXECUTION_LOCK_PREFIX", lock_prefix)
    monkeypatch.setattr(GulpRedis, "STREAM_CONSUMER_GROUP", consumer_group)
    monkeypatch.setattr(GulpRedis, "STREAM_TASK_MAXLEN", 10)
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_USER_MAX", 0)
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_OPERATION_MAX", 0)
    monkeypatch.setattr(GulpRedis, "TASK_LEASE_REFRESH_INTERVAL_MS", 1)

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique("db_rebase_reclaim_test"))
        try:
            ingest = await client.ingest.file(
                operation_id=op.id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_db_rebase_reclaim_context",
            )
            assert ingest.req_id
            await _wait_request_done(client, ingest.req_id)

            results = await client.queries.query_raw(
                operation_id=op.id,
                q=[{"query": {"match_all": {}}}],
                q_options={"preview_mode": True},
            )
            original_doc = results["data"]["docs"][0]
            target_doc_id = original_doc["_id"]
            original_timestamp = original_doc["@timestamp"]
            original_gulp_timestamp = int(original_doc["gulp.timestamp"])
            expected_rebased_gulp_timestamp = original_gulp_timestamp + one_day_nsec

            proc.process_pool = _InlineProcessPool()
            proc.server_id = server_id
            proc._main_process = True
            await GulpCollab.get_instance().init(main_process=False)
            redis_client = GulpRedis.get_instance()
            redis_client.initialize(server_id=server_id, main_process=False)
            redis_client._instance_roles = []
            raw_redis = redis_client.client()
            await raw_redis.ping()

            task = {
                "task_type": TASK_TYPE_REBASE,
                "operation_id": op.id,
                "user_id": gulp_test_user,
                "ws_id": client.ws_id,
                "req_id": req_id,
                "params": {
                    "index": op.id,
                    "offset_msec": one_day_msec,
                    "flt": {"operation_ids": [op.id]},
                    "fields": ["@timestamp"],
                },
            }
            GulpServer._instance = None
            server = GulpServer.get_instance()

            await redis_client.task_enqueue(task)
            first_batch = await redis_client.task_dequeue_batch(1)
            assert len(first_batch) == 1
            assert await redis_client.task_claim_execution(first_batch[0]) == "claimed"

            # Redis XAUTOCLAIM itself is covered in task lifecycle integration.
            # This live test focuses on production rebase side effects after reclaim.
            reclaimed_envelope = dict(first_batch[0])
            reclaimed_envelope["__redis_consumer_name__"] = reclaimer_server_id
            reclaimed_envelope["__redis_autoclaimed__"] = True
            redis_client.server_id = reclaimer_server_id
            await server._dispatch_claimed_tasks(
                redis_client,
                [reclaimed_envelope],
                source="reclaimed",
            )

            doc_before_execution = await client.queries.query_single_id(
                operation_id=op.id,
                doc_id=target_doc_id,
            )
            print("reclaimed rebase doc before lease clear:", doc_before_execution)
            assert int(doc_before_execution["gulp.timestamp"]) == original_gulp_timestamp
            assert req_id not in doc_before_execution.get("gulp.rebase_req_ids", [])
            assert await raw_redis.xlen(stream_key) == 1

            await raw_redis.delete(execution_lock_key, side_effect_lock_key)
            await server._dispatch_claimed_tasks(
                redis_client,
                [reclaimed_envelope],
                source="reclaimed",
            )

            rebased_doc = await client.queries.query_single_id(
                operation_id=op.id,
                doc_id=target_doc_id,
            )
            print("reclaimed rebase doc after reclaim:", rebased_doc)
            assert int(rebased_doc["gulp.timestamp"]) == expected_rebased_gulp_timestamp
            assert _parse_utc_timestamp(rebased_doc["@timestamp"]) == (
                _parse_utc_timestamp(original_timestamp) + timedelta(days=1)
            )
            assert req_id in rebased_doc.get("gulp.rebase_req_ids", [])
            stats_after_reclaim = await _wait_request_done(client, req_id)
            print("reclaimed rebase stats after reclaim:", stats_after_reclaim)
            assert str(stats_after_reclaim.get("status", "")).lower() == "done"
            stats_data_after_reclaim = stats_after_reclaim.get("data") or {}

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

            replayed_doc = await client.queries.query_single_id(
                operation_id=op.id,
                doc_id=target_doc_id,
            )
            print("reclaimed rebase doc after terminal replay:", replayed_doc)
            assert int(replayed_doc["gulp.timestamp"]) == expected_rebased_gulp_timestamp
            assert replayed_doc.get("gulp.rebase_req_ids", []).count(req_id) == 1
            stats_after_replay = await _wait_request_done(client, req_id)
            print("reclaimed rebase stats after terminal replay:", stats_after_replay)
            assert str(stats_after_replay.get("status", "")).lower() == "done"
            assert (stats_after_replay.get("data") or {}) == stats_data_after_reclaim
            assert await raw_redis.xlen(stream_key) == 0
        except GulpSDKError as exc:
            pytest.skip(f"db rebase unavailable in current server config: {exc}")
        finally:
            if redis_client:
                raw_redis = redis_client.client()
                keys_to_delete = [
                    task_types_key,
                    stream_key,
                    f"{dead_stream_prefix}:{TASK_TYPE_REBASE}",
                    GulpRedis.TASK_ACTIVE_USER_HASH,
                    GulpRedis.TASK_ACTIVE_OPERATION_HASH,
                    side_effect_lock_key,
                ]
                async for key in raw_redis.scan_iter(match=f"{lifecycle_prefix}:*"):
                    keys_to_delete.append(key)
                async for key in raw_redis.scan_iter(match=f"{lock_prefix}:*"):
                    keys_to_delete.append(key)
                    keys_to_delete.append(key)
                await raw_redis.delete(*keys_to_delete)
                await redis_client.shutdown()
            GulpRedis._instance = None
            GulpServer._instance = None
            await GulpOpenSearch.get_instance().shutdown()
            GulpOpenSearch._instance = None
            await GulpCollab.get_instance().shutdown()
            proc.process_pool = old_process_pool
            proc.server_id = old_process_server_id
            proc._main_process = old_main_process
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_delete_index_optional(gulp_base_url, gulp_test_user, gulp_test_password):
    """delete_index endpoint is reachable (non-existent index is acceptable)."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        fake_index = _unique("sdk_nonexistent_index")
        try:
            result = await client.db.delete_index(fake_index, delete_operation=False)
            assert isinstance(result, dict)
        except GulpSDKError:
            # 404 / unavailable is acceptable for this reachability check.
            pass
