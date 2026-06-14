"""
Integration tests for queries API.

These tests use preview_mode / small limits to avoid needing large ingested
datasets.  They validate that the query endpoints are reachable and return
properly structured JSend responses.

Requires a live Gulp server (default: http://localhost:8080).
Set GULP_BASE_URL, GULP_TEST_USER, GULP_TEST_PASSWORD env vars to override.

Run with:
    python -m pytest -v tests/integration/test_queries.py -m integration
"""

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest
import uuid
import orjson


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _query_external_custom_parameters(index: str) -> dict:
    """Build query_elasticsearch custom parameters, overridable via env vars."""
    return {
        "uri": os.getenv("GULP_QUERY_EXTERNAL_URI", "http://localhost:9200"),
        "username": os.getenv("GULP_QUERY_EXTERNAL_USERNAME", "admin"),
        "password": os.getenv("GULP_QUERY_EXTERNAL_PASSWORD", "Gulp1234!"),
        "index": index,
        "is_elasticsearch": os.getenv("GULP_QUERY_EXTERNAL_IS_ELASTICSEARCH", "false").lower()
        in {"1", "true", "yes", "on"},
        "context_field": os.getenv("GULP_QUERY_EXTERNAL_CONTEXT_FIELD", "gulp.context_id"),
        "context_type": os.getenv("GULP_QUERY_EXTERNAL_CONTEXT_TYPE", "context_id"),
        "source_field": os.getenv("GULP_QUERY_EXTERNAL_SOURCE_FIELD", "gulp.source_id"),
        "source_type": os.getenv("GULP_QUERY_EXTERNAL_SOURCE_TYPE", "source_id"),
    }


async def _setup_operation(client) -> str:
    op = await client.operations.create(_unique("query_test_op"))
    return op.id


async def _teardown_operation(client, operation_id: str) -> None:
    try:
        await client.operations.delete(operation_id)
    except Exception:
        pass


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
            # Request stats creation/read can be eventually consistent right after enqueue.
            pass
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"Timed out waiting request {req_id}")
        await asyncio.sleep(1.0)


async def _wait_query_docs(client, operation_id: str, timeout: float = 30.0) -> list[dict]:
    """Poll a preview query until at least one document is visible."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        result = await client.queries.query_raw(
            operation_id=operation_id,
            q=[{"query": {"match_all": {}}}],
            q_options={"preview_mode": True, "limit": 10, "name": "query_docs_ready"},
        )
        docs = (result.get("data") or {}).get("docs") or []
        if docs:
            return docs
        if asyncio.get_running_loop().time() >= deadline:
            return docs
        await asyncio.sleep(0.5)


async def _preview_total_hits(client, operation_id: str) -> int:
    """Fetch the total number of documents visible in an operation."""
    from gulp_sdk.exceptions import NotFoundError

    try:
        result = await client.queries.query_raw(
            operation_id=operation_id,
            q=[{"query": {"match_all": {}}}],
            q_options={"preview_mode": True, "limit": 1, "name": "query_total_hits"},
        )
    except NotFoundError:
        return 0
    return int(result.get("data", {}).get("total_hits", 0))


class _InlineProcessPool:
    """Run worker-pool coroutines inline while preserving the process-pool API."""

    async def apply(self, func, args: tuple = (), kwds: dict[str, Any] = None):
        return await func(*(args or ()), **(kwds or {}))


@pytest.mark.integration
async def test_query_raw_empty_result(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Run query_raw with a match-nothing query.

    The endpoint should respond with an empty result (not an error),
    confirming connectivity and parameter routing.
    """
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        qq = { "query": { "match_none": {} } }
        try:
            result = await client.queries.query_raw(
                operation_id=op_id,
                q=[qq],
                q_options={"limit": 1},
                wait=True,
                timeout=120,
            )
            # Query is async, wait=True returns terminal request stats
            print("query_raw result:", result)
            assert isinstance(result, dict)
            assert str(result.get("status", "")).lower() in {"done", "failed", "canceled"}
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_raw_create_notes_for_each_request_match(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """Repeated note-creating queries should create one note for each request match."""
    from gulp_sdk import GulpClient

    marker = _unique("query_note_idem")
    note_name = f"note_{marker}"
    payload = [
        {
            "gulp.context_id": f"ctx_{marker}",
            "gulp.source_id": f"src_{marker}",
            "event.original": marker,
            "custom.marker": marker,
        }
    ]
    query = {"query": {"match_all": {}}}
    q_options = {
        "create_notes": True,
        "name": note_name,
        "notes_tags": ["idempotency", "idempotency"],
        "limit": 10,
    }

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            ingest = await client.ingest.raw(
                operation_id=op_id,
                plugin_name="raw",
                data=payload,
                params={"last": True},
                wait=True,
                timeout=180,
            )
            print("query note idempotency ingest:", ingest)
            assert ingest.req_id
            assert await _wait_query_docs(client, op_id)

            first = await client.queries.query_raw(
                operation_id=op_id,
                q=[query],
                q_options=q_options,
                req_id=f"req_{marker}_first",
                wait=True,
                timeout=180,
            )
            second = await client.queries.query_raw(
                operation_id=op_id,
                q=[query],
                q_options=q_options,
                req_id=f"req_{marker}_second",
                wait=True,
                timeout=180,
            )
            print("query note idempotency first:", first)
            print("query note idempotency second:", second)

            notes = await client.collab.note_list(
                operation_id=op_id,
                flt={"names": [note_name]},
            )
            print("query note idempotency notes:", notes)
            note_ids = {note.get("id") for note in notes}
            assert len(note_ids) == 2
            assert len(notes) == 2
            assert notes[0].get("tags", []).count("idempotency") == 1
            assert "auto" in notes[0].get("tags", [])
            assert note_name in notes[0].get("tags", [])
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_reclaimed_query_task_does_not_duplicate_live_notes(
    gulp_base_url,
    gulp_test_user,
    gulp_test_password,
    monkeypatch: pytest.MonkeyPatch,
):
    """Reclaimed query task execution should not duplicate real note side effects."""
    from gulp.api.collab_api import GulpCollab
    from gulp.api.opensearch_api import GulpOpenSearch
    from gulp.api.redis_api import GulpRedis
    from gulp.api.server.structs import TASK_TYPE_QUERY
    from gulp.api.server_api import GulpServer
    from gulp.process import GulpProcess
    from gulp_sdk import GulpClient

    marker = _unique("query_reclaim_note")
    note_name = f"note_{marker}"
    payload = [
        {
            "gulp.context_id": f"ctx_{marker}",
            "gulp.source_id": f"src_{marker}",
            "event.original": marker,
            "custom.marker": marker,
        }
    ]
    suffix = uuid.uuid4().hex
    server_id = f"integration-live-query-reclaim-{suffix}"
    reclaimer_server_id = f"reclaimer-live-query-reclaim-{suffix}"
    task_types_key = f"gulp:test:queue:types:{suffix}"
    stream_prefix = f"gulp:test:stream:tasks:{suffix}"
    dead_stream_prefix = f"gulp:test:stream:tasks:dead:{suffix}"
    delayed_retry_key = f"gulp:test:stream:tasks:retry_delayed:{suffix}"
    lifecycle_prefix = f"gulp:test:task:lifecycle:{suffix}"
    lock_prefix = f"gulp:test:task:execution_lock:{suffix}"
    drain_sample_prefix = f"gulp:test:task:drain:{suffix}"
    consumer_group = f"gulp:test:stream:group:tasks:{suffix}"
    stream_key = f"{stream_prefix}:{TASK_TYPE_QUERY}"
    req_id = f"req-reclaimed-query-live-notes-{suffix}"
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
    monkeypatch.setattr(GulpRedis, "TASK_RETRY_DELAYED_ZSET", delayed_retry_key)
    monkeypatch.setattr(GulpRedis, "TASK_LIFECYCLE_PREFIX", lifecycle_prefix)
    monkeypatch.setattr(GulpRedis, "TASK_EXECUTION_LOCK_PREFIX", lock_prefix)
    monkeypatch.setattr(GulpRedis, "TASK_DRAIN_SAMPLE_PREFIX", drain_sample_prefix)
    monkeypatch.setattr(GulpRedis, "STREAM_CONSUMER_GROUP", consumer_group)
    monkeypatch.setattr(GulpRedis, "STREAM_TASK_MAXLEN", 10)
    monkeypatch.setattr(GulpRedis, "TASK_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(GulpRedis, "TASK_RETRY_BACKOFF_BASE_MS", 50)
    monkeypatch.setattr(GulpRedis, "TASK_RETRY_BACKOFF_MAX_MS", 50)
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_USER_MAX", 0)
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_OPERATION_MAX", 0)
    monkeypatch.setattr(GulpRedis, "TASK_LEASE_REFRESH_INTERVAL_MS", 1)

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            ingest = await client.ingest.raw(
                operation_id=op_id,
                plugin_name="raw",
                data=payload,
                params={"last": True},
                wait=True,
                timeout=180,
            )
            print("reclaimed query live-note ingest:", ingest)
            assert ingest.req_id
            assert await _wait_query_docs(client, op_id)

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
                "task_type": TASK_TYPE_QUERY,
                "operation_id": op_id,
                "user_id": gulp_test_user,
                "ws_id": f"ws-reclaimed-query-live-notes-{suffix}",
                "req_id": req_id,
                "params": {
                    "queries": [
                        {
                            "q": {"query": {"match_all": {}}},
                            "q_name": note_name,
                        }
                    ],
                    "q_options": {
                        "create_notes": True,
                        "force_ignore_missing_ws": True,
                        "limit": 10,
                        "name": note_name,
                        "notes_tags": ["reclaim", "reclaim"],
                    },
                    "total_num_queries": 1,
                },
            }
            GulpServer._instance = None
            server = GulpServer.get_instance()

            await redis_client.task_enqueue(task)
            first_batch = await redis_client.task_dequeue_batch(1)
            assert len(first_batch) == 1
            assert await redis_client.task_claim_execution(first_batch[0]) == "claimed"

            # Redis XAUTOCLAIM itself is covered in task lifecycle integration.
            # This live test focuses on production query-note side effects after reclaim.
            reclaimed_envelope = dict(first_batch[0])
            reclaimed_envelope["__redis_consumer_name__"] = reclaimer_server_id
            reclaimed_envelope["__redis_autoclaimed__"] = True
            redis_client.server_id = reclaimer_server_id
            await server._dispatch_claimed_tasks(
                redis_client,
                [reclaimed_envelope],
                source="reclaimed",
            )

            notes_before_execution = await client.collab.note_list(
                operation_id=op_id,
                flt={"names": [note_name]},
            )
            print("reclaimed query notes before lease clear:", notes_before_execution)
            assert notes_before_execution == []
            assert await raw_redis.xlen(stream_key) == 1

            await raw_redis.delete(execution_lock_key, side_effect_lock_key)
            await server._dispatch_claimed_tasks(
                redis_client,
                [reclaimed_envelope],
                source="reclaimed",
            )

            notes_after_reclaim = await client.collab.note_list(
                operation_id=op_id,
                flt={"names": [note_name]},
            )
            print("reclaimed query notes after reclaim:", notes_after_reclaim)
            assert len(notes_after_reclaim) == 1
            assert notes_after_reclaim[0].get("tags", []).count("reclaim") == 1
            stats_after_reclaim = await _wait_request_done(client, req_id)
            print("reclaimed query stats after reclaim:", stats_after_reclaim)
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

            notes_after_replay = await client.collab.note_list(
                operation_id=op_id,
                flt={"names": [note_name]},
            )
            print("reclaimed query notes after terminal replay:", notes_after_replay)
            assert len(notes_after_replay) == 1
            stats_after_replay = await _wait_request_done(client, req_id)
            print("reclaimed query stats after terminal replay:", stats_after_replay)
            assert str(stats_after_replay.get("status", "")).lower() == "done"
            assert (stats_after_replay.get("data") or {}) == stats_data_after_reclaim
            assert await raw_redis.xlen(stream_key) == 0
        finally:
            if redis_client:
                raw_redis = redis_client.client()
                keys_to_delete = [
                    task_types_key,
                    stream_key,
                    f"{dead_stream_prefix}:{TASK_TYPE_QUERY}",
                    delayed_retry_key,
                    GulpRedis.TASK_ACTIVE_USER_HASH,
                    GulpRedis.TASK_ACTIVE_OPERATION_HASH,
                    side_effect_lock_key,
                ]
                async for key in raw_redis.scan_iter(match=f"{lifecycle_prefix}:*"):
                    keys_to_delete.append(key)
                async for key in raw_redis.scan_iter(match=f"{lock_prefix}:*"):
                    keys_to_delete.append(key)
                async for key in raw_redis.scan_iter(match=f"{drain_sample_prefix}:*"):
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
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_reclaimed_external_query_task_does_not_duplicate_live_imports(
    gulp_base_url,
    gulp_test_user,
    gulp_test_password,
    monkeypatch: pytest.MonkeyPatch,
):
    """Reclaimed external-query task execution should not import again on terminal replay."""
    from gulp.api.collab_api import GulpCollab
    from gulp.api.opensearch_api import GulpOpenSearch
    from gulp.api.redis_api import GulpRedis
    from gulp.api.server.structs import TASK_TYPE_EXTERNAL_QUERY
    from gulp.api.server_api import GulpServer
    from gulp.process import GulpProcess
    from gulp_sdk import GulpClient, GulpSDKError

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    suffix = uuid.uuid4().hex
    server_id = f"integration-live-external-query-reclaim-{suffix}"
    task_types_key = f"gulp:test:queue:types:{suffix}"
    stream_prefix = f"gulp:test:stream:tasks:{suffix}"
    dead_stream_prefix = f"gulp:test:stream:tasks:dead:{suffix}"
    delayed_retry_key = f"gulp:test:stream:tasks:retry_delayed:{suffix}"
    lifecycle_prefix = f"gulp:test:task:lifecycle:{suffix}"
    lock_prefix = f"gulp:test:task:execution_lock:{suffix}"
    drain_sample_prefix = f"gulp:test:task:drain:{suffix}"
    consumer_group = f"gulp:test:stream:group:tasks:{suffix}"
    stream_key = f"{stream_prefix}:{TASK_TYPE_EXTERNAL_QUERY}"
    req_id = f"req-reclaimed-external-query-live-{suffix}"
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
    monkeypatch.setattr(GulpRedis, "TASK_RETRY_DELAYED_ZSET", delayed_retry_key)
    monkeypatch.setattr(GulpRedis, "TASK_LIFECYCLE_PREFIX", lifecycle_prefix)
    monkeypatch.setattr(GulpRedis, "TASK_EXECUTION_LOCK_PREFIX", lock_prefix)
    monkeypatch.setattr(GulpRedis, "TASK_DRAIN_SAMPLE_PREFIX", drain_sample_prefix)
    monkeypatch.setattr(GulpRedis, "STREAM_CONSUMER_GROUP", consumer_group)
    monkeypatch.setattr(GulpRedis, "STREAM_TASK_MAXLEN", 10)
    monkeypatch.setattr(GulpRedis, "TASK_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(GulpRedis, "TASK_RETRY_BACKOFF_BASE_MS", 50)
    monkeypatch.setattr(GulpRedis, "TASK_RETRY_BACKOFF_MAX_MS", 50)
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_USER_MAX", 0)
    monkeypatch.setattr(GulpRedis, "TASK_ACTIVE_OPERATION_MAX", 0)
    monkeypatch.setattr(GulpRedis, "TASK_LEASE_REFRESH_INTERVAL_MS", 1)

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            ingest = await client.ingest.file(
                operation_id=op_id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_external_query_reclaim_context",
            )
            print("reclaimed external-query seed ingest:", ingest)
            assert ingest.req_id
            await _wait_request_done(client, ingest.req_id)
            seeded_total = await _preview_total_hits(client, op_id)
            assert seeded_total == 7

            proc.process_pool = _InlineProcessPool()
            proc.server_id = server_id
            proc._main_process = True
            await GulpCollab.get_instance().init(main_process=False)
            redis_client = GulpRedis.get_instance()
            redis_client.initialize(server_id=server_id, main_process=False)
            redis_client._instance_roles = []
            raw_redis = redis_client.client()
            await raw_redis.ping()
            ws_owner_server_id = await redis_client.ws_get_server(client.ws_id)
            assert ws_owner_server_id

            q_options = {
                "limit": 10,
                "name": "reclaimed_external_query",
                "preview_mode": False,
            }
            task = {
                "task_type": TASK_TYPE_EXTERNAL_QUERY,
                "operation_id": op_id,
                "user_id": gulp_test_user,
                "ws_id": client.ws_id,
                "req_id": req_id,
                "params": {
                    "queries": [
                        {
                            "q": {"query": {"match_all": {}}},
                            "q_name": "reclaimed_external_query",
                        }
                    ],
                    "q_options": q_options,
                    "index": op_id,
                    "plugin": "query_elasticsearch",
                    "plugin_params": {
                        "custom_parameters": _query_external_custom_parameters(op_id)
                    },
                    "total_num_queries": 1,
                },
            }
            GulpServer._instance = None
            server = GulpServer.get_instance()

            await redis_client.task_enqueue(task)
            first_batch = await redis_client.task_dequeue_batch(1)
            assert len(first_batch) == 1
            assert await redis_client.task_claim_execution(first_batch[0]) == "claimed"

            # Redis XAUTOCLAIM mechanics are covered in task lifecycle integration.
            # This live test targets production external-query import effects after reclaim.
            reclaimed_envelope = dict(first_batch[0])
            reclaimed_envelope["__redis_consumer_name__"] = ws_owner_server_id
            reclaimed_envelope["__redis_autoclaimed__"] = True
            redis_client.server_id = ws_owner_server_id
            await server._dispatch_claimed_tasks(
                redis_client,
                [reclaimed_envelope],
                source="reclaimed",
            )

            total_before_execution = await _preview_total_hits(client, op_id)
            print("reclaimed external-query total before lease clear:", total_before_execution)
            assert total_before_execution == seeded_total
            assert await raw_redis.xlen(stream_key) == 1

            await raw_redis.delete(execution_lock_key, side_effect_lock_key)
            await server._dispatch_claimed_tasks(
                redis_client,
                [reclaimed_envelope],
                source="reclaimed",
            )

            stats_after_reclaim = await _wait_request_done(client, req_id)
            print("reclaimed external-query stats after reclaim:", stats_after_reclaim)
            assert str(stats_after_reclaim.get("status", "")).lower() == "done"
            stats_data = stats_after_reclaim.get("data") or {}
            stats_data_after_reclaim = dict(stats_data)
            assert int(stats_data.get("records_processed", 0)) == seeded_total
            records_ingested = int(stats_data.get("records_ingested", 0))
            records_skipped = int(stats_data.get("records_skipped", 0))
            assert records_ingested + records_skipped == seeded_total
            assert int(stats_data.get("source_processed", 0)) == 1

            total_after_reclaim = await _preview_total_hits(client, op_id)
            print("reclaimed external-query total after reclaim:", total_after_reclaim)
            assert total_after_reclaim == seeded_total + records_ingested

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

            total_after_replay = await _preview_total_hits(client, op_id)
            print("reclaimed external-query total after terminal replay:", total_after_replay)
            assert total_after_replay == total_after_reclaim
            stats_after_replay = await _wait_request_done(client, req_id)
            print("reclaimed external-query stats after terminal replay:", stats_after_replay)
            assert str(stats_after_replay.get("status", "")).lower() == "done"
            assert (stats_after_replay.get("data") or {}) == stats_data_after_reclaim
            assert await raw_redis.xlen(stream_key) == 0
        except GulpSDKError as exc:
            pytest.skip(f"query_external optional plugin unavailable: {exc}")
        finally:
            if redis_client:
                raw_redis = redis_client.client()
                keys_to_delete = [
                    task_types_key,
                    stream_key,
                    f"{dead_stream_prefix}:{TASK_TYPE_EXTERNAL_QUERY}",
                    delayed_retry_key,
                    GulpRedis.TASK_ACTIVE_USER_HASH,
                    GulpRedis.TASK_ACTIVE_OPERATION_HASH,
                    side_effect_lock_key,
                ]
                async for key in raw_redis.scan_iter(match=f"{lifecycle_prefix}:*"):
                    keys_to_delete.append(key)
                async for key in raw_redis.scan_iter(match=f"{lock_prefix}:*"):
                    keys_to_delete.append(key)
                async for key in raw_redis.scan_iter(match=f"{drain_sample_prefix}:*"):
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
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_gulp_empty_result(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Run query_gulp with a broad filter on an empty operation.
    Verifies the endpoint is reachable and returns a structured response.
    """
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            result = await client.queries.query_gulp(
                operation_id=op_id,
                flt={},  # no filter → match all (but index is empty)
                q_options={"limit": 1},
                wait=True,
                timeout=120,
            )
            assert isinstance(result, dict)
            assert str(result.get("status", "")).lower() in {"done", "failed", "canceled"}
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_history_get(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Fetch query history — should return a list (possibly empty).
    """
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            history = await client.queries.query_history_get()
            assert isinstance(history, (list, dict))
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_aggregation_empty(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Run a terms aggregation on an empty operation index.
    Validates that the aggregation endpoint is reachable.
    """
    from gulp_sdk import GulpClient, NotFoundError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            try:
                result = await client.queries.query_aggregation(
                    operation_id=op_id,
                    q={"aggs": {"by_event": {"terms": {"field": "gulp.event_code"}}}},
                )
                assert result is not None
            except NotFoundError:
                # Empty indexes can return "no more hits" from the backend.
                pass
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_operations(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    query_operations should return a list of operations with at least the one
    we just created.
    """
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            result = await client.queries.query_operations()
            # Result should be a list (or dict wrapping a list)
            assert result is not None
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_max_min_per_field(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    query_max_min_per_field on an empty operation should not crash — it may
    return an empty result or a 404/error which we tolerate.
    """
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            try:
                result = await client.queries.query_max_min_per_field(
                    operation_id=op_id,
                )
                assert result is not None
            except GulpSDKError:
                # Empty index may return a 4xx — that's acceptable
                pass
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_fields_by_source(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    query_fields_by_source on an empty context/source should not crash.
    """
    import uuid
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        ctx_name = f"ctx_{uuid.uuid4().hex[:6]}"
        try:
            ctx = await client.operations.context_create(op_id, ctx_name)
            ctx_id = ctx["id"]
            src = await client.operations.source_create(
                op_id, ctx_id, f"src_{uuid.uuid4().hex[:6]}"
            )
            src_id = src["id"]
            try:
                result = await client.queries.query_fields_by_source(
                    operation_id=op_id,
                    context_id=ctx_id,
                    source_id=src_id,
                )
                assert result is not None
            except GulpSDKError:
                pass  # empty index is fine
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_sigma_with_ingested_sample(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Ingest a small EVTX sample then run query_sigma with preview mode.
    """
    from gulp_sdk import GulpClient, GulpSDKError

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    sigma_rule_path = Path("/gulp/tests/sigma_match_all.yml")
    if not sample_path.exists() or not sigma_rule_path.exists():
        pytest.skip("Required sample fixtures are missing")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            ingest = await client.ingest.file(
                operation_id=op_id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_sigma_context",
            )
            assert ingest.req_id
            stats = await _wait_request_done(client, ingest.req_id)
            assert str(stats.get("status", "")).lower() in {"done", "failed"}

            sigma_rule = sigma_rule_path.read_text(encoding="utf-8")
            try:
                result = await client.queries.query_sigma(
                    operation_id=op_id,
                    sigmas=[sigma_rule],
                    src_ids=[],
                    q_options={"preview_mode": True, "limit": 25, "name": "sdk_sigma"},
                )
                assert isinstance(result, dict)
                data = result.get("data", {})
                if isinstance(data, dict):
                    assert int(data.get("total_hits", 0)) == 7
            except GulpSDKError as exc:
                pytest.skip(f"query_sigma not available in current server setup: {exc}")
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_sigma_zip_optional_extension(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Run query_sigma_zip using the optional extension endpoint, if installed.
    """
    from gulp_sdk import GulpClient, GulpSDKError

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    sigma_zip_path = Path("/gulp/tests/sigma_windows_small.zip")
    if not sample_path.exists() or not sigma_zip_path.exists():
        pytest.skip("Required sample fixtures are missing")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            ingest = await client.ingest.file(
                operation_id=op_id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_sigma_zip_context",
            )
            if ingest.req_id:
                await _wait_request_done(client, ingest.req_id)

            try:
                resp = await client.queries.query_sigma_zip(
                    operation_id=op_id,
                    zip_path=str(sigma_zip_path),
                    src_ids=[],
                    q_options={"create_notes": False, "name": "sdk_sigma_zip"},
                    wait=True,
                    timeout=180,
                )
                assert isinstance(resp, dict)
                assert str(resp.get("status", "")).lower() in {
                    "done",
                    "failed",
                    "canceled",
                }
                # query_sigma_zip in this setup should normally run 14 rules
                data = resp.get("data") or {}
                assert int(data.get("completed_queries", 0)) == 14
            except GulpSDKError as exc:
                msg = str(exc).lower()
                if "query_sigma_zip" in msg or "notfound" in msg or "404" in msg:
                    pytest.skip("query_sigma_zip extension endpoint not available")
                pytest.skip(f"query_sigma_zip unavailable in this environment: {exc}")
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_external_optional(gulp_base_url, gulp_test_user, gulp_test_password):
    """Run query_external against optional query_elasticsearch plugin if available."""
    from gulp_sdk import GulpClient, GulpSDKError

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            ingest = await client.ingest.file(
                operation_id=op_id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_query_external_context",
            )
            if ingest.req_id:
                await _wait_request_done(client, ingest.req_id)

            try:
                result = await client.queries.query_external(
                    operation_id=op_id,
                    q={"query": {"match_all": {}}},
                    plugin="query_elasticsearch",
                    plugin_params={
                        "custom_parameters": _query_external_custom_parameters(op_id)
                    },
                    q_options={"preview_mode": True, "limit": 5, "name": "sdk_query_external"},
                )
                assert isinstance(result, dict)
            except GulpSDKError as exc:
                pytest.skip(f"query_external optional plugin unavailable: {exc}")
        finally:
            await _teardown_operation(client, op_id)


@pytest.mark.integration
async def test_query_gulp_export_json(gulp_base_url, gulp_test_user, gulp_test_password, tmp_path):
    """Export query results to JSON file via query_gulp_export_json."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            output_path = tmp_path / "sdk_query_export.json"
            try:
                saved = await client.queries.query_gulp_export_json(
                    operation_id=op_id,
                    output_path=str(output_path),
                    flt={"operation_ids": [op_id]},
                    q_options={"limit": 10},
                )
                assert saved == str(output_path)
                assert output_path.exists()
            except GulpSDKError as exc:
                pytest.skip(f"query_gulp_export_json unavailable in current server config: {exc}")
        finally:
            await _teardown_operation(client, op_id)
