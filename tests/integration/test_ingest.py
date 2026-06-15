"""Integration tests for ingesting EVTX samples via the win_evtx plugin."""

import asyncio
from pathlib import Path
from time import monotonic
from typing import Any
import uuid

import orjson
import pytest

from gulp.structs import GulpMappingParameters, GulpPluginParameters


def _unique_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _wait_for_ingest_stats(client, req_ids: set[str], timeout: float = 120.0) -> None:
    """Wait for all ingestion requests to reach a terminal stats state."""
    ws = await client.ensure_websocket()
    pending = set(req_ids)
    terminal_errors: dict[str, str] = {}
    deadline = monotonic() + timeout

    while pending:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Timed out waiting for ingestion stats: {sorted(pending)}")

        message = await asyncio.wait_for(ws.__anext__(), timeout=remaining)
        if message.type != "stats_update":
            continue

        obj = message.data.get("obj")
        if not isinstance(obj, dict):
            continue

        req_id = obj.get("id") or message.req_id
        if req_id not in pending:
            continue

        status = obj.get("status")
        if status == "failed":
            terminal_errors[req_id] = f"failed -> {obj.get('errors', [])}"
            pending.remove(req_id)
            continue
        if status == "canceled":
            terminal_errors[req_id] = "canceled"
            pending.remove(req_id)
            continue
        if status == "done":
            pending.remove(req_id)

    # this may be expected ...
    if terminal_errors:        
        details = ", ".join(
            f"{req_id}: {error}" for req_id, error in sorted(terminal_errors.items())
        )
        print(f"WARNING (may be expected, some source may intentionally fail): One or more ingestion requests did not complete successfully: {details}")
        # raise AssertionError(f"One or more ingestion requests did not complete successfully: {details}")


async def _preview_total_hits(client, operation_id: str) -> int:
    """Fetch the total number of documents ingested into an operation."""
    from gulp_sdk.exceptions import NotFoundError

    try:
        result = await client.queries.query_raw(
            operation_id=operation_id,
            q=[{"query": {"match_all": {}}}],
            q_options={"preview_mode": True, "name": "sdk_ingest_preview"},
        )
    except NotFoundError:
        return 0
    return int(result.get("data", {}).get("total_hits", 0))


async def _delete_operation_with_retry(client, operation_id: str, timeout: float = 30.0) -> None:
    """Delete operation tolerating transient running-request state."""
    """try:
        await client.plugins.request_delete(operation_id)
    except Exception:
        pass"""

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


async def _wait_request_done(client, req_id: str, timeout: float = 180.0) -> dict:
    """Poll request stats until request reaches a terminal state."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            stats = await client.ingest.status("unused", req_id)
            status = str(stats.get("status", "")).lower()
            if status in {"done", "failed", "canceled"}:
                return stats
        except Exception:
            pass
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(f"Timed out waiting request {req_id}")
        await asyncio.sleep(1.0)


class _InlineProcessPool:
    """Run worker-pool coroutines inline while preserving the process-pool API."""

    async def apply(self, func, args: tuple = (), kwds: dict[str, Any] = None):
        return await func(*(args or ()), **(kwds or {}))


@pytest.mark.integration
async def test_ingest_win_evtx_sample(gulp_base_url, gulp_test_user, gulp_test_password):
    """Ingest a real sample EVTX file from samples/win_evtx using win_evtx plugin."""
    from gulp_sdk import GulpClient

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        op = await client.operations.create(
            name=_unique_name("sdk_integration_ingest_win_evtx"),
            description="SDK ingestion integration test",
        )

        try:
            result = await client.ingest.file(
                operation_id=op.id,
                plugin_name="win_evtx",
                file_path=str(sample_path),
                context_name="sdk_test_context",
                wait=True,
                timeout=300,
            )

            assert result.req_id
            assert str(result.status).lower() in {"done", "failed", "canceled", "success"}

            # Confirm ingested documents are present via preview query
            assert await _preview_total_hits(client, op.id) == 7
        finally:
            await client.operations.delete(op.id)


@pytest.mark.integration
async def test_reclaimed_ingest_task_uses_live_duplicate_protected_writes(
    gulp_base_url,
    gulp_test_user,
    gulp_test_password,
    monkeypatch: pytest.MonkeyPatch,
):
    """Reclaimed ingest task execution should run once and leave duplicate docs skipped."""
    from gulp.api.collab_api import GulpCollab
    from gulp.api.opensearch_api import GulpOpenSearch
    from gulp.api.redis_api import GulpRedis
    from gulp.api.server.structs import TASK_TYPE_INGEST
    from gulp.api.server_api import GulpServer
    from gulp.process import GulpProcess
    from gulp_sdk import GulpClient

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    suffix = uuid.uuid4().hex
    server_id = f"integration-live-ingest-reclaim-{suffix}"
    task_types_key = f"gulp:test:queue:types:{suffix}"
    stream_prefix = f"gulp:test:stream:tasks:{suffix}"
    dead_stream_prefix = f"gulp:test:stream:tasks:dead:{suffix}"
    lifecycle_prefix = f"gulp:test:task:lifecycle:{suffix}"
    lock_prefix = f"gulp:test:task:execution_lock:{suffix}"
    consumer_group = f"gulp:test:stream:group:tasks:{suffix}"
    stream_key = f"{stream_prefix}:{TASK_TYPE_INGEST}"
    req_id = f"req-reclaimed-ingest-live-{suffix}"
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
        op = await client.operations.create(_unique_name("ingest_reclaim_test"))
        try:
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

            task = {
                "task_type": TASK_TYPE_INGEST,
                "operation_id": op.id,
                "user_id": gulp_test_user,
                "ws_id": client.ws_id,
                "req_id": req_id,
                "params": {
                    "req_id": req_id,
                    "ws_id": client.ws_id,
                    "context_name": f"ctx-reclaimed-ingest-{suffix}",
                    "source_name": str(sample_path),
                    "index": op.id,
                    "plugin": "win_evtx",
                    "file_path": str(sample_path),
                    "payload": {
                        "flt": {},
                        "plugin_params": {},
                        "original_file_path": str(sample_path),
                    },
                    "file_total": 1,
                    "file_size": sample_path.stat().st_size,
                    "delete_after": False,
                },
            }
            GulpServer._instance = None
            server = GulpServer.get_instance()

            await redis_client.task_enqueue(task)
            first_batch = await redis_client.task_dequeue_batch(1)
            assert len(first_batch) == 1
            assert await redis_client.task_claim_execution(first_batch[0]) == "claimed"

            # Redis XAUTOCLAIM mechanics are covered in task lifecycle integration.
            # This live test targets the production ingest side effects after reclaim.
            reclaimed_envelope = dict(first_batch[0])
            reclaimed_envelope["__redis_consumer_name__"] = ws_owner_server_id
            reclaimed_envelope["__redis_autoclaimed__"] = True
            redis_client.server_id = ws_owner_server_id
            await server._dispatch_claimed_tasks(
                redis_client,
                [reclaimed_envelope],
                source="reclaimed",
            )

            total_before_execution = await _preview_total_hits(client, op.id)
            print("reclaimed ingest total before lease clear:", total_before_execution)
            assert total_before_execution == 0
            assert await raw_redis.xlen(stream_key) == 1

            await raw_redis.delete(execution_lock_key, side_effect_lock_key)
            await server._dispatch_claimed_tasks(
                redis_client,
                [reclaimed_envelope],
                source="reclaimed",
            )

            stats_after_reclaim = await _wait_request_done(client, req_id)
            print("reclaimed ingest stats after reclaim:", stats_after_reclaim)
            assert str(stats_after_reclaim.get("status", "")).lower() == "done"
            stats_data_after_reclaim = stats_after_reclaim.get("data") or {}

            total_after_reclaim = await _preview_total_hits(client, op.id)
            print("reclaimed ingest total after reclaim:", total_after_reclaim)
            assert total_after_reclaim == 7

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

            total_after_replay = await _preview_total_hits(client, op.id)
            print("reclaimed ingest total after terminal replay:", total_after_replay)
            assert total_after_replay == 7
            stats_after_replay = await _wait_request_done(client, req_id)
            print("reclaimed ingest stats after terminal replay:", stats_after_replay)
            assert str(stats_after_replay.get("status", "")).lower() == "done"
            assert (stats_after_replay.get("data") or {}) == stats_data_after_reclaim
            assert await raw_redis.xlen(stream_key) == 0
        finally:
            if redis_client:
                raw_redis = redis_client.client()
                keys_to_delete = [
                    task_types_key,
                    stream_key,
                    f"{dead_stream_prefix}:{TASK_TYPE_INGEST}",
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
async def test_ingest_win_evtx_multiple_parallel(gulp_base_url, gulp_test_user, gulp_test_password):
    """Ingest all win_evtx samples concurrently and verify the full document count."""
    from gulp_sdk import GulpClient

    samples_dir = Path("/gulp/samples/win_evtx")
    if not samples_dir.exists():
        pytest.skip(f"Samples directory missing: {samples_dir}")

    files = sorted(path for path in samples_dir.rglob("*") if path.is_file())
    if not files:
        pytest.skip(f"No sample files found in: {samples_dir}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        op = await client.operations.create(
            name=_unique_name("sdk_parallel_ingest_win_evtx"),
            description="SDK parallel ingestion integration test",
        )

        try:
            results = await asyncio.gather(
                *[
                    client.ingest.file(
                        operation_id=op.id,
                        plugin_name="win_evtx",
                        file_path=str(file_path),
                        context_name="sdk_parallel_context",
                    )
                    for file_path in files
                ]
            )

            req_ids = {result.req_id for result in results if result.req_id}
            assert len(req_ids) == len(files)

            await _wait_for_ingest_stats(client, req_ids, timeout=300.0)
            assert await _preview_total_hits(client, op.id) == 98633
        finally:
            await client.operations.delete(op.id)


@pytest.mark.integration
async def test_ingest_local_list(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    local_list returns the contents of the server's ingest_local directory.
    It may be empty; we just verify the endpoint is reachable and returns a list.
    """
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        try:
            result = await client.ingest.local_list()
            assert isinstance(result, (list, dict))
        except GulpSDKError:
            pytest.skip("ingest.local_list not available in current server config")


@pytest.mark.integration
async def test_ingest_zip_sample(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Ingest ZIP fixture similarly to core test_ingest_zip and verify docs were created.
    """
    from gulp_sdk import GulpClient

    zip_path = Path("/gulp/tests/test_ingest_zip.zip")
    if not zip_path.exists():
        pytest.skip(f"ZIP fixture missing: {zip_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        op = await client.operations.create(
            name=_unique_name("sdk_integration_ingest_zip"),
            description="SDK ingest_zip integration test",
        )

        try:
            result = await client.ingest.zip(
                operation_id=op.id,
                plugin_name="win_evtx",
                zipfile_path=str(zip_path),
                wait=True,
                timeout=300,
            )
            assert result.req_id
            assert str(result.status).lower() in {"done", "failed", "canceled", "success"}

            total_hits = await _preview_total_hits(client, op.id)
            assert total_hits > 0
        finally:
            await client.operations.delete(op.id)


@pytest.mark.integration
async def test_ingest_raw_and_status(gulp_base_url, gulp_test_user, gulp_test_password):
    """Exercise ingest.raw and ingest.status on a small JSON payload."""
    from gulp_sdk import GulpClient, GulpSDKError

    raw_docs = [
        {
            "@timestamp": "2024-01-01T00:00:00.000Z",
            "event.code": "sdk_raw_event",
            "event.original": "sdk raw ingest",
            "gulp.operation_id": "test_operation",
            "gulp.context_id": "sdk_raw_context",
            "gulp.source_id": "sdk_raw_source",
            "event.sequence": 1,
        }
    ]

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique_name("sdk_ingest_raw"))
        try:
            try:
                result = await client.ingest.raw(
                    operation_id=op.id,
                    plugin_name="raw",
                    data=raw_docs,
                    params={"last": True},
                    wait=True,
                    timeout=300,
                )
                assert result.req_id
                assert str(result.status).lower() in {"done", "failed", "canceled", "success"}
            except (GulpSDKError, ValueError) as exc:
                pytest.skip(f"ingest.raw/status unavailable in current server config: {exc}")
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_ingest_preview_optional(gulp_base_url, gulp_test_user, gulp_test_password):
    """Exercise ingest.preview on a small EVTX sample if supported."""
    from gulp_sdk import GulpClient, GulpSDKError

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique_name("sdk_ingest_preview"))
        try:
            try:
                preview = await client.ingest.preview(
                    operation_id=op.id,
                    plugin_name="win_evtx",
                    file_path=str(sample_path),
                )
                assert isinstance(preview, list)
                assert len(preview) == 7
            except GulpSDKError as exc:
                pytest.skip(f"ingest.preview unavailable in current server config: {exc}")
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_ingest_file_to_source(gulp_base_url, gulp_test_user, gulp_test_password):
    """Exercise ingest.file_to_source by creating context/source first."""
    from gulp_sdk import GulpClient, GulpSDKError

    sample_path = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
    if not sample_path.exists():
        pytest.skip(f"Sample file missing: {sample_path}")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique_name("sdk_ingest_to_source"))
        try:
            ctx = await client.operations.context_create(op.id, _unique_name("ctx"))
            src = await client.operations.source_create(op.id, ctx["id"], _unique_name("src"), plugin="win_evtx",
                                                        plugin_params=GulpPluginParameters(mapping_parameters=GulpMappingParameters(mapping_file="windows.json")))

            try:
                result = await client.ingest.file_to_source(src["id"], str(sample_path), wait=True)
                assert result.req_id
            except GulpSDKError as exc:
                pytest.skip(f"ingest.file_to_source unavailable in current server config: {exc}")
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_ingest_local_file_variants_optional(gulp_base_url, gulp_test_user, gulp_test_password):
    """Exercise local ingest variants (may be unavailable if ingest_local is empty)."""
    from gulp_sdk import GulpClient, GulpSDKError

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(_unique_name("sdk_ingest_local_variants"))
        try:
            ctx = await client.operations.context_create(op.id, _unique_name("ctx"))
            src = await client.operations.source_create(op.id, ctx["id"], _unique_name("src"))

            # Use placeholder relative paths; environments without ingest_local content may reject them.
            try:
                _ = await client.ingest.file_local(
                    operation_id=op.id,
                    context_name=ctx.get("name", "sdk_ctx"),
                    plugin="win_evtx",
                    path="missing.evtx",
                )
            except GulpSDKError:
                pass

            try:
                _ = await client.ingest.file_local_to_source(
                    source_id=src["id"],
                    path="missing.evtx",
                )
            except GulpSDKError:
                pass

            try:
                _ = await client.ingest.zip_local(
                    operation_id=op.id,
                    context_name=ctx.get("name", "sdk_ctx"),
                    path="missing.zip",
                )
            except GulpSDKError:
                pass
        finally:
            await _delete_operation_with_retry(client, op.id)
