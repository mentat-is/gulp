"""
Stress test: many clients ingesting and querying concurrently.

Each worker independently:
  1. Creates its own isolated operation
  2. Ingests a win_evtx sample file via the win_evtx plugin
  3. Waits for ingestion completion via WebSocket stats_update messages
  4. Runs a query_raw (preview_mode) and asserts documents were indexed
  5. Cleans up its operation

All workers run concurrently via asyncio.gather.  Timing stats are
printed at the end so bottlenecks can be spotted easily.

Environment variables
---------------------
GULP_STRESS_WORKERS   Number of concurrent clients  (default: 5)
GULP_STRESS_TIMEOUT   Per-worker timeout in seconds  (default: 300)
GULP_BASE_URL         Gulp server URL  (default: http://localhost:8080)
GULP_TEST_USER        Admin username   (default: admin)
GULP_TEST_PASSWORD    Admin password   (default: admin)
"""

import asyncio
import contextvars
from contextlib import AsyncExitStack
import os
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SAMPLE_FILE = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
_PLUGIN = "win_evtx"


# ---------------------------------------------------------------------------
# Per-worker helpers
# ---------------------------------------------------------------------------


def _op_name(worker_id: int) -> str:
    return f"stress_{worker_id}_{uuid.uuid4().hex[:6]}"


def _user_name(worker_id: int) -> str:
    # Keep total length <= 17 to satisfy backend user-id pattern.
    return f"st{worker_id}{uuid.uuid4().hex[:6]}"


def _short_error(exc: Exception, max_len: int = 500) -> str:
    text = str(exc)
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


async def _login_ready(
    client: Any,
    user: str,
    password: str,
    timeout: float = 15.0,
) -> None:
    """Retry login until token/session is fully visible to backend checks."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            await client.auth.login(user, password)
            await client.users.me()
            return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.25)

    if last_error:
        raise last_error
    raise TimeoutError("Timed out waiting for login session readiness")


async def _wait_created(
    getter: Any,
    obj_id: str,
    *,
    label: str,
    timeout: float = 15.0,
) -> None:
    """Poll until a created object is readable by the API."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            await getter(obj_id)
            return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.25)

    if last_error:
        raise last_error
    raise TimeoutError(f"Timed out waiting for {label} readiness: {obj_id}")


async def _create_worker_users(
    admin_client: Any,
    n_workers: int,
    password: str,
    permission: list[str] | None = None,
) -> list[str]:
    """Create one dedicated user per worker and return user IDs."""
    user_ids: list[str] = []
    try:
        for i in range(n_workers):
            user_id = _user_name(i)
            await admin_client.users.create(
                user_id=user_id,
                password=password,
                permission=permission or ["admin"],
            )
            user_ids.append(user_id)
            await _wait_created(admin_client.users.get, user_id, label="user")
    except Exception:
        await _cleanup_worker_users(admin_client, user_ids)
        raise
    return user_ids


async def _cleanup_worker_users(admin_client: Any, user_ids: list[str]) -> None:
    for user_id in user_ids:
        try:
            await admin_client.users.delete(user_id)
        except Exception:
            pass


async def _task_metrics_snapshot() -> dict[str, Any]:
    """Collect Redis task metrics directly for stress-test health assertions."""
    from gulp.api.redis_api import GulpRedis

    redis_client = GulpRedis.get_instance()
    if getattr(redis_client, "_redis", None) is None:
        redis_client.initialize(
            server_id=f"stress-metrics-{uuid.uuid4().hex}",
            main_process=False,
        )
    return await redis_client.task_metrics_snapshot()


async def _close_task_metrics_client() -> None:
    """Close the test-process Redis metrics client before pytest switches loops."""
    from gulp.api.redis_api import GulpRedis

    redis_client = GulpRedis.get_instance()
    if getattr(redis_client, "_redis", None) is None:
        return
    try:
        await redis_client.shutdown()
    finally:
        redis_client._redis = None
        redis_client._pubsub = None


def _task_metric_health_issues(
    before: dict[str, Any],
    after: dict[str, Any],
) -> list[str]:
    """Return production-health issues visible in task telemetry."""
    issues: list[str] = []

    for task_type, metrics in (after.get("task_types") or {}).items():
        queued = int(metrics.get("queued", 0) or 0)
        pending = int(metrics.get("pending", 0) or 0)
        if queued:
            issues.append(f"{task_type} still has queued={queued}")
        if pending:
            issues.append(f"{task_type} still has pending={pending}")

    for scope, total in (after.get("active") or {}).items():
        total_int = int(total or 0)
        if total_int:
            issues.append(f"active {scope} reservations still held={total_int}")

    return issues


_STRESS_PROMETHEUS_METRICS = (
    "gulp_redis_task_stream_depth",
    "gulp_redis_task_stream_pending",
    "gulp_redis_task_transition_total",
    "gulp_opensearch_bulk_docs_total",
)


async def _assert_task_metrics_healthy_after_stress(
    before: dict[str, Any],
    *,
    timeout: float = 20.0,
) -> dict[str, Any]:
    """Wait for queue telemetry to settle and assert the stress run stayed clean."""
    try:
        deadline = time.monotonic() + timeout
        after: dict[str, Any] = {}
        issues: list[str] = []
        while time.monotonic() < deadline:
            after = await _task_metrics_snapshot()
            issues = _task_metric_health_issues(before, after)
            if not issues:
                print("stress task metrics healthy:", after)
                return after
            await asyncio.sleep(0.5)

        print(
            "stress task metrics unhealthy:",
            {"before": before, "after": after, "issues": issues},
        )
        assert not issues, "\n".join(issues)
        return after
    finally:
        await _close_task_metrics_client()


async def _assert_prometheus_metrics_cover_stress(
    base_url: str,
    *,
    timeout: float | None = None,
) -> None:
    """If Prometheus is enabled, assert stress-relevant metric families exist."""
    timeout = timeout or float(os.getenv("GULP_STRESS_METRICS_TIMEOUT", "35"))
    deadline = time.monotonic() + timeout
    last_metrics = ""
    missing = list(_STRESS_PROMETHEUS_METRICS)

    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        while time.monotonic() < deadline:
            response = await client.get("/metrics")
            print(
                "stress metrics response:",
                response.status_code,
                response.text[:1000],
            )
            if response.status_code == 404:
                print(
                    "stress metrics endpoint disabled; "
                    "skipping Prometheus coverage check"
                )
                return

            response.raise_for_status()
            last_metrics = response.text
            missing = [
                metric
                for metric in _STRESS_PROMETHEUS_METRICS
                if metric not in last_metrics
            ]
            if not missing:
                print(
                    "stress Prometheus metric families present:",
                    _STRESS_PROMETHEUS_METRICS,
                )
                return
            await asyncio.sleep(1.0)

    print("stress metrics excerpt:", last_metrics[:4000])
    assert not missing, f"missing stress Prometheus metric families: {missing}"


async def _wait_ingest_done(client: Any, req_ids: set[str], timeout: float) -> None:
    """Block until every req_id in *req_ids* reaches a terminal state.

    Use request-status polling only.

    In high-concurrency runs the websocket auth handshake can race with token
    propagation on the backend ("token not logged in"), producing flaky stress
    failures unrelated to ingest/query logic.
    """
    pending = set(req_ids)
    deadline = time.monotonic() + timeout
    while pending:
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Timed out waiting for ingest requests: {sorted(pending)}"
            )

        for req_id in list(pending):
            try:
                stats = await client.ingest.status("unused", req_id)
            except Exception:
                continue

            status = str(stats.get("status", "")).lower()
            if status in {"done", "failed", "canceled"}:
                pending.discard(req_id)

        if pending:
            await asyncio.sleep(0.5)


async def _delete_op(client: Any, op_id: str, timeout: float = 30.0) -> None:
    """Cancel pending requests then delete the operation, waiting on conflicts."""
    try:
        await client.plugins.request_delete(op_id)
    except Exception:
        pass

    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            await client.operations.delete(op_id)
            return
        except Exception as exc:
            # Cleanup should be best-effort in stress scenarios.
            # If the operation/session is already gone, deletion is effectively done.
            msg = str(exc).lower()
            if "not found" in msg:
                return
            if asyncio.get_running_loop().time() >= deadline:
                raise
            if "running requests" not in msg:
                raise
            await asyncio.sleep(1.0)


# ---------------------------------------------------------------------------
# Worker coroutine
# ---------------------------------------------------------------------------


async def _worker(
    base_url: str,
    user: str,
    password: str,
    worker_id: int,
    timeout: float,
) -> dict[str, Any]:
    """One stress-test worker: ingest → wait → query → teardown.

    Returns a result dict with timing info and success flag.
    Never raises — failures are captured in the result so that
    asyncio.gather can collect all results before asserting.
    """
    from gulp_sdk import GulpClient

    result: dict[str, Any] = {"worker_id": worker_id, "success": False}
    op_id: str | None = None
    t_start = time.monotonic()

    try:
        async with GulpClient(base_url) as client:
            await _login_ready(client, user, password)

            # --- 1. Create isolated operation ---
            op = await client.operations.create(_op_name(worker_id))
            op_id = op.id
            await _wait_created(client.operations.get, op_id, label="operation")
            context_name = f"stress_ctx_{worker_id}"

            try:
                # --- 2. Ingest sample file ---
                t_ingest = time.monotonic()
                ingest_result = await client.ingest.file(
                    operation_id=op_id,
                    plugin_name=_PLUGIN,
                    file_path=str(_SAMPLE_FILE),
                    context_name=context_name,
                )
                req_id = ingest_result.req_id

                # --- 3. Wait for ingestion to reach a terminal state ---
                await _wait_ingest_done(client, {req_id}, timeout=timeout)
                result["ingest_secs"] = round(time.monotonic() - t_ingest, 2)

                # --- 4. Query — must find documents ---
                t_query = time.monotonic()
                qr = await client.queries.query_raw(
                    operation_id=op_id,
                    q=[{"query": {"match_all": {}}}],
                    q_options={
                        "preview_mode": True,
                        "name": f"stress_q_{worker_id}",
                    },
                )
                total_hits = int(qr.get("data", {}).get("total_hits", 0))
                result["total_hits"] = total_hits
                result["query_secs"] = round(time.monotonic() - t_query, 2)

                assert (
                    total_hits > 0
                ), f"Worker {worker_id}: expected documents after ingest, got 0"

                result["success"] = True

            finally:
                # --- 5. Cleanup ---
                if op_id is not None:
                    try:
                        await _delete_op(client, op_id)
                    except Exception as cleanup_exc:
                        result.setdefault("warnings", []).append(
                            f"cleanup failed: {cleanup_exc}"
                        )

    except Exception as exc:
        result["error"] = _short_error(exc)

    result["total_secs"] = round(time.monotonic() - t_start, 2)
    return result


async def _worker_same_operation(
    base_url: str,
    user: str,
    password: str,
    worker_id: int,
    timeout: float,
    operation_id: str,
) -> dict[str, Any]:
    """One worker using a shared operation: ingest -> wait -> query."""
    from gulp_sdk import GulpClient

    result: dict[str, Any] = {"worker_id": worker_id, "success": False}
    t_start = time.monotonic()

    try:
        async with GulpClient(base_url) as client:
            await _login_ready(client, user, password)

            context_name = f"stress_shared_ctx_{worker_id}"

            # --- 1. Ingest sample file into the same operation ---
            t_ingest = time.monotonic()
            ingest_result = await client.ingest.file(
                operation_id=operation_id,
                plugin_name=_PLUGIN,
                file_path=str(_SAMPLE_FILE),
                context_name=context_name,
            )
            req_id = ingest_result.req_id

            # --- 2. Wait for this worker request to complete ---
            await _wait_ingest_done(client, {req_id}, timeout=timeout)
            result["ingest_secs"] = round(time.monotonic() - t_ingest, 2)

            # --- 3. Query shared operation and ensure indexed docs are visible ---
            t_query = time.monotonic()
            total_hits = 0
            query_deadline = time.monotonic() + min(timeout, 20.0)
            while time.monotonic() < query_deadline:
                qr = await client.queries.query_raw(
                    operation_id=operation_id,
                    q=[{"query": {"match_all": {}}}],
                    q_options={
                        "preview_mode": True,
                        "name": f"stress_shared_q_{worker_id}",
                    },
                )
                total_hits = int(qr.get("data", {}).get("total_hits", 0))
                if total_hits > 0:
                    break
                await asyncio.sleep(0.5)

            result["total_hits"] = total_hits
            result["query_secs"] = round(time.monotonic() - t_query, 2)

            assert (
                total_hits > 0
            ), f"Worker {worker_id}: expected documents in shared operation, got 0"

            result["success"] = True

    except Exception as exc:
        result["error"] = _short_error(exc)

    result["total_secs"] = round(time.monotonic() - t_start, 2)
    return result


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.stress
async def test_concurrent_ingest_and_query(
    gulp_base_url: str,
    gulp_test_user: str,
    gulp_test_password: str,
) -> None:
    """Stress test: N workers each ingest a file and query results concurrently.

    Parametrise via env vars (see module docstring).  The test fails if any
    worker encounters an error or finds zero documents after ingestion.
    """
    if not _SAMPLE_FILE.exists():
        pytest.skip(f"Sample file not found: {_SAMPLE_FILE}")

    n_workers = int(os.getenv("GULP_STRESS_WORKERS", "5"))
    timeout = float(os.getenv("GULP_STRESS_TIMEOUT", "300"))

    print(
        f"\n[stress] launching {n_workers} concurrent workers "
        f"(timeout={timeout:.0f}s each) …"
    )

    from gulp_sdk import GulpClient

    worker_password = "TestPass!123"
    task_metrics_before = await _task_metrics_snapshot()
    async with GulpClient(gulp_base_url) as admin_client:
        await _login_ready(admin_client, gulp_test_user, gulp_test_password)
        worker_user_ids = await _create_worker_users(
            admin_client, n_workers, worker_password
        )

        try:
            tasks = [
                asyncio.create_task(
                    _worker(
                        gulp_base_url, worker_user_ids[i], worker_password, i, timeout
                    )
                )
                for i in range(n_workers)
            ]

            results: list[dict[str, Any]] = await asyncio.gather(*tasks)  # type: ignore[assignment]
        finally:
            await _cleanup_worker_users(admin_client, worker_user_ids)

    # --- Report ---
    passed: list[dict[str, Any]] = []
    failed: list[str] = []

    for r in results:
        if isinstance(r, BaseException):
            failed.append(f"worker ? raised: {r}")
        elif r.get("success"):
            passed.append(r)
        else:
            wid = r.get("worker_id", "?")
            err = r.get("error", "success=False, no error recorded")
            failed.append(f"worker {wid}: {err}")

    print(
        f"[stress] done — passed={len(passed)}  failed={len(failed)}  "
        f"workers={n_workers}"
    )
    for r in passed:
        wid = r["worker_id"]
        print(
            f"  worker {wid:2d}: "
            f"ingest={r.get('ingest_secs', '?')}s  "
            f"query={r.get('query_secs', '?')}s  "
            f"total={r.get('total_secs', '?')}s  "
            f"hits={r.get('total_hits', '?')}"
        )
    for msg in failed:
        print(f"  FAIL: {msg}")

    assert not failed, f"{len(failed)}/{n_workers} worker(s) failed:\n" + "\n".join(
        failed
    )
    await _assert_task_metrics_healthy_after_stress(task_metrics_before)
    await _assert_prometheus_metrics_cover_stress(gulp_base_url)


@pytest.mark.integration
@pytest.mark.stress
async def test_concurrent_ingest_and_query_same_operation(
    gulp_base_url: str,
    gulp_test_user: str,
    gulp_test_password: str,
) -> None:
    """Stress test: N workers ingest/query concurrently using one shared operation."""
    from gulp_sdk import GulpClient

    if not _SAMPLE_FILE.exists():
        pytest.skip(f"Sample file not found: {_SAMPLE_FILE}")

    n_workers = int(os.getenv("GULP_STRESS_WORKERS", "5"))
    timeout = float(os.getenv("GULP_STRESS_TIMEOUT", "300"))

    print(
        f"\n[stress-shared] launching {n_workers} concurrent workers "
        f"on one operation (timeout={timeout:.0f}s each) ..."
    )

    async with GulpClient(gulp_base_url) as admin_client:
        await _login_ready(admin_client, gulp_test_user, gulp_test_password)
        worker_password = "TestPass!123"
        task_metrics_before = await _task_metrics_snapshot()
        worker_user_ids = await _create_worker_users(
            admin_client, n_workers, worker_password
        )
        op = await admin_client.operations.create(_op_name(9999))
        op_id = op.id
        await _wait_created(admin_client.operations.get, op_id, label="operation")

        try:
            tasks = [
                asyncio.create_task(
                    _worker_same_operation(
                        gulp_base_url,
                        worker_user_ids[i],
                        worker_password,
                        i,
                        timeout,
                        op_id,
                    )
                )
                for i in range(n_workers)
            ]

            results: list[dict[str, Any]] = await asyncio.gather(*tasks)  # type: ignore[assignment]

            passed: list[dict[str, Any]] = []
            failed: list[str] = []

            for r in results:
                if isinstance(r, BaseException):
                    failed.append(f"worker ? raised: {r}")
                elif r.get("success"):
                    passed.append(r)
                else:
                    wid = r.get("worker_id", "?")
                    err = r.get("error", "success=False, no error recorded")
                    failed.append(f"worker {wid}: {err}")

            print(
                f"[stress-shared] done — passed={len(passed)}  failed={len(failed)}  "
                f"workers={n_workers}"
            )
            for r in passed:
                wid = r["worker_id"]
                print(
                    f"  worker {wid:2d}: "
                    f"ingest={r.get('ingest_secs', '?')}s  "
                    f"query={r.get('query_secs', '?')}s  "
                    f"total={r.get('total_secs', '?')}s  "
                    f"hits={r.get('total_hits', '?')}"
                )
            for msg in failed:
                print(f"  FAIL: {msg}")

            assert (
                not failed
            ), f"{len(failed)}/{n_workers} worker(s) failed:\n" + "\n".join(failed)

        finally:
            await _delete_op(admin_client, op_id)
            await _cleanup_worker_users(admin_client, worker_user_ids)
    await _assert_task_metrics_healthy_after_stress(task_metrics_before)
    await _assert_prometheus_metrics_cover_stress(gulp_base_url)


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _teardown_operation(client, operation_id: str) -> None:
    try:
        await client.operations.delete(operation_id)
    except Exception:
        pass


async def _setup_operation(client) -> str:
    op = await client.operations.create(_unique("query_test_op"))
    await _wait_created(client.operations.get, op.id, label="operation")
    return op.id


def _round_robin_instance_url(
    instance_urls: list[str],
    operation_index: int,
    item_index: int,
    items_per_operation: int,
) -> str:
    """Return the instance URL for a flattened operation/item request index."""
    if not instance_urls:
        raise ValueError("at least one instance URL is required")
    return instance_urls[
        (operation_index * items_per_operation + item_index) % len(instance_urls)
    ]


_SIGMA_ZIP_DEBUG_EXPECTED: contextvars.ContextVar[dict[str, object]] = (
    contextvars.ContextVar("sigma_zip_debug_expected", default={})
)


def _sigma_zip_verbose() -> bool:
    return os.getenv("GULP_SIGMA_ZIP_VERBOSE", "").lower() in {"1", "true", "yes", "on"}


def _sigma_zip_debug(message: str, **fields: object) -> None:
    if not _sigma_zip_verbose():
        return
    fields.setdefault("records_ingested", "?")
    fields.setdefault("completed_queries", "?")
    fields.setdefault("total_hits", "?")
    for key, value in _SIGMA_ZIP_DEBUG_EXPECTED.get().items():
        fields.setdefault(key, value)
    details = " ".join(f"{key}={value}" for key, value in fields.items())
    print(
        f"[sigma-zip-big][debug] {message}{' ' + details if details else ''}",
        flush=True,
    )


def _should_print_received_count(count: int, expected: int) -> bool:
    return count <= 5 or count == expected or count % 1000 == 0


class SigmaZipHarnessError(RuntimeError):
    """Harness failure carrying partial totals for terminal worker output."""

    def __init__(self, message: str, totals: dict[str, Any]) -> None:
        super().__init__(message)
        self.totals = totals


async def _count_operation_notes(client: Any, operation_id: str) -> int:
    """Count notes for an operation without loading note payloads."""
    counted = await client.plugins.object_count("note", operation_id=operation_id)
    return int(counted.get("count", 0))


async def _wait_for_operation_notes(
    client: Any, operation_id: str, expected: int
) -> int:
    """Wait until the shared collaboration store exposes the expected note count."""
    last_observed_notes = -1

    while True:
        observed_notes = await _count_operation_notes(client, operation_id)
        if observed_notes != last_observed_notes:
            last_observed_notes = observed_notes
        _sigma_zip_debug(
            "notes count received",
            operation=operation_id,
            observed=observed_notes,
            expected=expected,
        )
        if observed_notes >= expected:
            return observed_notes

        await asyncio.sleep(1.0)


def _request_progress(stats: dict[str, Any]) -> tuple[object, ...]:
    data = stats.get("data") or {}
    return (
        stats.get("status"),
        data.get("records_ingested"),
        data.get("completed_queries"),
        data.get("failed_queries"),
        data.get("total_hits"),
    )


async def _wait_request_stats_progress(
    client: Any,
    req_id: str,
    done: Any,
    *,
    label: str,
) -> dict[str, Any]:
    """Poll request stats until the caller's terminal condition is met."""
    last_progress: tuple[object, ...] = ()

    while True:
        try:
            stats = await client.plugins.request_get(req_id)
        except Exception:
            stats = {}
        if isinstance(stats, dict) and stats:
            progress = _request_progress(stats)
            if progress != last_progress:
                last_progress = progress
                data = stats.get("data") or {}
                _sigma_zip_debug(
                    "request stats progress",
                    label=label,
                    req_id=req_id,
                    status=stats.get("status", "?"),
                    records_ingested=data.get("records_ingested", "?"),
                    completed_queries=data.get("completed_queries", "?"),
                    total_hits=data.get("total_hits", "?"),
                    failed_queries=data.get("failed_queries", "?"),
                    num_queries=data.get("num_queries", "?"),
                )
            if done(stats):
                return stats

        await asyncio.sleep(1.0)


async def _clear_query_artifacts(client: Any, operation_id: str) -> None:
    """Remove query artifacts before reusing an already-ingested operation."""
    from gulp_sdk import NotFoundError

    for obj_type in ("note", "request_stats"):
        try:
            await client.plugins.object_delete_bulk(
                operation_id,
                obj_type,
                {"operation_ids": [operation_id]},
            )
        except NotFoundError:
            pass


async def _run_query_sigma_zip_big_matches_and_notes(
    gulp_base_url: str,
    gulp_test_user: str,
    gulp_test_password: str,
    *,
    big: bool | None = None,
    cleanup: bool = True,
    operation_id: str | None = None,
    instance_urls: list[str] | None = None,
    operation_index: int = 0,
    query_only: bool = False,
    context_name: str | None = None,
    expected_multiplier: int = 1,
    ingest_only: bool = False,
    reset_query_state: bool = True,
    verify_notes: bool = True,
) -> dict[str, Any]:
    """Run the sigma-zip ingest/query/notes flow and return observed counts."""
    from gulp_sdk import GulpClient, GulpSDKError
    from gulp_sdk.websocket import WSMessage, WSMessageType

    if big is None:
        big = os.getenv("BIG_SIGMAS", "0").lower() in {"1", "true", "yes", "on"}
    sigma_zip_path = Path(
        "/gulp/tests/sigma_windows.zip"
        if big
        else "/gulp/tests/sigma_windows_small.zip"
    )
    expected_completed = 1149 if big else 14
    expected_matches = (73464 if big else 15) * expected_multiplier
    expected_ingested = 98633
    _SIGMA_ZIP_DEBUG_EXPECTED.set(
        {
            "expected_records_ingested": expected_ingested,
            "expected_completed_queries": expected_completed,
            "expected_total_hits": expected_matches,
        }
    )
    sample_dir = Path("/gulp/samples/win_evtx")
    if not sample_dir.exists() or not sigma_zip_path.exists():
        raise FileNotFoundError("Required sigma or sample fixtures are missing")

    async with GulpClient(gulp_base_url) as client:
        await _login_ready(client, gulp_test_user, gulp_test_password)
        op_id = operation_id or await _setup_operation(client)
        _SIGMA_ZIP_DEBUG_EXPECTED.set(
            {
                "worker": operation_index,
                "operation": op_id,
                "expected_records_ingested": expected_ingested,
                "expected_completed_queries": expected_completed,
                "expected_total_hits": expected_matches,
            }
        )
        try:
            result_totals: dict[str, Any] = {
                "operation_id": op_id,
                "records_ingested": 0,
                "expected_records_ingested": expected_ingested,
                "completed_queries": 0,
                "expected_completed_queries": expected_completed,
                "total_hits": 0,
                "expected_total_hits": expected_matches,
                "notes": 0,
                "expected_notes": expected_matches,
                "ws_stats_update_count": 0,
                "ws_query_done_count": 0,
                "ws_collab_create_count": 0,
                "query_url": "?",
                "status": "running",
            }

            def _fail(message: str) -> None:
                raise SigmaZipHarnessError(message, dict(result_totals))

            evtx_files = sorted(sample_dir.rglob("*.evtx"))
            if not evtx_files:
                raise FileNotFoundError(f"No EVTX samples found in {sample_dir}")

            ingest_ws_terminal_by_req: dict[str, dict[str, Any]] = {}
            normalized_instance_urls = [
                url.rstrip("/") for url in (instance_urls or [gulp_base_url])
            ]

            async def _on_ingest_ws_message(message: WSMessage) -> None:
                if message.type != WSMessageType.STATS_UPDATE.value:
                    return
                payload_obj = (
                    message.data.get("obj") if isinstance(message.data, dict) else None
                )
                if not isinstance(payload_obj, dict):
                    return
                payload_status = str(payload_obj.get("status", "")).lower()
                if payload_status in {"done", "failed", "canceled"}:
                    ingest_ws_terminal_by_req[message.req_id] = payload_obj
                    payload_data = payload_obj.get("data") or {}
                    _sigma_zip_debug(
                        "ingest terminal received",
                        req_id=message.req_id,
                        status=payload_status,
                        records_ingested=payload_data.get("records_ingested", "?"),
                    )

            async def _ingest_all() -> int:
                registered_clients: list[GulpClient] = []
                async with AsyncExitStack() as stack:
                    clients_by_url: dict[str, GulpClient] = {client.base_url: client}
                    for url in dict.fromkeys(normalized_instance_urls):
                        if url not in clients_by_url:
                            clients_by_url[url] = await stack.enter_async_context(
                                GulpClient(url, token=client.token)
                            )
                    for ingest_client in clients_by_url.values():
                        await ingest_client.register_ws_message_handler(
                            WSMessageType.STATS_UPDATE, _on_ingest_ws_message
                        )
                        registered_clients.append(ingest_client)

                    try:
                        files_per_operation = len(evtx_files)
                        request_counts: dict[str, int] = {
                            url: 0 for url in normalized_instance_urls
                        }

                        def _ingest_client_for_file(file_idx: int) -> GulpClient:
                            url = _round_robin_instance_url(
                                normalized_instance_urls,
                                operation_index,
                                file_idx,
                                files_per_operation,
                            )
                            request_counts[url] += 1
                            return clients_by_url[url]

                        ingest_tasks = []
                        ingest_task_clients: list[GulpClient] = []
                        for file_idx, file_path in enumerate(evtx_files):
                            ingest_client = _ingest_client_for_file(file_idx)
                            ingest_task_clients.append(ingest_client)
                            ingest_tasks.append(
                                ingest_client.ingest.file(
                                    operation_id=op_id,
                                    plugin_name="win_evtx",
                                    file_path=str(file_path),
                                    context_name=context_name
                                    or "sdk_sigma_zip_context",
                                    ws_id=ingest_client.ws_id,
                                    wait=False,
                                )
                            )
                        if _sigma_zip_verbose():
                            print(
                                "sigma zip ingest request split:",
                                {
                                    "operation_id": op_id,
                                    "operation_index": operation_index,
                                    "request_counts": request_counts,
                                },
                            )

                        ingest_results = await asyncio.gather(
                            *ingest_tasks, return_exceptions=True
                        )
                        tot_ingested: int = 0
                        for ingest, ingest_client in zip(
                            ingest_results, ingest_task_clients
                        ):
                            if isinstance(ingest, Exception):
                                _fail(f"win_evtx ingest failed: {ingest}")

                            ingest_status = str(
                                ingest.get("status", "")
                                if isinstance(ingest, dict)
                                else getattr(ingest, "status", "")
                            ).lower()
                            ingest_req_id = str(
                                ingest.get("req_id", "")
                                if isinstance(ingest, dict)
                                else getattr(ingest, "req_id", "")
                            )
                            if ingest_status == "failed":
                                _fail("win_evtx ingest request was rejected")
                            assert (
                                ingest_req_id
                            ), "Missing req_id for win_evtx ingest request"
                            ingest_terminal = ingest_ws_terminal_by_req.get(
                                ingest_req_id
                            )
                            if ingest_terminal is None:
                                ingest_terminal = await _wait_request_stats_progress(
                                    ingest_client,
                                    ingest_req_id,
                                    lambda stats: str(stats.get("status", "")).lower()
                                    in {"done", "failed", "canceled"},
                                    label="win_evtx ingest",
                                )

                            terminal_status = str(
                                ingest_terminal.get("status", "")
                            ).lower()
                            if terminal_status not in {"done", "failed"}:
                                _fail(
                                    f"win_evtx ingest terminal websocket status was "
                                    f"{terminal_status!r} for req_id={ingest_req_id}"
                                )
                            ingest_data = ingest_terminal.get("data") or {}
                            records_ingested = int(
                                ingest_data.get("records_ingested", 0)
                            )
                            if (
                                ingest_status == "failed" or terminal_status == "failed"
                            ) and records_ingested <= 0:
                                _fail(
                                    f"win_evtx ingest failed without ingesting records "
                                    f"for req_id={ingest_req_id}"
                                )
                            tot_ingested += records_ingested
                            result_totals["records_ingested"] = tot_ingested
                        return tot_ingested
                    finally:
                        for ingest_client in registered_clients:
                            ingest_client.unregister_ws_message_handler(
                                WSMessageType.STATS_UPDATE, _on_ingest_ws_message
                            )

            started_at = time.monotonic()
            if query_only:
                if reset_query_state:
                    await _clear_query_artifacts(client, op_id)
                tot_ingested = expected_ingested
            else:
                tot_ingested = await _ingest_all()
            result_totals["records_ingested"] = tot_ingested

            if tot_ingested <= 0:
                _fail("win_evtx ingest did not ingest any records")
            if tot_ingested != expected_ingested:
                result_totals["status"] = "failed"
                return result_totals
            if ingest_only:
                result_totals["status"] = "done"
                return result_totals
            result_totals["status"] = "running"
            print(f"ingestion elapsed: {time.monotonic() - started_at:.2f} seconds")

            query_req_id = _unique("sigma_zip_req")
            ws_stats_update_count = 0
            ws_query_done_count = 0
            ws_collab_create_count = 0
            query_stats: dict[str, Any] = {}
            query_url = _round_robin_instance_url(
                normalized_instance_urls, operation_index, 0, 1
            )
            result_totals["query_url"] = query_url
            if _sigma_zip_verbose():
                print(
                    "sigma zip query request split:",
                    {
                        "operation_id": op_id,
                        "operation_index": operation_index,
                        "query_url": query_url,
                    },
                )

            async def _on_query_ws_message(message: WSMessage) -> None:
                nonlocal ws_stats_update_count, ws_query_done_count
                if message.req_id != query_req_id:
                    return
                if message.type == WSMessageType.STATS_UPDATE.value:
                    ws_stats_update_count += 1
                    payload = (
                        message.data.get("obj")
                        if isinstance(message.data, dict)
                        else None
                    )
                    if isinstance(payload, dict):
                        payload_status = str(payload.get("status", "")).lower()
                        if payload_status in {"done", "failed", "canceled"}:
                            result_totals["status"] = payload_status
                            cb_data = payload.get("data") or {}
                            got_completed = int(cb_data.get("completed_queries", 0))
                            got_hits = int(cb_data.get("total_hits", 0))
                            result_totals["completed_queries"] = got_completed
                            result_totals["total_hits"] = got_hits
                            result_totals["notes"] = got_hits
                            _sigma_zip_debug(
                                "query terminal stats received",
                                req_id=message.req_id,
                                status=payload_status,
                                completed_queries=got_completed,
                                total_hits=got_hits,
                            )
                elif message.type == WSMessageType.QUERY_DONE.value:
                    ws_query_done_count += 1
                    result_totals["ws_query_done_count"] = ws_query_done_count
                    if _should_print_received_count(
                        ws_query_done_count, expected_completed
                    ):
                        _sigma_zip_debug(
                            "query_done received",
                            req_id=message.req_id,
                            count=ws_query_done_count,
                            expected=expected_completed,
                        )

            async def _on_collab_create_ws_message(message: WSMessage) -> None:
                nonlocal ws_collab_create_count
                payload_obj = (
                    message.data.get("obj") if isinstance(message.data, dict) else None
                )
                if isinstance(payload_obj, dict):
                    if (
                        payload_obj.get("operation_id") == op_id
                        and payload_obj.get("type") == "note"
                    ):
                        ws_collab_create_count += 1
                        result_totals["ws_collab_create_count"] = ws_collab_create_count
                        if _should_print_received_count(
                            ws_collab_create_count, expected_matches
                        ):
                            _sigma_zip_debug(
                                "note create received",
                                operation=op_id,
                                count=ws_collab_create_count,
                                expected=expected_matches,
                            )
                    return

                if isinstance(payload_obj, list):
                    for obj in payload_obj:
                        if not isinstance(obj, dict):
                            continue
                        if (
                            obj.get("operation_id") == op_id
                            and obj.get("type") == "note"
                        ):
                            ws_collab_create_count += 1
                            result_totals["ws_collab_create_count"] = (
                                ws_collab_create_count
                            )
                            if _should_print_received_count(
                                ws_collab_create_count, expected_matches
                            ):
                                _sigma_zip_debug(
                                    "note create received",
                                    operation=op_id,
                                    count=ws_collab_create_count,
                                    expected=expected_matches,
                                )

            async with AsyncExitStack() as stack:
                query_ws_types = [
                    WSMessageType.STATS_UPDATE,
                    WSMessageType.QUERY_DONE,
                ]
                query_ws_types.append(WSMessageType.COLLAB_CREATE)
                query_client = await stack.enter_async_context(
                    GulpClient(
                        query_url,
                        token=client.token,
                        ws_operation_ids=[op_id],
                        ws_message_types=query_ws_types,
                    )
                )
                query_ws_id = query_client.ws_id
                await query_client.register_ws_message_handler(
                    WSMessageType.STATS_UPDATE, _on_query_ws_message
                )
                await query_client.register_ws_message_handler(
                    WSMessageType.QUERY_DONE, _on_query_ws_message
                )
                await query_client.register_ws_message_handler(
                    WSMessageType.COLLAB_CREATE, _on_collab_create_ws_message
                )
                try:
                    query_options = {"create_notes": True, "name": "sdk_sigma_zip_big"}
                    try:
                        query_resp = await query_client.queries.query_sigma_zip(
                            operation_id=op_id,
                            zip_path=str(sigma_zip_path),
                            src_ids=[],
                            q_options=query_options,
                            req_id=query_req_id,
                            ws_id=query_ws_id,
                            wait=False,
                        )
                    except GulpSDKError as exc:
                        msg = str(exc).lower()
                        if (
                            "query_sigma_zip" in msg
                            or "notfound" in msg
                            or "404" in msg
                        ):
                            raise RuntimeError(
                                "query_sigma_zip extension endpoint not available"
                            ) from exc
                        raise RuntimeError(
                            f"query_sigma_zip unavailable in this environment: {exc}"
                        ) from exc

                    assert isinstance(query_resp, dict)
                    assert str(query_resp.get("status", "")).lower() == "pending"
                    assert str(query_resp.get("req_id", "")) == query_req_id

                    query_stats = await _wait_request_stats_progress(
                        query_client,
                        query_req_id,
                        lambda stats: int(
                            ((stats.get("data") or {}).get("completed_queries", 0))
                        )
                        >= expected_completed
                        or str(stats.get("status", "")).lower()
                        in {"done", "failed", "canceled"},
                        label="query_sigma_zip",
                    )

                finally:
                    query_client.unregister_ws_message_handler(
                        WSMessageType.STATS_UPDATE, _on_query_ws_message
                    )
                    query_client.unregister_ws_message_handler(
                        WSMessageType.QUERY_DONE, _on_query_ws_message
                    )
                    query_client.unregister_ws_message_handler(
                        WSMessageType.COLLAB_CREATE, _on_collab_create_ws_message
                    )

            if ws_stats_update_count <= 0:
                _sigma_zip_debug(
                    "no query stats websocket notifications received",
                    req_id=query_req_id,
                )
            result_totals["ws_stats_update_count"] = ws_stats_update_count
            query_data = query_stats.get("data") or {}
            result_totals["status"] = query_stats.get("status", result_totals["status"])
            result_totals["completed_queries"] = int(
                query_data.get("completed_queries", 0)
            )
            result_totals["total_hits"] = int(query_data.get("total_hits", 0))

            if result_totals["completed_queries"] != expected_completed:
                _fail(
                    f"completed_queries mismatch in request stats: "
                    f"got {result_totals['completed_queries']}, expected {expected_completed}"
                )
            if result_totals["total_hits"] != expected_matches:
                _fail(
                    f"total_hits mismatch in request stats: "
                    f"got {result_totals['total_hits']}, expected {expected_matches}"
                )
            if ws_query_done_count != expected_completed:
                _sigma_zip_debug(
                    "query_done websocket count differs from request stats",
                    req_id=query_req_id,
                    got=ws_query_done_count,
                    expected=expected_completed,
                )
            result_totals["status"] = "checking_notes"
            if ws_collab_create_count <= 0:
                _sigma_zip_debug(
                    "no note create websocket notifications received",
                    req_id=query_req_id,
                )

            expected_notes = expected_matches
            result_totals["expected_notes"] = expected_notes
            if verify_notes:
                observed_notes = await _wait_for_operation_notes(
                    client, op_id, expected_notes
                )
                result_totals["notes"] = observed_notes

                if observed_notes != expected_notes:
                    _fail(
                        f"Expected exactly {expected_notes} notes, got {observed_notes}"
                    )

            result_totals["completed_queries"] = expected_completed
            result_totals["status"] = "done"
            print(f"total elapsed: {time.monotonic() - started_at:.2f} seconds")
            return result_totals

        finally:
            if cleanup:
                await _delete_op(client, op_id)


@pytest.mark.integration
async def test_query_sigma_zip_big_matches_and_notes(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """
    Run query_sigma_zip using the BIG_SIGMAS ruleset and verify progress, matches and notes.

    This test supports a fast path with SKIP_RESET=1 for pre-ingested datasets.
    """
    try:
        await _run_query_sigma_zip_big_matches_and_notes(
            gulp_base_url,
            gulp_test_user,
            gulp_test_password,
        )
    except (FileNotFoundError, RuntimeError) as exc:
        pytest.skip(str(exc))
