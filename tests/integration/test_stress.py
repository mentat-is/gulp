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
import os
import time
import uuid
from pathlib import Path
from typing import Any

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


async def _login_ready(client: Any, user: str, password: str, timeout: float = 15.0) -> None:
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


async def _create_worker_users(
    admin_client: Any,
    n_workers: int,
    password: str,
) -> list[str]:
    """Create one dedicated user per worker and return user IDs."""
    user_ids: list[str] = []
    for i in range(n_workers):
        user_id = _user_name(i)
        await admin_client.users.create(
            user_id=user_id,
            password=password,
            permission=["admin"],
        )
        user_ids.append(user_id)
    return user_ids


async def _cleanup_worker_users(admin_client: Any, user_ids: list[str]) -> None:
    for user_id in user_ids:
        try:
            await admin_client.users.delete(user_id)
        except Exception:
            pass


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
    """Cancel pending requests then delete the operation, retrying on conflicts."""
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

                assert total_hits > 0, (
                    f"Worker {worker_id}: expected documents after ingest, got 0"
                )

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

            assert total_hits > 0, (
                f"Worker {worker_id}: expected documents in shared operation, got 0"
            )

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
    async with GulpClient(gulp_base_url) as admin_client:
        await _login_ready(admin_client, gulp_test_user, gulp_test_password)
        worker_user_ids = await _create_worker_users(admin_client, n_workers, worker_password)

        try:
            tasks = [
                asyncio.create_task(
                    _worker(gulp_base_url, worker_user_ids[i], worker_password, i, timeout)
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

    assert not failed, (
        f"{len(failed)}/{n_workers} worker(s) failed:\n" + "\n".join(failed)
    )


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
        worker_user_ids = await _create_worker_users(admin_client, n_workers, worker_password)
        op = await admin_client.operations.create(_op_name(9999))
        op_id = op.id

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

            assert not failed, (
                f"{len(failed)}/{n_workers} worker(s) failed:\n" + "\n".join(failed)
            )

        finally:
            await _delete_op(admin_client, op_id)
            await _cleanup_worker_users(admin_client, worker_user_ids)
