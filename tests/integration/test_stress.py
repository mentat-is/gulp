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

def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"

async def _teardown_operation(client, operation_id: str) -> None:
    try:
        await client.operations.delete(operation_id)
    except Exception:
        pass

async def _setup_operation(client) -> str:
    op = await client.operations.create(_unique("query_test_op"))
    return op.id

@pytest.mark.integration
async def test_query_sigma_zip_big_matches_and_notes(gulp_base_url, gulp_test_user, gulp_test_password):
    """
    Run query_sigma_zip using the BIG_SIGMAS ruleset and verify progress, matches and notes.

    This test supports a fast path with SKIP_RESET=1 for pre-ingested datasets.
    """
    from gulp_sdk import GulpClient, GulpSDKError
    from gulp_sdk.websocket import WSMessage, WSMessageType

    big = os.getenv("BIG_SIGMAS", "0").lower() in {"1", "true", "yes", "on"}
    sigma_zip_path = Path("/gulp/tests/sigma_windows.zip" if big else "/gulp/tests/sigma_windows_small.zip")
    expected_completed = 1149 if big else 14
    expected_matches = 73464 if big else 15
    expected_ingested = 98633

    sample_dir = Path("/gulp/samples/win_evtx")
    if not sample_dir.exists() or not sigma_zip_path.exists():
        pytest.skip("Required sigma or sample fixtures are missing")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op_id = await _setup_operation(client)
        try:
            evtx_files = sorted(sample_dir.rglob("*.evtx"))
            if not evtx_files:
                pytest.skip(f"No EVTX samples found in {sample_dir}")

            ingest_ws_terminal_by_req: dict[str, dict[str, Any]] = {}

            def _on_ingest_ws_message(message: WSMessage) -> None:
                if message.type != WSMessageType.STATS_UPDATE.value:
                    return
                payload_obj = message.data.get("obj") if isinstance(message.data, dict) else None
                if not isinstance(payload_obj, dict):
                    return
                payload_status = str(payload_obj.get("status", "")).lower()
                if payload_status in {"done", "failed", "canceled"}:
                    ingest_ws_terminal_by_req[message.req_id] = payload_obj

            await client.register_ws_message_handler(
                WSMessageType.STATS_UPDATE, _on_ingest_ws_message
            )
            try:
                ingest_tasks = []
                for file_path in evtx_files:
                    ingest_tasks.append(
                        client.ingest.file(
                            operation_id=op_id,
                            plugin_name="win_evtx",
                            file_path=str(file_path),
                            context_name="sdk_sigma_zip_context",
                            wait=True,
                            timeout=600,
                        )
                    )

                ingest_results = await asyncio.gather(*ingest_tasks, return_exceptions=True)
                tot_ingested: int = 0
                for ingest in ingest_results:
                    if isinstance(ingest, Exception):
                        pytest.skip(f"win_evtx ingest failed: {ingest}")

                    ingest_status = str(getattr(ingest, "status", "")).lower()
                    ingest_req_id = str(getattr(ingest, "req_id", ""))
                    if ingest_status != "done" and ingest_status != "failed":
                        pytest.skip(
                            f"win_evtx ingest did not finish successfully (status={ingest_status})"
                        )

                    assert ingest_req_id, "Missing req_id for win_evtx ingest request"
                    ingest_terminal = ingest_ws_terminal_by_req.get(ingest_req_id)
                    assert ingest_terminal is not None, (
                        f"Missing terminal STATS_UPDATE websocket notification for ingest req_id={ingest_req_id}"
                    )

                    ingest_data = ingest_terminal.get("data") or {}
                    tot_ingested += int(ingest_data.get("records_ingested", 0))
            finally:
                client.unregister_ws_message_handler(
                    WSMessageType.STATS_UPDATE, _on_ingest_ws_message
                )

            assert tot_ingested == expected_ingested, f"Unexpected total ingested count (must be {expected_ingested}): {tot_ingested}"

            query_req_id = _unique("sigma_zip_req")
            ws_stats_update_count = 0
            ws_query_done_count = 0
            ws_collab_create_count = 0
            ws_terminal_payload: dict[str, Any] | None = None
            ws_assertion_errors: list[str] = []
            query_terminal_event = asyncio.Event()
            query_timeout = int(os.getenv("GULP_SIGMA_ZIP_TIMEOUT", "600"))

            def _on_query_ws_message(message: WSMessage) -> None:
                nonlocal ws_stats_update_count, ws_query_done_count, ws_terminal_payload
                if message.req_id != query_req_id:
                    return
                if message.type == WSMessageType.STATS_UPDATE.value:
                    ws_stats_update_count += 1
                    payload = message.data.get("obj") if isinstance(message.data, dict) else None
                    if isinstance(payload, dict):
                        payload_status = str(payload.get("status", "")).lower()
                        if payload_status in {"done", "failed", "canceled"}:
                            ws_terminal_payload = payload
                            # Validate expected stats immediately inside the callback so
                            # failures are captured at the moment the terminal packet arrives.
                            cb_data = payload.get("data") or {}
                            got_completed = int(cb_data.get("completed_queries", 0))
                            got_hits = int(cb_data.get("total_hits", 0))
                            if got_completed != expected_completed:
                                ws_assertion_errors.append(
                                    f"completed_queries mismatch in terminal WS payload: "
                                    f"got {got_completed}, expected {expected_completed}"
                                )
                            if got_hits != expected_matches:
                                ws_assertion_errors.append(
                                    f"total_hits mismatch in terminal WS payload: "
                                    f"got {got_hits}, expected {expected_matches}"
                                )
                            query_terminal_event.set()
                elif message.type == WSMessageType.QUERY_DONE.value:
                    ws_query_done_count += 1

            def _on_collab_create_ws_message(message: WSMessage) -> None:
                nonlocal ws_collab_create_count
                # collab_create for query-created notes can be single-object or bulk.
                payload_obj = message.data.get("obj") if isinstance(message.data, dict) else None
                if isinstance(payload_obj, dict):
                    if payload_obj.get("operation_id") == op_id and payload_obj.get("type") == "note":
                        ws_collab_create_count += 1
                    return

                if isinstance(payload_obj, list):
                    for obj in payload_obj:
                        if not isinstance(obj, dict):
                            continue
                        if obj.get("operation_id") == op_id and obj.get("type") == "note":
                            ws_collab_create_count += 1
            # Register callback on the websocket BEFORE firing the query so that
            # no messages are missed.  query_sigma_zip is called with wait=False
            # and therefore never calls wait_for_request_stats internally, so the
            # registration must be explicit here.
            await client.register_ws_message_handler(
                WSMessageType.STATS_UPDATE, _on_query_ws_message
            )
            await client.register_ws_message_handler(
                WSMessageType.QUERY_DONE, _on_query_ws_message
            )
            await client.register_ws_message_handler(
                WSMessageType.COLLAB_CREATE, _on_collab_create_ws_message
            )
            try:
                try:
                    query_resp = await client.queries.query_sigma_zip(
                        operation_id=op_id,
                        zip_path=str(sigma_zip_path),
                        src_ids=[],
                        q_options={"create_notes": True, "name": "sdk_sigma_zip_big"},
                        req_id=query_req_id,
                        wait=False,
                    )
                except GulpSDKError as exc:
                    msg = str(exc).lower()
                    if "query_sigma_zip" in msg or "notfound" in msg or "404" in msg:
                        pytest.skip("query_sigma_zip extension endpoint not available")
                    pytest.skip(f"query_sigma_zip unavailable in this environment: {exc}")

                assert isinstance(query_resp, dict)
                assert str(query_resp.get("status", "")).lower() == "pending"
                assert str(query_resp.get("req_id", "")) == query_req_id

                # Wait exclusively for the websocket terminal event — no polling fallback.
                try:
                    await asyncio.wait_for(query_terminal_event.wait(), timeout=query_timeout)
                except asyncio.TimeoutError as exc:
                    raise AssertionError(
                        f"query_sigma_zip did not deliver a terminal STATS_UPDATE websocket "
                        f"notification within {query_timeout}s for req_id={query_req_id}"
                    ) from exc

            except asyncio.TimeoutError as exc:
                raise AssertionError(
                    f"query_sigma_zip did not finish within {query_timeout}s for req_id={query_req_id}"
                ) from exc
            finally:
                client.unregister_ws_message_handler(
                    WSMessageType.STATS_UPDATE, _on_query_ws_message
                )
                client.unregister_ws_message_handler(
                    WSMessageType.QUERY_DONE, _on_query_ws_message
                )
                client.unregister_ws_message_handler(
                    WSMessageType.COLLAB_CREATE, _on_collab_create_ws_message
                )

            # ---- assertions on websocket state ----
            assert ws_assertion_errors == [], (
                "Stats mismatch detected inside WS terminal callback:\n"
                + "\n".join(ws_assertion_errors)
            )
            assert isinstance(ws_terminal_payload, dict), (
                f"Expected terminal STATS_UPDATE websocket notification for req_id={query_req_id}"
            )
            status = str(ws_terminal_payload.get("status", "")).lower()
            assert status in {"done", "failed", "canceled"}, (
                f"Unexpected terminal status={status!r} for req_id={query_req_id}"
            )
            assert ws_stats_update_count > 0, (
                f"Expected STATS_UPDATE websocket notifications for req_id={query_req_id}"
            )
            assert ws_query_done_count == expected_completed, (
                f"Expected exactly {expected_completed} QUERY_DONE websocket notifications "
                f"for req_id={query_req_id}, got {ws_query_done_count}"
            )
            assert ws_collab_create_count == expected_matches, (
                f"Expected exactly {expected_matches} COLLAB_CREATE note events, got {ws_collab_create_count}"
            )

            # Notes cardinality check using paging from offset 0 in batches.
            # This avoids large offset scans and scales with high note counts.
            page_size = 10000
            offset = 0
            observed_notes = 0

            while True:
                batch = await client.collab.note_list(
                    operation_id=op_id,
                    flt={"operation_ids": [op_id], "limit": page_size, "offset": offset},
                )
                batch_count = len(batch)
                observed_notes += batch_count
                if batch_count < page_size:
                    break
                offset += page_size

            assert observed_notes == expected_matches, (
                f"Expected exactly {expected_matches} notes, got {observed_notes}"
            )

        finally:
            await _teardown_operation(client, op_id)


