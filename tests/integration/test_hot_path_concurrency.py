"""Live hot-path concurrency tests for query, ingest, and enrich."""

import asyncio
import time
import uuid

import httpx
import pytest


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _require_live_server(base_url: str) -> None:
    """Skip when the configured live backend is unavailable."""
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            response = await client.get(f"{base_url.rstrip('/')}/docs")
            response.raise_for_status()
    except Exception as exc:
        pytest.skip(f"live gulp backend unavailable at {base_url}: {exc}")


def _raw_docs(marker: str, count: int) -> list[dict]:
    return [
        {
            "gulp.context_id": f"ctx_{marker}",
            "gulp.source_id": f"src_{marker}",
            "event.original": f"{marker} document {idx}",
            "hot_path.marker": marker,
            "hot_path.sequence": idx,
        }
        for idx in range(count)
    ]


def _response_req_id(response) -> str:
    if hasattr(response, "req_id"):
        return response.req_id
    if isinstance(response, dict):
        return response.get("req_id") or response.get("id")
    return None


async def _wait_terminal_stats_ws(
    client,
    req_ids: set[str],
    timeout: float = 180.0,
) -> dict[str, dict]:
    """Wait for terminal request stats through websocket stats_update events."""
    ws = await client.ensure_websocket()
    pending = set(req_ids)
    terminal: dict[str, dict] = {}
    deadline = time.monotonic() + timeout

    while pending:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(
                f"timed out waiting for websocket terminal stats: {sorted(pending)}"
            )

        message = await asyncio.wait_for(ws.__anext__(), timeout=remaining)
        if message.type != "stats_update":
            continue

        obj = message.data.get("obj") if isinstance(message.data, dict) else None
        if not isinstance(obj, dict):
            continue

        req_id = obj.get("id") or message.req_id
        if req_id not in pending:
            continue

        status = str(obj.get("status", "")).lower()
        if status in {"done", "failed", "canceled"}:
            terminal[req_id] = obj
            pending.remove(req_id)

    print("websocket terminal stats:", terminal)
    return terminal


async def _delete_operation_with_retry(
    client, operation_id: str, timeout: float = 30.0
) -> None:
    """Delete an operation, tolerating transient running-request state."""
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
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


@pytest.mark.integration
async def test_live_multi_query_request_reports_all_units_via_websocket(
    gulp_base_url,
    gulp_test_user,
    gulp_test_password,
):
    """A multi-query request should complete every query unit on a live backend."""
    from gulp_sdk import GulpClient

    await _require_live_server(gulp_base_url)
    marker = _unique("hot_query")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        await client.ensure_websocket()
        op = await client.operations.create(_unique("hot_query_op"))
        try:
            seed = await client.ingest.raw(
                operation_id=op.id,
                plugin_name="raw",
                data=_raw_docs(marker, 24),
                params={"last": True},
            )
            seed_req_id = _response_req_id(seed)
            assert seed_req_id
            await _wait_terminal_stats_ws(client, {seed_req_id})

            query_count = 8
            query_req_id = _unique("req_hot_multi_query")
            response = await client.queries.query_raw(
                operation_id=op.id,
                q=[{"query": {"match_all": {}}} for _ in range(query_count)],
                q_options={"limit": 1, "name": "hot_multi_query"},
                req_id=query_req_id,
            )
            print("live multi-query response:", response)
            assert _response_req_id(response) == query_req_id

            terminal = await _wait_terminal_stats_ws(client, {query_req_id})
            stats = terminal[query_req_id]
            assert str(stats.get("status", "")).lower() == "done"
            data = stats.get("data") or {}
            assert int(data.get("num_queries", 0)) == query_count
            assert int(data.get("completed_queries", 0)) == query_count
        finally:
            await _delete_operation_with_retry(client, op.id)


@pytest.mark.integration
async def test_live_ingest_enrich_and_query_hot_paths_overlap_via_websocket(
    gulp_base_url,
    gulp_test_user,
    gulp_test_password,
):
    """Live ingest, enrich, and query work should run concurrently and finish."""
    from gulp_sdk import GulpClient

    await _require_live_server(gulp_base_url)
    marker = _unique("hot_mix")

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        await client.ensure_websocket()
        op = await client.operations.create(_unique("hot_mix_op"))
        try:
            seed = await client.ingest.raw(
                operation_id=op.id,
                plugin_name="raw",
                data=_raw_docs(f"{marker}_seed", 12),
                params={"last": True},
            )
            seed_req_id = _response_req_id(seed)
            assert seed_req_id
            await _wait_terminal_stats_ws(client, {seed_req_id})

            ingest_task = asyncio.create_task(
                client.ingest.raw(
                    operation_id=op.id,
                    plugin_name="raw",
                    data=_raw_docs(f"{marker}_concurrent", 60),
                    params={"last": True},
                )
            )
            enrich_task = asyncio.create_task(
                client.enrich.tag_documents(
                    operation_id=op.id,
                    tags=[f"tag_{marker}"],
                    flt={"operation_ids": [op.id]},
                )
            )
            query_req_id = _unique("req_hot_mix_query")
            query_task = asyncio.create_task(
                client.queries.query_raw(
                    operation_id=op.id,
                    q=[{"query": {"match_all": {}}} for _ in range(4)],
                    q_options={"limit": 1, "name": "hot_mix_query"},
                    req_id=query_req_id,
                )
            )

            responses = await asyncio.gather(ingest_task, enrich_task, query_task)
            print("live hot path enqueue responses:", responses)
            req_ids = {_response_req_id(response) for response in responses}
            req_ids.add(query_req_id)
            req_ids = {req_id for req_id in req_ids if req_id}
            assert len(req_ids) == 3

            terminal = await _wait_terminal_stats_ws(client, req_ids, timeout=300.0)
            statuses = {
                req_id: str(stats.get("status", "")).lower()
                for req_id, stats in terminal.items()
            }
            print("live hot path terminal statuses:", statuses)
            assert set(statuses) == req_ids
            assert all(status in {"done", "failed"} for status in statuses.values())
            assert statuses[query_req_id] == "done"
        finally:
            await _delete_operation_with_retry(client, op.id)
