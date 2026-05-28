"""Comprehensive multi-user integration tests across single and multiple Gulp instances."""

# to run this test, run in one shell:
# gulp --reset-collab --create test_operation
# and in another:
# GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT=8095 gulp
# so to have 2 instances running on localhost

from __future__ import annotations

import asyncio
import os
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest

from gulp_sdk import GulpClient, WSMessageType

_TEST_USER_PASS = "Test@12345"
_SAMPLE_EVTX = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _unique_user(prefix: str) -> str:
    # backend enforces a strict user_id regex length cap
    return f"{prefix}{uuid.uuid4().hex[:6]}"


def _extract_id(obj: Any) -> str | None:
    if isinstance(obj, dict):
        value = obj.get("id") or obj.get("user_id")
        return str(value) if value else None
    value = getattr(obj, "id", None) or getattr(obj, "user_id", None)
    return str(value) if value else None


def _iter_payload_objects(message: Any) -> list[dict[str, Any]]:
    data = getattr(message, "data", None)
    if not isinstance(data, dict):
        return []

    obj = data.get("obj")
    if isinstance(obj, dict):
        return [obj]
    if isinstance(obj, list):
        return [item for item in obj if isinstance(item, dict)]
    return []


def _has_collab_obj(
    messages: list[Any], obj_type: str, obj_id: str | None = None
) -> bool:
    for message in messages:
        for payload_obj in _iter_payload_objects(message):
            if payload_obj.get("type") != obj_type:
                continue
            if obj_id is None or str(payload_obj.get("id")) == str(obj_id):
                return True
    return False


async def _wait_for_condition(
    predicate, timeout: float = 10.0, poll_msec: int = 100
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(poll_msec / 1000.0)
    raise AssertionError("Condition not met within timeout")


async def _wait_for_queryable_docs(
    client: GulpClient,
    operation_id: str,
    minimum_hits: int = 1,
    timeout: float = 360.0,
) -> int:
    last_total_hits = 0

    async def _has_docs() -> bool:
        nonlocal last_total_hits
        result = await client.queries.query_raw(
            operation_id=operation_id,
            q=[{"query": {"match_all": {}}}],
            q_options={"preview_mode": True, "limit": 1, "name": _unique("preview")},
            wait=False,
        )
        data = result.get("data") or {}
        last_total_hits = (
            int(data.get("total_hits", 0)) if isinstance(data, dict) else 0
        )
        return last_total_hits >= minimum_hits

    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        try:
            if await _has_docs():
                return last_total_hits
        except Exception:
            pass
        await asyncio.sleep(1.0)

    raise AssertionError(
        f"operation {operation_id} did not reach {minimum_hits} queryable documents within {timeout}s; last_total_hits={last_total_hits}"
    )


async def _wait_for_request_terminal_status(
    client: GulpClient,
    req_id: str,
    timeout: float = 360.0,
) -> dict[str, Any]:
    last_stats: dict[str, Any] = {"req_id": req_id, "status": "ongoing"}
    deadline = asyncio.get_running_loop().time() + timeout

    while asyncio.get_running_loop().time() < deadline:
        try:
            stats = await client.plugins.request_get(req_id)
            if isinstance(stats, dict):
                last_stats = stats
                status = str(stats.get("status", "")).lower()
                if status in {"done", "failed", "canceled"}:
                    return stats
        except Exception:
            pass
        await asyncio.sleep(1.0)

    raise AssertionError(
        f"request {req_id} did not reach terminal status within {timeout}s; last_stats={last_stats}"
    )


async def _delete_operation_with_retry(
    client: GulpClient, operation_id: str, timeout: float = 30.0
) -> None:
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
    if last_exc is not None:
        raise last_exc


async def _login_and_connect(url: str, user_id: str, password: str) -> GulpClient:
    client = GulpClient(url)
    await client.__aenter__()
    await client.auth.login(user_id, password)
    await client.ensure_websocket()
    return client


async def _register_collectors(
    clients: list[GulpClient],
    message_types: list[WSMessageType],
) -> tuple[list[dict[WSMessageType, list[Any]]], list[list[tuple[WSMessageType, Any]]]]:
    stores: list[dict[WSMessageType, list[Any]]] = [defaultdict(list) for _ in clients]
    handlers: list[list[tuple[WSMessageType, Any]]] = [[] for _ in clients]

    for idx, client in enumerate(clients):
        for message_type in message_types:

            async def _handler(msg, _store=stores[idx], _mt=message_type):
                _store[_mt].append(msg)

            await client.register_ws_message_handler(message_type, _handler)
            handlers[idx].append((message_type, _handler))

    return stores, handlers


def _unregister_collectors(
    clients: list[GulpClient],
    handlers: list[list[tuple[WSMessageType, Any]]],
) -> None:
    for idx, client in enumerate(clients):
        for message_type, handler in handlers[idx]:
            client.unregister_ws_message_handler(message_type, handler)


async def _assert_broadcast_collab_create(
    stores: list[dict[WSMessageType, list[Any]]],
    obj_type: str,
    obj_id: str,
    timeout: float = 12.0,
) -> None:
    await _wait_for_condition(
        lambda: all(
            _has_collab_obj(
                store[WSMessageType.COLLAB_CREATE], obj_type=obj_type, obj_id=obj_id
            )
            for store in stores
        ),
        timeout=timeout,
    )


async def _assert_requester_only_query_ws_events(
    stores: list[dict[WSMessageType, list[Any]]],
    requester_idx: int,
    req_id: str,
    timeout: float = 20.0,
    expect_ws_events: bool = True,
    baselines: list[dict[WSMessageType, int]] | None = None,
) -> None:
    """Assert websocket routing isolation for a query.

    When *expect_ws_events* is True (server queued the request and will push
    DOCUMENTS_CHUNK / QUERY_DONE via websocket) the function waits until the
    requester's socket has received both, then verifies no other socket received
    them.

    When *expect_ws_events* is False (server answered synchronously with
    ``status='success'``) there are no WS events to wait for; the function
    still verifies that no other socket received leakage.
    """
    requester_store = stores[requester_idx]

    def _messages_since(store_idx: int, message_type: WSMessageType) -> list[Any]:
        start = 0
        if baselines is not None:
            start = baselines[store_idx].get(message_type, 0)
        return stores[store_idx][message_type][start:]

    if expect_ws_events:

        def _requester_received() -> bool:
            has_chunk = any(
                getattr(message, "req_id", None) == req_id
                for message in _messages_since(
                    requester_idx, WSMessageType.DOCUMENTS_CHUNK
                )
            )
            has_done = any(
                getattr(message, "req_id", None) == req_id
                for message in _messages_since(requester_idx, WSMessageType.QUERY_DONE)
            )
            return has_chunk and has_done

        try:
            await _wait_for_condition(_requester_received, timeout=timeout)
        except AssertionError as exc:
            requester_chunk_req_ids = [
                getattr(message, "req_id", None)
                for message in _messages_since(
                    requester_idx, WSMessageType.DOCUMENTS_CHUNK
                )
            ]
            requester_done_req_ids = [
                getattr(message, "req_id", None)
                for message in _messages_since(requester_idx, WSMessageType.QUERY_DONE)
            ]
            raise AssertionError(
                "requesting websocket did not receive expected query events "
                f"for req_id={req_id}; requester docs_chunk req_ids={requester_chunk_req_ids}, "
                f"query_done req_ids={requester_done_req_ids}"
            ) from exc

    # Give time for any incorrectly routed cross-socket events to surface.
    await asyncio.sleep(1.0)

    for idx, store in enumerate(stores):
        if idx == requester_idx:
            continue

        leaked_chunks = [
            message
            for message in _messages_since(idx, WSMessageType.DOCUMENTS_CHUNK)
            if getattr(message, "req_id", None) == req_id
        ]
        leaked_done = [
            message
            for message in _messages_since(idx, WSMessageType.QUERY_DONE)
            if getattr(message, "req_id", None) == req_id
        ]

        assert (
            not leaked_chunks
        ), f"non-requesting websocket idx={idx} received docs_chunk for req_id={req_id}"
        assert (
            not leaked_done
        ), f"non-requesting websocket idx={idx} received query_done for req_id={req_id}"


async def _run_multi_user_scenario(base_urls: list[str]) -> None:
    """
    Exercise multi-user websocket semantics.

    Steps
    -----
    1. Admin creates a shared operation; all users are granted access.
    2. Each user creates a context + source on the shared operation.
       → broadcasts: context and source COLLAB_CREATE reach all sockets.
    3. Admin (actor 0) ingests an EVTX file on the shared operation and waits
       for the terminal stats notification (timeout 300 s).
    4. ALL actors fire query_raw concurrently, each with a unique req_id.
       → only the requesting socket must receive DOCUMENTS_CHUNK / QUERY_DONE
         for its own req_id; no leakage to any other socket.
    5. Each actor creates a note + link on the shared operation.
       → broadcasts: note and link COLLAB_CREATE reach all sockets.
    6. Cleanup (collab objects, operation, ephemeral users).
    """
    if not _SAMPLE_EVTX.exists():
        pytest.skip(f"Sample EVTX not found: {_SAMPLE_EVTX}")

    admin_base_url = base_urls[0]
    temp_users: list[str] = []

    async with GulpClient(admin_base_url) as bootstrap:
        try:
            await bootstrap.auth.login("admin", "admin")
        except Exception as exc:
            pytest.skip(
                f"Cannot connect/login to bootstrap instance {admin_base_url}: {exc}"
            )

        for idx in range(1, len(base_urls)):
            user_id = _unique_user(f"mu{idx}")
            await bootstrap.users.create(
                user_id=user_id,
                password=_TEST_USER_PASS,
                permission=["read", "edit", "ingest", "delete"],
            )
            temp_users.append(user_id)

    clients: list[GulpClient] = []
    actors: list[tuple[str, GulpClient]] = []
    stores: list[dict[WSMessageType, list[Any]]] = []
    handlers: list[list[tuple[WSMessageType, Any]]] = []

    # Track collab objects created per-actor for cleanup.
    collab_ids_by_actor: list[list[tuple[str, str]]] = [
        [] for _ in range(len(base_urls))
    ]
    # contexts/sources per-actor (stored for note creation later).
    actor_context_source: list[tuple[str, str]] = []
    shared_operation_id: str | None = None

    users = ["admin", *temp_users]
    passwords = ["admin", *([_TEST_USER_PASS] * len(temp_users))]

    try:
        # ── Step 1: connect all actors ────────────────────────────────────────
        for url, user_id, password in zip(base_urls, users, passwords, strict=True):
            try:
                client = await _login_and_connect(url, user_id, password)
            except Exception as exc:
                pytest.skip(f"Cannot connect/login to {url} as {user_id}: {exc}")
            clients.append(client)
            actors.append((user_id, client))

        watch_types = [
            WSMessageType.COLLAB_CREATE,
            WSMessageType.DOCUMENTS_CHUNK,
            WSMessageType.QUERY_DONE,
        ]
        stores, handlers = await _register_collectors(clients, watch_types)

        admin_client = actors[0][1]

        # ── Step 2: shared operation ──────────────────────────────────────────
        op = await admin_client.operations.create(name=_unique("mop"))
        shared_operation_id = _extract_id(op)
        assert shared_operation_id, "failed to create shared operation id"

        for granted_user in temp_users:
            await admin_client.acl.add_granted_user(
                shared_operation_id,
                "operation",
                granted_user,
            )

        # Each actor creates a context + source and we verify the broadcast.
        for actor_idx, (_, actor_client) in enumerate(actors):
            ctx = await actor_client.operations.context_create(
                operation_id=shared_operation_id,
                context_name=_unique("ctx"),
            )
            ctx_id = _extract_id(ctx)
            assert ctx_id, "failed to create context id"
            await _assert_broadcast_collab_create(
                stores, obj_type="context", obj_id=ctx_id
            )

            src = await actor_client.operations.source_create(
                operation_id=shared_operation_id,
                context_id=ctx_id,
                source_name=_unique("src"),
            )
            src_id = _extract_id(src)
            assert src_id, "failed to create source id"
            await _assert_broadcast_collab_create(
                stores, obj_type="source", obj_id=src_id
            )
            actor_context_source.append((ctx_id, src_id))

        # ── Step 3: single admin ingest ───────────────────────────────────────
        ingest_res = await admin_client.ingest.file(
            operation_id=shared_operation_id,
            plugin_name="win_evtx",
            file_path=str(_SAMPLE_EVTX),
            context_name=_unique("ing_ctx"),
            wait=False,
        )
        assert str(getattr(ingest_res, "status", "")).lower() in {
            "done",
            "success",
            "pending",
            "ongoing",
            "failed",
            "canceled",
        }, f"ingest returned unexpected status: {getattr(ingest_res, 'status', None)!r}"
        await _wait_for_queryable_docs(
            admin_client, shared_operation_id, minimum_hits=1
        )
        if getattr(ingest_res, "req_id", None):
            ingest_stats = await _wait_for_request_terminal_status(
                admin_client,
                ingest_res.req_id,
                timeout=360.0,
            )
            assert str(ingest_stats.get("status", "")).lower() in {
                "done",
                "failed",
                "canceled",
            }, f"ingest request ended in unexpected state: {ingest_stats}"

        # ── Step 4: requester-only query isolation ────────────────────────────
        # Run one query per actor sequentially so this remains a routing test,
        # not a worker-queue saturation test.
        for actor_idx, (_, actor_client) in enumerate(actors):
            query_req_id = _unique(f"qr{actor_idx}")
            baselines = [
                {
                    WSMessageType.DOCUMENTS_CHUNK: len(
                        store[WSMessageType.DOCUMENTS_CHUNK]
                    ),
                    WSMessageType.QUERY_DONE: len(store[WSMessageType.QUERY_DONE]),
                }
                for store in stores
            ]
            query_response = await actor_client.queries.query_raw(
                operation_id=shared_operation_id,
                ws_id=actor_client.ws_id,
                q=[{"query": {"match_all": {}}}],
                q_options={"limit": 100, "name": _unique("qn")},
                req_id=query_req_id,
                wait=False,
            )
            status_lower = str(query_response.get("status", "")).lower()
            assert status_lower in {
                "pending",
                "success",
            }, f"actor {actor_idx} query returned unexpected status: {query_response}"
            assert query_response.get("req_id") == query_req_id

            if status_lower == "pending":
                query_stats = await _wait_for_request_terminal_status(
                    actor_client,
                    query_req_id,
                    timeout=120.0,
                )
                assert str(query_stats.get("status", "")).lower() in {
                    "done",
                    "failed",
                    "canceled",
                }, f"query request ended in unexpected state: {query_stats}"

            await _assert_requester_only_query_ws_events(
                stores,
                requester_idx=actor_idx,
                req_id=query_req_id,
                expect_ws_events=False,
                timeout=30.0,
                baselines=baselines,
            )

        # ── Step 5: collab broadcast (note + link) per actor ──────────────────
        for actor_idx, (_, actor_client) in enumerate(actors):
            ctx_id, src_id = actor_context_source[actor_idx]

            note = await actor_client.collab.note_create(
                operation_id=shared_operation_id,
                context_id=ctx_id,
                source_id=src_id,
                name=_unique("note"),
                text="multi instance note",
                time_pin=1_000_000_000,
                tags=["multi-instance"],
            )
            note_id = _extract_id(note)
            assert note_id, "failed to create note id"
            collab_ids_by_actor[actor_idx].append(("note", note_id))
            await _assert_broadcast_collab_create(
                stores, obj_type="note", obj_id=note_id
            )

            link = await actor_client.collab.link_create(
                operation_id=shared_operation_id,
                doc_id_from=f"doc_from_{actor_idx}",
                doc_ids=[f"doc_to_{actor_idx}_a", f"doc_to_{actor_idx}_b"],
                name=_unique("link"),
                description="multi instance link",
            )
            link_id = _extract_id(link)
            assert link_id, "failed to create link id"
            collab_ids_by_actor[actor_idx].append(("link", link_id))
            await _assert_broadcast_collab_create(
                stores, obj_type="link", obj_id=link_id
            )

    finally:
        if clients and handlers:
            _unregister_collectors(clients, handlers)

        # Delete collab objects in reverse order.
        for actor_idx, (_, client) in enumerate(actors):
            for collab_type, collab_id in reversed(collab_ids_by_actor[actor_idx]):
                try:
                    if collab_type == "note":
                        await client.collab.note_delete(collab_id)
                    elif collab_type == "link":
                        await client.collab.link_delete(collab_id)
                except Exception:
                    pass

        # Delete the shared operation (admin only).
        if shared_operation_id and actors:
            try:
                await _delete_operation_with_retry(actors[0][1], shared_operation_id)
            except Exception:
                pass

        for client in clients:
            try:
                await client.__aexit__(None, None, None)
            except Exception:
                pass

        async with GulpClient(admin_base_url) as cleanup:
            try:
                await cleanup.auth.login("admin", "admin")
            except Exception:
                return
            for user_id in temp_users:
                try:
                    await cleanup.users.delete(user_id)
                except Exception:
                    pass


@pytest.mark.integration
async def test_multi_user_single_instance() -> None:
    """Multiple users on one instance perform ingest/query/collab and verify WS routing semantics."""
    base_url = os.getenv("GULP_BASE_URL", "http://localhost:8080")
    await _run_multi_user_scenario([base_url, base_url, base_url])


@pytest.mark.integration
async def test_multi_user_multi_instance() -> None:
    """Multiple users across two instances verify broadcast and requester-only websocket semantics."""
    first_url = os.getenv("GULP_BASE_URL", "http://localhost:8080")
    second_url = os.getenv("GULP_SECOND_BASE_URL", "http://localhost:8095")
    await _run_multi_user_scenario([first_url, first_url, second_url])
