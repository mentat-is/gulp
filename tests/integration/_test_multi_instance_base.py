"""Comprehensive multi-user integration tests across single and multiple Gulp instances."""

# to run this test, run in one shell:
# gulp --reset-collab --create test_operation
# and in another:
# GULP_BIND_TO_ADDR=0.0.0.0 GULP_BIND_TO_PORT=8100 gulp
# so to have 2 instances running on localhost

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

import pytest
import websockets

from gulp_sdk import GulpClient, WSMessageType

_TEST_USER_PASS = "Test@12345"
_SAMPLE_EVTX = Path("/gulp/samples/win_evtx/Security_short_selected.evtx")
_LARGE_CLIENT_DATA_BLOB = "client-data-large-payload-" + ("x" * (300 * 1024))
_LARGE_COLLAB_NOTE_TEXT = "collab-large-broadcast-note-" + ("y" * (300 * 1024))
_LARGE_QUERY_EVENT_ORIGINAL = "query-large-targeted-payload-" + ("z" * (300 * 1024))
_DEFAULT_LARGE_POINTER_STRESS_COUNT = 3
_DEFAULT_LARGE_COLLAB_STRESS_COUNT = 1
_DEFAULT_MULTI_INSTANCE_RAW_INGEST_DOCS = 25


def _env_positive_int(name: str, default: int) -> int:
    """Read a positive integer environment variable with a safe default."""
    try:
        return max(1, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _env_non_negative_float(name: str, default: float = 0.0) -> float:
    """Read a non-negative float environment variable with a safe default."""
    try:
        return max(0.0, float(os.getenv(name, str(default))))
    except ValueError:
        return default


def _large_pointer_stress_count() -> int:
    """Return minimum large-payload iterations for multi-instance fanout checks."""
    return _env_positive_int(
        "GULP_MULTI_INSTANCE_POINTER_STRESS_COUNT",
        _DEFAULT_LARGE_POINTER_STRESS_COUNT,
    )


def _large_pointer_soak_seconds() -> float:
    """Return optional extra duration for repeated large-payload fanout checks."""
    return _env_non_negative_float("GULP_MULTI_INSTANCE_POINTER_SOAK_SECONDS")


def _large_collab_stress_count() -> int:
    """Return large-note collab iterations without tying them to soak duration."""
    return _env_positive_int(
        "GULP_MULTI_INSTANCE_COLLAB_STRESS_COUNT",
        _DEFAULT_LARGE_COLLAB_STRESS_COUNT,
    )


def _multi_instance_raw_ingest_docs() -> int:
    """Return how many raw documents each multi-instance actor ingests."""
    return _env_positive_int(
        "GULP_MULTI_INSTANCE_RAW_INGEST_DOCS",
        _DEFAULT_MULTI_INSTANCE_RAW_INGEST_DOCS,
    )


def _large_pointer_soak_deadline() -> float | None:
    """Return an event-loop deadline for optional soak mode, or None."""
    soak_seconds = _large_pointer_soak_seconds()
    if soak_seconds <= 0:
        return None
    return asyncio.get_running_loop().time() + soak_seconds


def _continue_large_pointer_stress(iteration: int, deadline: float | None) -> bool:
    """Return whether another large-payload fanout iteration should run."""
    if iteration < _large_pointer_stress_count():
        return True
    return deadline is not None and asyncio.get_running_loop().time() < deadline


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
    return _find_collab_obj(messages, obj_type=obj_type, obj_id=obj_id) is not None


def _find_collab_obj(
    messages: list[Any], obj_type: str, obj_id: str | None = None
) -> dict[str, Any] | None:
    for message in messages:
        for payload_obj in _iter_payload_objects(message):
            if payload_obj.get("type") != obj_type:
                continue
            if obj_id is None or str(payload_obj.get("id")) == str(obj_id):
                return payload_obj
    return None


def _has_collab_delete(
    messages: list[Any], obj_type: str, obj_id: str | None = None
) -> bool:
    for message in messages:
        data = getattr(message, "data", None)
        if not isinstance(data, dict):
            continue
        if data.get("type") != obj_type:
            continue
        if obj_id is None or str(data.get("id")) == str(obj_id):
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
) -> list[dict[str, Any]]:
    await _wait_for_condition(
        lambda: all(
            _has_collab_obj(
                store[WSMessageType.COLLAB_CREATE], obj_type=obj_type, obj_id=obj_id
            )
            for store in stores
        ),
        timeout=timeout,
    )
    matched = [
        _find_collab_obj(
            store[WSMessageType.COLLAB_CREATE], obj_type=obj_type, obj_id=obj_id
        )
        for store in stores
    ]
    return [obj for obj in matched if obj is not None]


async def _assert_broadcast_collab_event(
    stores: list[dict[WSMessageType, list[Any]]],
    message_type: WSMessageType,
    obj_type: str,
    obj_id: str,
    timeout: float = 12.0,
) -> None:
    def _store_has_event(store: dict[WSMessageType, list[Any]]) -> bool:
        if message_type == WSMessageType.COLLAB_DELETE:
            return _has_collab_delete(store[message_type], obj_type, obj_id)
        return _has_collab_obj(store[message_type], obj_type, obj_id)

    await _wait_for_condition(
        lambda: all(_store_has_event(store) for store in stores),
        timeout=timeout,
    )


async def _assert_requester_only_query_ws_events(
    stores: list[dict[WSMessageType, list[Any]]],
    requester_idx: int,
    req_id: str,
    timeout: float = 20.0,
    expect_ws_events: bool = True,
    baselines: list[dict[WSMessageType, int]] | None = None,
    expected_docs_chunk_marker: str | None = None,
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

        if expected_docs_chunk_marker is not None:
            matching_chunks = [
                message
                for message in _messages_since(
                    requester_idx, WSMessageType.DOCUMENTS_CHUNK
                )
                if getattr(message, "req_id", None) == req_id
            ]
            assert any(
                expected_docs_chunk_marker in json.dumps(message.data)
                for message in matching_chunks
            ), (
                f"requesting websocket did not receive expected marker "
                f"{expected_docs_chunk_marker!r} in docs_chunk for req_id={req_id}"
            )

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


async def _assert_cross_instance_query_ws_routing(
    request_url: str,
    websocket_owner_client: GulpClient,
    stores: list[dict[WSMessageType, list[Any]]],
    websocket_owner_idx: int,
    operation_id: str,
    req_id_prefix: str,
    q: list[dict[str, Any]] | None = None,
    q_options: dict[str, Any] | None = None,
    expected_docs_chunk_marker: str | None = None,
) -> None:
    """Send a query through one instance and assert delivery to another instance's websocket."""
    query_req_id = _unique(req_id_prefix)
    baselines = [
        {
            WSMessageType.DOCUMENTS_CHUNK: len(store[WSMessageType.DOCUMENTS_CHUNK]),
            WSMessageType.QUERY_DONE: len(store[WSMessageType.QUERY_DONE]),
        }
        for store in stores
    ]

    async with GulpClient(
        request_url,
        token=websocket_owner_client.token,
        ws_auto_connect=False,
    ) as request_client:
        query_response = await request_client.queries.query_raw(
            operation_id=operation_id,
            ws_id=websocket_owner_client.ws_id,
            q=q or [{"query": {"match_all": {}}}],
            q_options=q_options or {"limit": 25, "name": _unique("cross_qn")},
            req_id=query_req_id,
            wait=False,
        )
        print("cross-instance query_raw response:", query_response)

        assert query_response.get("req_id") == query_req_id
        assert str(query_response.get("status", "")).lower() == "pending"

        query_stats = await _wait_for_request_terminal_status(
            request_client,
            query_req_id,
            timeout=120.0,
        )
        print("cross-instance query stats:", query_stats)
        assert str(query_stats.get("status", "")).lower() in {
            "done",
            "failed",
            "canceled",
        }

    await _assert_requester_only_query_ws_events(
        stores,
        requester_idx=websocket_owner_idx,
        req_id=query_req_id,
        expect_ws_events=True,
        timeout=30.0,
        baselines=baselines,
        expected_docs_chunk_marker=expected_docs_chunk_marker,
    )


def _ws_url(base_url: str, path: str) -> str:
    """Build a websocket URL from an HTTP base URL."""
    return f"{base_url.replace('http://', 'ws://').replace('https://', 'wss://')}{path}"


async def _connect_raw_ws(
    base_url: str,
    token: str,
    path: str,
    *,
    ws_id: str | None = None,
    operation_ids: list[str] | None = None,
) -> Any:
    """Connect and authenticate a raw websocket endpoint."""
    ws_id = ws_id or _unique("raw_ws")
    ws = await websockets.connect(
        _ws_url(base_url, path),
        ping_interval=None,
        ping_timeout=None,
        close_timeout=5,
        max_size=None,
        max_queue=128,
    )
    auth_packet: dict[str, Any] = {
        "token": token,
        "ws_id": ws_id,
        "req_id": _unique("raw_auth"),
    }
    if operation_ids is not None:
        auth_packet["operation_ids"] = operation_ids
    await ws.send(json.dumps(auth_packet))
    ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=10.0))
    assert ack.get("type") == "ws_connected", ack
    assert ack.get("ws_id") == ws_id, ack
    return ws


async def _recv_ws_json(ws: Any, timeout: float = 5.0) -> dict[str, Any]:
    """Receive one websocket JSON message."""
    return json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))


async def _assert_no_client_data(ws: Any, timeout: float = 1.0) -> None:
    """Assert no client_data message is received before timeout."""
    try:
        while True:
            message = await _recv_ws_json(ws, timeout=timeout)
            assert message.get("type") != "client_data", message
    except asyncio.TimeoutError:
        return


async def _assert_client_data_message(
    ws: Any,
    *,
    expected_kind: str,
    expected_operation_id: str,
    timeout: float = 8.0,
) -> dict[str, Any]:
    """Wait until a matching client_data message is received."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        remaining = max(0.1, deadline - asyncio.get_running_loop().time())
        message = await _recv_ws_json(ws, timeout=remaining)
        if message.get("type") != "client_data":
            continue
        payload = message.get("payload") or {}
        data = payload.get("data") or {}
        if (
            payload.get("operation_id") == expected_operation_id
            and data.get("kind") == expected_kind
        ):
            return message
    raise AssertionError(f"client_data kind={expected_kind!r} not received")


async def _assert_client_data_ws_routing(
    actors: list[tuple[str, GulpClient]],
    operation_id: str,
    base_urls: list[str],
) -> None:
    """Exercise /ws_client_data routing across users and instances."""
    if len(actors) < 2:
        return

    client_data_sockets: list[Any] = []
    default_probe = None
    same_instance = len(set(base_urls)) == 1
    recipient_idx = 1 if same_instance else next(
        idx for idx, (_, actor_client) in enumerate(actors) if actor_client.base_url != base_urls[0]
    )

    try:
        for idx, (_, actor_client) in enumerate(actors):
            ws = await _connect_raw_ws(
                actor_client.base_url,
                actor_client.token,
                "/ws_client_data",
                ws_id=f"client-data-{idx}-{uuid.uuid4().hex[:8]}",
                operation_ids=[operation_id],
            )
            client_data_sockets.append(ws)

        # A default websocket using the same token must not receive UI protocol
        # packets while /ws_client_data is still a dedicated socket type.
        default_probe = await _connect_raw_ws(
            actors[0][1].base_url,
            actors[0][1].token,
            "/ws",
            ws_id=f"default-probe-{uuid.uuid4().hex[:8]}",
            operation_ids=[operation_id],
        )

        soak_deadline = _large_pointer_soak_deadline()
        targeted_seqs: list[int] = []
        msg_idx = 0
        while _continue_large_pointer_stress(msg_idx, soak_deadline):
            sender_idx = msg_idx % len(client_data_sockets)
            current_recipient_idx = (
                (sender_idx + 1) % len(client_data_sockets)
                if len(client_data_sockets) > 1
                else recipient_idx
            )
            target_user_id = actors[current_recipient_idx][0]
            targeted_kind = f"cursor_targeted_{msg_idx}"
            targeted_payload = {
                "operation_id": operation_id,
                "target_user_ids": [target_user_id],
                "data": {
                    "kind": targeted_kind,
                    "x": 123 + msg_idx,
                    "y": 456 + msg_idx,
                    "blob": _LARGE_CLIENT_DATA_BLOB,
                },
            }
            await client_data_sockets[sender_idx].send(json.dumps(targeted_payload))
            targeted_msg = await _assert_client_data_message(
                client_data_sockets[current_recipient_idx],
                expected_kind=targeted_kind,
                expected_operation_id=operation_id,
            )
            print(
                "client_data targeted message:",
                {
                    "kind": targeted_kind,
                    "seq": targeted_msg.get("seq"),
                    "blob_len": len(targeted_msg["payload"]["data"]["blob"]),
                },
            )
            assert isinstance(targeted_msg.get("seq"), int)
            assert targeted_msg["seq"] > 0
            targeted_seqs.append(targeted_msg["seq"])
            assert (
                targeted_msg["payload"]["data"]["blob"]
                == _LARGE_CLIENT_DATA_BLOB
            )
            for idx, ws in enumerate(client_data_sockets):
                if idx == current_recipient_idx:
                    continue
                await _assert_no_client_data(ws, timeout=0.2)
            msg_idx += 1
        assert targeted_seqs == sorted(targeted_seqs)

        await _assert_no_client_data(default_probe)

        broadcast_seqs: list[int] = []
        msg_idx = 0
        while _continue_large_pointer_stress(msg_idx, soak_deadline):
            sender_idx = msg_idx % len(client_data_sockets)
            broadcast_kind = f"cursor_broadcast_{msg_idx}"
            broadcast_payload = {
                "operation_id": operation_id,
                "data": {
                    "kind": broadcast_kind,
                    "x": 321 + msg_idx,
                    "y": 654 + msg_idx,
                    "blob": _LARGE_CLIENT_DATA_BLOB,
                },
            }
            await client_data_sockets[sender_idx].send(json.dumps(broadcast_payload))
            broadcast_seq: int | None = None
            for ws in client_data_sockets:
                broadcast_msg = await _assert_client_data_message(
                    ws,
                    expected_kind=broadcast_kind,
                    expected_operation_id=operation_id,
                )
                print(
                    "client_data broadcast message:",
                    {
                        "kind": broadcast_kind,
                        "seq": broadcast_msg.get("seq"),
                        "blob_len": len(broadcast_msg["payload"]["data"]["blob"]),
                    },
                )
                assert isinstance(broadcast_msg.get("seq"), int)
                assert broadcast_msg["seq"] > 0
                assert (
                    broadcast_msg["payload"]["data"]["blob"]
                    == _LARGE_CLIENT_DATA_BLOB
                )
                if broadcast_seq is None:
                    broadcast_seq = broadcast_msg["seq"]
                else:
                    assert broadcast_msg["seq"] == broadcast_seq
            assert broadcast_seq is not None
            broadcast_seqs.append(broadcast_seq)
            msg_idx += 1
        assert broadcast_seqs == sorted(broadcast_seqs)
        await _assert_no_client_data(default_probe)
    finally:
        for ws in client_data_sockets:
            with contextlib.suppress(Exception):
                await ws.close()
        if default_probe is not None:
            with contextlib.suppress(Exception):
                await default_probe.close()


async def _run_multi_user_scenario(
    base_urls: list[str],
    *,
    run_client_data: bool = True,
    run_cross_instance_query: bool = True,
    run_query_isolation: bool = True,
    run_collab_lifecycle: bool = True,
    run_ingest: bool = True,
) -> None:
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
    actor_context_source: list[tuple[str, str] | None] = [
        None for _ in range(len(base_urls))
    ]
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
            WSMessageType.COLLAB_UPDATE,
            WSMessageType.COLLAB_DELETE,
            WSMessageType.DOCUMENTS_CHUNK,
            WSMessageType.QUERY_DONE,
        ]
        stores, handlers = await _register_collectors(clients, watch_types)

        admin_client = actors[0][1]
        is_multi_instance = len(set(base_urls)) > 1

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

        if is_multi_instance:
            # Exercise ingestion through every live instance without adding the
            # cost of another file ingest. This gives the routing launcher
            # ingestion traffic on both :8080 and :8100.
            docs_per_actor = _multi_instance_raw_ingest_docs()

            async def _raw_ingest_from_actor(
                actor_idx: int, actor_client: GulpClient
            ) -> str:
                marker = _unique(f"multi_instance_raw_{actor_idx}")
                result = await actor_client.ingest.raw(
                    operation_id=shared_operation_id,
                    plugin_name="raw",
                    data=[
                        {
                            "@timestamp": "2026-01-01T00:00:00.000Z",
                            "event.code": marker,
                            "event.original": (
                                f"{marker} raw ingest doc {doc_idx} from actor {actor_idx} "
                                f"via {actor_client.base_url}"
                            ),
                            "event.sequence": doc_idx,
                            "gulp.context_id": f"multi_instance_raw_context_{actor_idx}",
                            "gulp.source_id": f"multi_instance_raw_source_{actor_idx}",
                        }
                        for doc_idx in range(docs_per_actor)
                    ],
                    params={
                        "last": True,
                        "req_id": _unique(f"req_multi_raw_{actor_idx}"),
                    },
                    wait=True,
                    timeout=180,
                )
                assert result.req_id
                assert str(result.status).lower() in {
                    "done",
                    "success",
                }, f"raw ingest via {actor_client.base_url} ended unexpectedly: {result}"
                print(
                    "multi-instance raw ingest:",
                    {
                        "actor_idx": actor_idx,
                        "base_url": actor_client.base_url,
                        "marker": marker,
                        "docs": docs_per_actor,
                        "req_id": result.req_id,
                        "status": str(result.status),
                    },
                )
                return marker

            raw_markers = await asyncio.gather(
                *(
                    _raw_ingest_from_actor(actor_idx, actor_client)
                    for actor_idx, (_, actor_client) in enumerate(actors)
                )
            )
            await _wait_for_queryable_docs(
                admin_client,
                shared_operation_id,
                minimum_hits=len(raw_markers) * docs_per_actor,
            )

        if run_client_data:
            # ── Step 2a: client_data websocket routing ───────────────────────
            # /ws_client_data is a separate websocket endpoint used by UI clients
            # for their own protocol data, e.g. cursor positions. The backend must
            # only route these packets among WS_CLIENT_DATA sockets.
            await _assert_client_data_ws_routing(
                actors,
                operation_id=shared_operation_id,
                base_urls=base_urls,
            )

        if run_collab_lifecycle:
            # Each actor creates a context + source and we verify the broadcast.
            async def _create_actor_context_source(
                actor_idx: int, actor_client: GulpClient
            ) -> None:
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
                actor_context_source[actor_idx] = (ctx_id, src_id)

            if is_multi_instance:
                await asyncio.gather(
                    *(
                        _create_actor_context_source(actor_idx, actor_client)
                        for actor_idx, (_, actor_client) in enumerate(actors)
                    )
                )
            else:
                for actor_idx, (_, actor_client) in enumerate(actors):
                    await _create_actor_context_source(actor_idx, actor_client)

        if run_ingest:
            # ── Step 3: single admin ingest ───────────────────────────────────
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

        if run_cross_instance_query:
            # ── Step 4: explicit cross-instance targeted websocket routing ────
            # The websocket owner and the HTTP request ingress are deliberately
            # different instances. This exercises Redis ws_id ownership lookup and
            # targeted pub/sub delivery to the owning instance.
            unique_base_urls = list(dict.fromkeys(base_urls))
            if len(unique_base_urls) >= 2:
                large_query_marker = _unique("large_targeted_query")
                large_ingest = await admin_client.ingest.raw(
                    operation_id=shared_operation_id,
                    plugin_name="raw",
                    data=[
                        {
                            "@timestamp": "2026-01-01T00:00:00.000Z",
                            "event.code": large_query_marker,
                            "event.original": (
                                f"{large_query_marker} {_LARGE_QUERY_EVENT_ORIGINAL}"
                            ),
                            "gulp.context_id": "multi_instance_large_query_context",
                            "gulp.source_id": "multi_instance_large_query_source",
                        }
                    ],
                    params={
                        "last": True,
                        "req_id": _unique("req_large_query_ingest"),
                    },
                    wait=True,
                    timeout=180,
                )
                print("large targeted query ingest:", large_ingest)
                assert large_ingest.req_id
                assert str(large_ingest.status).lower() in {
                    "done",
                    "success",
                }
                large_query = [
                    {
                        "query": {
                            "match_all": {},
                        }
                    }
                ]
                large_query_options = {
                    "limit": 1,
                    "fields": "*",
                    "sort": {"@timestamp": "desc"},
                    "name": _unique("cross_large_qn"),
                }

                remote_owner_idx = next(
                    idx
                    for idx, (_, actor_client) in enumerate(actors)
                    if actor_client.base_url == unique_base_urls[1]
                )
                await asyncio.gather(
                    _assert_cross_instance_query_ws_routing(
                        request_url=unique_base_urls[1],
                        websocket_owner_client=actors[0][1],
                        stores=stores,
                        websocket_owner_idx=0,
                        operation_id=shared_operation_id,
                        req_id_prefix="cross_a",
                        q=large_query,
                        q_options=large_query_options,
                        expected_docs_chunk_marker=large_query_marker,
                    ),
                    _assert_cross_instance_query_ws_routing(
                        request_url=unique_base_urls[0],
                        websocket_owner_client=actors[remote_owner_idx][1],
                        stores=stores,
                        websocket_owner_idx=remote_owner_idx,
                        operation_id=shared_operation_id,
                        req_id_prefix="cross_b",
                        q=large_query,
                        q_options=large_query_options,
                        expected_docs_chunk_marker=large_query_marker,
                    ),
                )

        if run_query_isolation:
            # ── Step 4: requester-only query isolation ────────────────────────
            # Multi-instance runs issue one query per actor concurrently so both
            # ingress points are active while still checking requester isolation.
            async def _run_requester_query(
                actor_idx: int, actor_client: GulpClient
            ) -> None:
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
                    expect_ws_events=True,
                    timeout=30.0,
                    baselines=baselines,
                )

            if is_multi_instance:
                await asyncio.gather(
                    *(
                        _run_requester_query(actor_idx, actor_client)
                        for actor_idx, (_, actor_client) in enumerate(actors)
                    )
                )
            else:
                for actor_idx, (_, actor_client) in enumerate(actors):
                    await _run_requester_query(actor_idx, actor_client)

        if run_collab_lifecycle:
            # ── Step 5: collab broadcast lifecycle per actor ──────────────────
            # Exercise create/update/delete fanout for each collab type that is
            # commonly used by the UI. Each actor owns one lifecycle so both local
            # and cross-instance broadcast paths are covered.
            async def _run_actor_collab_lifecycle(
                actor_idx: int, actor_client: GulpClient
            ) -> None:
                context_source = actor_context_source[actor_idx]
                assert context_source is not None
                ctx_id, src_id = context_source

                msg_idx = 0
                while msg_idx < _large_collab_stress_count():
                    large_note_text = f"{_LARGE_COLLAB_NOTE_TEXT}-{msg_idx}"
                    note = await actor_client.collab.note_create(
                        operation_id=shared_operation_id,
                        context_id=ctx_id,
                        source_id=src_id,
                        name=_unique(f"note_{actor_idx}_{msg_idx}"),
                        text=large_note_text,
                        time_pin=1_000_000_000 + (actor_idx * 10_000) + msg_idx,
                        tags=["multi-instance"],
                    )
                    note_id = _extract_id(note)
                    assert note_id, "failed to create note id"
                    collab_ids_by_actor[actor_idx].append(("note", note_id))
                    note_create_payloads = await _assert_broadcast_collab_create(
                        stores, obj_type="note", obj_id=note_id
                    )
                    assert len(note_create_payloads) == len(stores)
                    assert all(
                        payload.get("text") == large_note_text
                        for payload in note_create_payloads
                    )
                    print(
                        "large note_create broadcast:",
                        {
                            "actor_idx": actor_idx,
                            "msg_idx": msg_idx,
                            "note_id": note_id,
                            "payloads": len(note_create_payloads),
                            "text_len": len(large_note_text),
                        },
                    )
                    updated_note = await actor_client.collab.note_update(
                        note_id,
                        name=_unique("note_upd"),
                        text="multi instance note updated",
                        tags=["multi-instance", "updated"],
                    )
                    print("note_update response:", updated_note)
                    await _assert_broadcast_collab_event(
                        stores,
                        WSMessageType.COLLAB_UPDATE,
                        obj_type="note",
                        obj_id=note_id,
                    )
                    deleted_note = await actor_client.collab.note_delete(note_id)
                    print("note_delete response:", deleted_note)
                    collab_ids_by_actor[actor_idx].remove(("note", note_id))
                    await _assert_broadcast_collab_event(
                        stores,
                        WSMessageType.COLLAB_DELETE,
                        obj_type="note",
                        obj_id=note_id,
                    )
                    msg_idx += 1

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
                updated_link = await actor_client.collab.link_update(
                    link_id,
                    name=_unique("link_upd"),
                    description="multi instance link updated",
                    tags=["multi-instance", "updated"],
                    doc_ids=[f"doc_to_{actor_idx}_c"],
                )
                print("link_update response:", updated_link)
                await _assert_broadcast_collab_event(
                    stores,
                    WSMessageType.COLLAB_UPDATE,
                    obj_type="link",
                    obj_id=link_id,
                )
                deleted_link = await actor_client.collab.link_delete(link_id)
                print("link_delete response:", deleted_link)
                collab_ids_by_actor[actor_idx].remove(("link", link_id))
                await _assert_broadcast_collab_event(
                    stores,
                    WSMessageType.COLLAB_DELETE,
                    obj_type="link",
                    obj_id=link_id,
                )

                highlight = await actor_client.collab.highlight_create(
                    operation_id=shared_operation_id,
                    time_range=[1_000_000_000 + actor_idx, 1_000_000_100 + actor_idx],
                    name=_unique("highlight"),
                    description="multi instance highlight",
                    tags=["multi-instance"],
                )
                highlight_id = _extract_id(highlight)
                assert highlight_id, "failed to create highlight id"
                collab_ids_by_actor[actor_idx].append(("highlight", highlight_id))
                await _assert_broadcast_collab_create(
                    stores, obj_type="highlight", obj_id=highlight_id
                )
                updated_highlight = await actor_client.collab.highlight_update(
                    highlight_id,
                    name=_unique("highlight_upd"),
                    description="multi instance highlight updated",
                    tags=["multi-instance", "updated"],
                    time_range=[
                        1_000_000_200 + actor_idx,
                        1_000_000_300 + actor_idx,
                    ],
                )
                print("highlight_update response:", updated_highlight)
                await _assert_broadcast_collab_event(
                    stores,
                    WSMessageType.COLLAB_UPDATE,
                    obj_type="highlight",
                    obj_id=highlight_id,
                )
                deleted_highlight = await actor_client.collab.highlight_delete(
                    highlight_id
                )
                print("highlight_delete response:", deleted_highlight)
                collab_ids_by_actor[actor_idx].remove(("highlight", highlight_id))
                await _assert_broadcast_collab_event(
                    stores,
                    WSMessageType.COLLAB_DELETE,
                    obj_type="highlight",
                    obj_id=highlight_id,
                )

            if is_multi_instance:
                await asyncio.gather(
                    *(
                        _run_actor_collab_lifecycle(actor_idx, actor_client)
                        for actor_idx, (_, actor_client) in enumerate(actors)
                    )
                )
            else:
                for actor_idx, (_, actor_client) in enumerate(actors):
                    await _run_actor_collab_lifecycle(actor_idx, actor_client)

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
                    elif collab_type == "highlight":
                        await client.collab.highlight_delete(collab_id)
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

        try:
            async with GulpClient(admin_base_url) as cleanup:
                await cleanup.auth.login("admin", "admin")
                for user_id in temp_users:
                    try:
                        await cleanup.users.delete(user_id)
                    except Exception:
                        pass
        except Exception:
            pass
