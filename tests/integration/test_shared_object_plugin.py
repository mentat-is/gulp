"""Integration tests for shared_object extension plugin APIs and websocket broadcasts."""

import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import Any

import pytest

from gulp_sdk import GulpClient, WSMessageType
from gulp_sdk.exceptions import NotFoundError

TEST_OPERATION_ID = "test_operation"

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

# passwords for the ephemeral test users created inside the broadcast test
_TEST_USER_PASS = "Test@12345"


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def _login_and_connect(url: str, user: str, password: str) -> GulpClient:
    """Return an *unmanaged* GulpClient that is logged-in and WS-connected."""
    client = GulpClient(url)
    await client.__aenter__()
    await client.auth.login(user, password)
    await client.ensure_websocket()
    return client


async def _arm_listeners(
    clients: list[GulpClient],
    message_type: WSMessageType,
    predicate,
) -> tuple[list[asyncio.Event], list[list[Any]]]:
    """Register a matching handler on every client; return (events, buckets)."""
    events: list[asyncio.Event] = [asyncio.Event() for _ in clients]
    buckets: list[list[Any]] = [[] for _ in clients]

    for i, client in enumerate(clients):
        evt = events[i]
        bkt = buckets[i]

        async def _handler(msg, _evt=evt, _bkt=bkt):
            if predicate(msg):
                _bkt.append(msg)
                _evt.set()

        await client.register_ws_message_handler(message_type, _handler)
        # store handler ref so we can deregister later
        client.__dict__.setdefault("_test_handlers", {})[message_type] = _handler

    return events, buckets


async def _wait_all_events(
    events: list[asyncio.Event],
    action,
    timeout: float = 15.0,
) -> None:
    """Fire *action* then wait until every event is set (or timeout)."""
    await action()
    remaining = [asyncio.wait_for(e.wait(), timeout=timeout) for e in events]
    await asyncio.gather(*remaining)


def _disarm_listeners(
    clients: list[GulpClient],
    message_type: WSMessageType,
) -> None:
    for client in clients:
        handler = client.__dict__.get("_test_handlers", {}).pop(message_type, None)
        if handler is not None:
            client.unregister_ws_message_handler(message_type, handler)


# ──────────────────────────────────────────────────────────────────────────────
# REST API tests  (single client, no WS assertions)
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_shared_object_crud(gulp_base_url, gulp_test_user, gulp_test_password):
    """Exercise the full REST CRUD lifecycle of shared_object."""
    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        plugins = await client.plugins.list()
        if not any(
            p.get("filename") == "shared_object.py"
            or p.get("display_name") == "Shared objects"
            for p in plugins
        ):
            pytest.skip("shared_object extension plugin not available on this server")

        create_name = _unique("crud_obj")
        create_obj = {"query": {"field": "test"}, "limit": 10}
        created_obj_id = None

        try:
            # ── create ────────────────────────────────────────────────────────
            created = (
                await client._request(
                    "POST",
                    "/shared_object_create",
                    params={
                        "name": create_name,
                        "operation_id": TEST_OPERATION_ID,
                        "obj_type": "query",
                    },
                    json={"obj": create_obj},
                )
            ).get("data", {})
            created_obj_id = created.get("id")
            assert created_obj_id, "create returned no id"
            assert created.get("type") == "shared_object"
            assert created.get("operation_id") == TEST_OPERATION_ID
            assert created.get("obj_type") == "query"
            assert created.get("obj") == create_obj

            # ── get by id ─────────────────────────────────────────────────────
            fetched = (
                await client._request(
                    "GET",
                    "/shared_object_get_by_id",
                    params={"obj_id": created_obj_id},
                )
            ).get("data", {})
            assert fetched.get("id") == created_obj_id
            assert fetched.get("obj_type") == "query"

            # ── list with filter ──────────────────────────────────────────────
            listed = (
                await client._request(
                    "POST",
                    "/shared_object_list",
                    json={"ids": [created_obj_id]},
                )
            ).get("data", [])
            assert any(o.get("id") == created_obj_id for o in listed)

            # ── update ────────────────────────────────────────────────────────
            updated_obj = {"query": {"field": "updated"}, "limit": 99}
            updated = (
                await client._request(
                    "PATCH",
                    "/shared_object_update",
                    params={
                        "obj_id": created_obj_id,
                        "name": f"{create_name}_updated",
                    },
                    json={"obj": updated_obj},
                )
            ).get("data", {})
            assert updated.get("id") == created_obj_id
            assert updated.get("name") == f"{create_name}_updated"
            assert updated.get("obj") == updated_obj

            # ── delete ────────────────────────────────────────────────────────
            await client._request(
                "DELETE",
                "/shared_object_delete",
                params={"obj_id": created_obj_id, "ws_id": client.ws_id},
            )
            created_obj_id = None

            # ── confirm gone ─────────────────────────────────────────────────
            with pytest.raises(NotFoundError):
                await client._request(
                    "GET",
                    "/shared_object_get_by_id",
                    params={"obj_id": created.get("id")},
                )
        finally:
            if created_obj_id:
                try:
                    await client._request(
                        "DELETE",
                        "/shared_object_delete",
                        params={"obj_id": created_obj_id, "ws_id": client.ws_id},
                    )
                except Exception:
                    pass


# ──────────────────────────────────────────────────────────────────────────────
# Multi-client broadcast tests
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_shared_object_broadcast_multi_client(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """
    Verify that shared_object create and delete events are broadcast to
    **all** connected websocket clients, regardless of who triggered them.

    Three independent clients are used:
      - actor      (admin)            — performs the REST mutations
      - listener_1 (ephemeral user)   — read+edit permission, distinct session
      - listener_2 (ephemeral user)   — read+edit permission, distinct session

    Both listeners must independently receive COLLAB_CREATE and COLLAB_DELETE
    before the test passes.  The two listener accounts are created at the start
    and deleted in the finally block to keep the DB clean.
    """
    # ── bootstrap: check plugin availability and create ephemeral users ───────
    lu1 = _unique("tl1")
    lu2 = _unique("tl2")

    async with GulpClient(gulp_base_url) as bootstrap:
        await bootstrap.auth.login(gulp_test_user, gulp_test_password)
        plugins = await bootstrap.plugins.list()
        if not any(
            p.get("filename") == "shared_object.py"
            or p.get("display_name") == "Shared objects"
            for p in plugins
        ):
            pytest.skip("shared_object extension plugin not available on this server")
        await bootstrap.users.create(lu1, _TEST_USER_PASS, permission=["read", "edit"])
        await bootstrap.users.create(lu2, _TEST_USER_PASS, permission=["read", "edit"])

    actor = await _login_and_connect(gulp_base_url, gulp_test_user, gulp_test_password)
    listener_1 = await _login_and_connect(gulp_base_url, lu1, _TEST_USER_PASS)
    listener_2 = await _login_and_connect(gulp_base_url, lu2, _TEST_USER_PASS)
    all_clients = [actor, listener_1, listener_2]
    listeners = [listener_1, listener_2]

    created_obj_id = None

    try:
        create_name = _unique("bc_obj")
        create_obj = {"payload": "broadcast_test", "ts": create_name}
        create_result: dict = {}

        # ── arm all listeners for COLLAB_CREATE ───────────────────────────────
        def _is_create(m: Any) -> bool:
            return (
                isinstance(m.data, dict)
                and isinstance(m.data.get("obj"), dict)
                and m.data["obj"].get("type") == "shared_object"
                and m.data["obj"].get("name") == create_name
            )

        create_events, create_buckets = await _arm_listeners(
            listeners, WSMessageType.COLLAB_CREATE, _is_create
        )

        async def _do_create():
            nonlocal create_result
            create_result = await actor._request(
                "POST",
                "/shared_object_create",
                params={
                    "name": create_name,
                    "operation_id": TEST_OPERATION_ID,
                    "obj_type": "dashboard",
                },
                json={"obj": create_obj},
            )

        await _wait_all_events(create_events, _do_create, timeout=15.0)
        _disarm_listeners(listeners, WSMessageType.COLLAB_CREATE)

        # ── assertions for create ─────────────────────────────────────────────
        created = create_result.get("data", {})
        created_obj_id = created.get("id")
        assert created_obj_id, "create returned no id"
        assert created.get("operation_id") == TEST_OPERATION_ID
        assert created.get("obj") == create_obj

        for idx, bucket in enumerate(create_buckets):
            assert (
                len(bucket) >= 1
            ), f"listener_{idx + 1} did NOT receive COLLAB_CREATE for shared_object"
            msg = bucket[0]
            assert (
                msg.data["obj"]["id"] == created_obj_id
            ), f"listener_{idx + 1} received wrong object id in COLLAB_CREATE"
            assert msg.data["obj"]["type"] == "shared_object"
            print(f"listener_{idx + 1} received COLLAB_CREATE with correct shared_object id: {msg.data}, obj: {msg.data['obj']}")
        # ── arm all listeners for COLLAB_UPDATE ───────────────────────────────
        updated_obj = {"payload": "updated_value", "ts": create_name}
        update_result: dict = {}

        def _is_update(m: Any) -> bool:
            return (
                isinstance(m.data, dict)
                and isinstance(m.data.get("obj"), dict)
                and m.data["obj"].get("type") == "shared_object"
                and m.data["obj"].get("id") == created_obj_id
            )

        update_events, update_buckets = await _arm_listeners(
            listeners, WSMessageType.COLLAB_UPDATE, _is_update
        )

        async def _do_update():
            nonlocal update_result
            update_result = await actor._request(
                "PATCH",
                "/shared_object_update",
                params={
                    "obj_id": created_obj_id,
                    "name": f"{create_name}_updated",
                },
                json={"obj": updated_obj},
            )

        await _wait_all_events(update_events, _do_update, timeout=15.0)
        _disarm_listeners(listeners, WSMessageType.COLLAB_UPDATE)

        # ── assertions for update ─────────────────────────────────────────────
        updated = update_result.get("data", {})
        assert updated.get("id") == created_obj_id
        assert updated.get("obj") == updated_obj

        for idx, bucket in enumerate(update_buckets):
            assert (
                len(bucket) >= 1
            ), f"listener_{idx + 1} did NOT receive COLLAB_UPDATE for shared_object"
            msg = bucket[0]
            assert (
                msg.data["obj"]["id"] == created_obj_id
            ), f"listener_{idx + 1} received wrong object id in COLLAB_UPDATE"
            assert msg.data["obj"]["type"] == "shared_object"
            assert msg.data["obj"]["obj"] == updated_obj
            print(f"listener_{idx + 1} received COLLAB_UPDATE with correct shared_object id: {msg.data}, obj: {msg.data['obj']}")

        # ── arm all listeners for COLLAB_DELETE ───────────────────────────────
        _oid = created_obj_id  # capture for lambda closure

        def _is_delete(m: Any) -> bool:
            return (
                isinstance(m.data, dict)
                and m.data.get("type") == "shared_object"
                and m.data.get("id") == _oid
            )

        delete_events, delete_buckets = await _arm_listeners(
            listeners, WSMessageType.COLLAB_DELETE, _is_delete
        )

        async def _do_delete():
            await actor._request(
                "DELETE",
                "/shared_object_delete",
                params={"obj_id": _oid, "ws_id": actor.ws_id},
            )

        await _wait_all_events(delete_events, _do_delete, timeout=15.0)
        _disarm_listeners(listeners, WSMessageType.COLLAB_DELETE)
        created_obj_id = None

        # ── assertions for delete ─────────────────────────────────────────────
        for idx, bucket in enumerate(delete_buckets):
            assert (
                len(bucket) >= 1
            ), f"listener_{idx + 1} did NOT receive COLLAB_DELETE for shared_object"
            msg = bucket[0]
            assert (
                msg.data.get("id") == _oid
            ), f"listener_{idx + 1} received wrong object id in COLLAB_DELETE"
            assert msg.data.get("type") == "shared_object"
            print(f"listener_{idx + 1} received COLLAB_DELETE with correct shared_object id: {msg.data}")

    finally:
        if created_obj_id:
            try:
                await actor._request(
                    "DELETE",
                    "/shared_object_delete",
                    params={"obj_id": created_obj_id, "ws_id": actor.ws_id},
                )
            except Exception:
                pass
        for client in all_clients:
            try:
                await client.__aexit__(None, None, None)
            except Exception:
                pass
        # delete the ephemeral test users
        async with GulpClient(gulp_base_url) as admin_cleanup:
            await admin_cleanup.auth.login(gulp_test_user, gulp_test_password)
            for uid in (lu1, lu2):
                try:
                    await admin_cleanup.users.delete(uid)
                except Exception:
                    pass
