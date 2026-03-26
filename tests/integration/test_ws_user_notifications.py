"""Integration tests for websocket user/session notifications."""

import asyncio

import pytest

from gulp_sdk import GulpClient, WSMessageType


async def _wait_for_event(
    client: GulpClient,
    message_type: WSMessageType,
    predicate,
    action,
    timeout: float = 15.0,
):
    event = asyncio.Event()
    received = []

    async def _handler(msg):
        if predicate(msg):
            received.append(msg)
            event.set()

    await client.register_ws_message_handler(message_type, _handler)
    try:
        await action()
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return received[0]
    finally:
        client.unregister_ws_message_handler(message_type, _handler)


@pytest.mark.integration
async def test_ws_connected_acknowledgment(gulp_base_url, gulp_test_user, gulp_test_password):
    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        ws = await client.ensure_websocket()

        assert ws is not None
        assert ws.is_connected
        assert ws.ws_id


@pytest.mark.integration
async def test_ws_user_logout_notification_on_same_socket(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        await client.ensure_websocket()

        msg = await _wait_for_event(
            client,
            WSMessageType.USER_LOGOUT,
            lambda m: isinstance(m.data, dict)
            and m.data.get("user_id") == gulp_test_user
            and m.data.get("login") is False,
            client.auth.logout,
        )
        assert msg.type == WSMessageType.USER_LOGOUT.value


@pytest.mark.integration
async def test_ws_user_logout_notification_other_user_broadcast(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    async with GulpClient(gulp_base_url) as listener:
        await listener.auth.login(gulp_test_user, gulp_test_password)
        await listener.ensure_websocket()

        seen = []

        async def _on_logout(msg):
            if isinstance(msg.data, dict) and msg.data.get("user_id") == "guest":
                seen.append(msg)

        await listener.register_ws_message_handler(WSMessageType.USER_LOGOUT, _on_logout)
        try:
            async with GulpClient(gulp_base_url) as actor:
                await actor.auth.login("guest", "guest")
                await actor.ensure_websocket()
                await actor.auth.logout()

            # USER_LOGOUT is observed on other connected default websockets.
            await asyncio.sleep(1.0)
            assert len(seen) >= 1
        finally:
            listener.unregister_ws_message_handler(WSMessageType.USER_LOGOUT, _on_logout)
