"""Integration coverage for raw websocket ingest disconnect terminal state."""

import asyncio
import json
import uuid

import pytest
import websockets


def _ws_url(base_url: str, path: str) -> str:
    """Build a websocket URL from an HTTP base URL."""
    return f"{base_url.replace('http://', 'ws://').replace('https://', 'wss://')}{path}"


async def _wait_request_status(client, req_id: str, timeout: float = 30.0) -> dict:
    """Poll request stats until the request becomes visible and terminal."""
    deadline = asyncio.get_running_loop().time() + timeout
    last_error: Exception | None = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            stats = await client.plugins.request_get(req_id)
            status = str(stats.get("status", "")).lower()
            if status in {"done", "failed", "canceled"}:
                return stats
        except Exception as exc:
            last_error = exc
        await asyncio.sleep(0.25)
    raise TimeoutError(
        f"timed out waiting for request {req_id} to become terminal; last_error={last_error}"
    )


@pytest.mark.integration
async def test_ws_ingest_raw_disconnect_before_last_marks_request_failed(
    gulp_base_url,
    gulp_test_user,
    gulp_test_password,
):
    """Closing /ws_ingest_raw before last=True leaves failed request stats."""
    from gulp_sdk import GulpClient

    suffix = uuid.uuid4().hex[:8]
    req_id = f"req_ws_disconnect_{suffix}"
    ws_id = f"ws_disconnect_{suffix}"

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        op = await client.operations.create(f"ws_disconnect_{suffix}")
        try:
            ws = await websockets.connect(
                _ws_url(gulp_base_url, "/ws_ingest_raw"),
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                max_size=None,
            )
            try:
                await ws.send(
                    json.dumps(
                        {
                            "token": client.token,
                            "ws_id": ws_id,
                            "req_id": f"auth_{req_id}",
                        }
                    )
                )
                ack = json.loads(await asyncio.wait_for(ws.recv(), timeout=10.0))
                print("ws ingest disconnect ack:", ack)
                assert ack.get("type") == "ws_connected"

                await ws.send(
                    json.dumps(
                        {
                            "index": op.index,
                            "operation_id": op.id,
                            "ws_id": ws_id,
                            "req_id": req_id,
                            "plugin": "raw",
                            "last": False,
                        }
                    )
                )
                await ws.send(
                    json.dumps(
                        [
                            {
                                "@timestamp": "2024-01-01T00:00:00.000Z",
                                "event": {"original": "disconnect-before-last"},
                            }
                        ]
                    ).encode()
                )
            finally:
                await ws.close()

            stats = await _wait_request_status(client, req_id)
            print("ws ingest disconnect stats:", stats)
            assert str(stats.get("status", "")).lower() == "failed"
            assert any(
                "disconnected before final packet" in str(error)
                for error in (stats.get("errors") or [])
            )
        finally:
            await client.operations.delete(op.id)
