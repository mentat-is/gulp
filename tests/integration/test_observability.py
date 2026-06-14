"""Integration tests for the Prometheus observability endpoint."""

import asyncio

import httpx
import pytest


async def _fetch_metrics(base_url: str) -> str:
    """Fetch the Prometheus exposition text from a live gULP backend."""
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as client:
        response = await client.get("/metrics")
        print("metrics response:", response.status_code, response.text[:1000])
        if response.status_code == 404:
            pytest.skip("Prometheus endpoint is disabled in gulp_cfg.json")
        response.raise_for_status()
        return response.text


@pytest.mark.integration
async def test_prometheus_websocket_metrics_are_exposed(
    gulp_base_url,
    gulp_test_user,
    gulp_test_password,
):
    """Opening a real websocket should expose per-type websocket metrics."""
    from gulp_sdk import GulpClient

    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)
        ws = await client.ensure_websocket()
        print("observability websocket:", {"ws_id": ws.ws_id, "connected": ws.is_connected})

        deadline = asyncio.get_running_loop().time() + 25.0
        metrics_text = ""
        while asyncio.get_running_loop().time() < deadline:
            metrics_text = await _fetch_metrics(gulp_base_url)
            if (
                "gulp_ws_connected_sockets_by_type" in metrics_text
                and "gulp_ws_queue_high_watermark" in metrics_text
                and "gulp_redis_publish_by_route_total" in metrics_text
                and "gulp_redis_publish_bytes_by_route_total" in metrics_text
            ):
                break
            await asyncio.sleep(1.0)

        print("websocket metrics excerpt:", metrics_text[:2000])
        assert "gulp_ws_connected_sockets_by_type" in metrics_text
        assert 'socket_type="default"' in metrics_text
        assert "gulp_ws_queue_high_watermark" in metrics_text
        assert "gulp_redis_publish_by_route_total" in metrics_text
        assert 'route_target="broadcast"' in metrics_text
        assert 'channel_scope="main"' in metrics_text
        assert "gulp_redis_publish_bytes_by_route_total" in metrics_text
