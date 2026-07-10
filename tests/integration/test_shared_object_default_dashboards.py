"""Integration tests for built-in shared dashboard seeding."""

import pytest

from gulp_sdk import GulpClient

DEFAULT_DASHBOARDS = {
    "dash_log_source_distribution": "vertical_chart",
    "dash_global_event_rate": "line_chart",
}


@pytest.mark.integration
async def test_default_shared_dashboards_are_seeded(
    gulp_base_url, gulp_test_user, gulp_test_password
):
    """Verify default dashboard shared objects exist after server startup."""
    async with GulpClient(gulp_base_url) as client:
        await client.auth.login(gulp_test_user, gulp_test_password)

        plugins = await client.plugins.list()
        if not any(
            p.get("filename") == "shared_object.py"
            or p.get("filename") == "__shared_object.py"
            or p.get("display_name") == "Shared objects"
            for p in plugins
        ):
            pytest.skip("shared_object extension plugin not available on this server")

        for dashboard_id, dashboard_kind in DEFAULT_DASHBOARDS.items():
            fetched = (
                await client._request(
                    "GET",
                    "/shared_object_get_by_id",
                    params={"obj_id": dashboard_id},
                )
            ).get("data", {})
            print(f"default shared dashboard {dashboard_id}:", fetched)

            assert fetched.get("id") == dashboard_id
            assert fetched.get("type") == "shared_object"
            assert fetched.get("obj_type") == "dashboard"
            assert fetched.get("obj", {}).get("dashboard") == dashboard_kind
