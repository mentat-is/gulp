"""Live coverage for the ASGI middlewares registered by extension plugins."""

import uuid

import httpx
import pytest

PROBE_HEADER = "x-extension-middleware-probe"


def _seen(status: dict, path: str) -> list[dict]:
    return [entry for entry in status["seen"] if entry["path"] == path]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_registered_middleware_sees_every_request(gulp_base_url):
    """A middleware registered after startup wraps core and plugin routes, until removed."""
    missing_path = f"/extension_middleware_probe_missing_{uuid.uuid4().hex[:8]}"
    async with httpx.AsyncClient(base_url=gulp_base_url, timeout=30.0) as http:
        probe = await http.get("/extension_middleware_probe_status")
        if probe.status_code == 404:
            pytest.skip(
                "requires tests/fixtures/plugins/extension_middleware_probe.py "
                "installed as an extension (see docs/testing.md)"
            )
        probe.raise_for_status()

        try:
            # not registered yet: requests are served untouched
            before = await http.get("/docs")
            assert before.status_code == 200
            assert PROBE_HEADER not in before.headers

            configured = await http.post(
                "/extension_middleware_probe_configure",
                params={"enabled": True, "reset": True},
            )
            configured.raise_for_status()
            assert configured.json()["enabled"] is True

            # a core route, a core route answering with an error, and no route at all
            ok = await http.get("/docs")
            denied = await http.get("/user_list", headers={"token": "not-a-token"})
            missing = await http.get(missing_path)
            assert (ok.status_code, denied.status_code, missing.status_code) == (
                200,
                401,
                404,
            )
            for response in (ok, denied, missing):
                assert response.headers[PROBE_HEADER] == "1"

            status = (await http.get("/extension_middleware_probe_status")).json()
            assert _seen(status, "/docs") == [
                {"method": "GET", "path": "/docs", "status": 200}
            ]
            assert _seen(status, "/user_list") == [
                {"method": "GET", "path": "/user_list", "status": 401}
            ]
            assert _seen(status, missing_path) == [
                {"method": "GET", "path": missing_path, "status": 404}
            ]

            # registering it again changes nothing: still one record per request
            again = await http.post(
                "/extension_middleware_probe_configure",
                params={"enabled": True, "reset": True},
            )
            again.raise_for_status()
            await http.get("/docs")
            status = (await http.get("/extension_middleware_probe_status")).json()
            assert len(_seen(status, "/docs")) == 1
        finally:
            removed = await http.post(
                "/extension_middleware_probe_configure",
                params={"enabled": False, "reset": True},
            )
        removed.raise_for_status()

        # removed: requests are served untouched again and are not recorded. the request
        # that removed the middleware was already being served through it, so it is the
        # last one recorded
        after = await http.get("/docs")
        assert after.status_code == 200
        assert PROBE_HEADER not in after.headers
        status = (await http.get("/extension_middleware_probe_status")).json()
        assert status == {
            "enabled": False,
            "seen": [
                {
                    "method": "POST",
                    "path": "/extension_middleware_probe_configure",
                    "status": 200,
                }
            ],
        }
