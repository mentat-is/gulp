"""Live coverage for the read-only request observers registered by extension plugins."""

import uuid

import httpx
import pytest

STATUS = "/request_observer_probe_status"
CONFIGURE = "/request_observer_probe_configure"


def _seen(status: dict, path: str) -> list[dict]:
    return [entry for entry in status["seen"] if entry["path"] == path]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_registered_observer_is_told_of_every_request(gulp_base_url):
    """An observer registered after startup sees core routes and their outcome, until removed."""
    marker = uuid.uuid4().hex[:8]
    missing_path = f"/request_observer_probe_missing_{marker}"
    unknown_user = f"request_observer_probe_{marker}"
    async with httpx.AsyncClient(base_url=gulp_base_url, timeout=30.0) as http:
        probe = await http.get(STATUS)
        if probe.status_code == 404:
            pytest.skip(
                "requires tests/fixtures/plugins/request_observer_probe.py "
                "installed as an extension (see docs/testing.md)"
            )
        probe.raise_for_status()

        try:
            configured = await http.post(
                CONFIGURE, params={"enabled": True, "reset": True}
            )
            configured.raise_for_status()
            assert configured.json()["enabled"] is True

            # a route the observer is not interested in, a core route answering with an
            # error, no route at all, and a login whose bodies the observer asked for
            ignored = await http.get("/docs")
            denied = await http.get("/user_list", headers={"token": "not-a-token"})
            missing = await http.get(missing_path)
            login = await http.post(
                "/login", json={"user_id": unknown_user, "password": "not-the-password"}
            )
            assert (ignored.status_code, denied.status_code, missing.status_code) == (
                200,
                401,
                404,
            )
            assert login.status_code == 404

            status = (await http.get(STATUS)).json()
            assert _seen(status, "/docs") == []
            assert _seen(status, "/user_list") == [
                {
                    "method": "GET",
                    "path": "/user_list",
                    "route": "/user_list",
                    "status": 401,
                    "has_token": True,
                    "client_known": True,
                    "response_head": "",
                }
            ]
            assert _seen(status, missing_path) == [
                {
                    "method": "GET",
                    "path": missing_path,
                    "route": None,
                    "status": 404,
                    "has_token": False,
                    "client_known": True,
                    "response_head": "",
                }
            ]
            assert _seen(status, "/login") == [
                {
                    "method": "POST",
                    "path": "/login",
                    "route": "/login",
                    "status": 404,
                    "has_token": False,
                    "client_known": True,
                    # no more of the response than the observer asked for
                    "response_head": login.text[:16],
                    "login_user_id": unknown_user,
                }
            ]
            # observing changed nothing of what the client got
            assert login.json()["status"] == "error"

            # registering it again changes nothing: still one record per request
            again = await http.post(CONFIGURE, params={"enabled": True, "reset": True})
            again.raise_for_status()
            await http.get(missing_path)
            status = (await http.get(STATUS)).json()
            assert len(_seen(status, missing_path)) == 1
        finally:
            removed = await http.post(
                CONFIGURE, params={"enabled": False, "reset": True}
            )
        removed.raise_for_status()

        # removed: nothing is recorded any more. the request that removed the observer
        # had already started with it, so it is the last one recorded
        await http.get(missing_path)
        status = (await http.get(STATUS)).json()
        assert status["enabled"] is False
        assert [entry["path"] for entry in status["seen"]] == [CONFIGURE]
