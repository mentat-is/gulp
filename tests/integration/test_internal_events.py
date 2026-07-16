"""Live worker-to-main integration coverage for the unified event bus."""

import asyncio
import os
import uuid
from collections import Counter

import httpx
import pytest


async def _wait_for_probe(
    http: httpx.AsyncClient, req_id: str, timeout: float = 30.0
) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    last: dict = {}
    while asyncio.get_running_loop().time() < deadline:
        response = await http.get("/internal_events_probe_status")
        response.raise_for_status()
        last = response.json()
        if req_id in last["pre_req_ids"] and req_id in last["post_req_ids"]:
            return last
        await asyncio.sleep(0.1)
    raise AssertionError(f"probe did not observe pre/post events: {last}")


async def _query_docs(client, operation_id: str) -> list[dict]:
    result = await client.queries.query_raw(
        operation_id=operation_id,
        q={"query": {"match_all": {}}},
        q_options={"preview_mode": True, "limit": 50, "name": "event_probe"},
    )
    return (result.get("data") or {}).get("docs") or []


async def _wait_for_broadcast(
    http: httpx.AsyncClient, marker: str, timeout: float = 30.0
) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    last: dict = {}
    while asyncio.get_running_loop().time() < deadline:
        response = await http.get("/internal_events_probe_status")
        response.raise_for_status()
        last = response.json()
        matching = [
            event for event in last["broadcast_events"] if event["marker"] == marker
        ]
        if len(matching) == 1:
            return matching[0]
        if len(matching) > 1:
            raise AssertionError(f"broadcast delivered more than once: {last}")
        await asyncio.sleep(0.1)
    raise AssertionError(f"broadcast was not delivered: {last}")


async def _wait_for_stress_events(
    http: httpx.AsyncClient,
    prefix: str,
    count: int,
    timeout: float = 180.0,
) -> list[dict]:
    deadline = asyncio.get_running_loop().time() + timeout
    matching: list[dict] = []
    while asyncio.get_running_loop().time() < deadline:
        response = await http.get("/internal_events_probe_status")
        response.raise_for_status()
        matching = [
            event
            for event in response.json()["broadcast_events"]
            if event["marker"].startswith(prefix)
        ]
        if len(matching) >= count:
            return matching
        await asyncio.sleep(0.1)
    raise AssertionError(
        f"expected {count} events with prefix {prefix}, observed {len(matching)}"
    )


def _assert_exactly_once(events_seen: list[dict], prefix: str, count: int) -> dict:
    expected = {f"{prefix}{index}" for index in range(count)}
    counts = Counter(event["marker"] for event in events_seen)
    assert set(counts) == expected
    assert set(counts.values()) == {1}
    return {event["marker"]: event["event_id"] for event in events_seen}


async def _emit_stress_batch(
    http: httpx.AsyncClient,
    prefix: str,
    count: int,
    *,
    wait: bool,
    broadcast: bool,
    large: bool,
) -> list[dict]:
    async def emit_one(index: int) -> dict:
        response = await http.post(
            "/internal_events_probe_broadcast",
            params={
                "marker": f"{prefix}{index}",
                "wait": wait,
                "broadcast": broadcast,
                "large": large,
            },
        )
        response.raise_for_status()
        return response.json()

    return await asyncio.gather(*(emit_one(index) for index in range(count)))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_worker_pre_post_and_no_listener_bypass(
    gulp_test_user, gulp_test_password
):
    """A real worker transforms pre data, notifies post, then bypasses both."""
    from gulp_sdk import GulpClient

    base_url = os.getenv("GULP_INTERNAL_EVENTS_BASE_URL", "http://localhost:8100")
    marker = uuid.uuid4().hex[:8]
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as http:
        probe = await http.get("/internal_events_probe_status")
        assert probe.status_code == 200, (
            "start a secondary server on port >=8100 with "
            "tests/fixtures/plugins/internal_events_probe.py installed as an extension"
        )
        configured = await http.post(
            "/internal_events_probe_configure", params={"enabled": True, "reset": True}
        )
        configured.raise_for_status()
        configured_data = configured.json()
        print("internal event probe enabled:", configured_data)
        assert configured_data["mask"] == 3

        async with GulpClient(base_url) as client:
            await client.auth.login(gulp_test_user, gulp_test_password)
            operation = await client.operations.create(f"event_probe_{marker}")
            try:
                first_req_id = f"event_probe_enabled_{marker}"
                first = await client.ingest.raw(
                    operation_id=operation.id,
                    plugin_name="raw",
                    data=[
                        {
                            "gulp.context_id": f"context_{marker}",
                            "gulp.source_id": f"source_enabled_{marker}",
                            "event.original": "enabled",
                        }
                    ],
                    params={"last": True, "req_id": first_req_id},
                    wait=True,
                    wait_for_worker=True,
                    timeout=300,
                )
                print("enabled event ingest response:", first.model_dump())
                assert str(first.status).lower() == "success"

                status = await _wait_for_probe(http, first_req_id)
                print("internal event probe delivery:", status)
                docs = await _query_docs(client, operation.id)
                assert any(
                    document.get("event.internal_event_probe") == first_req_id
                    for document in docs
                )

                disabled = await http.post(
                    "/internal_events_probe_configure",
                    params={"enabled": False},
                )
                disabled.raise_for_status()
                disabled_data = disabled.json()
                print("internal event probe disabled:", disabled_data)
                assert disabled_data["mask"] == 0

                second_req_id = f"event_probe_disabled_{marker}"
                second = await client.ingest.raw(
                    operation_id=operation.id,
                    plugin_name="raw",
                    data=[
                        {
                            "gulp.context_id": f"context_{marker}",
                            "gulp.source_id": f"source_disabled_{marker}",
                            "event.original": "disabled",
                        }
                    ],
                    params={"last": True, "req_id": second_req_id},
                    wait=True,
                    wait_for_worker=True,
                    timeout=300,
                )
                print("disabled event ingest response:", second.model_dump())
                assert str(second.status).lower() == "success"
                await asyncio.sleep(0.5)
                final_status = (await http.get("/internal_events_probe_status")).json()
                print("internal event probe final status:", final_status)
                assert second_req_id not in final_status["pre_req_ids"]
                assert second_req_id not in final_status["post_req_ids"]
            finally:
                await client.operations.delete(operation.id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_large_broadcast_reaches_origin_and_peer_once():
    """One large broadcast reaches both instances with the same event ID."""
    first_url = os.getenv("GULP_INTERNAL_EVENTS_BASE_URL", "http://localhost:8100")
    second_url = os.getenv("GULP_INTERNAL_EVENTS_SECOND_BASE_URL")
    if not second_url:
        pytest.skip("GULP_INTERNAL_EVENTS_SECOND_BASE_URL is not set")

    marker = f"broadcast_{uuid.uuid4().hex[:8]}"
    async with (
        httpx.AsyncClient(base_url=first_url, timeout=30.0) as first,
        httpx.AsyncClient(base_url=second_url, timeout=30.0) as second,
    ):
        for client in (first, second):
            response = await client.post(
                "/internal_events_probe_configure",
                params={"enabled": False, "reset": True},
            )
            response.raise_for_status()

        published = await first.post(
            "/internal_events_probe_broadcast",
            params={"marker": marker, "large": True},
        )
        published.raise_for_status()
        print("internal event broadcast response:", published.json())

        origin, peer = await asyncio.gather(
            _wait_for_broadcast(first, marker),
            _wait_for_broadcast(second, marker),
        )
        print("internal event broadcast deliveries:", origin, peer)
        assert origin["event_id"] == peer["event_id"]

        await asyncio.sleep(0.5)
        for client in (first, second):
            status = (await client.get("/internal_events_probe_status")).json()
            matching = [
                event
                for event in status["broadcast_events"]
                if event["marker"] == marker
            ]
            assert len(matching) == 1


async def _run_internal_event_stress(first_url: str, second_url: str) -> None:
    """Stress waited responses plus inline and pointer-backed broadcasts."""
    waited_count = max(1, int(os.getenv("GULP_INTERNAL_EVENTS_STRESS_WAITED", "200")))
    broadcast_count = max(
        1, int(os.getenv("GULP_INTERNAL_EVENTS_STRESS_BROADCAST", "500"))
    )
    large_count = max(1, int(os.getenv("GULP_INTERNAL_EVENTS_STRESS_LARGE", "20")))
    run_id = uuid.uuid4().hex[:8]
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=100)
    async with (
        httpx.AsyncClient(base_url=first_url, timeout=300.0, limits=limits) as first,
        httpx.AsyncClient(base_url=second_url, timeout=300.0, limits=limits) as second,
    ):
        for client in (first, second):
            response = await client.post(
                "/internal_events_probe_configure",
                params={"enabled": False, "reset": True},
            )
            response.raise_for_status()

        waited_prefix = f"waited_{run_id}_"
        started = asyncio.get_running_loop().time()
        waited_responses = await _emit_stress_batch(
            first,
            waited_prefix,
            waited_count,
            wait=True,
            broadcast=False,
            large=False,
        )
        waited_events = await _wait_for_stress_events(
            first, waited_prefix, waited_count
        )
        waited_elapsed = asyncio.get_running_loop().time() - started
        waited_ids = _assert_exactly_once(waited_events, waited_prefix, waited_count)
        assert {response["event_id"] for response in waited_responses} == set(
            waited_ids.values()
        )

        peer_status = (await second.get("/internal_events_probe_status")).json()
        assert not any(
            event["marker"].startswith(waited_prefix)
            for event in peer_status["broadcast_events"]
        )

        broadcast_prefix = f"broadcast_{run_id}_"
        started = asyncio.get_running_loop().time()
        await _emit_stress_batch(
            first,
            broadcast_prefix,
            broadcast_count,
            wait=False,
            broadcast=True,
            large=False,
        )
        origin_broadcasts, peer_broadcasts = await asyncio.gather(
            _wait_for_stress_events(first, broadcast_prefix, broadcast_count),
            _wait_for_stress_events(second, broadcast_prefix, broadcast_count),
        )
        broadcast_elapsed = asyncio.get_running_loop().time() - started
        origin_ids = _assert_exactly_once(
            origin_broadcasts, broadcast_prefix, broadcast_count
        )
        peer_ids = _assert_exactly_once(
            peer_broadcasts, broadcast_prefix, broadcast_count
        )
        assert origin_ids == peer_ids

        large_prefix = f"large_{run_id}_"
        started = asyncio.get_running_loop().time()
        await _emit_stress_batch(
            first,
            large_prefix,
            large_count,
            wait=False,
            broadcast=True,
            large=True,
        )
        origin_large, peer_large = await asyncio.gather(
            _wait_for_stress_events(first, large_prefix, large_count),
            _wait_for_stress_events(second, large_prefix, large_count),
        )
        large_elapsed = asyncio.get_running_loop().time() - started
        assert _assert_exactly_once(
            origin_large, large_prefix, large_count
        ) == _assert_exactly_once(peer_large, large_prefix, large_count)

        await asyncio.sleep(1.0)
        for client, expected in (
            (
                first,
                (
                    (waited_prefix, waited_count),
                    (broadcast_prefix, broadcast_count),
                    (large_prefix, large_count),
                ),
            ),
            (
                second,
                (
                    (broadcast_prefix, broadcast_count),
                    (large_prefix, large_count),
                ),
            ),
        ):
            status = (await client.get("/internal_events_probe_status")).json()
            for prefix, count in expected:
                _assert_exactly_once(
                    [
                        event
                        for event in status["broadcast_events"]
                        if event["marker"].startswith(prefix)
                    ],
                    prefix,
                    count,
                )

        print(
            "internal event stress results:",
            {
                "waited": {
                    "count": waited_count,
                    "seconds": round(waited_elapsed, 3),
                    "events_per_second": round(waited_count / waited_elapsed, 1),
                },
                "broadcast": {
                    "count": broadcast_count,
                    "seconds": round(broadcast_elapsed, 3),
                    "events_per_second": round(broadcast_count / broadcast_elapsed, 1),
                },
                "large_broadcast": {
                    "count": large_count,
                    "seconds": round(large_elapsed, 3),
                    "events_per_second": round(large_count / large_elapsed, 1),
                },
            },
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_internal_event_stress() -> None:
    """Run internal-event stress against two probe-enabled instances."""
    first_url = os.getenv("GULP_INTERNAL_EVENTS_BASE_URL", "http://localhost:8100")
    second_url = os.getenv("GULP_INTERNAL_EVENTS_SECOND_BASE_URL")
    if not second_url:
        pytest.skip("GULP_INTERNAL_EVENTS_SECOND_BASE_URL is not set")
    await _run_internal_event_stress(first_url, second_url)
