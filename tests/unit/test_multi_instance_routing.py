"""
Unit tests simulating multiple gulp instances processing pubsub messages through
GulpRedisBroker._handle_pubsub_message.

Exercises all three GulpMessageRoutingTarget values and the GulpWsData.internal flag:

  BROADCAST
    - origin instance: skips (locally routed already via _put_common)
    - non-origin instance: routes message to its local WS_DEFAULT sockets

  WORKER_TO_MAIN
    - internal=False: message routed to local websockets (main-process delivery)
    - internal=True, same origin, propagate=False: dispatches to GulpInternalEventsManager
    - internal=True, different origin, propagate=False: falls through to local WS routing
    - internal=True, different origin, propagate=True: dispatches on every instance
    - internal=True, same origin, response_key set: dispatches AND writes result back to Redis

  CLIENT_DATA (UI↔UI)
    - origin instance: skips
    - non-origin instance: routes to WS_CLIENT_DATA sockets

Stress scenarios
  BROADCAST high-volume burst: N concurrent foreign-origin messages, all must be routed
  BROADCAST origin-skip burst: N concurrent own-origin messages, none must be routed
  WORKER_TO_MAIN internal fan-out: N concurrent internal events, all dispatched exactly once
  Mixed routing under load: 4 routing flavours fired simultaneously, verify per-class counts
  Multi-instance competing: K instances each receive the same N broadcast messages, only
    non-origin instances route (expect (K-1)*N total route calls)
  Concurrent response_key writes: N parallel request/response internal events, each unique
    response key written exactly once
  Propagation correctness under load: propagated internal event dispatches on ALL instances
"""
import asyncio
import pytest
import orjson
from unittest.mock import AsyncMock, MagicMock, call

from gulp.api.ws_api import (
    GulpMessageRoutingTarget,
    GulpRedisBroker,
    GulpWsData,
)

# Two symbolic server identifiers used through baseline tests
INSTANCE_A = "gulp-instance-A"
INSTANCE_B = "gulp-instance-B"

# Stress test scale factors – keep fast but meaningful
_N_BURST = 200   # messages per burst test
_N_MIX = 50     # messages per flavour in the mixed-load test
_N_INST = 5     # number of simulated instances in the multi-instance test
_N_RESP = 80    # concurrent response-key writes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pubsub_message(
    route_target_type: str,
    origin_server_id: str,
    *,
    msg_type: str = "test_event",
    internal: bool = False,
    propagate_internal: bool = False,
    ws_id: str = None,
    response_key: str = None,
) -> dict:
    """Build a pubsub message dict as it arrives from the Redis subscriber loop.

    The broker pops ``route_target_type``, ``origin_server_id``, and
    ``__redis_channel__`` before validating the remainder into a GulpWsData.
    """
    msg: dict = {
        "@timestamp": 1_000_000,
        "type": msg_type,
        "internal": internal,
        "propagate_internal": propagate_internal,
        "route_target_type": route_target_type,
        "origin_server_id": origin_server_id,
        "__redis_channel__": GulpMessageRoutingTarget.BROADCAST.redis_channel_name(),
    }
    if ws_id is not None:
        msg["ws_id"] = ws_id
    if response_key is not None:
        msg["__response_key__"] = response_key
    return msg


def _fake_redis(server_id: str) -> MagicMock:
    """Return a minimal fake GulpRedis stub with the given server_id."""
    r = MagicMock()
    r.server_id = server_id
    return r


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_broker():
    """Ensure a fresh GulpRedisBroker singleton per test to avoid state leaks."""
    GulpRedisBroker._instance = None
    yield
    GulpRedisBroker._instance = None


# ---------------------------------------------------------------------------
# BROADCAST routing
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "our_server_id, expect_routed",
    [
        (INSTANCE_A, False),  # origin instance — already delivered locally in _put_common
        (INSTANCE_B, True),   # different instance — must route to its own WS_DEFAULT sockets
    ],
    ids=["broadcast_origin_skips", "broadcast_nonorigin_routes"],
)
async def test_broadcast_routing(monkeypatch, our_server_id, expect_routed):
    """BROADCAST: origin skips; every other instance routes to its local websockets."""
    from gulp.api import ws_api as ws_api_module

    route_ws = AsyncMock()
    mock_sockets = MagicMock()
    mock_sockets.route_message_to_local_websockets = route_ws

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: _fake_redis(our_server_id))
    monkeypatch.setattr(ws_api_module.GulpConnectedSockets, "get_instance", lambda: mock_sockets)

    broker = GulpRedisBroker.get_instance()
    msg = _make_pubsub_message(
        GulpMessageRoutingTarget.BROADCAST.value,
        origin_server_id=INSTANCE_A,
        msg_type="ingest_source_done",
    )
    await broker._handle_pubsub_message(msg)

    if expect_routed:
        route_ws.assert_awaited_once()
    else:
        route_ws.assert_not_awaited()


# ---------------------------------------------------------------------------
# WORKER_TO_MAIN routing
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_to_main_non_internal_routes_to_local_ws(monkeypatch):
    """WORKER_TO_MAIN + internal=False → routed to local WS_DEFAULT sockets (not plugin dispatch)."""
    from gulp.api import ws_api as ws_api_module

    route_ws = AsyncMock()
    mock_sockets = MagicMock()
    mock_sockets.route_message_to_local_websockets = route_ws

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: _fake_redis(INSTANCE_A))
    monkeypatch.setattr(ws_api_module.GulpConnectedSockets, "get_instance", lambda: mock_sockets)

    broker = GulpRedisBroker.get_instance()
    msg = _make_pubsub_message(
        GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
        origin_server_id=INSTANCE_A,
        internal=False,
    )
    await broker._handle_pubsub_message(msg)

    route_ws.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "our_server_id, propagate, expect_dispatched, expect_routed",
    [
        # same origin, no propagate → condition True → internal dispatch
        (INSTANCE_A, False, True, False),
        # different origin, no propagate → condition False → falls through to local WS routing
        (INSTANCE_B, False, False, True),
        # different origin, propagate=True → condition True → dispatches on every instance
        (INSTANCE_B, True, True, False),
    ],
    ids=[
        "internal_same_origin",
        "internal_diff_origin_no_propagate",
        "internal_diff_origin_propagate",
    ],
)
async def test_worker_to_main_internal_routing(
    monkeypatch, our_server_id, propagate, expect_dispatched, expect_routed
):
    """WORKER_TO_MAIN + internal=True: routing depends on origin_server_id and propagate_internal.

    Condition in _handle_pubsub_message:
        if wsd.internal and (wsd.propagate_internal or origin_server_id == redis_client.server_id)
    True  → dispatch_internal_event (plugin callbacks)
    False → route_message_to_local_websockets
    """
    from gulp.api import ws_api as ws_api_module
    import gulp.plugin as plugin_module

    route_ws = AsyncMock()
    mock_sockets = MagicMock()
    mock_sockets.route_message_to_local_websockets = route_ws

    dispatch = AsyncMock(return_value={})
    mock_mgr = MagicMock()
    mock_mgr.dispatch_internal_event = dispatch

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: _fake_redis(our_server_id))
    monkeypatch.setattr(ws_api_module.GulpConnectedSockets, "get_instance", lambda: mock_sockets)
    # patch at class level — the function does `from gulp.plugin import GulpInternalEventsManager`
    monkeypatch.setattr(
        plugin_module.GulpInternalEventsManager, "get_instance", staticmethod(lambda: mock_mgr)
    )

    broker = GulpRedisBroker.get_instance()
    msg = _make_pubsub_message(
        GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
        origin_server_id=INSTANCE_A,
        msg_type="my_plugin_event",
        internal=True,
        propagate_internal=propagate,
    )
    await broker._handle_pubsub_message(msg)

    if expect_dispatched:
        dispatch.assert_awaited_once()
        route_ws.assert_not_awaited()
    if expect_routed:
        route_ws.assert_awaited_once()
        dispatch.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_to_main_internal_response_key_writes_back(monkeypatch):
    """WORKER_TO_MAIN + internal=True + response_key → dispatches AND writes result to Redis."""
    from gulp.api import ws_api as ws_api_module
    import gulp.plugin as plugin_module

    dispatch_result = {"handled": True, "plugin": "my_plugin"}
    dispatch = AsyncMock(return_value=dispatch_result)
    mock_mgr = MagicMock()
    mock_mgr.dispatch_internal_event = dispatch

    redis_set = AsyncMock(return_value=True)
    inner_redis = MagicMock()
    inner_redis.set = redis_set

    fake_redis = _fake_redis(INSTANCE_A)
    fake_redis._redis = inner_redis

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        plugin_module.GulpInternalEventsManager, "get_instance", staticmethod(lambda: mock_mgr)
    )

    response_key = "gulp:internal:wait:test-key-123"

    broker = GulpRedisBroker.get_instance()
    msg = _make_pubsub_message(
        GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
        origin_server_id=INSTANCE_A,
        msg_type="my_internal_event",
        internal=True,
        propagate_internal=False,
        response_key=response_key,
    )
    await broker._handle_pubsub_message(msg)

    # dispatch must have been called
    dispatch.assert_awaited_once()

    # result must have been written back to the response key
    redis_set.assert_awaited_once()
    call_args = redis_set.call_args
    assert call_args.args[0] == response_key
    written = orjson.loads(call_args.args[1])
    assert written == dispatch_result


# ---------------------------------------------------------------------------
# CLIENT_DATA routing (UI ↔ UI)
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "our_server_id, expect_routed",
    [
        (INSTANCE_A, False),  # origin instance — skip
        (INSTANCE_B, True),   # different instance — route to WS_CLIENT_DATA sockets
    ],
    ids=["client_data_origin_skips", "client_data_nonorigin_routes"],
)
async def test_client_data_routing(monkeypatch, our_server_id, expect_routed):
    """CLIENT_DATA: origin skips; every other instance routes to its WS_CLIENT_DATA sockets."""
    from gulp.api import ws_api as ws_api_module

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: _fake_redis(our_server_id))

    broker = GulpRedisBroker.get_instance()

    route_client = AsyncMock()
    monkeypatch.setattr(broker, "_route_message_to_local_client_data_websockets", route_client)

    msg = _make_pubsub_message(
        GulpMessageRoutingTarget.CLIENT_DATA.value,
        origin_server_id=INSTANCE_A,
        msg_type="client_data",
    )
    await broker._handle_pubsub_message(msg)

    if expect_routed:
        route_client.assert_awaited_once()
    else:
        route_client.assert_not_awaited()


# ===========================================================================
# ███████╗████████╗██████╗ ███████╗███████╗███████╗    ████████╗███████╗███████╗████████╗███████╗
# ██╔════╝╚══██╔══╝██╔══██╗██╔════╝██╔════╝██╔════╝    ╚══██╔══╝██╔════╝██╔════╝╚══██╔══╝██╔════╝
# ███████╗   ██║   ██████╔╝█████╗  ███████╗███████╗       ██║   █████╗  ███████╗   ██║   ███████╗
# ╚════██║   ██║   ██╔══██╗██╔══╝  ╚════██║╚════██║       ██║   ██╔══╝  ╚════██║   ██║   ╚════██║
# ███████║   ██║   ██║  ██║███████╗███████║███████║       ██║   ███████╗███████║   ██║   ███████║
# ===========================================================================
#
# Each test fires many concurrent _handle_pubsub_message calls and asserts exact
# delivery counts with no leakage between routing targets.
# ===========================================================================


# ---------------------------------------------------------------------------
# Stress 1: BROADCAST burst from a foreign origin — every message must be routed
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_stress_broadcast_burst_foreign_origin(monkeypatch):
    """Fire _N_BURST BROADCAST messages from INSTANCE_A into an INSTANCE_B broker.
    Every single message must reach route_message_to_local_websockets exactly once."""
    from gulp.api import ws_api as ws_api_module

    route_ws = AsyncMock()
    mock_sockets = MagicMock()
    mock_sockets.route_message_to_local_websockets = route_ws

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: _fake_redis(INSTANCE_B))
    monkeypatch.setattr(ws_api_module.GulpConnectedSockets, "get_instance", lambda: mock_sockets)

    broker = GulpRedisBroker.get_instance()

    messages = [
        _make_pubsub_message(
            GulpMessageRoutingTarget.BROADCAST.value,
            origin_server_id=INSTANCE_A,
            msg_type="ingest_source_done",
        )
        for _ in range(_N_BURST)
    ]

    await asyncio.gather(*[broker._handle_pubsub_message(m) for m in messages])

    assert route_ws.await_count == _N_BURST


# ---------------------------------------------------------------------------
# Stress 2: BROADCAST burst from own origin — nothing must be routed
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_stress_broadcast_burst_own_origin_skips(monkeypatch):
    """Fire _N_BURST BROADCAST messages that originated from THIS instance.
    None must be forwarded (origin already delivered them via _put_common)."""
    from gulp.api import ws_api as ws_api_module

    route_ws = AsyncMock()
    mock_sockets = MagicMock()
    mock_sockets.route_message_to_local_websockets = route_ws

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: _fake_redis(INSTANCE_A))
    monkeypatch.setattr(ws_api_module.GulpConnectedSockets, "get_instance", lambda: mock_sockets)

    broker = GulpRedisBroker.get_instance()

    messages = [
        _make_pubsub_message(
            GulpMessageRoutingTarget.BROADCAST.value,
            origin_server_id=INSTANCE_A,
            msg_type="user_login",
        )
        for _ in range(_N_BURST)
    ]

    await asyncio.gather(*[broker._handle_pubsub_message(m) for m in messages])

    route_ws.assert_not_awaited()


# ---------------------------------------------------------------------------
# Stress 3: WORKER_TO_MAIN internal fan-out
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_stress_internal_fan_out(monkeypatch):
    """Fire _N_BURST concurrent internal events into the same-origin instance.
    GulpInternalEventsManager.dispatch_internal_event must be called exactly _N_BURST times."""
    from gulp.api import ws_api as ws_api_module
    import gulp.plugin as plugin_module

    dispatch_count = 0

    async def _counting_dispatch(t, **kwargs):
        nonlocal dispatch_count
        dispatch_count += 1
        return {}

    mock_mgr = MagicMock()
    mock_mgr.dispatch_internal_event = _counting_dispatch

    route_ws = AsyncMock()
    mock_sockets = MagicMock()
    mock_sockets.route_message_to_local_websockets = route_ws

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: _fake_redis(INSTANCE_A))
    monkeypatch.setattr(ws_api_module.GulpConnectedSockets, "get_instance", lambda: mock_sockets)
    monkeypatch.setattr(
        plugin_module.GulpInternalEventsManager, "get_instance", staticmethod(lambda: mock_mgr)
    )

    broker = GulpRedisBroker.get_instance()

    messages = [
        _make_pubsub_message(
            GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
            origin_server_id=INSTANCE_A,
            msg_type=f"plugin_event_{i}",
            internal=True,
            propagate_internal=False,
        )
        for i in range(_N_BURST)
    ]

    await asyncio.gather(*[broker._handle_pubsub_message(m) for m in messages])

    assert dispatch_count == _N_BURST
    route_ws.assert_not_awaited()


# ---------------------------------------------------------------------------
# Stress 4: Mixed routing targets fired simultaneously
#   _N_MIX BROADCAST (foreign)      → route_ws calls
#   _N_MIX WORKER_TO_MAIN non-int   → route_ws calls
#   _N_MIX WORKER_TO_MAIN internal  → dispatch calls
#   _N_MIX CLIENT_DATA (foreign)    → route_client calls
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_stress_mixed_routing_targets(monkeypatch):
    """All four routing flavours fire concurrently; per-type counts must be exact."""
    from gulp.api import ws_api as ws_api_module
    import gulp.plugin as plugin_module

    route_ws = AsyncMock()
    mock_sockets = MagicMock()
    mock_sockets.route_message_to_local_websockets = route_ws

    dispatch_count = 0

    async def _counting_dispatch(t, **kwargs):
        nonlocal dispatch_count
        dispatch_count += 1
        return {}

    mock_mgr = MagicMock()
    mock_mgr.dispatch_internal_event = _counting_dispatch

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: _fake_redis(INSTANCE_B))
    monkeypatch.setattr(ws_api_module.GulpConnectedSockets, "get_instance", lambda: mock_sockets)
    monkeypatch.setattr(
        plugin_module.GulpInternalEventsManager, "get_instance", staticmethod(lambda: mock_mgr)
    )

    broker = GulpRedisBroker.get_instance()
    route_client = AsyncMock()
    monkeypatch.setattr(broker, "_route_message_to_local_client_data_websockets", route_client)

    # BROADCAST from a foreign origin → INSTANCE_B broker must route
    broadcast_msgs = [
        _make_pubsub_message(
            GulpMessageRoutingTarget.BROADCAST.value,
            origin_server_id=INSTANCE_A,
            msg_type="ingest_source_done",
        )
        for _ in range(_N_MIX)
    ]

    # WORKER_TO_MAIN non-internal → route to local sockets
    w2m_nonintern_msgs = [
        _make_pubsub_message(
            GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
            origin_server_id=INSTANCE_B,
            msg_type="stats_update",
            internal=False,
        )
        for _ in range(_N_MIX)
    ]

    # WORKER_TO_MAIN internal, same origin as INSTANCE_B → dispatch; no propagate
    w2m_intern_msgs = [
        _make_pubsub_message(
            GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
            origin_server_id=INSTANCE_B,
            msg_type="my_plugin_event",
            internal=True,
            propagate_internal=False,
        )
        for _ in range(_N_MIX)
    ]

    # CLIENT_DATA from a foreign origin → INSTANCE_B broker must route
    client_data_msgs = [
        _make_pubsub_message(
            GulpMessageRoutingTarget.CLIENT_DATA.value,
            origin_server_id=INSTANCE_A,
            msg_type="client_data",
        )
        for _ in range(_N_MIX)
    ]

    all_msgs = broadcast_msgs + w2m_nonintern_msgs + w2m_intern_msgs + client_data_msgs
    await asyncio.gather(*[broker._handle_pubsub_message(m) for m in all_msgs])

    # BROADCAST + WORKER_TO_MAIN non-internal both go through route_message_to_local_websockets
    assert route_ws.await_count == _N_MIX * 2
    assert dispatch_count == _N_MIX
    assert route_client.await_count == _N_MIX


# ---------------------------------------------------------------------------
# Stress 5: Multiple competing instances receiving the same broadcast messages
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_stress_multi_instance_broadcast(monkeypatch):
    """Simulate _N_INST instances each receiving the same _N_BURST BROADCAST messages.
    Only (_N_INST - 1) instances should route; the origin must always skip.
    Total expected route calls across all brokers: (_N_INST-1) * _N_BURST.
    """
    from gulp.api import ws_api as ws_api_module

    ORIGIN = "gulp-origin"
    instances = [f"gulp-peer-{i}" for i in range(_N_INST - 1)]  # non-origin instances
    all_instances = [ORIGIN] + instances

    total_route_calls = 0

    async def _run_instance(server_id: str, msgs: list[dict]) -> int:
        """Run the broker for one simulated instance and return its route call count."""
        # Each instance gets its own isolated broker singleton
        GulpRedisBroker._instance = None
        broker = GulpRedisBroker.get_instance()

        route_ws = AsyncMock()
        mock_sockets = MagicMock()
        mock_sockets.route_message_to_local_websockets = route_ws

        with monkeypatch.context() as m:
            m.setattr(ws_api_module.GulpRedis, "get_instance", lambda: _fake_redis(server_id))
            m.setattr(ws_api_module.GulpConnectedSockets, "get_instance", lambda: mock_sockets)
            await asyncio.gather(*[broker._handle_pubsub_message(dict(msg)) for msg in msgs])

        return route_ws.await_count

    messages = [
        _make_pubsub_message(
            GulpMessageRoutingTarget.BROADCAST.value,
            origin_server_id=ORIGIN,
            msg_type="user_logout",
        )
        for _ in range(_N_BURST)
    ]

    for sid in all_instances:
        total_route_calls += await _run_instance(sid, messages)
        GulpRedisBroker._instance = None

    expected = (_N_INST - 1) * _N_BURST
    assert total_route_calls == expected, (
        f"expected {expected} route calls across {_N_INST} instances, got {total_route_calls}"
    )


# ---------------------------------------------------------------------------
# Stress 6: Concurrent response_key writes — each unique key written exactly once
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_stress_concurrent_response_key_writes(monkeypatch):
    """_N_RESP concurrent internal events each carry a unique response_key.
    The broker must write the dispatch result to each key exactly once — no
    collisions, no overwrites, no missing keys."""
    from gulp.api import ws_api as ws_api_module
    import gulp.plugin as plugin_module

    written: dict[str, int] = {}

    async def _counting_dispatch(t, **kwargs):
        return {"ok": True}

    mock_mgr = MagicMock()
    mock_mgr.dispatch_internal_event = _counting_dispatch

    async def _fake_set(key: str, value: bytes, ex: int = None):
        written[key] = written.get(key, 0) + 1
        return True

    inner_redis = MagicMock()
    inner_redis.set = _fake_set

    fake_redis = _fake_redis(INSTANCE_A)
    fake_redis._redis = inner_redis

    monkeypatch.setattr(ws_api_module.GulpRedis, "get_instance", lambda: fake_redis)
    monkeypatch.setattr(
        plugin_module.GulpInternalEventsManager, "get_instance", staticmethod(lambda: mock_mgr)
    )

    broker = GulpRedisBroker.get_instance()

    response_keys = [f"gulp:internal:wait:key-{i}" for i in range(_N_RESP)]
    messages = [
        _make_pubsub_message(
            GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
            origin_server_id=INSTANCE_A,
            msg_type="plugin_event",
            internal=True,
            propagate_internal=False,
            response_key=rk,
        )
        for rk in response_keys
    ]

    await asyncio.gather(*[broker._handle_pubsub_message(m) for m in messages])

    # every response key must have been written exactly once
    assert set(written.keys()) == set(response_keys), (
        f"missing keys: {set(response_keys) - set(written.keys())}"
    )
    overwrites = {k: v for k, v in written.items() if v != 1}
    assert not overwrites, f"keys written more than once: {overwrites}"


# ---------------------------------------------------------------------------
# Stress 7: Propagated internal events reach ALL instances under load
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_stress_propagated_internal_reaches_all_instances(monkeypatch):
    """When propagate_internal=True, every instance must dispatch the event —
    regardless of which instance originated it.
    Fire _N_MIX messages into each of _N_INST broker instances; total dispatch
    count must equal _N_INST * _N_MIX."""
    from gulp.api import ws_api as ws_api_module
    import gulp.plugin as plugin_module

    ORIGIN = "gulp-origin"
    instances = [ORIGIN] + [f"gulp-peer-{i}" for i in range(_N_INST - 1)]

    total_dispatches = 0

    async def _run_instance(server_id: str, msgs: list[dict]) -> int:
        GulpRedisBroker._instance = None
        broker = GulpRedisBroker.get_instance()

        dispatch_count = 0

        async def _counting_dispatch(t, **kwargs):
            nonlocal dispatch_count
            dispatch_count += 1
            return {}

        mock_mgr = MagicMock()
        mock_mgr.dispatch_internal_event = _counting_dispatch

        route_ws = AsyncMock()
        mock_sockets = MagicMock()
        mock_sockets.route_message_to_local_websockets = route_ws

        with monkeypatch.context() as m:
            m.setattr(ws_api_module.GulpRedis, "get_instance", lambda: _fake_redis(server_id))
            m.setattr(ws_api_module.GulpConnectedSockets, "get_instance", lambda: mock_sockets)
            m.setattr(
                plugin_module.GulpInternalEventsManager,
                "get_instance",
                staticmethod(lambda: mock_mgr),
            )
            await asyncio.gather(*[broker._handle_pubsub_message(dict(msg)) for msg in msgs])

        return dispatch_count

    messages = [
        _make_pubsub_message(
            GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
            origin_server_id=ORIGIN,
            msg_type="propagated_event",
            internal=True,
            propagate_internal=True,   # ← key: every instance must dispatch
        )
        for _ in range(_N_MIX)
    ]

    for sid in instances:
        total_dispatches += await _run_instance(sid, messages)
        GulpRedisBroker._instance = None

    expected = _N_INST * _N_MIX
    assert total_dispatches == expected, (
        f"expected {expected} dispatches across {_N_INST} instances, got {total_dispatches}"
    )
