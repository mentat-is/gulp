"""
Unit tests simulating multiple gulp instances processing pubsub messages through
GulpRedisBroker._handle_pubsub_message.

Exercises the websocket routing targets handled by the Redis broker:

  BROADCAST
    - origin instance: skips (locally routed already via _put_common)
    - non-origin instance: routes message to its local WS_DEFAULT sockets

  WORKER_TO_MAIN
    - worker messages are routed by the local main process

  CLIENT_DATA (UI↔UI)
    - origin instance: skips
    - non-origin instance: routes to WS_CLIENT_DATA sockets

Stress scenarios
  BROADCAST high-volume burst: N concurrent foreign-origin messages, all must be routed
  BROADCAST origin-skip burst: N concurrent own-origin messages, none must be routed
  Multi-instance competing: K instances each receive the same N broadcast messages, only
    non-origin instances route (expect (K-1)*N total route calls)
"""
import asyncio
import pytest
import orjson
from unittest.mock import AsyncMock, MagicMock

from gulp.api.ws_api import (
    GulpConnectedSockets,
    GulpMessageRoutingTarget,
    GulpRedisBroker,
    GulpWsData,
    GulpWsType,
)

# Two symbolic server identifiers used through baseline tests
INSTANCE_A = "gulp-instance-A"
INSTANCE_B = "gulp-instance-B"

# Stress test scale factors – keep fast but meaningful
_N_BURST = 200   # messages per burst test
_N_INST = 5     # number of simulated instances in the multi-instance test


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pubsub_message(
    route_target_type: str,
    origin_server_id: str,
    *,
    msg_type: str = "test_event",
    ws_id: str = None,
    operation_id: str = None,
) -> dict:
    """Build a pubsub message dict as it arrives from the Redis subscriber loop.

    The broker pops ``route_target_type``, ``origin_server_id``, and
    ``__redis_channel__`` before validating the remainder into a GulpWsData.
    """
    msg: dict = {
        "@timestamp": 1_000_000,
        "type": msg_type,
        "route_target_type": route_target_type,
        "origin_server_id": origin_server_id,
        "__redis_channel__": GulpMessageRoutingTarget.BROADCAST.redis_channel_name(),
    }
    if ws_id is not None:
        msg["ws_id"] = ws_id
    if operation_id is not None:
        msg["operation_id"] = operation_id
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
async def test_worker_broadcast_filter_matrix_matches_on_origin_and_peer(monkeypatch):
    """Worker broadcasts retain operation metadata and use one filtered local route."""
    from gulp.api import ws_api as ws_api_module
    import gulp.process as process_module

    filter_matrix = {
        "none": (None, None, True),
        "empty": ([], [], True),
        "type_match": (["ingest_source_done"], None, True),
        "type_mismatch": (["query_done"], None, False),
        "operation_match": (None, ["operation-a"], True),
        "operation_mismatch": (None, ["operation-b"], False),
        "both_match": (["ingest_source_done"], ["operation-a"], True),
        "both_type_mismatch": (["query_done"], ["operation-a"], False),
        "both_operation_mismatch": (
            ["ingest_source_done"],
            ["operation-b"],
            False,
        ),
    }

    def _make_sockets(prefix: str) -> tuple[GulpConnectedSockets, dict[str, MagicMock]]:
        sockets = object.__new__(GulpConnectedSockets)
        connected = {}
        for name, (types, operation_ids, _) in filter_matrix.items():
            socket = MagicMock()
            socket.ws_id = f"{prefix}-{name}"
            socket.socket_type = GulpWsType.WS_DEFAULT
            socket.types = types
            socket.operation_ids = operation_ids
            socket.enqueue_message = AsyncMock(return_value=True)
            connected[name] = socket
        sockets._sockets = {socket.ws_id: socket for socket in connected.values()}
        return sockets, connected

    origin_sockets, origin_connected = _make_sockets("origin")
    peer_sockets, peer_connected = _make_sockets("peer")

    origin_redis = _fake_redis(INSTANCE_A)
    origin_redis.publish = AsyncMock()
    origin_redis.worker_to_main_channel = lambda server_id=None: (
        f"gulpredis:server:{server_id or INSTANCE_A}"
    )
    peer_redis = _fake_redis(INSTANCE_B)
    current_redis = [origin_redis]
    current_sockets = [origin_sockets]

    process = MagicMock()
    process.is_main_process.return_value = True

    monkeypatch.setattr(
        ws_api_module.GulpRedis,
        "get_instance",
        lambda: current_redis[0],
    )
    monkeypatch.setattr(
        ws_api_module.GulpConnectedSockets,
        "get_instance",
        lambda: current_sockets[0],
    )
    monkeypatch.setattr(
        process_module.GulpProcess,
        "get_instance",
        staticmethod(lambda: process),
    )

    broker = GulpRedisBroker.get_instance()
    worker_message = _make_pubsub_message(
        GulpMessageRoutingTarget.WORKER_TO_MAIN.value,
        origin_server_id=INSTANCE_A,
        msg_type="ingest_source_done",
        ws_id="request-ws",
        operation_id="operation-a",
    )
    worker_message["__redis_channel__"] = origin_redis.worker_to_main_channel()
    await broker._handle_pubsub_message(worker_message)

    origin_redis.publish.assert_awaited_once()
    broadcast = origin_redis.publish.await_args.args[0]
    assert broadcast["route_target_type"] == GulpMessageRoutingTarget.BROADCAST.value
    assert broadcast["operation_id"] == "operation-a"

    # Redis echoes the broadcast to its origin, which must not enqueue it twice.
    origin_echo = dict(broadcast)
    origin_echo["__redis_channel__"] = (
        GulpMessageRoutingTarget.BROADCAST.redis_channel_name()
    )
    await broker._handle_pubsub_message(origin_echo)

    current_redis[0] = peer_redis
    current_sockets[0] = peer_sockets
    peer_delivery = dict(broadcast)
    peer_delivery["__redis_channel__"] = (
        GulpMessageRoutingTarget.BROADCAST.redis_channel_name()
    )
    await broker._handle_pubsub_message(peer_delivery)

    for name, (_, _, expected) in filter_matrix.items():
        for socket in (origin_connected[name], peer_connected[name]):
            assert socket.enqueue_message.await_count == int(expected)
            if expected:
                packet = orjson.loads(socket.enqueue_message.await_args.args[0])
                assert packet["type"] == "ingest_source_done"
                assert packet["operation_id"] == "operation-a"


# ---------------------------------------------------------------------------
# WORKER_TO_MAIN routing
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_to_main_routes_to_local_ws(monkeypatch):
    """WORKER_TO_MAIN messages are routed to local default websockets."""
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
    )
    await broker._handle_pubsub_message(msg)

    route_ws.assert_awaited_once()




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



@pytest.mark.unit
def test_large_pointer_stress_env_knobs(monkeypatch):
    from tests.integration._test_multi_instance_base import (
        _continue_large_pointer_stress,
        _large_pointer_stress_count,
        _large_pointer_soak_seconds,
    )

    monkeypatch.delenv("GULP_MULTI_INSTANCE_POINTER_STRESS_COUNT", raising=False)
    monkeypatch.delenv("GULP_MULTI_INSTANCE_POINTER_SOAK_SECONDS", raising=False)
    assert _large_pointer_stress_count() == 3
    assert _large_pointer_soak_seconds() == 0.0
    assert _continue_large_pointer_stress(0, None) is True
    assert _continue_large_pointer_stress(2, None) is True
    assert _continue_large_pointer_stress(3, None) is False

    monkeypatch.setenv("GULP_MULTI_INSTANCE_POINTER_STRESS_COUNT", "5")
    monkeypatch.setenv("GULP_MULTI_INSTANCE_POINTER_SOAK_SECONDS", "1.5")
    assert _large_pointer_stress_count() == 5
    assert _large_pointer_soak_seconds() == 1.5
    assert _continue_large_pointer_stress(4, None) is True
    assert _continue_large_pointer_stress(5, None) is False

    monkeypatch.setenv("GULP_MULTI_INSTANCE_POINTER_STRESS_COUNT", "not-int")
    monkeypatch.setenv("GULP_MULTI_INSTANCE_POINTER_SOAK_SECONDS", "not-float")
    assert _large_pointer_stress_count() == 3
    assert _large_pointer_soak_seconds() == 0.0
