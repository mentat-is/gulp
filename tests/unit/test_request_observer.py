"""The read-only request observers registered by extension plugins, and their dispatcher."""

import dataclasses
from types import SimpleNamespace

import pytest

from gulp.api import server_api
from gulp.api.server import request_observer
from gulp.api.server.request_observer import (
    OBSERVED_BODY_MAX_BYTES,
    GulpObservation,
    GulpObservedRequest,
    GulpObservedResponse,
    GulpRequestObserver,
    RequestObserverDispatcher,
    RequestObservers,
)
from gulp.api.server_api import GulpServer


class _Recorder(GulpRequestObserver):
    """records what it is told; `observation` is what it answers to on_request_start"""

    def __init__(self, observation: GulpObservation | None = GulpObservation()):
        self.observation = observation
        self.started: list[GulpObservedRequest] = []
        self.ended: list[tuple[GulpObservedRequest, GulpObservedResponse]] = []

    def on_request_start(self, request: GulpObservedRequest) -> GulpObservation | None:
        self.started.append(request)
        return self.observation

    def on_request_end(
        self, request: GulpObservedRequest, response: GulpObservedResponse
    ) -> None:
        self.ended.append((request, response))


def _scope(**overrides) -> dict:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/note_create",
        "query_string": b"operation_id=op1",
        "headers": [(b"Token", b"abc"), (b"x-multi", b"1"), (b"X-Multi", b"2")],
        "client": ("10.0.0.7", 51234),
    }
    scope.update(overrides)
    return scope


def _app(status: int = 200, body: bytes = b'{"status":"success"}', chunks: int = 1):
    """an application reading the whole request, then answering in `chunks` body messages"""
    received: list[bytes] = []

    async def app(scope, receive, send):
        while True:
            message = await receive()
            received.append(message.get("body", b""))
            if not message.get("more_body"):
                break
        scope["route"] = SimpleNamespace(path="/note_create")
        await send({"type": "http.response.start", "status": status, "headers": []})
        size = max(1, len(body) // chunks)
        parts = [body[i : i + size] for i in range(0, len(body), size)] or [b""]
        for index, part in enumerate(parts):
            await send(
                {
                    "type": "http.response.body",
                    "body": part,
                    "more_body": index < len(parts) - 1,
                }
            )

    app.received = received
    return app


async def _serve(dispatcher, scope=None, request_chunks=(b'{"user_id":"u"}',)):
    """runs one request through the dispatcher, returns what the client was sent"""
    pending = list(request_chunks)
    sent: list[dict] = []

    async def receive():
        body = pending.pop(0)
        return {"type": "http.request", "body": body, "more_body": bool(pending)}

    async def send(message):
        sent.append(message)

    await dispatcher(scope or _scope(), receive, send)
    return sent


def _dispatcher(app, *observers) -> RequestObserverDispatcher:
    registry = RequestObservers()
    for observer in observers:
        registry.add(observer)
    return RequestObserverDispatcher(app, registry)


@pytest.fixture
def exceptions(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """the messages the dispatcher logs when an observer fails"""
    logged: list[str] = []
    monkeypatch.setattr(
        request_observer.Glug,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(exception=logged.append)),
    )
    return logged


async def test_no_observer_forwards_untouched():
    app = _app()
    sent = await _serve(_dispatcher(app))

    assert [m["type"] for m in sent] == ["http.response.start", "http.response.body"]
    assert app.received == [b'{"user_id":"u"}']


async def test_observer_is_told_of_the_request_and_its_outcome():
    recorder = _Recorder()
    await _serve(_dispatcher(_app(status=201), recorder))

    assert len(recorder.started) == 1 and len(recorder.ended) == 1
    request, response = recorder.ended[0]
    assert request is recorder.started[0]
    assert (request.method, request.path) == ("POST", "/note_create")
    assert request.query_string == "operation_id=op1"
    assert request.headers == {"token": "abc", "x-multi": "1, 2"}
    assert request.client_host == "10.0.0.7"
    assert response.route == "/note_create"
    assert response.status_code == 201
    assert response.duration_msec >= 0
    # bodies are only copied when asked for
    assert (response.request_body, response.response_body) == (b"", b"")


async def test_what_the_observer_gets_cannot_be_changed():
    recorder = _Recorder()
    await _serve(_dispatcher(_app(), recorder))
    request, response = recorder.ended[0]

    with pytest.raises(dataclasses.FrozenInstanceError):
        request.path = "/other"
    with pytest.raises(TypeError):
        request.headers["token"] = "forged"
    with pytest.raises(dataclasses.FrozenInstanceError):
        response.status_code = 500


async def test_changing_the_copy_does_not_reach_the_application():
    """the scope handed to the application is the one the server gave, not the copy."""
    seen: dict = {}

    async def app(scope, receive, send):
        seen.update(scope)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    scope = _scope()
    await _serve(_dispatcher(app, _Recorder()), scope)

    assert seen["headers"] is scope["headers"]
    assert seen["query_string"] == b"operation_id=op1"


async def test_not_interested_means_no_end_call_and_no_wrapping():
    recorder = _Recorder(observation=None)
    passed: dict = {}

    async def app(scope, receive, send):
        passed["receive"], passed["send"] = receive, send

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(message):
        pass

    await _dispatcher(app, recorder)(_scope(), receive, send)

    assert len(recorder.started) == 1 and recorder.ended == []
    assert passed == {"receive": receive, "send": send}


async def test_bodies_are_copied_up_to_what_each_observer_asked():
    small = _Recorder(GulpObservation(request_body_bytes=4, response_body_bytes=6))
    large = _Recorder(GulpObservation(request_body_bytes=100, response_body_bytes=100))
    none = _Recorder()
    app = _app(body=b"0123456789ABCDEF", chunks=4)

    sent = await _serve(
        _dispatcher(app, small, large, none), request_chunks=(b"abc", b"def", b"ghi")
    )

    assert (small.ended[0][1].request_body, small.ended[0][1].response_body) == (
        b"abcd",
        b"012345",
    )
    assert (large.ended[0][1].request_body, large.ended[0][1].response_body) == (
        b"abcdefghi",
        b"0123456789ABCDEF",
    )
    assert (none.ended[0][1].request_body, none.ended[0][1].response_body) == (b"", b"")
    # the application and the client still got everything, in the same pieces
    assert app.received == [b"abc", b"def", b"ghi"]
    assert b"".join(m.get("body", b"") for m in sent) == b"0123456789ABCDEF"
    assert len(sent) == 5


async def test_body_copies_are_capped_whatever_is_asked():
    greedy = _Recorder(
        GulpObservation(request_body_bytes=10**9, response_body_bytes=10**9)
    )
    body = b"x" * (OBSERVED_BODY_MAX_BYTES + 1000)

    sent = await _serve(_dispatcher(_app(body=body), greedy), request_chunks=(body,))

    response = greedy.ended[0][1]
    assert len(response.request_body) == OBSERVED_BODY_MAX_BYTES
    assert len(response.response_body) == OBSERVED_BODY_MAX_BYTES
    assert sum(len(m.get("body", b"")) for m in sent) == len(body)


async def test_a_failing_application_is_still_reported_and_still_fails():
    recorder = _Recorder()

    async def app(scope, receive, send):
        raise RuntimeError("handler exploded")

    with pytest.raises(RuntimeError, match="handler exploded"):
        await _serve(_dispatcher(app, recorder))

    response = recorder.ended[0][1]
    assert response.status_code is None and response.route is None


async def test_a_failing_observer_does_not_affect_the_request_or_the_others(
    exceptions: list[str],
):
    class Broken(GulpRequestObserver):
        def on_request_start(self, request):
            raise RuntimeError("broken start")

    class BrokenAtEnd(GulpRequestObserver):
        def on_request_start(self, request):
            return GulpObservation()

        def on_request_end(self, request, response):
            raise RuntimeError("broken end")

    recorder = _Recorder()
    sent = await _serve(_dispatcher(_app(), Broken(), BrokenAtEnd(), recorder))

    assert [m["type"] for m in sent] == ["http.response.start", "http.response.body"]
    assert len(recorder.ended) == 1
    assert len(exceptions) == 2
    assert "on_request_start" in exceptions[0] and "on_request_end" in exceptions[1]


async def test_only_http_is_observed():
    recorder = _Recorder()
    calls: list[str] = []

    async def app(scope, receive, send):
        calls.append(scope["type"])

    dispatcher = _dispatcher(app, recorder)
    await dispatcher({"type": "websocket"}, None, None)
    await dispatcher({"type": "lifespan"}, None, None)

    assert calls == ["websocket", "lifespan"]
    assert recorder.started == []


async def test_a_contextvar_set_at_start_is_seen_by_the_application():
    from contextvars import ContextVar

    var: ContextVar[str | None] = ContextVar("probe", default=None)
    seen: list[str | None] = []

    class Tagger(GulpRequestObserver):
        def on_request_start(self, request):
            var.set("tagged")
            return None

    async def app(scope, receive, send):
        seen.append(var.get())

    await _serve(_dispatcher(app, Tagger()))

    assert seen == ["tagged"]


def test_registry_add_remove_clear():
    registry = RequestObservers()
    first, second = _Recorder(), _Recorder()

    assert registry.add(first) is True
    assert registry.add(first) is False
    assert registry.add(second) is True
    assert registry.observers() == (first, second)
    assert registry.remove(first) is True
    assert registry.remove(first) is False
    assert registry.observers() == (second,)
    registry.clear()
    assert registry.observers() == ()


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> GulpServer:
    """a server with an empty registry, in a process reported as the main one"""
    monkeypatch.setattr(
        server_api.GulpProcess,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(is_main_process=lambda: True)),
    )
    srv = GulpServer.get_instance()
    monkeypatch.setattr(srv, "_request_observers", RequestObservers())
    return srv


async def test_server_methods_register_and_remove(server: GulpServer):
    recorder = _Recorder()
    dispatcher = RequestObserverDispatcher(_app(), server._request_observers)

    server.add_request_observer(recorder)
    server.add_request_observer(recorder)  # ignored, with a warning
    await _serve(dispatcher)
    server.remove_request_observer(recorder)
    server.remove_request_observer(recorder)  # not registered any more: no error
    await _serve(dispatcher)

    assert len(recorder.started) == 1 and len(recorder.ended) == 1


def test_registering_from_a_worker_is_an_error(
    server: GulpServer, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(
        server_api.GulpProcess,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(is_main_process=lambda: False)),
    )

    with pytest.raises(RuntimeError, match="main-process-only"):
        server.add_request_observer(_Recorder())
