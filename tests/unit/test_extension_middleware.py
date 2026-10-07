"""The ASGI middlewares registered by extension plugins, and the dispatcher running them."""

from types import SimpleNamespace

import pytest

from gulp.api import server_api
from gulp.api.server_api import (
    GulpServer,
    _ExtensionMiddlewareDispatcher,
    _ExtensionMiddlewares,
)


def _recording(name: str, calls: list[str]):
    """an ASGI middleware factory recording when it is entered and left"""

    def factory(app):
        async def middleware(scope, receive, send):
            calls.append(f"{name}:in")
            await app(scope, receive, send)
            calls.append(f"{name}:out")

        return middleware

    return factory


def _app(calls: list[str]):
    async def app(scope, receive, send):
        calls.append("app")

    return app


async def _call(dispatcher: _ExtensionMiddlewareDispatcher, scope_type: str = "http"):
    await dispatcher({"type": scope_type}, None, None)


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> GulpServer:
    """a server with an empty registry, in a process reported as the main one"""
    monkeypatch.setattr(
        server_api.GulpProcess,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(is_main_process=lambda: True)),
    )
    srv = GulpServer.get_instance()
    monkeypatch.setattr(srv, "_extension_middlewares", _ExtensionMiddlewares())
    return srv


async def test_no_middleware_forwards_to_the_application():
    calls: list[str] = []
    dispatcher = _ExtensionMiddlewareDispatcher(_app(calls), _ExtensionMiddlewares())

    await _call(dispatcher)

    assert calls == ["app"]


async def test_first_registered_is_the_outermost():
    calls: list[str] = []
    registry = _ExtensionMiddlewares()
    registry.add(_recording("first", calls))
    registry.add(_recording("second", calls))
    dispatcher = _ExtensionMiddlewareDispatcher(_app(calls), registry)

    await _call(dispatcher)

    assert calls == ["first:in", "second:in", "app", "second:out", "first:out"]


async def test_add_and_remove_after_the_first_request():
    calls: list[str] = []
    registry = _ExtensionMiddlewares()
    dispatcher = _ExtensionMiddlewareDispatcher(_app(calls), registry)
    probe = _recording("probe", calls)

    await _call(dispatcher)
    assert registry.add(probe) is True
    await _call(dispatcher)
    assert registry.remove(probe) is True
    await _call(dispatcher)

    assert calls == ["app", "probe:in", "app", "probe:out", "app"]


async def test_registering_twice_runs_it_once():
    calls: list[str] = []
    registry = _ExtensionMiddlewares()
    probe = _recording("probe", calls)
    assert registry.add(probe) is True
    assert registry.add(probe) is False
    dispatcher = _ExtensionMiddlewareDispatcher(_app(calls), registry)

    await _call(dispatcher)

    assert calls == ["probe:in", "app", "probe:out"]


def test_removing_an_unknown_middleware_changes_nothing():
    registry = _ExtensionMiddlewares()
    version = registry.version

    assert registry.remove(_recording("probe", [])) is False
    assert registry.version == version


async def test_clear_removes_everything():
    calls: list[str] = []
    registry = _ExtensionMiddlewares()
    registry.add(_recording("probe", calls))
    dispatcher = _ExtensionMiddlewareDispatcher(_app(calls), registry)
    await _call(dispatcher)
    calls.clear()

    registry.clear()
    await _call(dispatcher)

    assert calls == ["app"]


@pytest.mark.parametrize(
    "scope_type, expected",
    [
        ("http", ["probe:in", "app", "probe:out"]),
        ("websocket", ["probe:in", "app", "probe:out"]),
        ("lifespan", ["app"]),
    ],
)
async def test_lifespan_bypasses_the_registered_middlewares(
    scope_type: str, expected: list[str]
):
    calls: list[str] = []
    registry = _ExtensionMiddlewares()
    registry.add(_recording("probe", calls))
    dispatcher = _ExtensionMiddlewareDispatcher(_app(calls), registry)

    await _call(dispatcher, scope_type)

    assert calls == expected


async def test_a_raising_middleware_fails_the_request():
    def factory(app):
        async def middleware(scope, receive, send):
            raise RuntimeError("broken middleware")

        return middleware

    registry = _ExtensionMiddlewares()
    registry.add(factory)
    dispatcher = _ExtensionMiddlewareDispatcher(_app([]), registry)

    with pytest.raises(RuntimeError, match="broken middleware"):
        await _call(dispatcher)


async def test_server_methods_register_and_remove(server: GulpServer):
    calls: list[str] = []
    probe = _recording("probe", calls)
    dispatcher = _ExtensionMiddlewareDispatcher(
        _app(calls), server._extension_middlewares
    )

    server.add_extension_middleware(probe)
    server.add_extension_middleware(probe)  # ignored, with a warning
    await _call(dispatcher)
    server.remove_extension_middleware(probe)
    server.remove_extension_middleware(probe)  # not registered any more: no error
    await _call(dispatcher)

    assert calls == ["probe:in", "app", "probe:out", "app"]


def test_registering_from_a_worker_is_an_error(
    server: GulpServer, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(
        server_api.GulpProcess,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(is_main_process=lambda: False)),
    )

    with pytest.raises(RuntimeError, match="main-process-only"):
        server.add_extension_middleware(_recording("probe", []))
