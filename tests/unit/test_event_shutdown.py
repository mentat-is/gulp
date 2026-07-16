"""Shutdown ordering regression for event listener dependencies."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.server_api import GulpServer
from gulp.events import events


@pytest.mark.unit
@pytest.mark.asyncio
async def test_workers_and_events_drain_before_extensions_and_storage(monkeypatch):
    from gulp.api.collab_api import GulpCollab
    from gulp.api.opensearch_api import GulpOpenSearch
    from gulp.api.redis_api import GulpRedis
    from gulp.api.s3_api import GulpS3
    from gulp.api.ws_api import GulpConnectedSockets, GulpRedisBroker
    from gulp.process import GulpProcess

    order: list[str] = []

    def dependency(methods: list[str]):
        instance = MagicMock()
        for method in methods:
            setattr(
                instance,
                method,
                AsyncMock(side_effect=lambda _method=method: order.append(_method)),
            )
        return instance

    sockets = dependency(["sockets"])
    process = dependency(["workers", "threads"])
    collab = dependency(["postgres"])
    opensearch = dependency(["opensearch"])
    s3 = dependency(["s3"])
    broker = dependency(["broker"])
    redis = dependency(["consumers", "redis"])
    redis.cleanup_consumers_for_server = AsyncMock(
        side_effect=lambda _server_id: order.append("consumers")
    )

    monkeypatch.setattr(sockets, "cancel_all", sockets.sockets)
    monkeypatch.setattr(process, "close_process_pool", process.workers)
    monkeypatch.setattr(process, "close_thread_pool", process.threads)
    monkeypatch.setattr(collab, "shutdown", collab.postgres)
    monkeypatch.setattr(opensearch, "shutdown", opensearch.opensearch)
    monkeypatch.setattr(s3, "shutdown", s3.s3)
    monkeypatch.setattr(broker, "shutdown", broker.broker)
    monkeypatch.setattr(redis, "shutdown", redis.redis)

    monkeypatch.setattr(
        GulpConnectedSockets, "get_instance", staticmethod(lambda: sockets)
    )
    monkeypatch.setattr(GulpProcess, "get_instance", staticmethod(lambda: process))
    monkeypatch.setattr(GulpCollab, "get_instance", staticmethod(lambda: collab))
    monkeypatch.setattr(
        GulpOpenSearch, "get_instance", staticmethod(lambda: opensearch)
    )
    monkeypatch.setattr(GulpS3, "get_instance", staticmethod(lambda: s3))
    monkeypatch.setattr(GulpRedisBroker, "get_instance", staticmethod(lambda: broker))
    monkeypatch.setattr(GulpRedis, "get_instance", staticmethod(lambda: redis))

    drain_count = 0

    async def drain():
        nonlocal drain_count
        drain_count += 1
        order.append(f"drain-{drain_count}")

    monkeypatch.setattr(events, "drain", drain)
    server = object.__new__(GulpServer)
    server._poll_tasks_task = None
    server.server_id = "server"
    server._unload_extension_plugins = AsyncMock(
        side_effect=lambda: order.append("unload")
    )

    await server._cleanup()

    assert order[:5] == ["sockets", "workers", "drain-1", "unload", "drain-2"]
    assert order.index("threads") < order.index("postgres")
    assert order.index("postgres") < order.index("broker")
    assert order[-1] == "redis"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cleanup_failure_does_not_skip_later_resources(monkeypatch):
    from gulp.api.collab_api import GulpCollab
    from gulp.api.opensearch_api import GulpOpenSearch
    from gulp.api.redis_api import GulpRedis
    from gulp.api.s3_api import GulpS3
    from gulp.api.ws_api import GulpConnectedSockets, GulpRedisBroker
    from gulp.process import GulpProcess

    sockets = MagicMock(
        cancel_all=AsyncMock(side_effect=RuntimeError("socket failure"))
    )
    process = MagicMock(
        close_process_pool=AsyncMock(),
        close_thread_pool=AsyncMock(),
    )
    collab = MagicMock(shutdown=AsyncMock(side_effect=RuntimeError("db failure")))
    opensearch = MagicMock(shutdown=AsyncMock())
    s3 = MagicMock(shutdown=AsyncMock())
    broker = MagicMock(shutdown=AsyncMock())
    redis = MagicMock(
        cleanup_consumers_for_server=AsyncMock(),
        shutdown=AsyncMock(),
    )
    monkeypatch.setattr(GulpConnectedSockets, "get_instance", lambda: sockets)
    monkeypatch.setattr(GulpProcess, "get_instance", lambda: process)
    monkeypatch.setattr(GulpCollab, "get_instance", lambda: collab)
    monkeypatch.setattr(GulpOpenSearch, "get_instance", lambda: opensearch)
    monkeypatch.setattr(GulpS3, "get_instance", lambda: s3)
    monkeypatch.setattr(GulpRedisBroker, "get_instance", lambda: broker)
    monkeypatch.setattr(GulpRedis, "get_instance", lambda: redis)
    monkeypatch.setattr(events, "drain", AsyncMock())

    server = object.__new__(GulpServer)
    server._poll_tasks_task = None
    server.server_id = "server"
    server._unload_extension_plugins = AsyncMock()

    await server._cleanup()

    opensearch.shutdown.assert_awaited_once()
    s3.shutdown.assert_awaited_once()
    broker.shutdown.assert_awaited_once()
    redis.shutdown.assert_awaited_once()
