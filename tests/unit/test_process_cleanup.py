"""Process-pool and worker resource cleanup regressions."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.api.redis_api import GulpRedis
from gulp.api.s3_api import GulpS3
from gulp.process import GulpProcess


@pytest.mark.unit
@pytest.mark.asyncio
async def test_forced_process_pool_is_terminated_and_joined(monkeypatch):
    join_released = asyncio.Event()

    class _Pool:
        def __init__(self):
            self.closed = False
            self.terminated = False
            self.joined = False

        def close(self):
            self.closed = True

        async def join(self):
            await join_released.wait()
            self.joined = True

        def terminate(self):
            self.terminated = True
            join_released.set()

    async def _timeout(_awaitable, timeout: float):
        raise asyncio.TimeoutError

    process = GulpProcess()
    pool = _Pool()
    process.process_pool = pool
    monkeypatch.setattr(asyncio, "wait_for", _timeout)

    await process.close_process_pool()

    assert pool.closed is True
    assert pool.terminated is True
    assert pool.joined is True
    assert process.process_pool is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cancelled_process_pool_cleanup_terminates_and_joins():
    join_started = asyncio.Event()
    join_released = asyncio.Event()

    class _Pool:
        def __init__(self):
            self.terminated = False
            self.joined = False

        def close(self):
            pass

        async def join(self):
            join_started.set()
            await join_released.wait()
            self.joined = True

        def terminate(self):
            self.terminated = True
            join_released.set()

    process = GulpProcess()
    pool = _Pool()
    process.process_pool = pool
    cleanup_task = asyncio.create_task(process.close_process_pool())
    await join_started.wait()

    cleanup_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cleanup_task

    print("cancelled process pool cleanup:", pool.terminated, pool.joined)
    assert pool.terminated is True
    assert pool.joined is True
    assert process.process_pool is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_process_pool_shutdown_releases_pending_result_drains():
    cleanup_finished = asyncio.Event()

    class _Pool:
        def close(self):
            pass

        async def join(self):
            pass

    async def _drain() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_finished.set()

    process = GulpProcess()
    process.process_pool = _Pool()
    drain_task = asyncio.create_task(_drain())
    process._worker_result_tasks.add(drain_task)

    await process.close_process_pool()

    assert cleanup_finished.is_set()
    assert process._worker_result_tasks == set()
    assert process.process_pool is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_worker_cleanup_continues_after_failure(monkeypatch):
    process = GulpProcess()
    cancel_bg_tasks = AsyncMock()
    monkeypatch.setattr(process, "cancel_bg_tasks", cancel_bg_tasks)
    process.close_thread_pool = AsyncMock(side_effect=RuntimeError("thread failure"))

    opensearch = MagicMock(shutdown=AsyncMock())
    collab = MagicMock(shutdown=AsyncMock())
    redis = MagicMock(shutdown=AsyncMock())
    s3 = MagicMock(shutdown=AsyncMock())
    monkeypatch.setattr(GulpOpenSearch, "get_instance", lambda: opensearch)
    monkeypatch.setattr(GulpCollab, "get_instance", lambda: collab)
    monkeypatch.setattr(GulpRedis, "get_instance", lambda: redis)
    monkeypatch.setattr(GulpS3, "get_instance", lambda: s3)

    await process.worker_cleanup()

    cancel_bg_tasks.assert_awaited_once()
    opensearch.shutdown.assert_awaited_once()
    collab.shutdown.assert_awaited_once()
    redis.shutdown.assert_awaited_once()
    s3.shutdown.assert_awaited_once()
