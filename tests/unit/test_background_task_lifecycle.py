"""Fire-and-forget task lifecycle regressions."""

import asyncio
from types import SimpleNamespace

from aiomultiprocess import Pool as AioProcessPool
import pytest

from gulp.api.server_api import GulpServer
from gulp.config import GulpConfig
from gulp.process import GulpProcess


async def _delayed_worker_result() -> str:
    """Return after the caller has time to cancel its waiter."""
    await asyncio.sleep(0.1)
    return "worker result"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_background_tasks_are_cancelled_and_released(monkeypatch):
    monkeypatch.setattr(GulpServer, "_instance", None)
    process = GulpProcess()
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def _run_forever() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    assert GulpServer.spawn_bg_task(_run_forever(), "lifecycle-test") is True
    await started.wait()
    print("tracked background tasks:", len(process._background_tasks))
    assert GulpServer._instance is None

    await process.cancel_bg_tasks()

    assert stopped.is_set()
    assert process._background_tasks == set()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cancelled_waited_worker_result_is_drained(monkeypatch):
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()
    cleaned = asyncio.Event()

    class _Pool:
        async def apply(self, func, args, kwds):
            started.set()
            await release.wait()
            finished.set()
            return "worker result"

    async def _worker() -> None:
        pass

    async def _cleanup() -> None:
        cleaned.set()

    process = GulpProcess()
    process.process_pool = _Pool()
    monkeypatch.setattr(GulpProcess, "get_instance", staticmethod(lambda: process))
    monkeypatch.setattr(
        GulpConfig,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(prometheus_enabled=lambda: False)),
    )

    waiter = asyncio.create_task(
        GulpServer().spawn_worker_task(
            _worker,
            wait=True,
            cancel_cleanup=_cleanup,
        )
    )
    await started.wait()
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    print("tracked cancelled worker results:", len(process._worker_result_tasks))
    assert len(process._worker_result_tasks) == 1
    release.set()
    await asyncio.wait_for(cleaned.wait(), timeout=1)
    await asyncio.sleep(0)

    assert finished.is_set()
    assert process._worker_result_tasks == set()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cancelled_waiter_does_not_retain_aiomultiprocess_result(monkeypatch):
    process = GulpProcess()
    pool = AioProcessPool(processes=1)
    process.process_pool = pool
    monkeypatch.setattr(GulpProcess, "get_instance", staticmethod(lambda: process))
    monkeypatch.setattr(
        GulpConfig,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(prometheus_enabled=lambda: False)),
    )

    try:
        waiter = asyncio.create_task(
            GulpServer().spawn_worker_task(_delayed_worker_result, wait=True)
        )
        await asyncio.sleep(0.02)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        async def _wait_until_drained() -> None:
            while process._worker_result_tasks:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(_wait_until_drained(), timeout=2)
        print("aiomultiprocess retained results:", pool._results)
        assert pool._results == {}
    finally:
        pool.close()
        await pool.join()
