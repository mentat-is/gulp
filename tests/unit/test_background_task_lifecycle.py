"""Fire-and-forget task lifecycle regressions."""

import asyncio

import pytest

from gulp.api.server_api import GulpServer
from gulp.process import GulpProcess


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
