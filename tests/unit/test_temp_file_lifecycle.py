"""Temporary file descriptor and failure cleanup regressions."""

import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import BackgroundTasks
from muty.jsend import JSendException
import pytest

from gulp.api.server import query, storage


@pytest.mark.unit
@pytest.mark.asyncio
async def test_export_json_closes_temporary_file_descriptor(monkeypatch, tmp_path):
    real_close = os.close
    real_mkstemp = query.tempfile.mkstemp
    closed: list[int] = []

    def _mkstemp(*args, **kwargs):
        return real_mkstemp(*args, dir=tmp_path, **kwargs)

    def _close(fd: int) -> None:
        closed.append(fd)
        real_close(fd)

    opensearch = SimpleNamespace(search_dsl=AsyncMock())
    monkeypatch.setattr(query.tempfile, "mkstemp", _mkstemp)
    monkeypatch.setattr(query.os, "close", _close)
    monkeypatch.setattr(query.GulpOpenSearch, "get_instance", lambda: opensearch)

    file_path = await query._export_json_internal("index", "op", {}, req_id="req")

    print(file_path)
    assert len(closed) == 1
    assert isinstance(file_path, str)
    os.unlink(file_path)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_storage_download_failure_closes_and_deletes_temporary_file(
    monkeypatch, tmp_path
):
    real_close = os.close
    real_mkstemp = storage.tempfile.mkstemp
    closed: list[int] = []
    created: list[str] = []

    def _mkstemp(*args, **kwargs):
        fd, path = real_mkstemp(*args, dir=tmp_path, **kwargs)
        created.append(path)
        return fd, path

    def _close(fd: int) -> None:
        closed.append(fd)
        real_close(fd)

    @asynccontextmanager
    async def _session():
        yield SimpleNamespace(commit=AsyncMock())

    collab = SimpleNamespace(session=_session)
    server = SimpleNamespace(
        spawn_worker_task=AsyncMock(return_value=RuntimeError("download failed"))
    )
    monkeypatch.setattr(storage.tempfile, "mkstemp", _mkstemp)
    monkeypatch.setattr(storage.os, "close", _close)
    monkeypatch.setattr(storage.GulpCollab, "get_instance", lambda: collab)
    monkeypatch.setattr(storage.GulpServer, "get_instance", lambda: server)
    monkeypatch.setattr(
        storage.GulpOperation,
        "get_by_id_wrapper",
        AsyncMock(return_value=(None, None, None)),
    )

    with pytest.raises(JSendException):
        await storage.storage_file_get_by_id_handler(
            BackgroundTasks(), "token", "op", "object/file", "req"
        )

    print(created)
    assert len(closed) == 1
    assert len(created) == 1
    assert not os.path.exists(created[0])
