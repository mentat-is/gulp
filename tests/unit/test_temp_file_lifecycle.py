"""Temporary file descriptor and failure cleanup regressions."""

import asyncio
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
async def test_export_json_uses_caller_owned_temporary_file(monkeypatch, tmp_path):
    opensearch = SimpleNamespace(search_dsl=AsyncMock())
    monkeypatch.setattr(query.GulpOpenSearch, "get_instance", lambda: opensearch)
    file_path = str(tmp_path / "export.json")

    result = await query._export_json_internal(
        "index", "op", {}, file_path, req_id="req"
    )

    print(result)
    assert result == file_path
    assert os.path.exists(file_path)
    os.unlink(file_path)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cancelled_export_deletes_temporary_file_after_worker_finishes(
    monkeypatch, tmp_path
):
    real_mkstemp = query.tempfile.mkstemp
    created: list[str] = []
    cleanup = None

    def _mkstemp(*args, **kwargs):
        fd, path = real_mkstemp(*args, dir=tmp_path, **kwargs)
        created.append(path)
        return fd, path

    @asynccontextmanager
    async def _session():
        yield SimpleNamespace(commit=AsyncMock())

    async def _spawn(*args, cancel_cleanup, **kwargs):
        nonlocal cleanup
        cleanup = cancel_cleanup
        raise asyncio.CancelledError

    collab = SimpleNamespace(session=_session)
    server = SimpleNamespace(spawn_worker_task=_spawn)
    monkeypatch.setattr(query.tempfile, "mkstemp", _mkstemp)
    monkeypatch.setattr(query.GulpCollab, "get_instance", lambda: collab)
    monkeypatch.setattr(query.GulpServer, "get_instance", lambda: server)
    monkeypatch.setattr(
        query.GulpOperation,
        "get_by_id",
        AsyncMock(return_value=SimpleNamespace(index="index")),
    )
    monkeypatch.setattr(query.GulpUserSession, "check_token", AsyncMock())

    with pytest.raises(asyncio.CancelledError):
        await query.query_gulp_export_json_handler(
            BackgroundTasks(),
            "token",
            "operation",
            query.GulpQueryFilter(),
            query.GulpQueryParameters(),
            "req",
        )

    print("cancelled export temporary files:", created)
    assert len(created) == 1
    assert os.path.exists(created[0])
    assert cleanup is not None
    await cleanup()
    assert not os.path.exists(created[0])


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


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cancelled_storage_download_deletes_file_after_worker_finishes(
    monkeypatch, tmp_path
):
    real_mkstemp = storage.tempfile.mkstemp
    created: list[str] = []
    cleanup = None

    def _mkstemp(*args, **kwargs):
        fd, path = real_mkstemp(*args, dir=tmp_path, **kwargs)
        created.append(path)
        return fd, path

    @asynccontextmanager
    async def _session():
        yield SimpleNamespace(commit=AsyncMock())

    async def _spawn(*args, cancel_cleanup, **kwargs):
        nonlocal cleanup
        cleanup = cancel_cleanup
        raise asyncio.CancelledError

    monkeypatch.setattr(storage.tempfile, "mkstemp", _mkstemp)
    monkeypatch.setattr(
        storage.GulpCollab,
        "get_instance",
        lambda: SimpleNamespace(session=_session),
    )
    monkeypatch.setattr(
        storage.GulpServer,
        "get_instance",
        lambda: SimpleNamespace(spawn_worker_task=_spawn),
    )
    monkeypatch.setattr(
        storage.GulpOperation,
        "get_by_id_wrapper",
        AsyncMock(return_value=(None, None, None)),
    )

    with pytest.raises(asyncio.CancelledError):
        await storage.storage_file_get_by_id_handler(
            BackgroundTasks(), "token", "op", "object/file", "req"
        )

    print("cancelled storage temporary files:", created)
    assert len(created) == 1
    assert os.path.exists(created[0])
    assert cleanup is not None
    await cleanup()
    assert not os.path.exists(created[0])
