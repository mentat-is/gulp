"""Whole-file ingestion upload ownership regressions."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.server import ingest
from gulp.api.server.ingest import GulpIngestPayload
from gulp.structs import GulpPluginParameters


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ingest_waits_for_upload_before_unloading_plugin(monkeypatch):
    release_upload = asyncio.Event()
    ingestion_finished = asyncio.Event()
    upload_task = asyncio.create_task(release_upload.wait())

    async def _finish_ingestion(**_kwargs):
        ingestion_finished.set()

    plugin = SimpleNamespace(
        upload_task=upload_task,
        ingest_file=AsyncMock(return_value=GulpRequestStatus.DONE),
        update_final_stats_and_flush=AsyncMock(side_effect=_finish_ingestion),
        unload=AsyncMock(),
    )

    @asynccontextmanager
    async def _session():
        yield SimpleNamespace(commit=AsyncMock())

    collab = SimpleNamespace(session=_session)
    monkeypatch.setattr(ingest.GulpCollab, "get_instance", lambda: collab)
    monkeypatch.setattr(ingest.GulpPluginBase, "load", AsyncMock(return_value=plugin))

    ingest_task = asyncio.create_task(
        ingest._ingest_file_internal(
            user_id="user",
            req_id="req",
            ws_id="ws",
            operation_id="op",
            context_name="context",
            source_name="source",
            index="index",
            plugin="plugin",
            file_path="/tmp/source",
            payload=GulpIngestPayload(
                plugin_params=GulpPluginParameters(preview_mode=True)
            ),
        )
    )
    await ingestion_finished.wait()

    assert not ingest_task.done()
    plugin.unload.assert_not_awaited()

    release_upload.set()
    status, preview = await ingest_task

    print(status, preview)
    assert status == GulpRequestStatus.DONE
    assert preview == []
    plugin.unload.assert_awaited_once_with()
