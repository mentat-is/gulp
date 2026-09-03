"""Unit coverage for source-level chunk event optimization and semantics."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.opensearch_api import GulpPreparedBulk
from gulp.events import GulpCoreEvent, GulpEventError, GulpEventResult, events
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginParameters


class _IngestionPlugin(GulpPluginBase):
    def display_name(self) -> str:
        return "test_ingestion_events"

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION


@pytest.fixture
def plugin(monkeypatch) -> _IngestionPlugin:
    from gulp import plugin as plugin_module

    instance = _IngestionPlugin(
        "/tmp/test_ingestion_events.py", "test_ingestion_events"
    )
    instance._sess = MagicMock()
    instance._docs_buffer = [{"event": {"original": "value"}}]
    instance._index = "test-index"
    instance._operation_id = "operation"
    instance._req_id = "request"
    instance._ws_id = "websocket"
    instance._user_id = "user"

    opensearch = MagicMock()

    async def prepare_bulk_ingest(documents, **kwargs):
        return GulpPreparedBulk(
            body=b"prepared\n",
            documents=tuple(documents),
            size_bytes=9,
            max_content_length_bytes=100 * 1024 * 1024,
        )

    async def bulk_ingest_prepared(_index, prepared, **kwargs):
        return 0, list(prepared.documents), 0

    opensearch.prepare_bulk_ingest = AsyncMock(side_effect=prepare_bulk_ingest)
    opensearch.bulk_ingest_prepared = AsyncMock(side_effect=bulk_ingest_prepared)
    monkeypatch.setattr(
        plugin_module.GulpOpenSearch, "get_instance", staticmethod(lambda: opensearch)
    )
    broker = MagicMock()
    broker.put = AsyncMock()
    monkeypatch.setattr(
        plugin_module.GulpRedisBroker, "get_instance", staticmethod(lambda: broker)
    )
    monkeypatch.setattr(GulpRequestStats, "is_canceled", AsyncMock(return_value=False))
    return instance


@pytest.mark.unit
@pytest.mark.asyncio
async def test_no_listener_skips_event_construction_and_keeps_websocket_fanout(
    plugin, monkeypatch
):
    from gulp import plugin as plugin_module

    plugin._event_listener_mask = 0
    constructor = MagicMock(side_effect=AssertionError("event model constructed"))
    emit = AsyncMock(side_effect=AssertionError("event published"))
    monkeypatch.setattr(plugin_module, "GulpChunkPrePostIngestEventData", constructor)
    monkeypatch.setattr(events, "emit", emit)

    skipped, ingested, failed = await plugin._flush_buffer_and_send_to_ws()

    assert (skipped, ingested, failed) == (0, 1, 0)
    constructor.assert_not_called()
    emit.assert_not_awaited()
    plugin_module.GulpRedisBroker.get_instance().put.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pre_only_transforms_before_ingestion(plugin, monkeypatch):
    from gulp import plugin as plugin_module

    plugin._event_listener_mask = events.CHUNK_PRE_INGEST
    transformed = [{"event": {"transformed": True}}]
    emit = AsyncMock(
        return_value=GulpEventResult(
            event_id="event",
            event=GulpCoreEvent.CHUNK_PRE_INGEST,
            result={"chunk": transformed},
        )
    )
    monkeypatch.setattr(events, "emit", emit)

    await plugin._flush_buffer_and_send_to_ws()

    assert plugin._docs_buffer == []
    prepared_documents = (
        plugin_module.GulpOpenSearch.get_instance().prepare_bulk_ingest.await_args.args[
            0
        ]
    )
    assert prepared_documents == transformed
    emit.assert_awaited_once()
    assert emit.await_args.args[0] == GulpCoreEvent.CHUNK_PRE_INGEST
    assert emit.await_args.kwargs["wait"] is True
    assert emit.await_args.kwargs["timeout"] == 30.0


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reply, error",
    [
        (
            GulpEventResult(
                event_id="event",
                event=GulpCoreEvent.CHUNK_PRE_INGEST,
                result={"chunk": "invalid"},
            ),
            ValueError,
        ),
        (GulpEventError("listener failed"), GulpEventError),
        (GulpEventError("timed out"), GulpEventError),
    ],
)
async def test_pre_invalid_error_and_timeout_fail_ingestion(
    plugin, monkeypatch, reply, error
):
    from gulp import plugin as plugin_module

    plugin._event_listener_mask = events.CHUNK_PRE_INGEST
    emit = (
        AsyncMock(side_effect=reply)
        if isinstance(reply, Exception)
        else AsyncMock(return_value=reply)
    )
    monkeypatch.setattr(events, "emit", emit)

    with pytest.raises(error):
        await plugin._flush_buffer_and_send_to_ws()
    plugin_module.GulpOpenSearch.get_instance().prepare_bulk_ingest.assert_not_awaited()
    plugin_module.GulpOpenSearch.get_instance().bulk_ingest_prepared.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_post_only_delivers_full_ingested_chunk(plugin, monkeypatch):
    plugin._event_listener_mask = events.CHUNK_POST_INGEST
    emit = AsyncMock(return_value=None)
    monkeypatch.setattr(events, "emit", emit)

    await plugin._flush_buffer_and_send_to_ws()

    emit.assert_awaited_once()
    assert emit.await_args.args[0] == GulpCoreEvent.CHUNK_POST_INGEST
    assert emit.await_args.kwargs["data"]["chunk"] == [{"event": {"original": "value"}}]
    assert "wait" not in emit.await_args.kwargs


@pytest.mark.unit
@pytest.mark.asyncio
async def test_post_delivery_failure_does_not_fail_ingestion(plugin, monkeypatch):
    plugin._event_listener_mask = events.CHUNK_POST_INGEST
    monkeypatch.setattr(
        events, "emit", AsyncMock(side_effect=RuntimeError("Redis failed"))
    )

    assert await plugin._flush_buffer_and_send_to_ws() == (0, 1, 0)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_listener_changes_are_snapshotted_on_next_source(plugin, monkeypatch):
    plugin._mappings = {"existing": MagicMock()}
    plugin._parse_custom_parameters = AsyncMock()
    plugin._fetch_index_type_mappings = AsyncMock()
    listener_mask = AsyncMock(
        side_effect=[events.CHUNK_PRE_INGEST, events.CHUNK_POST_INGEST]
    )
    monkeypatch.setattr(events, "listener_mask", listener_mask)

    await plugin._initialize(GulpPluginParameters())
    assert plugin._event_listener_mask == events.CHUNK_PRE_INGEST

    await plugin._initialize(GulpPluginParameters())
    assert plugin._event_listener_mask == events.CHUNK_POST_INGEST
