"""Regression coverage for final raw-ingestion refresh and stream markers."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.opensearch_api import GulpPreparedBulk
from gulp.plugin import GulpPluginBase, GulpPluginType


class _IngestionPlugin(GulpPluginBase):
    """Minimal ingestion plugin used to exercise the shared flush helper."""

    def display_name(self) -> str:
        return "test_ingestion_final_refresh"

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_ingestion,last_raw_chunk,wait_for_refresh,should_refresh",
    [
        (False, False, True, True),
        (False, False, False, False),
        (True, False, True, False),
        (True, True, True, True),
    ],
)
async def test_empty_buffer_honors_effective_refresh_policy(
    monkeypatch,
    raw_ingestion: bool,
    last_raw_chunk: bool,
    wait_for_refresh: bool,
    should_refresh: bool,
) -> None:
    """Refresh empty final buffers without refreshing intermediate raw chunks."""
    from gulp import plugin as plugin_module

    plugin = _IngestionPlugin(
        "/tmp/test_ingestion_final_refresh.py", "test_ingestion_final_refresh"
    )
    plugin._index = "test-index"
    plugin._raw_ingestion = raw_ingestion
    plugin._last_raw_chunk = last_raw_chunk
    opensearch = MagicMock(index_refresh=AsyncMock())
    redis_broker = MagicMock(put=AsyncMock())
    monkeypatch.setattr(
        plugin_module.GulpOpenSearch,
        "get_instance",
        staticmethod(lambda: opensearch),
    )
    monkeypatch.setattr(
        plugin_module.GulpRedisBroker,
        "get_instance",
        staticmethod(lambda: redis_broker),
    )

    assert await plugin._flush_buffer_and_send_to_ws(
        wait_for_refresh=wait_for_refresh
    ) == (0, 0, 0)
    if should_refresh:
        opensearch.index_refresh.assert_awaited_once_with("test-index")
    else:
        opensearch.index_refresh.assert_not_awaited()

    if raw_ingestion and last_raw_chunk:
        assert redis_broker.put.await_count == 2
        terminal_call = redis_broker.put.await_args_list[0].kwargs
        assert terminal_call["t"] == plugin_module.WSDATA_DOCUMENTS_CHUNK
        assert terminal_call["d"]["docs"] == []
        assert terminal_call["d"]["chunk_size"] == 0
        assert terminal_call["d"]["last"] is True
        progress_call = redis_broker.put.await_args_list[1].kwargs
        assert progress_call["t"] == plugin_module.WSDATA_INGEST_RAW_PROGRESS
        assert progress_call["d"]["last"] is True
    else:
        redis_broker.put.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_last_raw_chunk_emits_terminal_packet_when_all_docs_are_skipped(
    monkeypatch,
) -> None:
    """Complete the stream when OpenSearch accepts no document from the batch."""
    from gulp import plugin as plugin_module

    plugin = _IngestionPlugin(
        "/tmp/test_ingestion_final_refresh.py", "test_ingestion_final_refresh"
    )
    plugin._index = "test-index"
    plugin._raw_ingestion = True
    plugin._last_raw_chunk = True
    plugin._docs_buffer = [{"_id": "skipped"}]

    opensearch = MagicMock(
        prepare_bulk_ingest=AsyncMock(
            return_value=GulpPreparedBulk(
                body=b"prepared\n",
                documents=tuple(plugin._docs_buffer),
                size_bytes=9,
                max_content_length_bytes=100 * 1024 * 1024,
            )
        ),
        bulk_ingest_prepared=AsyncMock(return_value=(1, [], 0)),
    )
    redis_broker = MagicMock(put=AsyncMock())
    monkeypatch.setattr(
        plugin_module.GulpOpenSearch,
        "get_instance",
        staticmethod(lambda: opensearch),
    )
    monkeypatch.setattr(
        plugin_module.GulpRedisBroker,
        "get_instance",
        staticmethod(lambda: redis_broker),
    )
    monkeypatch.setattr(
        plugin_module.GulpRequestStats,
        "is_canceled",
        AsyncMock(return_value=False),
    )

    assert await plugin._flush_buffer_and_send_to_ws() == (1, 0, 0)
    assert redis_broker.put.await_count == 2
    terminal_call = redis_broker.put.await_args_list[0].kwargs
    assert terminal_call["t"] == plugin_module.WSDATA_DOCUMENTS_CHUNK
    assert terminal_call["d"]["docs"] == []
    assert terminal_call["d"]["last"] is True
