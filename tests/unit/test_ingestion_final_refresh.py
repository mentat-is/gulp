"""Regression coverage for final ingestion refreshes on empty buffers."""

from unittest.mock import AsyncMock, MagicMock

import pytest

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
    monkeypatch.setattr(
        plugin_module.GulpOpenSearch,
        "get_instance",
        staticmethod(lambda: opensearch),
    )

    assert await plugin._flush_buffer_and_send_to_ws(
        wait_for_refresh=wait_for_refresh
    ) == (0, 0, 0)
    if should_refresh:
        opensearch.index_refresh.assert_awaited_once_with("test-index")
    else:
        opensearch.index_refresh.assert_not_awaited()
