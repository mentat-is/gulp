"""PCAP reader lifecycle regressions."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.plugin import GulpPluginBase
from gulp.plugins import pcap
from gulp.structs import GulpPluginParameters


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pcap_reader_closes_when_preview_stops_early(monkeypatch):
    reader_instance = None

    class Reader:
        def __init__(self, _file_path: str):
            nonlocal reader_instance
            reader_instance = self
            self.closed = False

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            self.closed = True

        def __iter__(self):
            return iter([object()])

    async def prepare_plugin(plugin, **kwargs):
        plugin._plugin_params = kwargs["plugin_params"]

    monkeypatch.setattr(GulpPluginBase, "ingest_file", prepare_plugin)
    monkeypatch.setattr(pcap, "PcapReader", Reader)

    plugin = pcap.Plugin("/tmp/pcap.py", "gulp.plugins.pcap")
    plugin.process_record = AsyncMock(return_value=False)
    stats = MagicMock(status="done")

    result = await plugin.ingest_file(
        sess=MagicMock(),
        stats=stats,
        user_id="user",
        req_id="request",
        ws_id="websocket",
        index="index",
        operation_id="operation",
        context_id="context",
        source_id="source",
        file_path="capture.pcap",
        plugin_params=GulpPluginParameters(custom_parameters={}),
    )

    print("pcap reader closed after preview:", reader_instance.closed)
    assert result == "done"
    assert reader_instance.closed is True
    plugin.process_record.assert_awaited_once()
