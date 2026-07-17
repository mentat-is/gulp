"""MBOX parser lifecycle regressions."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.plugin import GulpPluginBase
from gulp.plugins import mbox
from gulp.structs import GulpPluginParameters


@pytest.mark.unit
@pytest.mark.asyncio
async def test_mbox_closes_parser_and_unloads_eml_on_failure(monkeypatch):
    class Mailbox:
        def __init__(self, _file_path: str):
            self.closed = False

        def itervalues(self):
            return iter([object()])

        def close(self):
            self.closed = True

    mailbox = Mailbox("mail.mbox")
    eml_parser = SimpleNamespace(unload=AsyncMock())

    async def prepare_plugin(plugin, **kwargs):
        plugin._plugin_params = kwargs["plugin_params"]

    monkeypatch.setattr(GulpPluginBase, "ingest_file", prepare_plugin)
    monkeypatch.setattr(mbox.mailbox, "mbox", lambda _path: mailbox)

    plugin = mbox.Plugin("/tmp/mbox.py", "gulp.plugins.mbox")
    plugin.load_plugin_direct = AsyncMock(return_value=eml_parser)
    plugin.process_record = AsyncMock(side_effect=RuntimeError("parse failed"))

    with pytest.raises(RuntimeError, match="parse failed"):
        await plugin.ingest_file(
            sess=MagicMock(),
            stats=MagicMock(status="failed"),
            user_id="user",
            req_id="request",
            ws_id="websocket",
            index="index",
            operation_id="operation",
            context_id="context",
            source_id="source",
            file_path="mail.mbox",
            plugin_params=GulpPluginParameters(store_file=True),
        )

    print("mbox cleanup after parser failure:", mailbox.closed)
    assert mailbox.closed is True
    eml_parser.unload.assert_awaited_once()
    assert (
        plugin.load_plugin_direct.await_args.kwargs["plugin_params"].store_file is False
    )
