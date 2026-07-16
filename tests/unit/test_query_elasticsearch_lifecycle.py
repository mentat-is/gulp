"""External search client lifecycle regressions."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from gulp.api.opensearch.structs import GulpQueryParameters
from gulp.plugin import GulpPluginBase
from gulp.plugins import query_elasticsearch
from gulp.plugins.query_elasticsearch import Plugin
from gulp.structs import GulpPluginParameters


@pytest.mark.unit
@pytest.mark.asyncio
async def test_invalid_external_query_is_rejected_before_client_creation(monkeypatch):
    plugin = object.__new__(Plugin)
    plugin._plugin_params = SimpleNamespace(
        custom_parameters={
            "is_elasticsearch": False,
            "uri": "http://localhost:9200",
            "username": "user",
            "password": "password",
            "index": "index",
            "source_field": "source",
            "source_type": "source_name",
            "context_type": "context_name",
        }
    )
    plugin.selected_mapping = lambda: SimpleNamespace(fields={"@timestamp": object()})
    client_constructor = Mock()
    monkeypatch.setattr(
        GulpPluginBase,
        "query_external",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(query_elasticsearch, "AsyncOpenSearch", client_constructor)

    with pytest.raises(TypeError, match="unsupported query payload type"):
        await plugin.query_external(
            sess=object(),
            stats=object(),
            user_id="user",
            req_id="request",
            ws_id="websocket",
            operation_id="operation",
            q=object(),
            index="index",
            plugin_params=GulpPluginParameters(),
            q_options=GulpQueryParameters(),
        )

    print("external client constructor calls:", client_constructor.call_count)
    client_constructor.assert_not_called()
