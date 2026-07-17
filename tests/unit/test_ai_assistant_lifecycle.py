"""AI assistant stream lifecycle regressions."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gulp.api.collab.stats import RequestCanceledError
from gulp.plugins.extension import ai_assistant


class _Response:
    status = 200

    def __init__(self):
        self.content = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def iter_chunked(self, _size: int):
        yield b'data: {"choices":[{"delta":{"content":"chunk"}}]}\n'


class _ClientSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def post(self, *_args, **_kwargs):
        return _Response()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_canceled_ai_stream_stops_reading(monkeypatch):
    session = SimpleNamespace(commit=AsyncMock())

    @asynccontextmanager
    async def _session():
        yield session

    monkeypatch.setattr(
        ai_assistant.GulpCollab,
        "get_instance",
        lambda: SimpleNamespace(session=_session),
    )
    monkeypatch.setattr(
        ai_assistant.GulpRequestStats,
        "is_canceled",
        AsyncMock(side_effect=[False, True]),
    )
    broker = SimpleNamespace(put=AsyncMock())
    monkeypatch.setattr(ai_assistant.GulpRedisBroker, "get_instance", lambda: broker)
    monkeypatch.setattr(ai_assistant.aiohttp, "ClientSession", _ClientSession)
    plugin = SimpleNamespace(
        _api_base_url="http://ai",
        _api_key="key",
        _model="model",
    )

    with pytest.raises(RequestCanceledError):
        await ai_assistant.Plugin._call_ai(
            plugin,
            operation_id="operation",
            ws_id="websocket",
            req_id="request",
            user_id="user",
            prompt="prompt",
        )

    print("AI stream canceled after checks:", session.commit.await_count)
    session.commit.assert_awaited_once()
    broker.put.assert_not_awaited()
