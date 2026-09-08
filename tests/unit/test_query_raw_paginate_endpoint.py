from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import orjson
import pytest

from gulp.api.opensearch.structs import GulpQueryParameters
from gulp.api.opensearch_api import GulpPaginatedSearchResult
from gulp.api.server import query as query_api


class _SessionContext:
    def __init__(self, session: MagicMock) -> None:
        self.session = session

    async def __aenter__(self) -> MagicMock:
        return self.session

    async def __aexit__(self, exc_type, exc, traceback) -> bool:
        return False


def _mock_acl(monkeypatch) -> tuple[MagicMock, AsyncMock]:
    session = MagicMock(commit=AsyncMock())
    collab = MagicMock()
    collab.session.return_value = _SessionContext(session)
    check_token = AsyncMock(return_value="user-1")
    monkeypatch.setattr(query_api.GulpCollab, "get_instance", lambda: collab)
    monkeypatch.setattr(
        query_api.GulpOperation,
        "get_by_id",
        AsyncMock(return_value=SimpleNamespace(index="operation-index")),
    )
    monkeypatch.setattr(query_api.GulpUserSession, "check_token", check_token)
    return session, check_token


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_raw_paginate_shallow_offset_keeps_legacy_path(
    monkeypatch,
) -> None:
    _, check_token = _mock_acl(monkeypatch)
    query_raw_sync = AsyncMock(return_value=(123, [{"_id": "doc-1"}]))
    query_paginated = AsyncMock()
    monkeypatch.setattr(query_api, "_query_raw_sync", query_raw_sync)
    monkeypatch.setattr(query_api, "_query_raw_paginated", query_paginated)

    response = await query_api.query_raw_paginate_handler(
        token="token",
        operation_id="operation-1",
        q={"query": {"match_all": {}}},
        q_options=GulpQueryParameters(limit=25, offset=50),
        req_id="req-1",
    )
    payload = orjson.loads(response.body)

    assert payload["data"] == {
        "total_hits": 123,
        "docs": [{"_id": "doc-1"}],
    }
    query_raw_sync.assert_awaited_once()
    query_paginated.assert_not_awaited()
    check_token.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_raw_paginate_deep_offset_uses_temporary_pit(
    monkeypatch,
) -> None:
    _mock_acl(monkeypatch)
    query_raw_sync = AsyncMock()
    query_paginated = AsyncMock(
        return_value=GulpPaginatedSearchResult(
            total_hits=20_000,
            docs=[{"_id": "doc-10000"}],
            pit_id=None,
            search_after=[10_000, 10_000],
        )
    )
    monkeypatch.setattr(query_api, "_query_raw_sync", query_raw_sync)
    monkeypatch.setattr(query_api, "_query_raw_paginated", query_paginated)
    options = GulpQueryParameters(limit=25, offset=9_990)

    response = await query_api.query_raw_paginate_handler(
        token="token",
        operation_id="operation-1",
        q={"query": {"match_all": {}}},
        q_options=options,
        req_id="req-1",
    )
    payload = orjson.loads(response.body)

    assert payload["data"] == {
        "total_hits": 20_000,
        "docs": [{"_id": "doc-10000"}],
    }
    query_raw_sync.assert_not_awaited()
    query_paginated.assert_awaited_once()
    assert query_paginated.await_args.args[3] == "operation-index"
    assert query_paginated.await_args.kwargs == {"keep_pit": False}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_raw_paginate_pit_mode_returns_session_and_cursor(
    monkeypatch,
) -> None:
    _mock_acl(monkeypatch)
    query_paginated = AsyncMock(
        return_value=GulpPaginatedSearchResult(
            total_hits=123,
            docs=[{"_id": "doc-1"}],
            pit_id="pit-1",
            search_after=[1, "doc-1", 7],
        )
    )
    monkeypatch.setattr(query_api, "_query_raw_sync", AsyncMock())
    monkeypatch.setattr(query_api, "_query_raw_paginated", query_paginated)
    options = GulpQueryParameters(
        pagination_mode="pit",
        limit=25,
        offset=0,
    )

    response = await query_api.query_raw_paginate_handler(
        token="token",
        operation_id="operation-1",
        q={"query": {"match_all": {}}},
        q_options=options,
        req_id="req-1",
    )
    payload = orjson.loads(response.body)

    assert payload["data"] == {
        "total_hits": 123,
        "docs": [{"_id": "doc-1"}],
        "pit_id": "pit-1",
        "search_after": [1, "doc-1", 7],
    }
    query_paginated.assert_awaited_once()
    assert query_paginated.await_args.kwargs == {"keep_pit": True}


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_raw_paginate_close_applies_acl_and_deletes_pit(
    monkeypatch,
) -> None:
    session, check_token = _mock_acl(monkeypatch)
    os_api = MagicMock(delete_pit=AsyncMock(return_value=True))
    monkeypatch.setattr(query_api.GulpOpenSearch, "get_instance", lambda: os_api)

    response = await query_api.query_raw_paginate_close_handler(
        token="token",
        operation_id="operation-1",
        pit_id="pit-1",
        req_id="req-1",
    )
    payload = orjson.loads(response.body)

    assert payload["data"] == {"closed": True}
    check_token.assert_awaited_once()
    session.commit.assert_awaited_once()
    os_api.delete_pit.assert_awaited_once_with("pit-1")
