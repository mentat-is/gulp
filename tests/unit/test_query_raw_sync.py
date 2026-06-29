from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.opensearch.structs import GulpQueryParameters
from gulp.api.server.query import _query_raw_sync


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_raw_sync_does_not_raise_on_empty_results(monkeypatch):
    os_api = MagicMock()
    os_api.search_dsl_sync = AsyncMock(return_value=(0, [], [], {}))
    monkeypatch.setattr(
        "gulp.api.server.query.GulpOpenSearch.get_instance",
        lambda: os_api,
    )

    q_options = GulpQueryParameters(preview_mode=True)
    total_hits, docs = await _query_raw_sync(
        MagicMock(),
        {"query": {"match_none": {}}},
        q_options,
        index="test_index",
    )

    assert total_hits == 0
    assert docs == []
    os_api.search_dsl_sync.assert_awaited_once()
    assert os_api.search_dsl_sync.await_args.kwargs["raise_on_error"] is False
