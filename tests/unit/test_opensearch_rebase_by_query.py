from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.opensearch_api import GulpOpenSearch


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rebase_by_query_rebases_extra_date_fields(monkeypatch):
    os_api = object.__new__(GulpOpenSearch)
    os_api._opensearch = MagicMock()
    os_api._opensearch.search = AsyncMock(
        side_effect=[
            {
                "hits": {
                    "total": {"value": 1},
                    "hits": [{"_id": "doc1", "sort": [1]}],
                }
            },
            {"hits": {"total": {"value": 1}, "hits": []}},
        ]
    )
    os_api._opensearch.update_by_query = AsyncMock(return_value={"updated": 1})

    os_api.datastream_get_field_types = AsyncMock(
        return_value={
            "@timestamp": "date",
            "gulp.timestamp": "long",
            "event.created": "date",
            "event.count": "long",
        }
    )

    monkeypatch.setattr(
        "gulp.api.opensearch_api.GulpConfig.get_instance",
        lambda: MagicMock(opensearch_request_timeout=MagicMock(return_value=0)),
    )
    monkeypatch.setattr(
        "gulp.api.opensearch_api.GulpRequestStats.is_canceled",
        AsyncMock(return_value=False),
    )

    total_hits, total_updated, errors = await os_api.opensearch_rebase_by_query(
        sess=MagicMock(),
        index="test_index",
        offset_msec=1000,
        req_id="req-1",
        fields=["event.created", "event.count"],
    )

    assert total_hits == 1
    assert total_updated == 1
    assert errors == []

    os_api._opensearch.update_by_query.assert_awaited_once()
    update_call = os_api._opensearch.update_by_query.await_args.kwargs
    script_source = update_call["body"]["script"]["source"]
    assert "event.created" in script_source
    assert "event.count" in script_source
    assert "ss.SSSSSSSSSX" in script_source
    assert "nnnnnnnnn" not in script_source
    assert (
        "long value = ((Number)ctx._source['event.count']).longValue();"
        in script_source
    )
    assert "100000000000L" in script_source
    assert "Math.round((double)rebased / (double)unit)" in script_source
    assert update_call["body"]["script"]["params"]["offset_nsec"] == 1000000000


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rebase_by_query_rejects_non_date_extra_fields(monkeypatch):
    os_api = object.__new__(GulpOpenSearch)
    os_api._opensearch = MagicMock()
    os_api._opensearch.search = AsyncMock()
    os_api._opensearch.update_by_query = AsyncMock()

    os_api.datastream_get_field_types = AsyncMock(
        return_value={
            "event.code": "keyword",
        }
    )

    monkeypatch.setattr(
        "gulp.api.opensearch_api.GulpConfig.get_instance",
        lambda: MagicMock(opensearch_request_timeout=MagicMock(return_value=0)),
    )

    with pytest.raises(ValueError, match="fields must be mapped as date"):
        await os_api.opensearch_rebase_by_query(
            sess=MagicMock(),
            index="test_index",
            offset_msec=1000,
            req_id="req-2",
            fields=["event.code"],
        )

    os_api._opensearch.update_by_query.assert_not_called()
