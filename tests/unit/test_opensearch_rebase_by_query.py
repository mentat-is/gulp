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
    assert update_call["body"]["script"]["params"]["rebase_req_id"] == "req-1"
    assert "gulp.rebase_req_ids" in script_source
    assert "ctx.op = 'noop';" in script_source
    assert ".contains(params.rebase_req_id)" in script_source


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rebase_by_query_script_marks_request_before_extra_fields(monkeypatch):
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
    os_api._opensearch.update_by_query = AsyncMock(return_value={"updated": 0})
    os_api.datastream_get_field_types = AsyncMock(
        return_value={
            "@timestamp": "date",
            "gulp.timestamp": "long",
            "event.created": "date",
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

    await os_api.opensearch_rebase_by_query(
        sess=MagicMock(),
        index="test_index",
        offset_msec=1000,
        req_id="req-duplicate",
        fields=["event.created"],
    )

    update_call = os_api._opensearch.update_by_query.await_args.kwargs
    script_source = update_call["body"]["script"]["source"]
    noop_idx = script_source.index("ctx.op = 'noop';")
    extra_field_idx = script_source.index("ctx._source['event.created']")
    marker_add_idx = script_source.index(
        "ctx._source['gulp.rebase_req_ids'].add(params.rebase_req_id);"
    )

    assert noop_idx < extra_field_idx
    assert marker_add_idx < extra_field_idx
    assert update_call["body"]["script"]["params"]["rebase_req_id"] == "req-duplicate"


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
