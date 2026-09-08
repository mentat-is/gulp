from unittest.mock import AsyncMock, MagicMock, call

from opensearchpy import NotFoundError, TransportError
import pytest

from gulp.api.opensearch.structs import GulpQueryParameters
from gulp.api.opensearch_api import GulpOpenSearch, GulpPITSearchError
from gulp.structs import GulpSortOrder, PaginationSnapshotExpired


def _hit(sequence: int) -> dict:
    return {
        "_id": f"doc-{sequence}",
        "_source": {"event.sequence": sequence},
        "sort": [sequence, sequence],
    }


def _response(
    *,
    total: int = 0,
    hits: list[dict] | None = None,
    pit_id: str | None = None,
) -> dict:
    response: dict = {
        "hits": {
            "total": {"value": total, "relation": "eq"},
            "hits": hits or [],
        }
    }
    if pit_id is not None:
        response["pit_id"] = pit_id
    return response


@pytest.fixture
def pagination_config(monkeypatch) -> MagicMock:
    config = MagicMock()
    config.query_pagination_pit_keep_alive.return_value = "5m"
    config.opensearch_request_timeout.return_value = 0
    config.query_circuit_breaker_backoff_attempts.return_value = 0
    config.query_circuit_breaker_min_limit.return_value = 1
    monkeypatch.setattr(
        "gulp.api.opensearch_api.GulpConfig.get_instance", lambda: config
    )
    return config


@pytest.mark.unit
def test_pit_sort_replaces_document_sort_with_one_shard_tie_breaker() -> None:
    sort = [
        {"@timestamp": {"order": "asc"}},
        {"event.sequence": {"order": "desc"}},
        {"_doc": {"order": "desc"}},
        {"_shard_doc": {"order": "asc"}},
    ]

    result = GulpOpenSearch._pit_sort(sort)

    assert result == [
        {"@timestamp": {"order": "asc"}},
        {"event.sequence": {"order": "desc"}},
        {"_shard_doc": {"order": "asc"}},
    ]


@pytest.mark.unit
def test_reverse_sort_inverts_every_requested_direction() -> None:
    assert GulpOpenSearch._reverse_sort(
        [
            {"@timestamp": {"order": "asc", "unmapped_type": "date"}},
            {"event.sequence": "desc"},
            {"_shard_doc": {"order": "asc"}},
        ]
    ) == [
        {"@timestamp": {"order": "desc", "unmapped_type": "date"}},
        {"event.sequence": "asc"},
        {"_shard_doc": {"order": "desc"}},
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_deep_forward_seek_is_chunked_and_temporary_pit_is_closed(
    pagination_config,
) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api.create_pit = AsyncMock(return_value="pit-new")
    os_api._delete_pit_best_effort = AsyncMock()
    os_api._search_pit = AsyncMock(
        side_effect=[
            _response(total=50_000, pit_id="pit-count"),
            _response(
                hits=[{"sort": [10_000, 10_000]}] * 10_000,
                pit_id="pit-seek-1",
            ),
            _response(
                hits=[{"sort": [20_000, 20_000]}] * 10_000,
                pit_id="pit-seek-2",
            ),
            _response(
                hits=[{"sort": [20_005, 20_005]}] * 5,
                pit_id="pit-seek-3",
            ),
            _response(
                hits=[_hit(sequence) for sequence in range(20_005, 20_015)],
                pit_id="pit-final",
            ),
        ]
    )
    options = GulpQueryParameters(
        limit=10,
        offset=20_005,
        fields="*",
        sort={"event.sequence": GulpSortOrder.ASC},
    )

    result = await os_api.search_dsl_paginated(
        "operation-index", {"query": {"match_all": {}}}, options, keep_pit=False
    )

    assert result.total_hits == 50_000
    assert [doc["event.sequence"] for doc in result.docs] == list(range(20_005, 20_015))
    assert result.pit_id is None
    assert result.search_after == [20_014, 20_014]
    os_api.create_pit.assert_awaited_once_with("operation-index", "5m")
    os_api._delete_pit_best_effort.assert_awaited_once_with("pit-final")

    calls = os_api._search_pit.await_args_list
    assert [search.args[0] for search in calls] == [
        "pit-new",
        "pit-count",
        "pit-seek-1",
        "pit-seek-2",
        "pit-seek-3",
    ]
    assert calls[0].kwargs == {
        "size": 0,
        "sort": None,
        "include_source": False,
        "track_total_hits": True,
    }
    assert [seek.kwargs["size"] for seek in calls[1:4]] == [10_000, 10_000, 5]
    assert all(seek.kwargs["include_source"] is False for seek in calls[1:4])
    assert all(seek.kwargs["track_total_hits"] is False for seek in calls[1:4])
    assert calls[4].kwargs["size"] == 10
    assert calls[4].kwargs["include_source"] is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_jump_near_end_seeks_in_reverse_and_restores_requested_order(
    pagination_config,
) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api.create_pit = AsyncMock()
    os_api._delete_pit_best_effort = AsyncMock()
    os_api._search_pit = AsyncMock(
        side_effect=[
            _response(total=100),
            _response(hits=[_hit(sequence) for sequence in range(99, 94, -1)]),
            _response(hits=[_hit(sequence) for sequence in range(94, 89, -1)]),
        ]
    )
    options = GulpQueryParameters(
        pagination_mode="pit",
        pit_id="pit-existing",
        limit=5,
        offset=90,
        fields="*",
        sort={"event.sequence": GulpSortOrder.ASC},
    )

    result = await os_api.search_dsl_paginated(
        "operation-index", {"query": {"match_all": {}}}, options, keep_pit=True
    )

    assert result.total_hits == 100
    assert [doc["event.sequence"] for doc in result.docs] == [90, 91, 92, 93, 94]
    assert result.pit_id == "pit-existing"
    assert result.search_after == [94, 94]
    os_api.create_pit.assert_not_awaited()
    os_api._delete_pit_best_effort.assert_not_awaited()

    reverse_sort = [
        {"event.sequence": {"order": "desc"}},
        {"_shard_doc": {"order": "desc"}},
    ]
    assert os_api._search_pit.await_args_list[1].kwargs["sort"] == reverse_sort
    assert os_api._search_pit.await_args_list[2].kwargs["sort"] == reverse_sort


@pytest.mark.unit
@pytest.mark.asyncio
async def test_known_cursor_reuses_pit_in_one_search(pagination_config) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api.create_pit = AsyncMock()
    os_api._delete_pit_best_effort = AsyncMock()
    os_api._search_pit = AsyncMock(
        return_value=_response(
            total=100,
            hits=[_hit(sequence) for sequence in range(10, 20)],
            pit_id="pit-rotated",
        )
    )
    cursor = [9, 9]
    options = GulpQueryParameters(
        pagination_mode="pit",
        pit_id="pit-existing",
        search_after=cursor,
        limit=10,
        offset=10,
        fields="*",
        sort={"event.sequence": GulpSortOrder.ASC},
    )

    result = await os_api.search_dsl_paginated(
        "operation-index", {"query": {"match_all": {}}}, options, keep_pit=True
    )

    assert os_api._search_pit.await_count == 1
    assert os_api._search_pit.await_args.kwargs["search_after"] == cursor
    assert os_api._search_pit.await_args.kwargs["track_total_hits"] is True
    assert result.total_hits == 100
    assert result.pit_id == "pit-rotated"
    assert [doc["event.sequence"] for doc in result.docs] == list(range(10, 20))


@pytest.mark.unit
@pytest.mark.asyncio
async def test_paginated_search_honors_total_limit(pagination_config) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api.create_pit = AsyncMock(return_value="pit-new")
    os_api._delete_pit_best_effort = AsyncMock()
    os_api._search_pit = AsyncMock(
        return_value=_response(
            total=100,
            hits=[_hit(sequence) for sequence in range(3)],
            pit_id="pit-final",
        )
    )
    options = GulpQueryParameters(limit=10, total_limit=3, offset=0)

    result = await os_api.search_dsl_paginated(
        "operation-index", {"query": {"match_all": {}}}, options, keep_pit=False
    )

    assert os_api._search_pit.await_args.kwargs["size"] == 3
    assert len(result.docs) == 3


@pytest.mark.unit
@pytest.mark.asyncio
async def test_offset_beyond_last_hit_retains_exact_total(pagination_config) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api.create_pit = AsyncMock()
    os_api._delete_pit_best_effort = AsyncMock()
    os_api._search_pit = AsyncMock(return_value=_response(total=123))
    options = GulpQueryParameters(
        pagination_mode="pit",
        pit_id="pit-existing",
        limit=10,
        offset=10_000,
    )

    result = await os_api.search_dsl_paginated(
        "operation-index", {"query": {"match_all": {}}}, options, keep_pit=True
    )

    assert result.total_hits == 123
    assert result.docs == []
    assert result.search_after is None
    assert result.pit_id == "pit-existing"
    assert os_api._search_pit.await_count == 1
    os_api._delete_pit_best_effort.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_new_pit_is_cleaned_up_when_search_fails(pagination_config) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api.create_pit = AsyncMock(return_value="pit-new")
    os_api._delete_pit_best_effort = AsyncMock()
    os_api._search_pit = AsyncMock(
        side_effect=PaginationSnapshotExpired("search failed", pit_id="pit-rotated")
    )
    options = GulpQueryParameters(
        pagination_mode="pit",
        limit=10,
        offset=0,
    )

    with pytest.raises(PaginationSnapshotExpired, match="search failed"):
        await os_api.search_dsl_paginated(
            "operation-index", {"query": {"match_all": {}}}, options, keep_pit=True
        )

    os_api._delete_pit_best_effort.assert_awaited_once_with("pit-rotated")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_expired_reused_pit_is_cleaned_up(pagination_config) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api.create_pit = AsyncMock()
    os_api._delete_pit_best_effort = AsyncMock()
    os_api._search_pit = AsyncMock(
        side_effect=PaginationSnapshotExpired(
            "search failed", pit_id="pit-rotated"
        )
    )
    options = GulpQueryParameters(
        pagination_mode="pit",
        pit_id="pit-current",
        limit=10,
        offset=0,
    )

    with pytest.raises(PaginationSnapshotExpired):
        await os_api.search_dsl_paginated(
            "operation-index", {"query": {"match_all": {}}}, options, keep_pit=True
        )

    os_api.create_pit.assert_not_awaited()
    os_api._delete_pit_best_effort.assert_awaited_once_with("pit-rotated")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pit_create_and_delete_use_opensearch_contract(
    pagination_config,
) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api._opensearch = MagicMock()
    os_api._opensearch.create_pit = AsyncMock(
        return_value={
            "pit_id": "pit-1",
            "_shards": {"total": 1, "successful": 1, "failed": 0},
        }
    )
    os_api._opensearch.delete_pit = AsyncMock(
        return_value={"pits": [{"pit_id": "pit-1", "successful": True}]}
    )

    pit_id = await os_api.create_pit("operation-index", "2m")
    closed = await os_api.delete_pit(pit_id)

    assert pit_id == "pit-1"
    assert closed is True
    assert os_api._opensearch.create_pit.await_args == call(
        index="operation-index",
        params={
            "keep_alive": "2m",
            "allow_partial_pit_creation": "false",
        },
        headers={"content-type": "application/json"},
    )
    assert os_api._opensearch.delete_pit.await_args == call(
        body={"pit_id": ["pit-1"]},
        headers={"content-type": "application/json"},
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_partial_pit_creation_is_closed_and_rejected(
    pagination_config,
) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api._opensearch = MagicMock()
    os_api._opensearch.create_pit = AsyncMock(
        return_value={
            "pit_id": "pit-partial",
            "_shards": {"total": 2, "successful": 1, "failed": 1},
        }
    )
    os_api._delete_pit_best_effort = AsyncMock()

    with pytest.raises(RuntimeError, match="partial PIT"):
        await os_api.create_pit("operation-index", "2m")

    os_api._delete_pit_best_effort.assert_awaited_once_with("pit-partial")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_unsuccessful_pit_delete_is_reported(pagination_config) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api._opensearch = MagicMock()
    os_api._opensearch.delete_pit = AsyncMock(
        return_value={"pits": [{"pit_id": "pit-1", "successful": False}]}
    )

    with pytest.raises(RuntimeError, match="failed to close"):
        await os_api.delete_pit("pit-1")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pit_search_removes_from_and_maps_missing_snapshot(
    pagination_config,
) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api._opensearch = MagicMock()
    os_api._opensearch.search = AsyncMock(
        side_effect=NotFoundError(404, "point in time missing", {})
    )

    with pytest.raises(PaginationSnapshotExpired):
        await os_api._search_pit(
            "pit-expired",
            "5m",
            {
                "query": {"match_all": {}},
                "from": 42,
                "search_after": [1],
            },
            size=10,
            sort=[{"_shard_doc": {"order": "asc"}}],
            search_after=[2],
            include_source=False,
        )

    body = os_api._opensearch.search.await_args.kwargs["body"]
    assert "from" not in body
    assert body["search_after"] == [2]
    assert body["_source"] is False
    assert body["pit"] == {"id": "pit-expired", "keep_alive": "5m"}
    assert os_api._opensearch.search.await_args.kwargs["params"] == {
        "allow_partial_search_results": "false"
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pit_partial_shard_context_loss_maps_to_snapshot_expired(
    pagination_config,
) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api._opensearch = MagicMock()
    os_api._opensearch.search = AsyncMock(
        return_value={
            "pit_id": "pit-rotated",
            "_shards": {
                "total": 2,
                "successful": 1,
                "failed": 1,
                "failures": [
                    {
                        "reason": {
                            "type": "search_context_missing_exception",
                            "reason": "No search context found for id [42]",
                        }
                    }
                ],
            },
            "hits": {"total": {"value": 1}, "hits": [_hit(1)]},
        }
    )

    with pytest.raises(PaginationSnapshotExpired) as exc_info:
        await os_api._search_pit(
            "pit-partially-expired",
            "5m",
            {"query": {"match_all": {}}},
            size=10,
            sort=[{"_shard_doc": {"order": "asc"}}],
        )

    assert exc_info.value.pit_id == "pit-rotated"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pit_search_rejects_non_expiry_shard_failures(
    pagination_config,
) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api._opensearch = MagicMock()
    os_api._opensearch.search = AsyncMock(
        return_value={
            "pit_id": "pit-rotated",
            "_shards": {
                "total": 2,
                "successful": 1,
                "failed": 1,
                "failures": [{"reason": {"type": "query_shard_exception"}}],
            },
            "hits": {"total": {"value": 1}, "hits": [_hit(1)]},
        }
    )

    with pytest.raises(GulpPITSearchError, match="failed on 1 shards") as exc_info:
        await os_api._search_pit(
            "pit-current",
            "5m",
            {"query": {"match_all": {}}},
            size=10,
            sort=[{"_shard_doc": {"order": "asc"}}],
        )

    assert exc_info.value.pit_id == "pit-rotated"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pit_transport_missing_context_maps_to_snapshot_expired(
    pagination_config,
) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api._opensearch = MagicMock()
    os_api._opensearch.search = AsyncMock(
        side_effect=TransportError(
            500,
            "search failed",
            {
                "pit_id": "pit-rotated",
                "error": {"type": "search_context_missing_exception"},
            },
        )
    )

    with pytest.raises(PaginationSnapshotExpired) as exc_info:
        await os_api._search_pit(
            "pit-current",
            "5m",
            {"query": {"match_all": {}}},
            size=10,
            sort=[{"_shard_doc": {"order": "asc"}}],
        )

    assert exc_info.value.pit_id == "pit-rotated"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_regular_search_after_removes_raw_query_from(
    pagination_config,
) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api._opensearch = MagicMock()
    os_api._opensearch.search = AsyncMock(
        return_value=_response(total=2, hits=[_hit(2)])
    )

    total_hits, docs, cursor, _ = await os_api._search_dsl_internal(
        "operation-index",
        {
            "sort": [{"event.sequence": {"order": "asc"}}],
            "size": 1,
            "search_after": [1, 1],
            "from": None,
        },
        {"query": {"match_all": {}}, "from": 9_999},
    )

    body = os_api._opensearch.search.await_args.kwargs["body"]
    assert "from" not in body
    assert body["search_after"] == [1, 1]
    assert total_hits == 2
    assert docs[0]["event.sequence"] == 2
    assert cursor == [2, 2]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_regular_offset_beyond_hits_retains_exact_total(
    pagination_config,
) -> None:
    os_api = object.__new__(GulpOpenSearch)
    os_api._opensearch = MagicMock()
    os_api._opensearch.search = AsyncMock(return_value=_response(total=5))

    total_hits, docs, cursor, aggregations = await os_api._search_dsl_internal(
        "operation-index",
        {
            "sort": [{"event.sequence": {"order": "asc"}}],
            "size": 10,
            "search_after": None,
            "from": 100,
        },
        {"query": {"match_all": {}}},
    )

    assert total_hits == 5
    assert docs == []
    assert cursor == []
    assert aggregations == {}
