from typing import Any

import pytest
from pydantic import ValidationError

from gulp.api.opensearch.structs import GulpQueryParameters
from gulp.structs import GulpSortOrder


@pytest.mark.unit
def test_query_pagination_parameters_keep_offset_compatibility() -> None:
    options = GulpQueryParameters(limit=50, offset=125_000)

    assert options.pagination_mode == "offset"
    assert options.pit_id is None
    assert options.search_after is None
    assert options.parse()["from"] == 125_000


@pytest.mark.unit
def test_query_pagination_parameters_accept_scalar_cursor_values() -> None:
    cursor: list[Any] = [1710000000000000000, "doc-42", 918273]

    options = GulpQueryParameters(
        pagination_mode="pit",
        pit_id="pit-1",
        offset=50,
        search_after=cursor,
    )

    assert options.search_after == cursor
    parsed = options.parse()
    assert parsed["search_after"] == cursor
    assert parsed["from"] is None


@pytest.mark.unit
def test_query_pagination_parameters_accept_null_cursor() -> None:
    options = GulpQueryParameters(search_after=None)

    assert options.search_after is None


@pytest.mark.unit
def test_query_pagination_parameters_reject_negative_offset() -> None:
    with pytest.raises(ValidationError):
        GulpQueryParameters(offset=-1)


@pytest.mark.unit
def test_query_pagination_parameters_reject_unknown_mode() -> None:
    with pytest.raises(ValidationError):
        GulpQueryParameters(pagination_mode="scroll")


@pytest.mark.unit
def test_query_sort_adds_doc_tie_breaker_only_once() -> None:
    options = GulpQueryParameters(
        sort={
            "@timestamp": GulpSortOrder.ASC,
            "event.sequence": GulpSortOrder.DESC,
        }
    )

    sort = options.parse()["sort"]
    sort_fields = [next(iter(item)) for item in sort]

    assert sort_fields[:2] == ["@timestamp", "event.sequence"]
    assert sort_fields.count("_doc") == 1


@pytest.mark.unit
def test_default_query_sort_keeps_document_tie_breaker_last() -> None:
    sort_fields = [next(iter(item)) for item in GulpQueryParameters().parse()["sort"]]

    assert sort_fields == ["@timestamp", "event.sequence", "_doc"]


@pytest.mark.unit
def test_query_sort_does_not_duplicate_explicit_doc_tie_breaker() -> None:
    options = GulpQueryParameters(
        sort={
            "@timestamp": GulpSortOrder.ASC,
            "_doc": GulpSortOrder.ASC,
        }
    )

    sort = options.parse()["sort"]
    sort_fields = [next(iter(item)) for item in sort]

    assert sort_fields == ["@timestamp", "_doc"]
