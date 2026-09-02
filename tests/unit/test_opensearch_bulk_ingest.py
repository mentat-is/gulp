"""Focused coverage for OpenSearch bulk-request preparation."""

from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api import opensearch_api
from gulp.api.opensearch.filters import (
    GulpDocumentFilterResult,
    GulpIngestionFilter,
)
from gulp.api.opensearch_api import GulpOpenSearch


@pytest.mark.unit
@pytest.mark.parametrize(
    ("time_range", "timestamp", "expected"),
    [
        ((10, 20), 5, GulpDocumentFilterResult.SKIP),
        ((10, 20), 10, GulpDocumentFilterResult.ACCEPT),
        ((10, 20), 15, GulpDocumentFilterResult.ACCEPT),
        ((10, 20), 20, GulpDocumentFilterResult.ACCEPT),
        ((10, 20), 25, GulpDocumentFilterResult.SKIP),
        ((10, 0), 9, GulpDocumentFilterResult.SKIP),
        ((10, 0), 10, GulpDocumentFilterResult.ACCEPT),
        ((0, 20), 20, GulpDocumentFilterResult.ACCEPT),
        ((0, 20), 21, GulpDocumentFilterResult.SKIP),
        ((0, 0), 5, GulpDocumentFilterResult.ACCEPT),
    ],
)
def test_ingestion_time_range_cases(
    time_range: tuple[int, int],
    timestamp: int,
    expected: GulpDocumentFilterResult,
) -> None:
    """Full and one-sided ranges accept only their inclusive bounds."""
    result = GulpIngestionFilter.filter_doc_for_ingestion(
        {"gulp.timestamp": timestamp},
        GulpIngestionFilter(time_range=time_range),
    )

    assert result == expected


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bulk_ingest_filters_while_building_and_preserves_input(
    monkeypatch,
) -> None:
    """Filtering and source preparation share one pass without mutating docs."""
    docs = [
        {"_id": "too-early", "gulp.timestamp": 5, "message": "skip"},
        {"_id": "accepted", "gulp.timestamp": 15, "message": "keep"},
    ]
    original_docs = deepcopy(docs)

    client = AsyncMock()
    client.bulk.return_value = {
        "errors": False,
        "items": [{"create": {"_id": "accepted", "status": 201}}],
    }
    instance = object.__new__(GulpOpenSearch)
    instance._opensearch = client

    config = MagicMock()
    config.opensearch_request_timeout.return_value = 0
    config.ingestion_retry_max.return_value = 1
    config.ingestion_retry_delay.return_value = 0
    config.prometheus_enabled.return_value = False
    monkeypatch.setattr(
        opensearch_api.GulpConfig,
        "get_instance",
        staticmethod(lambda: config),
    )

    result = await instance.bulk_ingest(
        "test-index",
        docs,
        flt=GulpIngestionFilter(time_range=(10, 20)),
    )

    assert result == (0, [original_docs[1]], 0)
    assert docs == original_docs
    client.bulk.assert_awaited_once_with(
        body=[
            {"create": {"_id": "accepted"}},
            {"gulp.timestamp": 15, "message": "keep"},
        ],
        index="test-index",
        params={},
        headers={
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        },
    )
