"""Focused coverage for OpenSearch bulk-request preparation."""

from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock

import pytest
from opensearchpy.serializer import JSONSerializer

from gulp.api import opensearch_api
from gulp.api.opensearch.filters import (
    GulpDocumentFilterResult,
    GulpIngestionFilter,
)
from gulp.api.opensearch_api import (
    GulpBulkRequestTooLargeError,
    GulpOpenSearch,
    GulpPreparedBulk,
)


def _opensearch_instance(
    client: AsyncMock, max_content_length: int = 100 * 1024 * 1024
) -> GulpOpenSearch:
    """Construct a focused singleton with the real request serializer."""
    client.transport.serializer = JSONSerializer()
    client.nodes.info.return_value = {
        "nodes": {
            "node-1": {"http": {"max_content_length_in_bytes": max_content_length}}
        }
    }
    instance = object.__new__(GulpOpenSearch)
    instance._opensearch = client
    return instance


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
    instance = _opensearch_instance(client)

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
        body=(
            b'{"create":{"_id":"accepted"}}\n'
            b'{"gulp.timestamp":15,"message":"keep"}\n'
        ),
        index="test-index",
        params={},
        headers={
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        },
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_prepare_bulk_counts_exact_utf8_body_and_terminal_newline(
    monkeypatch,
) -> None:
    """Action metadata, UTF-8 bytes, and both newlines count toward the body."""
    client = AsyncMock()
    instance = _opensearch_instance(client)
    config = MagicMock(prometheus_enabled=MagicMock(return_value=False))
    monkeypatch.setattr(
        opensearch_api.GulpConfig,
        "get_instance",
        staticmethod(lambda: config),
    )

    docs = [{"_id": "unicode", "message": "città 🧪\nseconda riga"}]
    prepared = await instance.prepare_bulk_ingest(docs)

    expected = (
        b'{"create":{"_id":"unicode"}}\n'
        + '{"message":"città 🧪\\nseconda riga"}\n'.encode()
    )
    assert prepared.body == expected
    assert prepared.size_bytes == len(expected)
    assert prepared.documents == tuple(docs)
    assert prepared.body.endswith(b"\n")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_prepare_bulk_uses_smallest_node_limit(monkeypatch) -> None:
    """The cached safety check uses the strictest advertised node limit."""
    client = AsyncMock()
    client.transport.serializer = JSONSerializer()
    client.nodes.info.return_value = {
        "nodes": {
            "large": {"http": {"max_content_length_in_bytes": 1000}},
            "small": {"http": {"max_content_length_in_bytes": 40}},
        }
    }
    instance = object.__new__(GulpOpenSearch)
    instance._opensearch = client
    config = MagicMock(prometheus_enabled=MagicMock(return_value=False))
    monkeypatch.setattr(
        opensearch_api.GulpConfig,
        "get_instance",
        staticmethod(lambda: config),
    )

    with pytest.raises(GulpBulkRequestTooLargeError, match="40-byte"):
        await instance.prepare_bulk_ingest([{"_id": "large", "message": "x" * 100}])

    client.bulk.assert_not_awaited()
    assert await instance.bulk_max_content_length_bytes() == 40
    client.nodes.info.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_prepared_bulk_rechecks_exact_body_before_http(monkeypatch) -> None:
    """Submission rejects an oversized prepared body without an HTTP call."""
    client = AsyncMock()
    instance = _opensearch_instance(client, max_content_length=40)
    config = MagicMock(prometheus_enabled=MagicMock(return_value=False))
    monkeypatch.setattr(
        opensearch_api.GulpConfig,
        "get_instance",
        staticmethod(lambda: config),
    )
    prepared = GulpPreparedBulk(
        body=b"x" * 41,
        documents=({"_id": "large"},),
        size_bytes=41,
        max_content_length_bytes=1000,
    )

    with pytest.raises(GulpBulkRequestTooLargeError, match="41 bytes.*40-byte"):
        await instance.bulk_ingest_prepared("test-index", prepared)

    client.bulk.assert_not_awaited()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bulk_limit_discovery_falls_back_once(monkeypatch) -> None:
    """Unavailable Nodes Info uses the documented 100 MiB fallback once."""
    client = AsyncMock()
    client.nodes.info.side_effect = PermissionError("forbidden")
    instance = _opensearch_instance(client)
    client.nodes.info.side_effect = PermissionError("forbidden")

    first = await instance.bulk_max_content_length_bytes()
    second = await instance.bulk_max_content_length_bytes()

    assert first == GulpOpenSearch.DEFAULT_MAX_CONTENT_LENGTH_BYTES
    assert second == first
    client.nodes.info.assert_awaited_once()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_prepared_bulk_retries_pressure_without_reserializing(
    monkeypatch,
) -> None:
    """A transient request failure reuses the exact immutable byte body."""
    docs = [{"_id": "one", "message": "value"}]
    client = AsyncMock()
    client.bulk.side_effect = [
        RuntimeError("pressure"),
        {
            "errors": False,
            "took": 3,
            "items": [{"create": {"_id": "one", "status": 201}}],
        },
    ]
    instance = _opensearch_instance(client)
    config = MagicMock()
    config.opensearch_request_timeout.return_value = 0
    config.ingestion_retry_max.return_value = 2
    config.ingestion_retry_delay.return_value = 0
    config.prometheus_enabled.return_value = False
    monkeypatch.setattr(
        opensearch_api.GulpConfig,
        "get_instance",
        staticmethod(lambda: config),
    )

    prepared = await instance.prepare_bulk_ingest(docs)
    result = await instance.bulk_ingest_prepared("test-index", prepared)

    assert result == (0, docs, 0)
    assert client.bulk.await_count == 2
    assert client.bulk.await_args_list[0].kwargs["body"] is prepared.body
    assert client.bulk.await_args_list[1].kwargs["body"] is prepared.body


@pytest.mark.unit
@pytest.mark.asyncio
async def test_prepared_bulk_aggregates_duplicates_and_failures(
    monkeypatch,
) -> None:
    """Response reconstruction preserves successful source documents in order."""
    docs = [
        {"_id": "ok", "message": "one"},
        {"_id": "duplicate", "message": "two"},
        {"_id": "failed", "message": "three"},
    ]
    client = AsyncMock()
    client.bulk.return_value = {
        "errors": True,
        "items": [
            {"create": {"_id": "ok", "status": 201}},
            {"create": {"_id": "duplicate", "status": 409}},
            {"create": {"_id": "failed", "status": 400, "error": "bad"}},
        ],
    }
    instance = _opensearch_instance(client)
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

    assert await instance.bulk_ingest("test-index", docs) == (
        1,
        [docs[0]],
        1,
    )
