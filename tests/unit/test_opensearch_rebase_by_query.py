from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.opensearch.structs import GulpQueryParameters
from gulp.api.opensearch_api import GulpOpenSearch


@pytest.mark.unit
def test_opensearch_client_uses_pool_and_compression_config(monkeypatch):
    captured = {}
    fake_client = object()

    class _FakeConfig:
        def opensearch_url(self):
            return "https://admin:secret@opensearch.example.com:9200/prefix"

        def opensearch_pool_maxsize(self):
            return 4

    def _async_opensearch(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return fake_client

    monkeypatch.setattr(
        "gulp.api.opensearch_api.GulpConfig.get_instance", lambda: _FakeConfig()
    )
    monkeypatch.setattr("gulp.api.opensearch_api.AsyncOpenSearch", _async_opensearch)

    client = object.__new__(GulpOpenSearch)._get_client()

    assert client is fake_client
    assert captured["args"] == (
        "https://admin:secret@opensearch.example.com:9200/prefix",
    )
    assert captured["kwargs"]["http_compress"] is True
    assert captured["kwargs"]["pool_maxsize"] == 4
    assert not {
        "use_ssl",
        "ca_certs",
        "client_cert",
        "client_key",
        "verify_certs",
    } & captured["kwargs"].keys()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_operations_omits_collab_sources_without_documents_by_default(
    monkeypatch,
):
    os_api = object.__new__(GulpOpenSearch)

    class _FakeSource:
        def __init__(self, source_id, name, plugin):
            self.id = source_id
            self.name = name
            self.plugin = plugin
            self.glyph_id = "File"
            self.color = None

    class _FakeContext:
        id = "ctx-1"
        name = "ctx"
        glyph_id = "Folder"
        color = "#123456"
        sources = [
            _FakeSource("src-with-docs", "with-docs.evtx", "win_evtx"),
            _FakeSource("src-empty", "empty.evtx", "win_evtx"),
        ]

    class _FakeOperation:
        id = "op-1"
        name = "op"
        index = "op-1"
        glyph_id = "Box"
        contexts = [_FakeContext()]

    class _FakeCollab:
        def session(self):
            class _Session:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

            return _Session()

    monkeypatch.setattr(
        "gulp.api.opensearch_api.GulpCollab.get_instance",
        lambda: _FakeCollab(),
    )
    monkeypatch.setattr(
        "gulp.api.opensearch_api.GulpOperation.get_by_filter",
        AsyncMock(return_value=[_FakeOperation()]),
    )

    result = await os_api._parse_operation_aggregation(
        {
            "operations": {
                "buckets": [
                    {
                        "key": "op-1",
                        "context_id": {
                            "buckets": [
                                {
                                    "key": "ctx-1",
                                    "doc_count": 2,
                                    "plugin": {
                                        "buckets": [
                                            {
                                                "key": "win_evtx",
                                                "source_id": {
                                                    "buckets": [
                                                        {
                                                            "key": "src-with-docs",
                                                            "doc_count": 2,
                                                            "max_event.code": {
                                                                "value": 2
                                                            },
                                                            "min_event.code": {
                                                                "value": 1
                                                            },
                                                            "min_gulp.timestamp": {
                                                                "value": 1000
                                                            },
                                                            "max_gulp.timestamp": {
                                                                "value": 2000
                                                            },
                                                        }
                                                    ]
                                                },
                                            }
                                        ]
                                    },
                                }
                            ]
                        },
                    }
                ]
            }
        },
        "admin",
    )

    sources = result[0]["contexts"][0]["plugins"][0]["sources"]
    assert [source["id"] for source in sources] == ["src-with-docs"]
    assert sources[0]["doc_count"] == 2
    assert sources[0]["min_gulp.timestamp"] == 1000


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_operations_includes_all_collab_sources_without_documents(
    monkeypatch,
):
    os_api = object.__new__(GulpOpenSearch)

    class _FakeSource:
        def __init__(self, source_id, name, plugin):
            self.id = source_id
            self.name = name
            self.plugin = plugin
            self.glyph_id = "File"
            self.color = None

    class _FakeContext:
        id = "ctx-1"
        name = "ctx"
        glyph_id = "Folder"
        color = "#123456"
        sources = [
            _FakeSource("src-with-docs", "with-docs.evtx", "win_evtx"),
            _FakeSource("src-empty-1", "empty-1.evtx", "win_evtx"),
            _FakeSource("src-empty-2", "empty-2.evtx", "win_evtx"),
        ]

    class _FakeOperation:
        id = "op-1"
        name = "op"
        index = "op-1"
        glyph_id = "Box"
        contexts = [_FakeContext()]

    class _FakeCollab:
        def session(self):
            class _Session:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

            return _Session()

    monkeypatch.setattr(
        "gulp.api.opensearch_api.GulpCollab.get_instance",
        lambda: _FakeCollab(),
    )
    monkeypatch.setattr(
        "gulp.api.opensearch_api.GulpOperation.get_by_filter",
        AsyncMock(return_value=[_FakeOperation()]),
    )

    result = await os_api._parse_operation_aggregation(
        {
            "operations": {
                "buckets": [
                    {
                        "key": "op-1",
                        "context_id": {
                            "buckets": [
                                {
                                    "key": "ctx-1",
                                    "doc_count": 2,
                                    "plugin": {
                                        "buckets": [
                                            {
                                                "key": "win_evtx",
                                                "source_id": {
                                                    "buckets": [
                                                        {
                                                            "key": "src-with-docs",
                                                            "doc_count": 2,
                                                            "max_event.code": {
                                                                "value": 2
                                                            },
                                                            "min_event.code": {
                                                                "value": 1
                                                            },
                                                            "min_gulp.timestamp": {
                                                                "value": 1000
                                                            },
                                                            "max_gulp.timestamp": {
                                                                "value": 2000
                                                            },
                                                        }
                                                    ]
                                                },
                                            }
                                        ]
                                    },
                                }
                            ]
                        },
                    }
                ]
            }
        },
        "admin",
        include_empty_sources=True,
    )

    sources = result[0]["contexts"][0]["plugins"][0]["sources"]
    assert [source["id"] for source in sources] == [
        "src-with-docs",
        "src-empty-1",
        "src-empty-2",
    ]
    assert sources[1]["doc_count"] == sources[2]["doc_count"] == 0
    assert "min_gulp.timestamp" not in sources[1]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_search_dsl_internal_returns_aggregations_without_hits(monkeypatch):
    os_api = object.__new__(GulpOpenSearch)
    os_api._opensearch = MagicMock()
    os_api._opensearch.search = AsyncMock(
        return_value={
            "hits": {"total": {"value": 42}, "hits": []},
            "aggregations": {
                "result": {
                    "buckets": [
                        {"key": "apache_access_clf", "doc_count": 40},
                        {"key": "win_evtx", "doc_count": 2},
                    ]
                }
            },
        }
    )
    monkeypatch.setattr(
        "gulp.api.opensearch_api.GulpConfig.get_instance",
        lambda: MagicMock(
            opensearch_request_timeout=MagicMock(return_value=0),
            query_circuit_breaker_backoff_attempts=MagicMock(return_value=0),
            query_circuit_breaker_min_limit=MagicMock(return_value=1),
        ),
    )

    total_hits, docs, search_after, aggregations = await os_api._search_dsl_internal(
        "test_index",
        {},
        {"size": 0, "aggs": {"result": {"terms": {"field": "agent.type"}}}},
        raise_on_error=True,
    )

    assert total_hits == 42
    assert docs == []
    assert search_after == []
    assert aggregations["result"]["buckets"][0]["key"] == "apache_access_clf"


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
    assert update_call["params"] == {
        "conflicts": "proceed",
        "refresh": "true",
        "wait_for_completion": "true",
    }
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
