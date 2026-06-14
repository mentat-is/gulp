from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


_QUERY_NOTE_DOC = {
    "_id": "doc-1",
    "@timestamp": 1720000000000,
    "gulp.operation_id": "op-query-note",
    "gulp.context_id": "ctx-query-note",
    "gulp.source_id": "src-query-note",
    "gulp.timestamp": 1720000000000000000,
}


class _FakeCollabSession:
    async def __aenter__(self):
        return "fake-session"

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeCollab:
    def session(self):
        return _FakeCollabSession()


class _ConcreteIngestPlugin:
    @staticmethod
    def build():
        from gulp.plugin import GulpPluginBase, GulpPluginType

        class _Plugin(GulpPluginBase):
            def display_name(self) -> str:
                return "test plugin"

            def type(self) -> GulpPluginType:
                return GulpPluginType.ENRICHMENT

        return _Plugin(path="/tmp/test_plugin.py", module_name="test_plugin")


class _FakeStatsSession:
    async def refresh(self, _obj):
        return None


@asynccontextmanager
async def _noop_advisory_lock(_sess, _obj_id):
    yield


def _make_request_stats(req_type, data, status="ongoing"):
    from gulp.api.collab.stats import GulpRequestStats

    return GulpRequestStats(
        id=f"req-{req_type}",
        type="request_stats",
        user_id="user-stats-key",
        name=f"req-{req_type}",
        time_created=0,
        time_updated=0,
        operation_id="op-stats-key",
        glyph_id=None,
        description=None,
        tags=[],
        color=None,
        granted_user_ids=[],
        granted_user_group_ids=[],
        server_id="server-stats-key",
        status=status,
        req_type=req_type,
        data=data,
    )


def _patch_stats_persistence(monkeypatch):
    from gulp.api.collab.stats import GulpRequestStats
    from gulp.api.collab.structs import GulpCollabBase

    updates = []

    async def _fake_update(self, *args, **kwargs):
        updates.append({"data": self.data.copy(), "status": self.status})
        return updates[-1]

    monkeypatch.setattr(
        GulpRequestStats,
        "advisory_lock",
        classmethod(lambda cls, sess, obj_id: _noop_advisory_lock(sess, obj_id)),
    )
    monkeypatch.setattr(GulpCollabBase, "update", _fake_update)
    return updates


@pytest.mark.unit
def test_query_note_id_ignores_replay_tag_order_and_duplicates():
    from gulp.api.collab.note import GulpNote

    first_tags = GulpNote._normalise_auto_note_tags(["extra", "auto"], "query-name")
    replay_tags = GulpNote._normalise_auto_note_tags(
        ["query-name", "extra", "extra", "auto"],
        "query-name",
    )

    first_id = GulpNote._deterministic_query_note_id(
        _QUERY_NOTE_DOC,
        name="query-name",
        q='{"query":{"match_all":{}}}',
        tags=first_tags,
        color="#ff0000",
        glyph_id="glyph-alert",
        sigma_yml="title: query-name",
    )
    replay_id = GulpNote._deterministic_query_note_id(
        _QUERY_NOTE_DOC,
        name="query-name",
        q='{"query":{"match_all":{}}}',
        tags=replay_tags,
        color="#ff0000",
        glyph_id="glyph-alert",
        sigma_yml="title: query-name",
    )

    assert first_tags == ["extra", "auto", "query-name"]
    assert replay_tags == ["query-name", "extra", "auto"]
    assert first_id == replay_id


@pytest.mark.unit
def test_query_note_id_changes_for_distinct_output_semantics():
    from gulp.api.collab.note import GulpNote

    base_tags = GulpNote._normalise_auto_note_tags(["extra"], "query-name")
    base_id = GulpNote._deterministic_query_note_id(
        _QUERY_NOTE_DOC,
        name="query-name",
        q='{"query":{"match_all":{}}}',
        tags=base_tags,
        color="#ff0000",
        glyph_id="glyph-alert",
    )
    changed_query_id = GulpNote._deterministic_query_note_id(
        _QUERY_NOTE_DOC,
        name="query-name",
        q='{"query":{"match_none":{}}}',
        tags=base_tags,
        color="#ff0000",
        glyph_id="glyph-alert",
    )
    changed_note_options_id = GulpNote._deterministic_query_note_id(
        _QUERY_NOTE_DOC,
        name="query-name",
        q='{"query":{"match_all":{}}}',
        tags=base_tags,
        color="#00ff00",
        glyph_id="glyph-alert",
    )

    assert changed_query_id != base_id
    assert changed_note_options_id != base_id


@pytest.mark.unit
def test_query_note_id_changes_for_duplicate_hit_ordinals():
    from gulp.api.collab.note import GulpNote

    base_tags = GulpNote._normalise_auto_note_tags(["extra"], "query-name")
    first_hit_id = GulpNote._deterministic_query_note_id(
        _QUERY_NOTE_DOC,
        name="query-name",
        q='{"query":{"match_all":{}}}',
        tags=base_tags,
        req_id="req-note-1",
        query_ordinal=0,
        match_ordinal=0,
    )
    duplicate_doc_second_hit_id = GulpNote._deterministic_query_note_id(
        _QUERY_NOTE_DOC,
        name="query-name",
        q='{"query":{"match_all":{}}}',
        tags=base_tags,
        req_id="req-note-1",
        query_ordinal=0,
        match_ordinal=1,
    )
    first_hit_replay_id = GulpNote._deterministic_query_note_id(
        _QUERY_NOTE_DOC,
        name="query-name",
        q='{"query":{"match_all":{}}}',
        tags=base_tags,
        req_id="req-note-1",
        query_ordinal=0,
        match_ordinal=0,
    )
    next_request_id = GulpNote._deterministic_query_note_id(
        _QUERY_NOTE_DOC,
        name="query-name",
        q='{"query":{"match_all":{}}}',
        tags=base_tags,
        req_id="req-note-2",
        query_ordinal=0,
        match_ordinal=0,
    )
    next_query_id = GulpNote._deterministic_query_note_id(
        _QUERY_NOTE_DOC,
        name="query-name",
        q='{"query":{"match_all":{}}}',
        tags=base_tags,
        req_id="req-note-1",
        query_ordinal=1,
        match_ordinal=0,
    )

    assert duplicate_doc_second_hit_id != first_hit_id
    assert first_hit_replay_id == first_hit_id
    assert next_request_id != first_hit_id
    assert next_query_id != first_hit_id


@pytest.mark.unit
def test_stats_update_key_claims_once():
    from gulp.api.collab.stats import GulpQueryStats, GulpRequestStats

    data = GulpQueryStats()

    assert GulpRequestStats._claim_stats_update_key(data, "query-batch-1") is True
    assert GulpRequestStats._claim_stats_update_key(data, "query-batch-1") is False
    assert GulpRequestStats._claim_stats_update_key(data, "query-batch-2") is True
    assert data.applied_update_keys == ["query-batch-1", "query-batch-2"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_stats_update_key_suppresses_duplicate_counter_update(monkeypatch):
    from gulp.api.collab.stats import GulpQueryStats, RequestStatsType
    from gulp.api.collab.structs import GulpRequestStatus

    updates = _patch_stats_persistence(monkeypatch)
    stats = _make_request_stats(
        RequestStatsType.REQUEST_TYPE_QUERY.value,
        GulpQueryStats(num_queries=2).model_dump(),
    )
    sess = _FakeStatsSession()

    await stats.update_query_stats(
        sess,
        hits=5,
        inc_completed=1,
        update_key="query_batch:req-stats:0:1",
    )
    await stats.update_query_stats(
        sess,
        hits=99,
        inc_completed=1,
        errors=["replayed failure must not be recorded"],
        update_key="query_batch:req-stats:0:1",
    )
    await stats.update_query_stats(
        sess,
        hits=4,
        inc_completed=1,
        update_key="query_batch:req-stats:1:1",
    )

    assert len(updates) == 2
    assert stats.errors == []
    assert stats.status == GulpRequestStatus.DONE.value
    assert stats.data["total_hits"] == 9
    assert stats.data["completed_queries"] == 2
    assert stats.data["failed_queries"] == 0
    assert stats.data["applied_update_keys"] == [
        "query_batch:req-stats:0:1",
        "query_batch:req-stats:1:1",
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_updatedocuments_stats_update_key_suppresses_duplicate_counter_update(
    monkeypatch,
):
    from gulp.api.collab.stats import GulpUpdateDocumentsStats, RequestStatsType
    from gulp.api.collab.structs import GulpRequestStatus

    updates = _patch_stats_persistence(monkeypatch)
    stats = _make_request_stats(
        RequestStatsType.REQUEST_TYPE_ENRICHMENT.value,
        GulpUpdateDocumentsStats(total_hits=3).model_dump(),
    )
    sess = _FakeStatsSession()

    await stats.update_updatedocuments_stats(
        sess,
        total_hits=3,
        updated=1,
        errors=[],
        last=False,
        update_key="enrich_documents:req-stats:0:False",
    )
    await stats.update_updatedocuments_stats(
        sess,
        total_hits=3,
        updated=2,
        errors=["replayed failure must not be recorded"],
        last=True,
        update_key="enrich_documents:req-stats:0:False",
    )
    await stats.update_updatedocuments_stats(
        sess,
        total_hits=3,
        updated=2,
        errors=[],
        last=True,
        update_key="enrich_documents:req-stats:1:True",
    )

    assert len(updates) == 2
    assert stats.errors == []
    assert stats.status == GulpRequestStatus.DONE.value
    assert stats.data["updated"] == 3
    assert stats.data["total_hits"] == 3
    assert stats.data["applied_update_keys"] == [
        "enrich_documents:req-stats:0:False",
        "enrich_documents:req-stats:1:True",
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_run_query_batch_passes_stats_update_key(monkeypatch):
    from gulp.api.server import query as query_mod

    update_query_stats = AsyncMock()
    stats = SimpleNamespace(update_query_stats=update_query_stats)
    monkeypatch.setattr(
        query_mod,
        "run_query",
        AsyncMock(return_value=(1, 2, "query-name", False)),
    )
    monkeypatch.setattr(
        query_mod.GulpCollab,
        "get_instance",
        lambda: _FakeCollab(),
    )
    monkeypatch.setattr(
        query_mod.GulpRequestStats,
        "get_by_id",
        AsyncMock(return_value=stats),
    )

    result = await query_mod.run_query_batch(
        user_id="user-query-key",
        req_id="req-query-key",
        operation_id="op-query-key",
        ws_id="ws-query-key",
        queries=[{"q": {"query": {"match_all": {}}}, "q_name": "query-name"}],
        q_options={"name": "query-name"},
        stats_update_key="query_batch:req-query-key:0:1",
    )

    assert result == [(1, 2, "query-name", False)]
    update_query_stats.assert_awaited_once()
    assert update_query_stats.await_args.kwargs["update_key"] == (
        "query_batch:req-query-key:0:1"
    )


@pytest.mark.unit
def test_query_batch_stats_update_key_uses_task_prefix_for_fanout_requests():
    from gulp.api.server import query as query_mod

    assert (
        query_mod._query_batch_stats_update_key(
            "req-shared",
            0,
            16,
            stats_update_prefix="req-shared:sigma_zip:1",
        )
        == "query_batch:req-shared:sigma_zip:1:0:16"
    )
    assert (
        query_mod._query_batch_stats_update_key(
            "req-shared",
            0,
            16,
            stats_update_prefix="req-shared:sigma_zip:2",
        )
        == "query_batch:req-shared:sigma_zip:2:0:16"
    )
    assert (
        query_mod._query_batch_stats_update_key("req-shared", 0, 16)
        == "query_batch:req-shared:0:16"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rebase_callback_passes_deterministic_stats_update_keys():
    from gulp.api.server import db as db_mod

    update_documents_stats = AsyncMock()
    stats = SimpleNamespace(
        user_id="user-rebase-key",
        update_updatedocuments_stats=update_documents_stats,
    )
    cb_context = {
        "flt": None,
        "errors": [],
        "stats": stats,
        "ws_id": "ws-rebase-key",
    }

    await db_mod._rebase_callback(
        "fake-session",
        total=2,
        current=1,
        req_id="req-rebase-key",
        last=False,
        cb_context=cb_context,
    )
    await db_mod._rebase_callback(
        "fake-session",
        total=2,
        current=0,
        req_id="req-rebase-key",
        last=True,
        cb_context=cb_context,
    )

    assert update_documents_stats.await_args_list[0].kwargs["update_key"] == (
        "rebase:req-rebase-key:0:False"
    )
    assert update_documents_stats.await_args_list[1].kwargs["update_key"] == (
        "rebase:req-rebase-key:1:True"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_modify_documents_chunk_passes_deterministic_stats_update_key(monkeypatch):
    from gulp.api.server import enrich as enrich_mod

    update_documents_stats = AsyncMock()
    update_documents = AsyncMock(return_value=(1, 0, []))
    stats = SimpleNamespace(
        user_id="user-modify-key",
        update_updatedocuments_stats=update_documents_stats,
    )
    monkeypatch.setattr(
        enrich_mod.GulpConfig,
        "get_instance",
        lambda: SimpleNamespace(debug_enrich_dry_run=lambda: False),
    )
    monkeypatch.setattr(
        enrich_mod.GulpOpenSearch,
        "get_instance",
        lambda: SimpleNamespace(update_documents=update_documents),
    )

    chunk = await enrich_mod._modify_documents_chunk(
        "fake-session",
        [{"_id": "doc-1", "field": "old"}],
        chunk_num=3,
        total_hits=5,
        index="idx-modify-key",
        last=True,
        req_id="req-modify-key",
        cb_context={
            "mutate_fn": lambda doc: doc.update({"field": "new"}) is None,
            "stats": stats,
            "ws_id": "ws-modify-key",
            "flt": None,
            "errors": [],
            "total_updated": 0,
        },
    )

    assert chunk[0]["field"] == "new"
    assert chunk[0]["gulp.update_req_ids"] == ["req-modify-key"]
    update_documents.assert_awaited_once()
    assert update_documents.await_args.args[1] == [chunk[0]]
    update_documents_stats.assert_awaited_once()
    assert update_documents_stats.await_args.kwargs["update_key"] == (
        "modify_documents:req-modify-key:3:True"
    )
    assert update_documents_stats.await_args.kwargs["updated"] == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_modify_documents_chunk_skips_already_marked_replay(monkeypatch):
    from gulp.api.server import enrich as enrich_mod

    update_documents_stats = AsyncMock()
    update_documents = AsyncMock(
        side_effect=AssertionError("marked replay should not update OpenSearch")
    )
    stats = SimpleNamespace(
        user_id="user-modify-replay",
        update_updatedocuments_stats=update_documents_stats,
    )
    monkeypatch.setattr(
        enrich_mod.GulpConfig,
        "get_instance",
        lambda: SimpleNamespace(debug_enrich_dry_run=lambda: False),
    )
    monkeypatch.setattr(
        enrich_mod.GulpOpenSearch,
        "get_instance",
        lambda: SimpleNamespace(update_documents=update_documents),
    )
    doc = {
        "_id": "doc-1",
        "field": "already-updated",
        "gulp.update_req_ids": ["req-modify-replay"],
    }

    chunk = await enrich_mod._modify_documents_chunk(
        "fake-session",
        [doc],
        chunk_num=3,
        total_hits=5,
        index="idx-modify-replay",
        last=True,
        req_id="req-modify-replay",
        cb_context={
            "mutate_fn": lambda _doc: (_ for _ in ()).throw(
                AssertionError("marked replay should not mutate document")
            ),
            "stats": stats,
            "ws_id": "ws-modify-replay",
            "flt": None,
            "errors": [],
            "total_updated": 0,
        },
    )

    assert chunk == [doc]
    update_documents.assert_not_called()
    update_documents_stats.assert_awaited_once()
    assert update_documents_stats.await_args.kwargs["updated"] == 1
    assert update_documents_stats.await_args.kwargs["update_key"] == (
        "modify_documents:req-modify-replay:3:True"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enrich_documents_wrapper_passes_deterministic_stats_update_key(monkeypatch):
    from gulp import plugin as plugin_mod

    update_documents_stats = AsyncMock()
    stats = SimpleNamespace(
        user_id="user-enrich-key",
        update_updatedocuments_stats=update_documents_stats,
    )
    update_documents = AsyncMock(return_value=(1, 0, []))
    mod = _ConcreteIngestPlugin.build()
    mod.name = "test_enrich"
    mod._enrich_documents_chunk_cb = AsyncMock(
        return_value=[{"_id": "doc-1", "field": "enriched"}]
    )
    monkeypatch.setattr(
        plugin_mod.GulpConfig,
        "get_instance",
        lambda: SimpleNamespace(debug_enrich_dry_run=lambda: False),
    )
    monkeypatch.setattr(
        plugin_mod.GulpOpenSearch,
        "get_instance",
        lambda: SimpleNamespace(update_documents=update_documents),
    )

    result = await mod._enrich_documents_chunk_wrapper(
        "fake-session",
        [{"_id": "doc-1", "field": "old"}],
        chunk_num=4,
        total_hits=6,
        index="idx-enrich-key",
        last=False,
        req_id="req-enrich-key",
        cb_context={
            "stats": stats,
            "ws_id": "ws-enrich-key",
            "flt": None,
            "errors": [],
            "total_updated": 0,
            "total_hits": 0,
        },
    )

    assert result == [
        {
            "_id": "doc-1",
            "field": "enriched",
            "gulp.update_req_ids": ["req-enrich-key"],
        }
    ]
    update_documents.assert_awaited_once()
    assert update_documents.await_args.args[1] == result
    update_documents_stats.assert_awaited_once()
    assert update_documents_stats.await_args.kwargs["update_key"] == (
        "enrich_documents:req-enrich-key:4:False"
    )
    assert update_documents_stats.await_args.kwargs["updated"] == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enrich_documents_wrapper_skips_already_marked_replay(monkeypatch):
    from gulp import plugin as plugin_mod

    update_documents_stats = AsyncMock()
    stats = SimpleNamespace(
        user_id="user-enrich-replay",
        update_updatedocuments_stats=update_documents_stats,
    )
    update_documents = AsyncMock(
        side_effect=AssertionError("marked replay should not update OpenSearch")
    )
    mod = _ConcreteIngestPlugin.build()
    mod.name = "test_enrich"
    mod._enrich_documents_chunk_cb = AsyncMock(
        side_effect=AssertionError("marked replay should not call plugin chunk")
    )
    monkeypatch.setattr(
        plugin_mod.GulpConfig,
        "get_instance",
        lambda: SimpleNamespace(debug_enrich_dry_run=lambda: False),
    )
    monkeypatch.setattr(
        plugin_mod.GulpOpenSearch,
        "get_instance",
        lambda: SimpleNamespace(update_documents=update_documents),
    )
    doc = {
        "_id": "doc-1",
        "field": "already-enriched",
        "gulp.update_req_ids": ["req-enrich-replay"],
    }

    result = await mod._enrich_documents_chunk_wrapper(
        "fake-session",
        [doc],
        chunk_num=4,
        total_hits=6,
        index="idx-enrich-replay",
        last=True,
        req_id="req-enrich-replay",
        cb_context={
            "stats": stats,
            "ws_id": "ws-enrich-replay",
            "flt": None,
            "errors": [],
            "total_updated": 0,
            "total_hits": 0,
        },
    )

    assert result == [doc]
    mod._enrich_documents_chunk_cb.assert_not_called()
    update_documents.assert_not_called()
    update_documents_stats.assert_awaited_once()
    assert update_documents_stats.await_args.kwargs["updated"] == 1
    assert update_documents_stats.await_args.kwargs["update_key"] == (
        "enrich_documents:req-enrich-replay:4:True"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_process_queries_skips_terminal_request_replay(monkeypatch):
    from gulp.api.collab.structs import GulpRequestStatus
    from gulp.api.server import query as query_mod

    stats = SimpleNamespace(status=GulpRequestStatus.DONE.value)
    create_or_get = AsyncMock(return_value=(stats, False))
    worker_apply = AsyncMock(side_effect=AssertionError("worker should not run"))
    monkeypatch.setattr(
        query_mod.GulpRequestStats,
        "create_or_get_existing",
        create_or_get,
    )
    monkeypatch.setattr(
        query_mod.GulpProcess,
        "get_instance",
        lambda: SimpleNamespace(process_pool=SimpleNamespace(apply=worker_apply)),
    )

    canceled = await query_mod.process_queries(
        sess="fake-session",
        user_id="user-terminal-query",
        req_id="req-terminal-query",
        operation_id="op-terminal-query",
        ws_id="ws-terminal-query",
        queries=[SimpleNamespace(q={"query": {"match_all": {}}})],
        num_total_queries=1,
        q_options=SimpleNamespace(
            group=None,
            force_ignore_missing_ws=False,
        ),
    )

    assert canceled is False
    create_or_get.assert_awaited_once()
    worker_apply.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_process_queries_reports_terminal_canceled_replay(monkeypatch):
    from gulp.api.collab.structs import GulpRequestStatus
    from gulp.api.server import query as query_mod

    stats = SimpleNamespace(status=GulpRequestStatus.CANCELED.value)
    monkeypatch.setattr(
        query_mod.GulpRequestStats,
        "create_or_get_existing",
        AsyncMock(return_value=(stats, False)),
    )

    canceled = await query_mod.process_queries(
        sess="fake-session",
        user_id="user-canceled-query",
        req_id="req-canceled-query",
        operation_id="op-canceled-query",
        ws_id="ws-canceled-query",
        queries=[SimpleNamespace(q={"query": {"match_all": {}}})],
        num_total_queries=1,
        q_options=SimpleNamespace(
            group=None,
            force_ignore_missing_ws=False,
        ),
    )

    assert canceled is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ingest_file_internal_skips_terminal_request_replay(monkeypatch):
    from gulp.api.collab.structs import GulpRequestStatus
    from gulp.api.server import ingest as ingest_mod

    stats = SimpleNamespace(status=GulpRequestStatus.DONE.value)
    monkeypatch.setattr(
        ingest_mod.GulpCollab,
        "get_instance",
        lambda: _FakeCollab(),
    )
    monkeypatch.setattr(
        ingest_mod.GulpRequestStats,
        "create_or_get_existing",
        AsyncMock(return_value=(stats, False)),
    )
    monkeypatch.setattr(
        ingest_mod.GulpOperation,
        "get_by_id",
        AsyncMock(side_effect=AssertionError("context/source should not be created")),
    )
    monkeypatch.setattr(
        ingest_mod.GulpPluginBase,
        "load",
        AsyncMock(side_effect=AssertionError("plugin should not be loaded")),
    )

    status, preview = await ingest_mod._ingest_file_internal(
        user_id="user-terminal-ingest",
        req_id="req-terminal-ingest",
        ws_id="ws-terminal-ingest",
        operation_id="op-terminal-ingest",
        context_name="ctx-terminal-ingest",
        source_name="src-terminal-ingest",
        index="idx-terminal-ingest",
        plugin="raw",
        file_path="/tmp/does-not-need-to-exist",
        payload=ingest_mod.GulpIngestPayload(),
    )

    assert status == GulpRequestStatus.DONE
    assert preview == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_rebase_by_query_internal_skips_terminal_request_replay(monkeypatch):
    from gulp.api.collab.structs import GulpRequestStatus
    from gulp.api.server import db as db_mod

    stats = SimpleNamespace(status=GulpRequestStatus.FAILED.value)
    rebase = AsyncMock(side_effect=AssertionError("OpenSearch rebase should not run"))
    publish = AsyncMock(side_effect=AssertionError("rebase done should not publish"))
    monkeypatch.setattr(
        db_mod.GulpCollab,
        "get_instance",
        lambda: _FakeCollab(),
    )
    monkeypatch.setattr(
        db_mod.GulpRequestStats,
        "create_or_get_existing",
        AsyncMock(return_value=(stats, False)),
    )
    monkeypatch.setattr(
        db_mod.GulpOpenSearch,
        "get_instance",
        lambda: SimpleNamespace(opensearch_rebase_by_query=rebase),
    )
    monkeypatch.setattr(
        db_mod.GulpRedisBroker,
        "get_instance",
        lambda: SimpleNamespace(put=publish),
    )

    await db_mod._rebase_by_query_internal(
        req_id="req-terminal-rebase",
        ws_id="ws-terminal-rebase",
        user_id="user-terminal-rebase",
        operation_id="op-terminal-rebase",
        index="idx-terminal-rebase",
        offset_msec=0,
        flt=None,
        fields=None,
    )

    rebase.assert_not_called()
    publish.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enrich_documents_internal_skips_terminal_request_replay(monkeypatch):
    from gulp.api.collab.structs import GulpRequestStatus
    from gulp.api.server import enrich as enrich_mod

    stats = SimpleNamespace(status=GulpRequestStatus.DONE.value)
    monkeypatch.setattr(
        enrich_mod.GulpCollab,
        "get_instance",
        lambda: _FakeCollab(),
    )
    monkeypatch.setattr(
        enrich_mod.GulpRequestStats,
        "create_or_get_existing",
        AsyncMock(return_value=(stats, False)),
    )
    monkeypatch.setattr(
        enrich_mod.GulpPluginBase,
        "load",
        AsyncMock(side_effect=AssertionError("enrich plugin should not be loaded")),
    )
    monkeypatch.setattr(
        enrich_mod.GulpOpenSearch,
        "get_instance",
        lambda: SimpleNamespace(
            datastream_update_source_field_types_by_flt=AsyncMock(
                side_effect=AssertionError("source field types should not update")
            )
        ),
    )

    await enrich_mod._enrich_documents_internal(
        user_id="user-terminal-enrich",
        req_id="req-terminal-enrich",
        ws_id="ws-terminal-enrich",
        flt=None,
        operation_id="op-terminal-enrich",
        index="idx-terminal-enrich",
        plugin="enrich_whois",
        fields={"source.ip": None},
        plugin_params=None,
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_enrich_remove_internal_skips_terminal_request_replay(monkeypatch):
    from gulp.api.collab.structs import GulpRequestStatus
    from gulp.api.opensearch.filters import GulpQueryFilter
    from gulp.api.server import enrich as enrich_mod

    stats = SimpleNamespace(status=GulpRequestStatus.DONE.value)
    update_by_query = AsyncMock(
        side_effect=AssertionError("OpenSearch update_by_query should not run")
    )
    field_types = AsyncMock(
        side_effect=AssertionError("source field types should not update")
    )
    monkeypatch.setattr(
        enrich_mod.GulpCollab,
        "get_instance",
        lambda: _FakeCollab(),
    )
    monkeypatch.setattr(
        enrich_mod.GulpRequestStats,
        "create_or_get_existing",
        AsyncMock(return_value=(stats, False)),
    )
    monkeypatch.setattr(
        enrich_mod.GulpOpenSearch,
        "get_instance",
        lambda: SimpleNamespace(
            _opensearch=SimpleNamespace(update_by_query=update_by_query),
            datastream_update_source_field_types_by_flt=field_types,
        ),
    )

    await enrich_mod._enrich_remove_internal(
        user_id="user-terminal-remove",
        ws_id="ws-terminal-remove",
        req_id="req-terminal-remove",
        operation_id="op-terminal-remove",
        index="idx-terminal-remove",
        flt=GulpQueryFilter(),
        fields=["gulp.enriched"],
    )

    update_by_query.assert_not_called()
    field_types.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_documents_internal_skips_terminal_request_replay(monkeypatch):
    from gulp.api.collab.structs import GulpRequestStatus
    from gulp.api.server import enrich as enrich_mod

    stats = SimpleNamespace(status=GulpRequestStatus.FAILED.value)
    search = AsyncMock(side_effect=AssertionError("OpenSearch search should not run"))
    field_types = AsyncMock(
        side_effect=AssertionError("source field types should not update")
    )
    monkeypatch.setattr(
        enrich_mod.GulpCollab,
        "get_instance",
        lambda: _FakeCollab(),
    )
    monkeypatch.setattr(
        enrich_mod.GulpRequestStats,
        "create_or_get_existing",
        AsyncMock(return_value=(stats, False)),
    )
    monkeypatch.setattr(
        enrich_mod.GulpOpenSearch,
        "get_instance",
        lambda: SimpleNamespace(
            search_dsl=search,
            datastream_update_source_field_types_by_flt=field_types,
        ),
    )

    await enrich_mod._update_documents_internal(
        user_id="user-terminal-update",
        ws_id="ws-terminal-update",
        req_id="req-terminal-update",
        operation_id="op-terminal-update",
        index="idx-terminal-update",
        flt=SimpleNamespace(is_empty=lambda: True),
        data={"custom": "value"},
    )

    search.assert_not_called()
    field_types.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_untag_documents_internal_skips_terminal_request_replay(monkeypatch):
    from gulp.api.collab.structs import GulpRequestStatus
    from gulp.api.server import enrich as enrich_mod

    stats = SimpleNamespace(status=GulpRequestStatus.CANCELED.value)
    search = AsyncMock(side_effect=AssertionError("OpenSearch search should not run"))
    field_types = AsyncMock(
        side_effect=AssertionError("source field types should not update")
    )
    monkeypatch.setattr(
        enrich_mod.GulpCollab,
        "get_instance",
        lambda: _FakeCollab(),
    )
    monkeypatch.setattr(
        enrich_mod.GulpRequestStats,
        "create_or_get_existing",
        AsyncMock(return_value=(stats, False)),
    )
    monkeypatch.setattr(
        enrich_mod.GulpOpenSearch,
        "get_instance",
        lambda: SimpleNamespace(
            search_dsl=search,
            datastream_update_source_field_types_by_flt=field_types,
        ),
    )

    await enrich_mod._untag_documents_internal(
        user_id="user-terminal-untag",
        ws_id="ws-terminal-untag",
        req_id="req-terminal-untag",
        operation_id="op-terminal-untag",
        index="idx-terminal-untag",
        flt=SimpleNamespace(is_empty=lambda: True),
        tags=["old-tag"],
    )

    search.assert_not_called()
    field_types.assert_not_called()
