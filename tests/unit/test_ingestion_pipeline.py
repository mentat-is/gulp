"""Unit coverage for adaptive sizing and one-ahead ingestion ownership."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.collab.stats import (
    GulpRequestStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch_api import GulpPreparedBulk
from gulp.events import GulpCoreEvent, GulpEventResult, events
from gulp.plugin import GulpPluginBase, GulpPluginType, _GulpIngestionChunk
from gulp.structs import GulpPluginParameters


class _IngestionPlugin(GulpPluginBase):
    """Minimal ingestion plugin for exercising shared scheduling."""

    def display_name(self) -> str:
        return "test_ingestion_pipeline"

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION


@pytest.fixture
def pipeline_plugin(monkeypatch) -> tuple[_IngestionPlugin, MagicMock, MagicMock]:
    """Create a fully isolated two-document pipeline."""
    from gulp import plugin as plugin_module

    plugin = _IngestionPlugin(
        "/tmp/test_ingestion_pipeline.py", "test_ingestion_pipeline"
    )
    plugin._sess = MagicMock(commit=AsyncMock())
    plugin._stats = MagicMock(
        status=GulpRequestStatus.ONGOING,
        update_ingestion_stats=AsyncMock(return_value={"status": "ongoing"}),
    )
    plugin._index = "test-index"
    plugin._operation_id = "operation"
    plugin._req_id = "request"
    plugin._ws_id = "websocket"
    plugin._user_id = "user"
    plugin._plugin_params = GulpPluginParameters()
    plugin._pipeline_enabled = True
    plugin._adaptive_chunk_enabled = False
    plugin._adaptive_chunk_size = 2
    plugin._record_to_gulp_documents_wrapper = AsyncMock(
        side_effect=lambda _record, record_idx, **_kwargs: [
            {"_id": str(record_idx), "event.sequence": record_idx}
        ]
    )

    config = MagicMock()
    config.ingestion_documents_chunk_size.return_value = 2
    config.ingestion_evt_failure_threshold.return_value = 0
    config.debug_abort_on_opensearch_ingestion_error.return_value = False
    config.prometheus_enabled.return_value = False
    monkeypatch.setattr(
        plugin_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: config),
    )
    monkeypatch.setattr(
        plugin_module.GulpRequestStats,
        "is_canceled",
        AsyncMock(return_value=False),
    )

    opensearch = MagicMock()

    async def prepare(documents, **_kwargs):
        return GulpPreparedBulk(
            body=b"prepared\n",
            documents=tuple(documents),
            size_bytes=9,
            max_content_length_bytes=100 * 1024 * 1024,
        )

    opensearch.prepare_bulk_ingest = AsyncMock(side_effect=prepare)
    opensearch.bulk_ingest_prepared = AsyncMock()
    opensearch.index_refresh = AsyncMock()
    monkeypatch.setattr(
        plugin_module.GulpOpenSearch,
        "get_instance",
        staticmethod(lambda: opensearch),
    )
    broker = MagicMock(put=AsyncMock())
    monkeypatch.setattr(
        plugin_module.GulpRedisBroker,
        "get_instance",
        staticmethod(lambda: broker),
    )
    return plugin, opensearch, config


@pytest.mark.unit
def test_adaptive_count_uses_observed_bytes_and_change_bounds() -> None:
    """Adaptive feedback remains one count boundary with bounded changes."""
    plugin = _IngestionPlugin(
        "/tmp/test_ingestion_pipeline.py", "test_ingestion_pipeline"
    )
    plugin._adaptive_chunk_enabled = True
    plugin._adaptive_chunk_size = 1000
    documents = [{}] * 1000
    chunk = _GulpIngestionChunk(
        documents=documents,
        records_processed=1000,
        records_failed=0,
        flt=None,
        prepared=GulpPreparedBulk(
            body=b"x",
            documents=tuple(documents),
            size_bytes=1024 * 1024,
            max_content_length_bytes=100 * 1024 * 1024,
        ),
    )

    plugin._update_adaptive_chunk_size(chunk)
    assert plugin._adaptive_chunk_size == 2000

    chunk.prepared = GulpPreparedBulk(
        body=b"x",
        documents=tuple(documents),
        size_bytes=20 * 1024 * 1024,
        max_content_length_bytes=100 * 1024 * 1024,
    )
    plugin._update_adaptive_chunk_size(chunk)
    assert plugin._adaptive_chunk_size == 1000

    plugin._adaptive_chunk_size = 6000
    chunk.prepared = GulpPreparedBulk(
        body=b"x",
        documents=tuple(documents),
        size_bytes=1024,
        max_content_length_bytes=100 * 1024 * 1024,
    )
    plugin._update_adaptive_chunk_size(chunk)
    assert plugin._adaptive_chunk_size == 8000


@pytest.mark.unit
def test_adaptive_count_uses_only_documents_in_the_nonempty_bulk() -> None:
    """Storage-filtered documents do not inflate the next count estimate."""
    plugin = _IngestionPlugin(
        "/tmp/test_ingestion_pipeline.py", "test_ingestion_pipeline"
    )
    plugin._adaptive_chunk_enabled = True
    plugin._adaptive_chunk_size = 4000
    accepted = tuple({"_id": str(index)} for index in range(500))
    chunk = _GulpIngestionChunk(
        documents=[{}] * 1000,
        records_processed=1000,
        records_failed=0,
        flt=None,
        prepared=GulpPreparedBulk(
            body=b"x",
            documents=accepted,
            size_bytes=1024 * 1024,
            max_content_length_bytes=100 * 1024 * 1024,
        ),
    )

    plugin._update_adaptive_chunk_size(chunk)

    assert plugin._adaptive_chunk_size == 2500


@pytest.mark.unit
def test_adaptive_count_preserves_ceil_half_bound_and_empty_target() -> None:
    """Integer feedback never drops below half and ignores empty bulks."""
    plugin = _IngestionPlugin(
        "/tmp/test_ingestion_pipeline.py", "test_ingestion_pipeline"
    )
    plugin._adaptive_chunk_enabled = True
    plugin._adaptive_chunk_size = 3
    chunk = _GulpIngestionChunk(
        documents=[{}] * 3,
        records_processed=3,
        records_failed=0,
        flt=None,
        prepared=GulpPreparedBulk(
            body=b"x",
            documents=({}, {}, {}),
            size_bytes=100 * 1024 * 1024,
            max_content_length_bytes=100 * 1024 * 1024,
        ),
    )

    plugin._update_adaptive_chunk_size(chunk)
    assert plugin._adaptive_chunk_size == 2

    chunk.prepared = GulpPreparedBulk(
        body=b"",
        documents=tuple(),
        size_bytes=0,
        max_content_length_bytes=100 * 1024 * 1024,
    )
    plugin._update_adaptive_chunk_size(chunk)
    assert plugin._adaptive_chunk_size == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_builds_next_chunk_while_only_one_bulk_is_active(
    pipeline_plugin,
) -> None:
    """Parsing progresses beside one bulk, never beside two bulk requests."""
    plugin, opensearch, _config = pipeline_plugin
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    active = 0
    max_active = 0

    async def bulk(_index, prepared, **_kwargs):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        if prepared.documents[0]["_id"] == "0":
            first_started.set()
            await release_first.wait()
        active -= 1
        return 0, list(prepared.documents), 0

    opensearch.bulk_ingest_prepared.side_effect = bulk

    assert await plugin.process_record("zero", 0)
    assert await plugin.process_record("one", 1)
    # The record parser above has no suspension point. The coordinator itself
    # must start the bulk before returning to build the next chunk.
    assert first_started.is_set()
    assert plugin._pipeline_active_chunk.sequence == 0
    assert await plugin.process_record("two", 2)
    assert [doc["_id"] for doc in plugin._docs_buffer] == ["2"]

    release_first.set()
    assert await plugin.process_record("three", 3)
    await plugin._acknowledge_pipeline_chunk(update_stats=True)

    assert max_active == 1
    assert opensearch.bulk_ingest_prepared.await_count == 2
    assert plugin._records_processed_total == 4
    assert plugin._records_ingested_total == 4
    assert plugin._docs_buffer == []


@pytest.mark.unit
@pytest.mark.asyncio
async def test_filtered_empty_chunk_still_observes_cancellation(
    pipeline_plugin, monkeypatch
) -> None:
    """A fully filtered chunk cannot bypass the cancellation checkpoint."""
    from gulp import plugin as plugin_module

    plugin, opensearch, _config = pipeline_plugin
    opensearch.prepare_bulk_ingest.side_effect = None
    opensearch.prepare_bulk_ingest.return_value = GulpPreparedBulk(
        body=b"",
        documents=tuple(),
        size_bytes=0,
        max_content_length_bytes=100 * 1024 * 1024,
    )
    monkeypatch.setattr(
        plugin_module.GulpRequestStats,
        "is_canceled",
        AsyncMock(return_value=True),
    )

    await plugin.process_record("zero", 0)
    await plugin.process_record("one", 1)
    with pytest.raises(RequestCanceledError):
        await plugin._acknowledge_pipeline_chunk(update_stats=True)

    opensearch.bulk_ingest_prepared.assert_not_awaited()
    assert plugin._records_processed_total == 2
    plugin._sess.commit.assert_awaited_once_with()


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize("pipeline_enabled", [False, True])
async def test_cancellation_poll_transaction_closes_before_next_record(
    pipeline_plugin, monkeypatch, pipeline_enabled: bool
) -> None:
    """Cancellation polling cannot hold a transaction during record parsing."""
    from gulp import plugin as plugin_module

    plugin, opensearch, _config = pipeline_plugin
    plugin._pipeline_enabled = pipeline_enabled
    transaction_open = False
    observed: list[str] = []

    async def bulk(_index, prepared, **_kwargs):
        return 0, list(prepared.documents), 0

    async def is_canceled(_sess, _req_id):
        nonlocal transaction_open
        assert transaction_open is False
        transaction_open = True
        observed.append("cancellation-check")
        return False

    async def commit():
        nonlocal transaction_open
        assert transaction_open is True
        transaction_open = False
        observed.append("commit")

    async def record_to_documents(_record, record_idx, **_kwargs):
        if record_idx == 2:
            assert transaction_open is False
            observed.append("next-record")
        return [{"_id": str(record_idx), "event.sequence": record_idx}]

    opensearch.bulk_ingest_prepared.side_effect = bulk
    plugin._sess.commit.side_effect = commit
    plugin._record_to_gulp_documents_wrapper.side_effect = record_to_documents
    monkeypatch.setattr(plugin_module.GulpRequestStats, "is_canceled", is_canceled)

    await plugin.process_record("zero", 0)
    await plugin.process_record("one", 1)
    if pipeline_enabled:
        await plugin._acknowledge_pipeline_chunk(update_stats=True)

    assert transaction_open is False
    await plugin.process_record("two", 2)

    assert observed == ["cancellation-check", "commit", "next-record"]
    assert plugin._stats.update_ingestion_stats.await_count == 1
    assert plugin._records_processed_total == 2
    assert plugin._records_ingested_total == 2


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pipeline_preserves_cross_chunk_event_order(
    pipeline_plugin, monkeypatch
) -> None:
    """The next pre-event cannot overtake the prior bulk/post acknowledgement."""
    plugin, opensearch, _config = pipeline_plugin
    observed: list[str] = []
    plugin._event_listener_mask = events.CHUNK_PRE_INGEST | events.CHUNK_POST_INGEST

    async def emit(event, **kwargs):
        first_id = kwargs["data"]["chunk"][0]["_id"]
        if event == GulpCoreEvent.CHUNK_PRE_INGEST:
            observed.append(f"pre-{first_id}")
            return GulpEventResult(
                event_id=f"pre-{first_id}",
                event=event,
                result={"chunk": kwargs["data"]["chunk"]},
            )
        observed.append(f"post-{first_id}")
        return None

    async def bulk(_index, prepared, **_kwargs):
        observed.append(f"bulk-{prepared.documents[0]['_id']}")
        return 0, list(prepared.documents), 0

    monkeypatch.setattr(events, "emit", AsyncMock(side_effect=emit))
    opensearch.bulk_ingest_prepared.side_effect = bulk

    for record_idx in range(4):
        await plugin.process_record(str(record_idx), record_idx)
    await plugin._acknowledge_pipeline_chunk(update_stats=True)

    assert observed == [
        "pre-0",
        "bulk-0",
        "post-0",
        "pre-2",
        "bulk-2",
        "post-2",
    ]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_cancellation_discards_constructed_next_chunk(
    pipeline_plugin, monkeypatch
) -> None:
    """A canceled acknowledgement prevents submission of speculative work."""
    from gulp import plugin as plugin_module

    plugin, opensearch, _config = pipeline_plugin

    async def bulk(_index, prepared, **_kwargs):
        await asyncio.sleep(0)
        return 0, list(prepared.documents), 0

    opensearch.bulk_ingest_prepared.side_effect = bulk
    monkeypatch.setattr(
        plugin_module.GulpRequestStats,
        "is_canceled",
        AsyncMock(return_value=True),
    )

    await plugin.process_record("zero", 0)
    await plugin.process_record("one", 1)
    await plugin.process_record("two", 2)
    with pytest.raises(RequestCanceledError):
        await plugin.process_record("three", 3)

    assert opensearch.prepare_bulk_ingest.await_count == 1
    assert opensearch.bulk_ingest_prepared.await_count == 1
    assert plugin._records_processed_total == 2
    assert plugin._records_ingested_total == 2
    assert plugin._docs_buffer == []
    assert plugin._pipeline_active_chunk is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bulk_error_discards_constructed_next_chunk(
    pipeline_plugin,
) -> None:
    """A failed active request cannot be followed by another submission."""
    plugin, opensearch, _config = pipeline_plugin
    opensearch.bulk_ingest_prepared.side_effect = RuntimeError("OpenSearch down")

    await plugin.process_record("zero", 0)
    await plugin.process_record("one", 1)
    await plugin.process_record("two", 2)
    with pytest.raises(RuntimeError, match="OpenSearch down"):
        await plugin.process_record("three", 3)

    assert opensearch.prepare_bulk_ingest.await_count == 1
    assert opensearch.bulk_ingest_prepared.await_count == 1
    assert plugin._records_processed_total == 2
    assert plugin._records_ingested_total == 0
    assert plugin._docs_buffer == []
    assert plugin._pipeline_active_chunk is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_bulk_failures_are_reported_and_consumed_before_thresholds(
    pipeline_plugin,
) -> None:
    """OpenSearch failures reach stats and the completed buffer cannot reflush."""
    plugin, opensearch, _config = pipeline_plugin
    plugin._pipeline_enabled = False
    opensearch.bulk_ingest_prepared.return_value = (
        0,
        [{"_id": "0", "event.sequence": 0}],
        1,
    )

    await plugin.process_record("zero", 0)
    await plugin.process_record("one", 1)

    stats_call = plugin._stats.update_ingestion_stats.await_args.kwargs
    assert stats_call["records_processed"] == 2
    assert stats_call["records_ingested"] == 1
    assert stats_call["records_failed"] == 1
    assert plugin._records_failed_total == 1
    assert plugin._docs_buffer == []

    await plugin._flush_buffer_and_send_to_ws(wait_for_refresh=True)
    assert opensearch.bulk_ingest_prepared.await_count == 1
    opensearch.index_refresh.assert_awaited_once_with("test-index")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_failure_threshold_cannot_reflush_acknowledged_chunk(
    pipeline_plugin,
) -> None:
    """A threshold exception happens after stats and detached-buffer consumption."""
    plugin, opensearch, config = pipeline_plugin
    plugin._pipeline_enabled = False
    config.ingestion_evt_failure_threshold.return_value = 1
    opensearch.bulk_ingest_prepared.return_value = (
        0,
        [{"_id": "0", "event.sequence": 0}],
        1,
    )

    await plugin.process_record("zero", 0)
    with pytest.raises(SourceCanceledError):
        await plugin.process_record("one", 1)

    assert plugin._docs_buffer == []
    assert plugin._stats.update_ingestion_stats.await_count == 1
    assert opensearch.bulk_ingest_prepared.await_count == 1

    await plugin._flush_buffer_and_send_to_ws(wait_for_refresh=True)
    assert opensearch.bulk_ingest_prepared.await_count == 1


@pytest.mark.unit
@pytest.mark.asyncio
async def test_empty_eof_after_active_bulk_refreshes_once(pipeline_plugin) -> None:
    """An exact-boundary EOF acknowledges the active bulk then refreshes once."""
    plugin, opensearch, _config = pipeline_plugin

    async def bulk(_index, prepared, **_kwargs):
        await asyncio.sleep(0)
        return 0, list(prepared.documents), 0

    opensearch.bulk_ingest_prepared.side_effect = bulk

    await plugin.process_record("zero", 0)
    await plugin.process_record("one", 1)
    await plugin._acknowledge_pipeline_chunk(update_stats=True)
    await plugin._flush_buffer_and_send_to_ws(wait_for_refresh=True)

    assert opensearch.bulk_ingest_prepared.await_count == 1
    opensearch.index_refresh.assert_awaited_once_with("test-index")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_finalization_reports_acknowledged_totals_once(
    pipeline_plugin, monkeypatch
) -> None:
    """Graceful EOF uses envelope totals for stats and the terminal packet."""
    from gulp.api.server_api import GulpServer

    plugin, opensearch, _config = pipeline_plugin
    plugin._source_id = "source"
    plugin._context_id = "context"
    plugin._file_path = "/tmp/source.log"
    plugin._docs_buffer = [{"_id": "0", "event.sequence": 0}]
    plugin._records_processed_per_chunk = 1
    plugin._stats.update_ingestion_stats.return_value = {"status": "done"}
    opensearch.bulk_ingest_prepared.return_value = (
        0,
        list(plugin._docs_buffer),
        0,
    )
    opensearch.datastream_update_source_field_types_by_src = AsyncMock()

    def consume_background(coro, _name):
        coro.close()

    monkeypatch.setattr(GulpServer, "spawn_bg_task", consume_background)
    monkeypatch.setattr(events, "emit", AsyncMock(return_value=None))

    status = await plugin.update_final_stats_and_flush()

    assert status == GulpRequestStatus.DONE
    assert plugin._records_processed_total == 1
    assert plugin._records_ingested_total == 1
    assert opensearch.bulk_ingest_prepared.await_count == 1
    final_stats = plugin._stats.update_ingestion_stats.await_args.kwargs
    assert final_stats["records_processed"] == 1
    assert final_stats["records_ingested"] == 1
    assert final_stats["records_failed"] == 0
    assert final_stats["status"] == GulpRequestStatus.DONE


@pytest.mark.unit
@pytest.mark.asyncio
async def test_unload_waits_for_active_bulk_task(pipeline_plugin, monkeypatch) -> None:
    """Plugin cleanup cannot leave an OpenSearch task running."""
    from gulp import plugin as plugin_module

    plugin, opensearch, _config = pipeline_plugin
    started = asyncio.Event()
    release = asyncio.Event()

    async def bulk(_index, prepared, **_kwargs):
        started.set()
        await release.wait()
        return 0, list(prepared.documents), 0

    opensearch.bulk_ingest_prepared.side_effect = bulk
    monkeypatch.setattr(
        plugin_module.GulpProcess,
        "get_instance",
        staticmethod(lambda: MagicMock(is_main_process=MagicMock(return_value=False))),
    )

    await plugin.process_record("zero", 0)
    await plugin.process_record("one", 1)
    await started.wait()
    unload_task = asyncio.create_task(plugin.unload())
    await asyncio.sleep(0)
    assert not unload_task.done()

    release.set()
    await unload_task
    assert plugin._pipeline_active_chunk is None
    assert plugin._records_ingested_total == 2


@pytest.mark.unit
def test_stacked_outer_plugin_uses_lowest_buffer_owner() -> None:
    """Terminal reporting reads the same instance that parsed and indexed."""
    upper = _IngestionPlugin("/tmp/upper.py", "upper")
    lower = _IngestionPlugin("/tmp/lower.py", "lower")
    upper._lower_instance = lower
    upper._ingestion_owner = lower
    lower._upper_instance = upper
    lower._records_processed_total = 9
    lower._records_ingested_total = 8
    lower._records_failed_total = 1

    owner = upper._ingestion_buffer_owner()
    upper._copy_ingestion_owner_state(owner)

    assert owner is lower
    assert upper._records_processed_total == 9
    assert upper._records_ingested_total == 8
    assert upper._records_failed_total == 1


@pytest.mark.unit
def test_nested_stacked_owner_survives_lifecycle_link_cleanup() -> None:
    """Unloading an intermediate plugin cannot orphan the deepest buffer."""
    upper = _IngestionPlugin("/tmp/upper.py", "upper")
    middle = _IngestionPlugin("/tmp/middle.py", "middle")
    lower = _IngestionPlugin("/tmp/lower.py", "lower")
    upper._lower_instance = middle
    upper._ingestion_owner = middle
    middle._lower_instance = lower
    middle._ingestion_owner = lower

    middle._lower_instance = None

    assert upper._ingestion_buffer_owner() is lower


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "adaptive_enabled",
        "pipeline_enabled",
        "override_chunk_size",
        "raw_ingestion",
        "preview_mode",
        "external_query",
        "expected_size",
        "expected_adaptive",
        "expected_pipeline",
    ),
    [
        (True, True, None, False, False, False, 1200, True, True),
        (True, True, 75, False, False, False, 75, False, True),
        (False, True, None, False, False, False, 1200, False, True),
        (True, True, None, True, False, False, 1200, True, False),
        (True, True, None, False, True, False, 1200, True, False),
        (True, True, None, False, False, True, 1200, True, False),
        (True, False, None, False, False, False, 1200, True, False),
    ],
)
async def test_file_ingestion_selects_one_count_policy_and_server_pipeline(
    monkeypatch,
    adaptive_enabled: bool,
    pipeline_enabled: bool,
    override_chunk_size: int | None,
    raw_ingestion: bool,
    preview_mode: bool,
    external_query: bool,
    expected_size: int,
    expected_adaptive: bool,
    expected_pipeline: bool,
) -> None:
    """Existing sizing inputs retain precedence; only gulp_cfg controls overlap."""
    from gulp import plugin as plugin_module

    plugin = _IngestionPlugin(
        "/tmp/test_ingestion_pipeline.py", "test_ingestion_pipeline"
    )
    plugin._raw_ingestion = raw_ingestion
    plugin._external_query = external_query
    params = GulpPluginParameters(
        override_chunk_size=override_chunk_size,
        preview_mode=preview_mode,
    )

    async def initialize(plugin_params):
        plugin._plugin_params = plugin_params

    plugin._initialize = AsyncMock(side_effect=initialize)
    plugin._update_source_mapping_parameters = AsyncMock()
    config = MagicMock()
    config.ingestion_documents_chunk_size.return_value = 1200
    config.ingestion_documents_adaptive_chunk_size.return_value = adaptive_enabled
    config.ingestion_pipeline_enabled.return_value = pipeline_enabled
    monkeypatch.setattr(
        plugin_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: config),
    )

    await plugin.ingest_file(
        sess=MagicMock(),
        stats=MagicMock(),
        user_id="user",
        req_id="request",
        ws_id="websocket",
        index="index",
        operation_id="operation",
        context_id="context",
        source_id="source",
        file_path="/not/read/by/adaptive-sizing.log",
        plugin_params=params,
    )

    assert plugin._adaptive_chunk_size == expected_size
    assert plugin._adaptive_chunk_enabled is expected_adaptive
    assert plugin._pipeline_enabled is expected_pipeline
    assert not hasattr(params, "override_ingestion_pipeline")


@pytest.mark.unit
@pytest.mark.asyncio
async def test_adaptive_initial_count_is_capped_but_fixed_count_is_not(
    monkeypatch,
) -> None:
    """The 8,000 ceiling belongs to adaptive mode, not fixed mode."""
    from gulp import plugin as plugin_module

    async def initialize(plugin_params):
        plugin._plugin_params = plugin_params

    config = MagicMock()
    config.ingestion_documents_chunk_size.return_value = 9000
    config.ingestion_documents_adaptive_chunk_size.side_effect = [True, False]
    config.ingestion_pipeline_enabled.return_value = False
    monkeypatch.setattr(
        plugin_module.GulpConfig,
        "get_instance",
        staticmethod(lambda: config),
    )

    plugin = _IngestionPlugin("/tmp/adaptive.py", "adaptive")
    plugin._initialize = AsyncMock(side_effect=initialize)
    plugin._update_source_mapping_parameters = AsyncMock()
    arguments = dict(
        sess=MagicMock(),
        stats=MagicMock(),
        user_id="user",
        req_id="request",
        ws_id="websocket",
        index="index",
        operation_id="operation",
        context_id="context",
        source_id="source",
        file_path="/not/read.log",
        plugin_params=GulpPluginParameters(),
    )

    await plugin.ingest_file(**arguments)
    assert plugin._adaptive_chunk_size == 8000

    await plugin.ingest_file(**arguments)
    assert plugin._adaptive_chunk_size == 9000
