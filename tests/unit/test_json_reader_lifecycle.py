"""JSON reader thread lifecycle regressions."""

import json
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gulp.api.collab.structs import GulpRequestStatus
from gulp.plugins import json as json_plugin
from gulp.plugins.json import Plugin


@pytest.mark.unit
@pytest.mark.asyncio
async def test_json_preview_waits_for_reader_thread_to_stop(monkeypatch, tmp_path):
    source = tmp_path / "records.json"
    source.write_text(json.dumps([{"value": n} for n in range(1_000)]))
    plugin = object.__new__(Plugin)
    plugin._plugin_params = SimpleNamespace(custom_parameters={"keys": None})
    plugin.process_record = AsyncMock(return_value=False)
    finished = threading.Event()
    original_reader = plugin._read_file

    def _tracked_reader(*args):
        try:
            original_reader(*args)
        finally:
            finished.set()

    plugin._read_file = _tracked_reader
    original_convert = json_plugin.json_stream.to_standard_types

    def _slow_convert(value):
        time.sleep(0.001)
        return original_convert(value)

    monkeypatch.setattr(json_plugin.json_stream, "to_standard_types", _slow_convert)
    executor = ThreadPoolExecutor(max_workers=1)
    monkeypatch.setattr(
        json_plugin.GulpProcess,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(thread_pool=executor)),
    )

    try:
        result = await plugin._ingest_jsonlist_and_jsondict(
            str(source),
            SimpleNamespace(status=GulpRequestStatus.DONE),
            encoding="utf-8",
        )
        print("JSON reader stopped after preview:", finished.is_set())
        assert result is None
        assert finished.is_set()
        plugin.process_record.assert_awaited_once()
    finally:
        executor.shutdown(wait=True)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_full_json_queue_applies_backpressure_without_losing_records(
    monkeypatch, tmp_path
):
    source = tmp_path / "records.json"
    expected = list(range(250))
    source.write_text(json.dumps([{"value": value} for value in expected]))
    plugin = object.__new__(Plugin)
    plugin.JSON_QUEUE_MAX_SIZE = 3
    plugin._plugin_params = SimpleNamespace(custom_parameters={"keys": None})
    processed: list[int] = []

    async def _process(record, *_args, **_kwargs):
        processed.append(record["value"])
        await json_plugin.asyncio.sleep(0.001)
        return True

    plugin.process_record = AsyncMock(side_effect=_process)
    queue_instances: list[queue.Queue] = []
    original_queue = queue.Queue

    class _TrackingQueue(original_queue):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.max_seen = 0
            queue_instances.append(self)

        def put(self, *args, **kwargs):
            result = super().put(*args, **kwargs)
            self.max_seen = max(self.max_seen, self.qsize())
            return result

    monkeypatch.setattr(json_plugin.queue, "Queue", _TrackingQueue)
    executor = ThreadPoolExecutor(max_workers=1)
    monkeypatch.setattr(
        json_plugin.GulpProcess,
        "get_instance",
        staticmethod(lambda: SimpleNamespace(thread_pool=executor)),
    )

    try:
        result = await plugin._ingest_jsonlist_and_jsondict(
            str(source),
            SimpleNamespace(status=GulpRequestStatus.DONE),
            encoding="utf-8",
        )
        print(
            "JSON queue max/processed:",
            queue_instances[0].max_seen,
            len(processed),
        )
        assert result == GulpRequestStatus.DONE
        assert queue_instances[0].max_seen == plugin.JSON_QUEUE_MAX_SIZE
        assert processed == expected
    finally:
        executor.shutdown(wait=True)
