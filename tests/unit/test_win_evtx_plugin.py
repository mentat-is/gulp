import pytest
import orjson

from gulp.plugins.win_evtx import Plugin


def _plugin() -> Plugin:
    """Create a win_evtx plugin with default runtime state for helper tests."""
    return Plugin("/tmp/win_evtx.py", "gulp.plugins.win_evtx")


def _ready_plugin() -> Plugin:
    """Create a win_evtx plugin with enough ingest state to build documents."""
    plugin = _plugin()
    plugin._operation_id = "op"
    plugin._context_id = "ctx"
    plugin._source_id = "src"
    plugin._file_path = "/tmp/source.evtx"
    return plugin


@pytest.mark.unit
def test_win_evtx_uses_system_time_when_parser_timestamp_is_filetime_zero():
    plugin = _plugin()
    record = {"timestamp": "1601-01-01T00:00:00Z UTC"}
    raw = {
        "Event": {
            "System": {
                "TimeCreated": {
                    "#attributes": {"SystemTime": "2019-05-16T01:38:19.630865Z"}
                }
            }
        }
    }

    assert plugin._event_timestamp(record, raw) == "2019-05-16T01:38:19.630865Z"


@pytest.mark.unit
def test_win_evtx_prefers_valid_parser_timestamp():
    plugin = _plugin()
    record = {"timestamp": "2020-08-26T05:09:33.5046471Z UTC"}
    raw = {
        "Event": {
            "System": {
                "TimeCreated": {
                    "#attributes": {"SystemTime": "2019-05-16T01:38:19.630865Z"}
                }
            }
        }
    }

    assert plugin._event_timestamp(record, raw) == "2020-08-26T05:09:33.5046471Z"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_win_evtx_missing_timestamps_become_invalid_document_timestamp():
    plugin = _ready_plugin()
    raw = {"Event": {"System": {"EventID": 1}}}
    record = {"data": orjson.dumps(raw).decode()}

    doc = await plugin._record_to_gulp_document(record, 0)

    assert doc.timestamp == "1970-01-01T00:00:00Z"
    assert doc.gulp_timestamp == 0
    assert doc.invalid_timestamp is True
