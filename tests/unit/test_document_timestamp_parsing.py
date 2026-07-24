"""Document construction regression coverage."""

from types import SimpleNamespace

import pytest
import muty.time

from gulp.api.mapping.models import GulpMapping
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.s3_api import GulpS3
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginParameters


@pytest.mark.unit
@pytest.mark.parametrize(
    ("value", "expected_nanos"),
    [
        ("1999-04-23T22:22:00+00:00", 924_906_120_000_000_000),
        ("2004-08-18T00:00:00+00:00", 1_092_787_200_000_000_000),
    ],
)
def test_ensure_timestamp_handles_nanoseconds_on_both_sides_of_1e18(
    value: str, expected_nanos: int
) -> None:
    timestamp, nanos, invalid = GulpDocument.ensure_timestamp(value)

    assert muty.time.nanos_from_unix_epoch_to_iso8601(expected_nanos) == value
    assert timestamp == value
    assert nanos == expected_nanos
    assert invalid is False


@pytest.mark.unit
def test_stored_file_documents_use_the_uploaded_object_id() -> None:
    """Mapped document IDs must not change the stored file object ID."""
    plugin = SimpleNamespace(
        _plugin_params=GulpPluginParameters(store_file=True),
        _file_path="/tmp/evidence.evtx",
        _operation_id="ingest-operation",
        _context_id="ingest-context",
        _source_id="ingest-source",
        _user_id="user",
        _req_id="request",
        stored_file_id=lambda: "user/ingest-operation/ingest-c/evidence.evtx",
        name="test",
        doc_value_cache=SimpleNamespace(
            get_value=lambda _: None, set_value=lambda *_: None
        ),
        selected_mapping=lambda: GulpMapping(),
        _get_timestamp_format_for_default_timestamp=lambda: None,
    )

    document = GulpDocument(
        plugin,
        "mapped-operation",
        "event",
        context_id="mapped-context",
        source_id="mapped-source",
        timestamp="2024-01-01T00:00:00Z",
    )

    assert document.storage_id == "user/ingest-operation/ingest-c/evidence.evtx"


@pytest.mark.unit
def test_stored_file_id_is_built_once_per_ingestion(monkeypatch) -> None:
    """The upload and all documents share one cached stored-file ID."""
    class Plugin(GulpPluginBase):
        def display_name(self) -> str:
            return "test"

        def type(self) -> GulpPluginType:
            return GulpPluginType.INGESTION

    plugin = Plugin("/tmp/test.py", "test")
    plugin._file_path = "/tmp/evidence.evtx"
    plugin._operation_id = "operation"
    plugin._context_id = "context"
    plugin._source_id = "source"
    plugin._user_id = "user"
    plugin._req_id = "request"
    calls: list[tuple] = []

    def build_object_name(*args: str) -> str:
        calls.append(args)
        return "user/operation/context/evidence.evtx"

    monkeypatch.setattr(GulpS3, "build_object_name", build_object_name)

    assert plugin.stored_file_id() == "user/operation/context/evidence.evtx"
    assert plugin.stored_file_id() == "user/operation/context/evidence.evtx"
    assert len(calls) == 1
