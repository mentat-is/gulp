"""Document construction and timestamp normalization regression coverage."""

import datetime
import os
import time
from types import SimpleNamespace

import pytest
import muty.crypto
import muty.time

from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.api.opensearch.structs import GulpDocument, GulpNormalizedTimestamp
from gulp.api.s3_api import GulpS3
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginParameters


def _document_plugin(
    mapping: GulpMapping | None = None,
    plugin_params: GulpPluginParameters | None = None,
) -> SimpleNamespace:
    """Build the runtime surface required by ``GulpDocument``."""
    selected_mapping = mapping or GulpMapping()

    def mapping_timestamp_format() -> str | None:
        for field in selected_mapping.fields.values():
            ecs = field.ecs if isinstance(field.ecs, list) else [field.ecs]
            if "@timestamp" in ecs:
                return field.timestamp_format
        return None

    return SimpleNamespace(
        _plugin_params=plugin_params or GulpPluginParameters(),
        name="timestamp_test",
        doc_value_cache=SimpleNamespace(
            get_value=lambda _: None, set_value=lambda *_: None
        ),
        selected_mapping=lambda: selected_mapping,
        _get_timestamp_format_for_default_timestamp=mapping_timestamp_format,
        stored_file_id=lambda: None,
    )


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
def test_explicit_format_normalization_matches_legacy_double_parse() -> None:
    """The one-pass path must preserve timestamp, nanoseconds, and offset."""
    raw = "20/Nov/2024:17:15:42 +0100"
    timestamp_format = "%d/%b/%Y:%H:%M:%S %z"
    params = GulpPluginParameters(timestamp_offset_msec=125)
    legacy_iso = datetime.datetime.strptime(raw, timestamp_format).isoformat()
    legacy = GulpDocument.ensure_timestamp(legacy_iso, plugin_params=params)

    normalized = GulpDocument.normalize_timestamp(
        raw,
        plugin_params=params,
        timestamp_format=timestamp_format,
    )

    assert (normalized.iso8601, normalized.nanos, normalized.invalid) == legacy


@pytest.mark.unit
def test_document_uses_timestamp_format_precedence_and_preserves_id() -> None:
    """Explicit format wins over mapping format without changing the ID contract."""
    mapping = GulpMapping(
        fields={
            "source_time": GulpMappingField(
                ecs="@timestamp", timestamp_format="%d/%m/%Y %H:%M:%S"
            )
        }
    )
    plugin = _document_plugin(mapping)
    raw = "03/04/2024 05:06:07"
    explicit_format = "%m/%d/%Y %H:%M:%S"
    expected = GulpDocument.normalize_timestamp(raw, timestamp_format=explicit_format)

    document = GulpDocument(
        plugin,
        operation_id="operation",
        context_id="context",
        source_id="source",
        event_original="event",
        event_sequence=7,
        timestamp=raw,
        timestamp_format=explicit_format,
        reject_invalid_timestamp=True,
    )

    assert document.timestamp == expected.iso8601
    assert document.gulp_timestamp == expected.nanos
    assert document.id == muty.crypto.hash_xxh128(
        f"event0operationcontextsource7{expected.nanos}"
    )

    mapped_document = GulpDocument(
        plugin,
        operation_id="operation",
        context_id="context",
        source_id="source",
        event_original="event",
        timestamp=raw,
        reject_invalid_timestamp=True,
    )
    mapped_expected = GulpDocument.normalize_timestamp(
        raw, timestamp_format="%d/%m/%Y %H:%M:%S"
    )
    assert mapped_document.gulp_timestamp == mapped_expected.nanos
    assert mapped_document.gulp_timestamp != document.gulp_timestamp


@pytest.mark.unit
def test_naive_timestamp_timezone_can_be_local_or_utc() -> None:
    """Naive timestamps default to local time and may explicitly select UTC."""
    previous_timezone = os.environ.get("TZ")
    os.environ["TZ"] = "America/New_York"
    time.tzset()
    try:
        local = GulpDocument.normalize_timestamp(
            "2024-01-15 12:00:00",
            timestamp_format="%Y-%m-%d %H:%M:%S",
        )
        utc = GulpDocument.normalize_timestamp(
            "2024-01-15 12:00:00",
            timestamp_format="%Y-%m-%d %H:%M:%S",
            timestamp_naive_timezone="utc",
        )
        autodetected_local = GulpDocument.normalize_timestamp(
            "2024-01-15 12:00:00"
        )
        autodetected_utc = GulpDocument.normalize_timestamp(
            "2024-01-15 12:00:00",
            timestamp_naive_timezone="utc",
        )
        legacy_timestamp = GulpDocument.ensure_timestamp("2024-01-15 12:00:00")
    finally:
        if previous_timezone is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous_timezone
        time.tzset()

    assert local.nanos - utc.nanos == 5 * 60 * 60 * 1_000_000_000
    assert autodetected_local.nanos == local.nanos
    assert autodetected_utc.nanos == utc.nanos
    assert legacy_timestamp[1] == utc.nanos


@pytest.mark.unit
def test_pre_normalized_timestamp_does_not_apply_offset_twice() -> None:
    """Passing a normalized value must bypass parsing and offset application."""
    params = GulpPluginParameters(timestamp_offset_msec=250)
    plugin = _document_plugin(plugin_params=params)
    normalized = GulpDocument.normalize_timestamp(
        "2024-01-01T00:00:00Z", plugin_params=params
    )

    document = GulpDocument(
        plugin,
        operation_id="operation",
        context_id="context",
        source_id="source",
        event_original="event",
        normalized_timestamp=normalized,
    )

    assert document.timestamp == normalized.iso8601
    assert document.gulp_timestamp == normalized.nanos


@pytest.mark.unit
def test_invalid_timestamp_rejection_and_removed_internal_flag() -> None:
    """Strict plugins reject invalid input and the removed flag never leaks."""
    plugin = _document_plugin()

    with pytest.raises(ValueError, match="invalid timestamp"):
        GulpDocument(
            plugin,
            operation_id="operation",
            context_id="context",
            source_id="source",
            event_original="event",
            timestamp="not-a-time",
            timestamp_format="%Y-%m-%d",
            reject_invalid_timestamp=True,
        )

    document = GulpDocument(
        plugin,
        operation_id="operation",
        context_id="context",
        source_id="source",
        event_original="event",
        timestamp="not-a-time",
        **{"__timestamp_format__": "%Y-%m-%d"},
    )

    assert document.timestamp == "1970-01-01T00:00:00Z"
    assert document.gulp_timestamp == 0
    assert document.invalid_timestamp is True
    assert "__timestamp_format__" not in document.model_dump()


@pytest.mark.unit
def test_ensure_timestamp_retains_formatted_fallback_compatibility() -> None:
    """External plugins using ensure_timestamp keep format-fallback behavior."""
    timestamp, nanos, invalid = GulpDocument.ensure_timestamp(
        "2024-01-02T03:04:05Z", format_string="%d/%m/%Y %H:%M:%S"
    )

    assert timestamp == "2024-01-02T03:04:05+00:00"
    assert nanos == 1_704_164_645_000_000_000
    assert invalid is False


@pytest.mark.unit
def test_normalized_timestamp_is_immutable() -> None:
    """Pre-normalized timestamp values are safe to pass between plugin stages."""
    normalized = GulpNormalizedTimestamp(
        "2024-01-01T00:00:00+00:00", 1_704_067_200_000_000_000, False
    )

    with pytest.raises((AttributeError, TypeError)):
        normalized.nanos = 0


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
