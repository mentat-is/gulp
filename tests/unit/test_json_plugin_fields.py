"""Isolated coverage for JSON field traversal with unmapped_as_is enabled."""

import pytest

from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.plugins.json import Plugin


@pytest.mark.unit
def test_json_preserves_geo_points_and_arrays_for_unmapped_as_is() -> None:
    """Keep typed leaves intact while visiting ordinary objects by field path."""
    plugin = Plugin("src/gulp/plugins/json.py", "gulp.plugins.json")
    plugin._index_type_mapping = {"destination.geo.location": "geo_point"}
    plugin._mappings = {
        "default": GulpMapping(
            fields={
                "destination.geo.location.lat": GulpMappingField(
                    ecs="custom.latitude"
                )
            }
        )
    }
    plugin._mapping_id = "default"

    record = {
        "destination": {
            "geo": {"location": {"lon": -87.6305, "lat": 41.8835}},
            "port": 443,
        },
        "event": {"category": ["network"], "type": ["connection", "start"]},
        "related": {"ip": ["10.135.85.18", "52.159.127.243"]},
    }

    assert dict(plugin._iter_preserved_fields(record)) == {
        "destination.geo.location": {"lon": -87.6305, "lat": 41.8835},
        "destination.geo.location.lat": 41.8835,
        "destination.port": 443,
        "event.category": ["network"],
        "event.type": ["connection", "start"],
        "related.ip": ["10.135.85.18", "52.159.127.243"],
    }


@pytest.mark.unit
def test_json_detects_non_numeric_values_for_existing_numeric_fields() -> None:
    """Reject a named pipe string from a dynamically numeric Zeek field."""
    plugin = Plugin("src/gulp/plugins/json.py", "gulp.plugins.json")
    plugin._index_type_mapping = {"zeek.dce_rpc.named_pipe": "long"}

    assert plugin._unmapped_numeric_conflict(
        "zeek.dce_rpc.named_pipe", r"\pipe\lsass"
    )
    assert not plugin._unmapped_numeric_conflict("zeek.dce_rpc.named_pipe", "123")
    assert plugin._unmapped_numeric_conflict("zeek.dce_rpc.named_pipe", [1, "pipe"])
    assert not plugin._unmapped_numeric_conflict("zeek.dce_rpc.endpoint", "pipe")


@pytest.mark.unit
def test_json_detects_numeric_type_created_during_same_request() -> None:
    """A numeric first value can make a later string invalid in the same file."""
    plugin = Plugin("src/gulp/plugins/json.py", "gulp.plugins.json")

    assert not plugin._unmapped_numeric_conflict("zeek.dce_rpc.named_pipe", "42")
    assert plugin._unmapped_numeric_conflict(
        "zeek.dce_rpc.named_pipe", r"\pipe\lsass"
    )
    assert not plugin._unmapped_numeric_conflict("zeek.dce_rpc.endpoint", "pipe")
    assert not plugin._unmapped_numeric_conflict("zeek.dce_rpc.endpoint", "42")
