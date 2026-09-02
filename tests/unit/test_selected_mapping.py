"""Selected-mapping fallback and dynamic-selection regressions."""

import pytest

from gulp.api.mapping.models import GulpMapping
from gulp.plugin import GulpPluginBase, GulpPluginType


class _IngestionPlugin(GulpPluginBase):
    """Minimal plugin used to exercise base mapping selection."""

    def display_name(self) -> str:
        return "selected_mapping_test"

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION


@pytest.mark.unit
def test_selected_mapping_reuses_one_empty_fallback_per_instance() -> None:
    """Missing mappings reuse a fallback without sharing it across plugins."""
    first_plugin = _IngestionPlugin("/tmp/first.py", "first")
    second_plugin = _IngestionPlugin("/tmp/second.py", "second")

    first_fallback = first_plugin.selected_mapping()

    assert first_fallback == GulpMapping()
    assert first_plugin.selected_mapping() is first_fallback
    assert second_plugin.selected_mapping() is not first_fallback


@pytest.mark.unit
def test_selected_mapping_keeps_mapping_id_lookup_dynamic() -> None:
    """Changing or populating the current mapping ID takes effect immediately."""
    plugin = _IngestionPlugin("/tmp/dynamic.py", "dynamic")
    mapping_a = GulpMapping(description="mapping A")
    mapping_b = GulpMapping(description="mapping B")
    plugin._mappings = {"a": mapping_a, "b": mapping_b}

    plugin._mapping_id = "a"
    assert plugin.selected_mapping() is mapping_a

    plugin._mapping_id = "b"
    assert plugin.selected_mapping() is mapping_b

    plugin._mapping_id = "added_later"
    fallback = plugin.selected_mapping()
    plugin._mappings["added_later"] = mapping_a

    assert plugin.selected_mapping() is mapping_a
    assert fallback is plugin._empty_mapping
