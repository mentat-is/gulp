import json

import pytest

from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.plugins.raw import Plugin
from gulp.structs import GulpPluginParameters


@pytest.mark.unit
@pytest.mark.asyncio
async def test_raw_plugin_defaults_event_original_to_serialized_record():
    plugin = Plugin("/tmp/raw.py", "gulp.plugins.raw")
    plugin._preview_mode = True
    plugin._operation_id = "op"
    plugin._plugin_params = plugin._ensure_plugin_params(
        GulpPluginParameters(),
        mappings={
            "raw_doc": GulpMapping(
                fields={
                    "gulp.context_id": GulpMappingField(is_gulp_type="context_id"),
                    "gulp.source_id": GulpMappingField(is_gulp_type="source_id"),
                    "ts": GulpMappingField(ecs=["@timestamp"]),
                    "EventCode": GulpMappingField(ecs=["event.code"]),
                }
            )
        },
        mapping_id="raw_doc",
    )
    plugin._mappings = plugin._plugin_params.mapping_parameters.mappings
    plugin._mapping_id = "raw_doc"

    record = {
        "ts": "2024-01-01T00:00:00Z",
        "EventCode": "4624",
        "gulp.context_id": "ctx-preview",
        "gulp.source_id": "src-preview",
    }

    doc = await plugin._record_to_gulp_document(record, 7)

    assert json.loads(doc.event_original) == record
    assert doc.event_code == "4624"
    assert doc.context_id == "preview"
    assert doc.source_id == "preview"
