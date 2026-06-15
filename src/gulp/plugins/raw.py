"""
a gulp plugin for ingesting `raw` (already processed, i.e. by an agent) GulpDocument entries without transformation.

the raw plugin may also be used by other plugins (i.e. `èxternal query` plugins in ingestion mode) to ingest the GulpDocument entries
they generate from the external source into the Gulp pipeline.

NOTE: should this broadcast ingestion internal event ? at the moment, it doesn't:
it would slow down a lot and generate really tons of data on postgres!
"""

from typing import override

from gulp_cli.commands import mapping
import orjson
from muty.dict import flatten
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import (
    GulpRequestStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import (
    GulpMappingParameters,
    GulpPluginCustomParameter,
    GulpPluginParameters,
)


class Plugin(GulpPluginBase):
    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def display_name(self) -> str:
        return "raw"

    @override
    def desc(self) -> str:
        return """raw GulpDocuments ingestion plugin.
- documents are expected to have `gulp.context_id` and `gulp.source_id` fields set to existing GulpContext and GulpSource: if they do not exist, they will be created with `name` set to the given id.
- if `source_id` custom parameter is set, it will override the `gulp.source_id` field in the document.
"""

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="override_source_id",
                type="str",
                desc="overrides source id with the given value",
                default_value=None,
            ),
        ]

    @override
    async def _record_to_gulp_document(
        self, record: dict, record_idx: int, **kwargs
    ) -> GulpDocument:
        event_original = record.get("event.original") or record.get("event_original")
        if event_original is None:
            try:
                event_original = orjson.dumps(record).decode("utf-8")
            except TypeError:
                event_original = str(record)

        # Process each input field through the standard mapping engine so
        # raw ingest can apply plugin_params.mapping_parameters end-to-end.
        d: dict = {}
        flattened = flatten(record)
        mapping = self.selected_mapping()
        for source_key, source_value in flattened.items():
            m = await self._process_key(source_key, source_value, d, **kwargs)
            if m:
                # if the field is mapped, use the mapped value in the ingested document
                d.update(m)
                field_mapping: GulpMappingField = mapping.fields.get(source_key)
                if field_mapping and field_mapping.is_gulp_type:
                    # if the field is a Gulp type, also keep the original value in the ingested document with dot notation (for backward compatibility and ease of querying)
                    d[source_key] = source_value
            else:
                # if the field is not mapped, keep it as is in the ingested document (flattened with dot notation)
                if not mapping.exclude or (
                    mapping.exclude and source_key not in mapping.exclude
                ):
                    d[source_key] = source_value

        # Keep source override behavior for SDK/CLI compatibility.
        override: str = self._plugin_params.custom_parameters.get("override_source_id")
        if override:
            m = await self._process_key("gulp.source_id", override, d, **kwargs)
            d.update(m)

        # Build the final document from mapped values, preserving record-level
        # event_original/event_sequence values when provided in the payload.
        # MutyLogger.get_instance().debug("raw: d=%s, flattened=%s" % (d, flattened))
        return GulpDocument(
            self,
            operation_id=self._operation_id,
            event_original=str(event_original),
            event_sequence=None,  # taken from the record
            **d,
        )

    @override
    async def ingest_raw(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        operation_id: str,
        chunk: bytes,
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
        last: bool = False,
        **kwargs,
    ) -> GulpRequestStatus:

        js: list[dict] = []
        # initialize plugin
        plugin_params = self._ensure_plugin_params(
            plugin_params,
            mappings={
                "default": GulpMapping(
                    fields={
                        # as default, treats these fields as GulpContext and GulpSource ids (creates them if not existing)
                        "gulp.context_id": GulpMappingField(is_gulp_type="context_id"),
                        "gulp.source_id": GulpMappingField(
                            is_gulp_type="source_id",
                        ),
                    }
                ),
            },
        )
        # no unmapped fields in raw ingestion
        plugin_params.override_allow_unmapped_fields = False

        await super().ingest_raw(
            sess,
            stats,
            user_id,
            req_id,
            ws_id,
            index,
            operation_id,
            chunk,
            flt=flt,
            plugin_params=plugin_params,
            last=last,
            **kwargs,
        )

        # chunk is a list of dicts, each dict being a GulpDocument record
        js = orjson.loads(chunk.decode("utf-8"))

        # walk each document in the chunk
        doc_idx: int = 0
        for rr in js:
            if not await self.process_record(rr, doc_idx, flt=flt):
                # stop processing (preview mode)
                break
            doc_idx += 1
        return stats.status
