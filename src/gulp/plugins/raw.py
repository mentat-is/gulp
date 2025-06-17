"""
a gulp plugin for ingesting `raw` (already processed, i.e. by an agent) GulpDocument entries without transformation.

the raw plugin may also be used by other plugins (i.e. `èxternal query` plugins in ingestion mode) to ingest the GulpDocument entries
they generate from the external source into the Gulp pipeline.

NOTE: should this broadcast ingestion internal event ? at the moment, it doesn't:
it would slow down a lot and generate really tons of data on postgres!
"""

import json
from typing import override
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import (
    GulpRequestStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpMappingParameters, GulpPluginParameters
from gulp.api.mapping.models import GulpMapping, GulpMappingField


class Plugin(GulpPluginBase):
    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    def display_name(self) -> str:
        return "raw"

    @override
    def desc(self) -> str:
        return """raw GulpDocuments ingestion plugin.
        
- documents are expected to have `gulp.context_id` and `gulp.source_id` fields set to existing GulpContext and GulpSource: if they do not exist, they will be created with `name` set to the given id.
"""

    @override
    async def _record_to_gulp_document(
        self, record: dict, record_idx: int, **kwargs
    ) -> GulpDocument:

        d: dict = record
        m = await self._process_key(
            "gulp.context_id", record["gulp.context_id"], record, **kwargs
        )
        d.update(m)
        m = await self._process_key(
            "gulp.source_id", d["gulp.source_id"], d, **kwargs
        )
        d.update(m)

        # create GulpDocument as is
        return GulpDocument(
            self,
            operation_id=self._operation_id,
            event_original=None,  # taken from the record
            event_sequence=None,  # taken from the record
            **d,
        )

    @override
    async def ingest_raw(
        self,
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        operation_id: str,
        chunk: bytes,
        stats: GulpRequestStats = None,
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
    ) -> GulpRequestStatus:

        js: list[dict] = []
        try:
            # initialize plugin
            if not plugin_params or plugin_params.mapping_parameters.is_empty():
                # using default mapping parameters
                mp: GulpMappingParameters = GulpMappingParameters(
                    mappings={
                        "raw_doc": GulpMapping(
                            fields={
                                "gulp.context_id": GulpMappingField(is_context=True),
                                "gulp.source_id": GulpMappingField(is_source=True),
                            }
                        ),
                    }
                )
                plugin_params = GulpPluginParameters(mapping_parameters=mp)
            await super().ingest_raw(
                sess=sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                index=index,
                operation_id=operation_id,
                chunk=chunk,
                stats=stats,
                flt=flt,
                plugin_params=plugin_params,
            )

            # chunk is a list of dicts, each dict being a GulpDocument record
            js = json.loads(chunk.decode("utf-8"))

        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            for rr in js:
                doc_idx += 1

                try:
                    await self.process_record(rr, doc_idx, flt=flt)
                except RequestCanceledError as ex:
                    MutyLogger.get_instance().exception(ex)
                    break
                except SourceCanceledError as ex:
                    await self._source_failed(ex)
                    break
        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
        return self._stats_status()
