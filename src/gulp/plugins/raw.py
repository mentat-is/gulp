"""
a gulp plugin for ingesting `raw` (already processed, i.e. by an agent) GulpDocument entries without transformation.

the raw plugin may also be used by other plugins (i.e. `èxternal query` plugins in ingestion mode) to ingest the GulpDocument entries
they generate from the external source into the Gulp pipeline.

NOTE: should this broadcast ingestion internal event ? at the moment, it doesn't:
it would slow down a lot and generate really tons of data on postgres!
"""

from typing import override
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import GulpRequestStats, RequestCanceledError, SourceCanceledError
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginParameters

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

        # create context/source if they do not exists
        self._context_id, self._source_id = await self._add_context_and_source_from_doc(
            record
        )

        # create GulpDocument as is
        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=None, # taken from the record
            event_sequence=None, # taken from the record
            **record,
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
        chunk: list[dict],
        stats: GulpRequestStats = None,
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
    ) -> GulpRequestStatus:
        try:
            # initialize plugin
            if not plugin_params:
                plugin_params = GulpPluginParameters()
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
        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            for rr in chunk:
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
