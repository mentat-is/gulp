from typing import Any, override

from sqlalchemy.ext.asyncio import AsyncSession
from muty.log import MutyLogger
from gulp.api.collab.stats import (
    GulpRequestStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import (
    GulpDocument,
    GulpRawDocument,
    GulpRawDocumentBaseFields,
)
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    ingests raw events

    this plugin is used to ingest raw events, without any transformation.

    the input for this plugin is a list of GulpDocument dictionaries coming from i.e. a SIEM agent.
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def display_name(self) -> str:
        return "raw"

    @override
    def desc(self) -> str:
        return "Raw GulpDocuments ingestion plugin"

    @override
    async def _record_to_gulp_document(
        self, record: dict, record_idx: int, data: Any = None
    ) -> GulpDocument:
        # create a gulp document
        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=None,  # will be set from record
            event_sequence=record_idx,
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
        context_id: str,
        source_id: str,
        chunk: list[dict],
        stats: GulpRequestStats = None,
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
    ) -> GulpRequestStatus:
        await super().ingest_raw(
            sess=sess,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            index=index,
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            chunk=chunk,
            stats=stats,
            plugin_params=plugin_params,
            flt=flt,
        )
        try:
            # initialize plugin
            if not plugin_params:
                plugin_params = GulpPluginParameters()
            await self._initialize(plugin_params)
        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            for rr in chunk:
                doc_idx += 1

                try:
                    await self.process_record(rr, doc_idx, flt)
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
