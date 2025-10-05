"""
this is just an example of a stacked plugin, to allow a gulp plugin to sit on top of another and post-process
the records before ingestion
"""

from typing import override
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginParameters


class Plugin(GulpPluginBase):
    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    def display_name(self) -> str:
        return "stacked_example"

    @override
    def desc(self) -> str:
        return """stacked plugin on top of csv example"""

    @override
    async def _enrich_documents_chunk(
        self,
        chunk: list[dict],
        chunk_num: int = 0,
        total_hits: int = 0,
        ws_id: str = None,
        user_id: str = None,
        req_id: str = None,
        operation_id: str = None,
        q_name: str = None,
        chunk_total: int = 0,
        q_group: str = None,
        last: bool = False,
        **kwargs,
    ) -> list[dict]:
        for doc in chunk:
            doc["enriched"] = True
        return chunk

    @override
    async def _record_to_gulp_document(
        self, record: dict, record_idx: int, **kwargs
    ) -> dict:

        # MutyLogger.get_instance().debug("record: %s" % record)
        # tweak event duration ...
        record["event.duration"] = 9999
        return record

    @override
    async def ingest_file(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        file_path: str,
        original_file_path: str = None,
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> GulpRequestStatus:

        await super().ingest_file(
            sess=sess,
            stats=stats,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            index=index,
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            file_path=file_path,
            original_file_path=original_file_path,
            plugin_params=plugin_params,
            flt=flt,
            **kwargs,
        )

        # set as stacked
        try:
            lower = await self.setup_stacked_plugin("csv")
        except Exception as ex:
            await self._source_failed(ex)
            return GulpRequestStatus.FAILED

        # call lower plugin, which in turn will call our record_to_gulp_document after its own processing
        res = await lower.ingest_file(
            sess=sess,
            stats=stats,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            index=index,
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            file_path=file_path,
            original_file_path=original_file_path,
            plugin_params=plugin_params,
            flt=flt,
        )
        await lower.unload()
        return res
