import mailbox
from typing import Any, override

from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.stats import (
    GulpRequestStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    this plugin demonstrates how to use another plugin to process the data, using the GulpPluginBase.load_plugin method

    this allow to stack one plugin on top of another the data is processed by calling the lower plugin directly, bypassing the engine
    """

    @override
    def desc(self) -> str:
        return """generic MBOX file processor"""

    def display_name(self) -> str:
        return "mbox"

    @override
    def depends_on(self) -> list[str]:
        return ["eml"]

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="decode",
                type="bool",
                desc="attempt to decode messages wherever possible",
                default_value=True,
            )
        ]

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, data: Any = None
    ) -> GulpDocument:
        # document is processed by eml plugin
        return await self._eml_parser._record_to_gulp_document(record, record_idx, data)

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
        plugin_params: GulpPluginParameters = None,
        flt: GulpIngestionFilter = None,
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
        )
        try:
            # initialize plugin
            await self._initialize(plugin_params)

            # load eml plugin
            self._eml_parser = await self.load_plugin(
                "eml",
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
            )

        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            mbox = mailbox.mbox(file_path)
            for message in mbox.itervalues():
                try:
                    await self.process_record(message, doc_idx, flt=flt)
                except (RequestCanceledError, SourceCanceledError) as ex:
                    MutyLogger.get_instance().exception(ex)
                    await self._source_failed(ex)
                doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
            await self._eml_parser.unload()
            return self._stats_status()
