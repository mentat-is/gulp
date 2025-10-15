"""
MBOX Plugin for Gulp

This module provides a plugin for processing MBOX files (mailbox format) by leveraging the EML plugin to handle individual email messages extracted from the MBOX container.

It acts as a bridge between MBOX files and email processing capabilities, demonstrating how to use another plugin to process
the data using GulpPluginBase.load_plugin_direct method: this allow to stack one plugin on top of another and process the data calling the lower plugin directly, bypassing the engine.
"""

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
    @override
    def desc(self) -> str:
        return """generic MBOX file processor"""

    def display_name(self) -> str:
        return "mbox"

    @override
    def depends_on(self) -> list[str]:
        return ["eml"]

    def regex(self) -> str:
        """regex to identify this format"""
        return None

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
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        # document is processed by eml plugin
        eml_parser: GulpPluginBase = kwargs["eml_parser"]

        # pylint: disable=W0212
        # call the eml plugin directly
        return await eml_parser._record_to_gulp_document(record, record_idx, **kwargs)

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
        try:
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

            # load eml plugin
            eml_parser = await self.load_plugin_direct(
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
            await self.source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            mbox = mailbox.mbox(file_path)
            for message in mbox.itervalues():
                try:
                    await self.process_record(
                        message, doc_idx, flt=flt, eml_parser=eml_parser
                    )
                except (RequestCanceledError, SourceCanceledError) as ex:
                    MutyLogger.get_instance().exception(ex)
                    await self._source_failed(ex)
                except PreviewDone:
                    # preview done, stop processing
                    pass
                doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self.source_done(flt)
            await self._eml_parser.unload()
        return self._stats_status()
