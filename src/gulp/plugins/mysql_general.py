import datetime, dateutil
import os
import re
from typing import Any, override
from urllib.parse import parse_qs, urlparse

import aiofiles
import muty.string
import muty.time
import muty.xml
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
    mysql general logs file processor.
    """

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def desc(self) -> str:
        return "mysql general logs file processor."

    def display_name(self) -> str:
        return "mysql_general"

    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return []

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        date_format = kwargs.get("date_format")
        event: dict = {}
        fields = record.split(",")
      
        d={}
        # map timestamp manually
        time_str = " ".join([event["Date"], event["Time"]])
        d["@timestamp"] = datetime.datetime.strptime(time_str, date_format).isoformat()

        # map
        for k, v in event.items():
            mapped = self._process_key(k, v)
            d.update(mapped)

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=record,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(self._file_path),
            **d,
        )

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

        date_format = self._custom_params.get("date_format", "%m/%d/%y %H:%M:%S")
        #log_format = log_format.lower()
        try:
            # if not plugin_params or plugin_params.is_empty():
            #     plugin_params = GulpPluginParameters(
            #         mapping_file="iis_access.json"
            #     )
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
        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            async with aiofiles.open(file_path, "r", encoding="utf8") as log_src:
                async for l in log_src:
                    # TODO: port to python https://gist.github.com/httpdss/948386
                    try:
                        await self.process_record(l, doc_idx, flt=flt, date_format=date_format)
                    except (RequestCanceledError, SourceCanceledError) as ex:
                        MutyLogger.get_instance().exception(ex)
                        await self._source_failed(ex)
                        break
                    doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
            return self._stats_status()
