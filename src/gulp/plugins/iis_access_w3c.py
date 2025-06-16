"""
Plugin for parsing and ingesting IIS access logs in W3C format.

This module contains a `Plugin` class that implements the base `GulpPluginBase` interface
for handling IIS access logs. The plugin parses W3C formatted log files, extracts fields
according to the W3C format directives, and converts them into `GulpDocument` objects
ready for ingestion.

The plugin handles W3C header directives like #Software, #Version, #Date, and #Fields
to correctly map log fields to structured data.

"""
import datetime
import os
from typing import Any, override

import aiofiles
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.stats import (
    GulpRequestStats,
    PreviewDone,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters

class Plugin(GulpPluginBase):
    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def desc(self) -> str:
        return "IIS access (W3C) logs file processor."

    def display_name(self) -> str:
        return "iis_access_w3c"

    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return []

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        metadata = kwargs.get("metadata")
        fields = metadata.get("fields", [])

        event: dict = {}

        record_split = record.split()
        i=0
        for r in record_split:
            event[fields[i]] = r
            i=i+1

        d={}
        # map timestamp manually
        date_format = "%Y-%d-%m %H:%M:%S"
        time_str = " ".join([event.get("date"), event.get("time")])
        d["@timestamp"] = datetime.datetime.strptime(time_str, date_format).isoformat()

        # map
        for k, v in event.items():
            mapped = await self._process_key(k, v, event, **kwargs)
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
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
         **kwargs
   ) -> GulpRequestStatus:
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
                **kwargs,
            )
        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        metadata={
            "software":"",
            "version":"",
            "date":"",
            "fields":[]
        }
        doc_idx = 0
        try:
            async with aiofiles.open(file_path, "r", encoding="utf8") as log_src:
                async for l in log_src:
                    if l.startswith("#"):
                        l = l.split(":")
                        k = l[0].lower().lstrip("#")

                        if k in ["software", "version"]:
                            metadata[k] = "".join(l[1:]).lstrip()
                        elif k == "date":
                            metadata[k] = "".join(l[1:]).lstrip()
                        elif k == "fields":
                            metadata[k] = "".join(l[1:]).lstrip().split()
                        continue

                    try:
                        await self.process_record(l, doc_idx, flt=flt, metadata=metadata)
                    except (RequestCanceledError, SourceCanceledError) as ex:
                        MutyLogger.get_instance().exception(ex)
                        await self._source_failed(ex)
                        break
                    except PreviewDone:
                        # preview done, stop processing
                        pass
                    doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
        return self._stats_status()
