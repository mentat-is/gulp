"""
IIS Access (NCSA) log parser plugin for Gulp.

This plugin processes IIS access logs in NCSA format, parsing each line
into structured fields such as remote host, username, timestamp, HTTP verb,
status code, and bytes sent.

The plugin supports custom date formatting through the 'date_format' parameter,
defaulting to '%d/%b/%Y:%H:%M:%S %z'.
"""
import datetime
import os
import re
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
        return "IIS access (NCSA) logs file processor."

    def display_name(self) -> str:
        return "iis_access_ncsa"

    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="date_format",
                type="str",
                desc="date format string to use",
                default_value="%d/%b/%Y:%H:%M:%S %z",
            ),
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        date_format = kwargs.get("date_format")
        event: dict = {}
        # fields = record.split(",")

        # TODO: move compile regex outside _record_to_gulp_document (also in apache plugin)
        regex = "".join([
                r"^(?P<remote_host>(.*)\s)-",
                r"\s(?P<username>.+)\s",
                r"\[(?P<timestamp>([^\]])+)\]\s",
                r"\"(?P<verb>([^ ])+) (?P<path>([^ ])+) (?P<version>[^\"]+)\"",
                r"\s(?P<status_code>([0-9]+))\s",
                r"(?P<bytes_sent>([0-9]+))$"
        ])
        pattern = re.compile(regex)
        matches = pattern.match(record.strip("\n"))

        event = {
            "remote_host": matches["remote_host"],
            "username": matches["username"],
            "timestamp": matches["timestamp"],
            "verb": matches["verb"],
            "status_code": matches["status_code"],
            "bytes_sent": matches["bytes_sent"]
        }

        d = {}
        # map timestamp manually
        time_str = event.get("timestamp")
        timestamp = datetime.datetime.strptime(
            time_str, date_format).isoformat()

        # map
        for k, v in event.items():
            mapped = await self._process_key(k, v, d, **kwargs)
            d.update(mapped)

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=record,
            event_sequence=record_idx,
            timestamp=timestamp,
            log_file_path=self._original_file_path or os.path.basename(
                self._file_path),
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

        date_format = self._plugin_params.custom_parameters.get(
            "date_format", "%d/%b/%Y:%H:%M:%S %z")
        doc_idx = 0
        try:
            async with aiofiles.open(file_path, "r", encoding="utf8") as log_src:
                async for l in log_src:
                    # if log_format == "w3c" and l.startswith("#"):
                    # TODO: parse header, skip other comments, is this needed/useful info, other than the fields part?
                    # continue

                    try:
                        await self.process_record(l, doc_idx, flt=flt, date_format=date_format)
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
