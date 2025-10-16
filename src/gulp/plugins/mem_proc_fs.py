"""
MemProcFS Plugin for Gulp

This module provides a plugin for processing MemProcFS files timeline_*.txt logs.
"""

import aiofiles
import muty.time
import re

from datetime import datetime
from typing import override, Any, Optional

from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import (
    GulpPluginBase,
    GulpPluginType,
    GulpPluginCustomParameter,
    GulpPluginParameters,
)

LOG_PATTERN = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\sUTC)\s+"
    r"(?P<type>\w+)\s+"
    r"(?P<action>\w+)\s+"
    r"(?P<pid>\d+)\s+"
    r"(?P<num>\d+)\s+"
    r"(?P<hex>[0-9a-fA-F]+)\s+"
    r"(?P<desc>.*)$"
)

DATE_FORMAT = "%Y-%m-%d %H:%M:%S %Z"


class Plugin(GulpPluginBase):

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="encoding",
                type="str",
                desc="encoding to use",
                default_value="utf-8",
            ),
        ]

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    def display_name(self) -> str:
        return "MemProcFS"

    @override
    def desc(self) -> str:
        return """ingest MemProcFS timeline_*.txt logs inside gulp"""

    @override
    async def _record_to_gulp_document(
        self, record: dict[str, Any], record_idx: int, **kwargs
    ) -> GulpDocument:
        """
        create a new GulpDocument to ingest
        """
        d = {}
        event_original = record["__line__"]
        ts = muty.time.datetime_to_nanos_from_unix_epoch(
            datetime.strptime(record["date"], DATE_FORMAT)
        )
        record["@timestamp"] = ts
        d["@timestamp"] = ts
        d["agent.type"] = self.display_name()

        # The columns NUM, HEX and DESC have different meaning depending on the type
        match record["type"]:
            case "PROC":
                d["process.parent.pid"] = record["num"]
                d["gulp.mem_proc_fs.eprocess_address"] = record["hex"]
            case "NTFS":
                d["file.size"] = record["num"]
                d["gulp.mem_proc_fs.mft_record_physical_address"] = record["hex"]
            case "THREAD":
                d["thread.id"] = record["num"]
                d["gulp.mem_proc_fs.ethread_address"] = record["hex"]
            case "KObj" | "Net":
                d["gulp.mem_proc_fs.object_address"] = record["hex"]

        d["gulp.mem_proc_fs.desc"] = record["desc"]
        d["event.category"] = record["type"]
        d["event.action"] = record["action"]
        d["process.pid"] = record["pid"]

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=event_original,  # taken from the record
            event_sequence=record_idx,  # taken from the record
            **d,
        )

    async def _parse_line(self, line: str, idx: int) -> Optional[dict[str, Any]]:
        """
        parse a MemProcFS timeline rows to extract information and return dict to injest in gulp
        """

        match = LOG_PATTERN.match(line)
        if match:
            result = match.groupdict()
            result["__line__"] = line
            return result

        return None

    @override
    async def ingest_file(
        self,
        sess,
        stats,
        user_id,
        req_id,
        ws_id,
        index,
        operation_id,
        context_id,
        source_id,
        file_path,
        original_file_path=None,
        flt=None,
        plugin_params=None,
        **kwargs,
    ) -> GulpRequestStatus:
        try:
            # initialize plugin
            if not plugin_params:
                plugin_params = GulpPluginParameters()

            await super().ingest_file(
                sess,
                stats,
                user_id,
                req_id,
                ws_id,
                index,
                operation_id,
                context_id,
                source_id,
                file_path,
                original_file_path,
                flt,
                plugin_params,
                **kwargs,
            )
        except Exception as ex:
            await self._source_failed(ex)
            await self.update_stats_and_flush(flt)
            return GulpRequestStatus.FAILED
        try:
            doc_idx = 0
            encoding = self._plugin_params.custom_parameters.get("encoding")
            async with aiofiles.open(
                file_path, mode="r", encoding=encoding, newline=""
            ) as f:
                async for row in f:
                    if not row:
                        continue
                    row = row.strip()
                    record = await self._parse_line(row, doc_idx)
                    if record:
                        await self.process_record(record, doc_idx, flt)
                        doc_idx += 1
        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self.update_stats_and_flush(flt)

        return self._stats_status()
