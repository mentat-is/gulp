"""
MemProcFS Plugin for Gulp

This module provides a plugin for processing MemProcFS files web.txt logs.
"""

import aiofiles
import muty.time
import re

from datetime import datetime
from typing import override, Any, Optional

from muty.log import MutyLogger

from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import (
    GulpPluginBase,
    GulpPluginType,
    GulpPluginCustomParameter,
    GulpPluginParameters,
)

LOG_PATTERN = re.compile(
    r"^\s*(?P<index>[0-9a-fA-F]+)\s+"
    r"(?P<pid>\d+)\s+"
    r"(?P<date>[\d-]{10}\s+[\d:]{8}\s+UTC)\s+"
    r"(?P<browser>[A-Z]+)\s+"
    r"(?P<type>[A-Z]+)\s+"
    r"(?P<url>.+?)\s+::\s+"
    r"(?P<info>.+)$"
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
        return """ingest MemProcFS web.txt logs inside gulp"""

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
        d["process.pid"] = record["pid"]
        d["user_agent.name"] = record["browser"]
        d["event.action"] = record["type"]
        d["url.full"] = record["url"]
        d["gulp.memprocfs.web.info"] = record["info"]

        MutyLogger.get_instance().warning(
            f"\n\n**********\ninfo: {record["info"]}\nurl: {record["url"]}\n*********\n\n"
        )

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
        parse a MemProcFS web rows to extract information and return dict to injest in gulp
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
            await self._source_done(flt)
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
            await self._source_done(flt)

        return self._stats_status()
