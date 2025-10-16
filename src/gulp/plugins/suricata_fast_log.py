"""
A Gulp plugin for processing suricata fast.log .

This plugin processes files by applying a regex pattern with named groups
to each line. It extracts the matched groups into a GulpDocument.
"""

import os
import re
from typing import Any, override, Optional
from dateutil.parser import parse
from datetime import datetime

import aiofiles
import muty.time
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import Match

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters

FASTLOG_REGEX = re.compile(
    r"^(?P<timestamp>\d{2}/\d{2}/\d{2,4}-\d{2}:\d{2}:\d{2}\.\d+)\s+"
    r"\[\*\*\]\s+\[(?P<gid>\d+):(?P<sid>\d+):(?P<rev>\d+)\]\s+"
    r"(?P<message>.+?)\s+\[\*\*\]\s+"
    r"\[Classification:\s*(?P<classification>.+?)\]\s+"
    r"\[Priority:\s*(?P<priority>\d+)\]\s+"
    r"\{(?P<protocol>\w+)\}\s+"
    r"(?P<src_ip>[\d\.x]+):(?P<src_port>\d+)\s+->\s+"
    r"(?P<dest_ip>[\d\.]+):(?P<dest_port>\d+)$"
)

PRIORITY_MAP = {"1": 3, "2": 2, "3": 1, "4": 0}  # High, Medium, Low, Info


class Plugin(GulpPluginBase):
    @override
    def desc(self) -> str:
        return """suricata fast.log file processor"""

    def display_name(self) -> str:
        return "suricata"

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

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

    @override
    async def _record_to_gulp_document(
        self, record: dict[str, Any], record_idx: int, **kwargs
    ) -> GulpDocument:
        """
        create a new GulpDocument to ingest
        """

        d = {}
        event_original = record["__line__"]
        try:
            ts = muty.time.datetime_to_nanos_from_unix_epoch(parse(record["timestamp"]))
        except Exception as ex:
            MutyLogger.get_instance().error(
                f"cannot parse timestamp for line {event_original}, error: {ex}"
            )
            raise ex
        record["@timestamp"] = ts
        d["@timestamp"] = ts
        d["agent.type"] = self.display_name()
        d["event.reason"] = record["message"]
        d["event.category"] = record["classification"]
        d["rule.gid"] = record["gid"]
        d["rule.id"] = record["sid"]
        d["rule.version"] = record["rev"]
        d["network.transport"] = record["protocol"]
        d["source.address"] = record["src_ip"]
        d["source.port"] = record["src_port"]
        d["destination.address"] = record["dest_ip"]
        d["destination.port"] = record["dest_port"]
        d["event.severity"] = PRIORITY_MAP.get(record["priority"], 0)

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=event_original,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(self._file_path),
            **d,
        )

    async def _parse_line(self, line: str, idx: int) -> Optional[dict[str, Any]]:
        match = FASTLOG_REGEX.match(line.strip())
        if not match:
            return None
        result = match.groupdict()
        result["__line__"] = line.strip()
        return result

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
            plugin_params = self._ensure_plugin_params(plugin_params)
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
