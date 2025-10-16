"""
MemProcFS Plugin for Gulp

This module provides a plugin for processing MemProcFS files ntfs_files.txt logs.
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
    r"(?P<phys_addr>[0-9a-fA-F]+)\s+"
    r"(?P<recid>[0-9a-fA-F]+)\s+"
    r"(?P<parent>[0-9a-fA-F]+)\s+"
    r"(?P<date_created>[\d-]{10}\s+[\d:]{8}\s+UTC|\s*\*{3})\s+:\s+"
    r"(?P<date_modified>[\d-]{10}\s+[\d:]{8}\s+UTC|\s*\*{3})\s+"
    r"(?P<size>[0-9a-fA-F]+)\s+"
    r"(?P<flag>([A-Z]\s+[A-Z])|([A-Z])|(\s+))\s+"
    r"(?P<path>.+)$"
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
        return """ingest MemProcFS ntfs_files.txt logs inside gulp"""

    @override
    async def _record_to_gulp_document(
        self, record: dict[str, Any], record_idx: int, **kwargs
    ) -> GulpDocument:
        """
        create a new GulpDocument to ingest
        """
        d = {}
        event_original = record.pop("__line__")
        time = record.pop("timestamp")
        d["@timestamp"] = time
        for k, v in record.items():
            try:
                mapped = await self._process_key(k, v, d, **kwargs)
                d.update(mapped)
            except Exception as ex:
                MutyLogger.get_instance().warning(
                    f"cannot map {k} with value: {v} inside GulpDocument, error: {ex}"
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

            plugin_params = self._ensure_plugin_params(
                plugin_params, mapping_file="mem_proc_fs.json", mapping_id="ntfs"
            )

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
                    try:
                        record = await self._parse_line(row, doc_idx)
                        if not record or (
                            "date_created" not in record
                            and "date_modified" not in record
                        ):
                            continue
                        if "date_created" in record and record["date_created"]:
                            ts = muty.time.datetime_to_nanos_from_unix_epoch(
                                datetime.strptime(
                                    record.pop("date_created"), DATE_FORMAT
                                )
                            )
                            record["timestamp"] = ts
                        elif "date_modified" in record and record["date_modified"]:
                            ts_modified = ts = (
                                muty.time.datetime_to_nanos_from_unix_epoch(
                                    datetime.strptime(
                                        record.pop("date_modified"), DATE_FORMAT
                                    )
                                )
                            )
                            if "timestamp" in record and not record["timestamp"]:
                                record["timestamp"] = ts_modified
                            else:
                                record["date_modified"] = ts_modified
                        await self.process_record(record, doc_idx, flt)
                        doc_idx += 1
                    except:
                        MutyLogger.get_instance().warning(f"cannot ingest row: {row}")
                        continue
        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self.update_stats_and_flush(flt)

        return self._stats_status()
