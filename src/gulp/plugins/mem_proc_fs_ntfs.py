"""
MemProcFS Plugin for Gulp

This module provides a plugin for processing MemProcFS files ntfs_files.txt logs.
"""

import muty.file
import re

from typing import override, Any, Optional

from muty.log import MutyLogger

from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.mapping.models import GulpMapping
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

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

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
        normalized_timestamp = record.pop("normalized_timestamp")
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
            normalized_timestamp=normalized_timestamp,
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
        doc_idx = 0
        mapping: GulpMapping = self.selected_mapping()
        encoding = mapping.default_encoding or "utf-8"
        async for row in muty.file.iter_text_file_lines(
            file_path, encoding=encoding
        ):
            if not row:
                continue
            row = row.strip()
            try:
                record = await self._parse_line(row, doc_idx)
            except Exception as ex:
                MutyLogger.get_instance().warning(
                    f"cannot parse line: {row}, error: {ex}"
                )
                continue
                
            if not record or (
                "date_created" not in record and "date_modified" not in record
            ):
                continue
            if "date_created" in record and record["date_created"]:
                normalized_timestamp = self._normalize_timestamp_cached(
                    record.pop("date_created"),
                    plugin_params=self._plugin_params,
                    timestamp_format=DATE_FORMAT,
                    cache_slot="date_created",
                )
                if normalized_timestamp.invalid:
                    raise ValueError("invalid MemProcFS NTFS creation timestamp")
                record["normalized_timestamp"] = normalized_timestamp
            elif "date_modified" in record and record["date_modified"]:
                use_modified_as_main = (
                    "normalized_timestamp" in record
                    and not record["normalized_timestamp"]
                )
                normalized_modified = self._normalize_timestamp_cached(
                    record.pop("date_modified"),
                    plugin_params=(
                        self._plugin_params if use_modified_as_main else None
                    ),
                    timestamp_format=DATE_FORMAT,
                    cache_slot="date_modified",
                )
                if normalized_modified.invalid:
                    raise ValueError("invalid MemProcFS NTFS modification timestamp")
                if use_modified_as_main:
                    record["normalized_timestamp"] = normalized_modified
                else:
                    record["date_modified"] = normalized_modified.nanos
            if not await self.process_record(record, doc_idx, flt):
                # stop processing (preview mode)
                break
            doc_idx += 1

        return stats.status
