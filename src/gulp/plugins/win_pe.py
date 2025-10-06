"""
Win PE File Processor Module

Provides a plugin to extract metadata from Windows PE (Portable Executable) files.
Parses PE files and extracts structured information about headers, sections,
imports/exports, and other PE-specific data.

Features:
- PE file format analysis (DLL, EXE, drivers)
- Optional entropy checks for malware/packing detection
- Configurable output detail levels (relocations, warnings, etc.)
- File timestamp extraction

Uses pefile and peutils libraries for PE parsing operations.
"""

import datetime
from typing import Any, override
import os
import muty.crypto
import muty.dict
import muty.os
import muty.time
import pefile
import peutils
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

muty.os.check_and_install_package("pefile", ">=2024.8.26")


class Plugin(GulpPluginBase):
    @override
    def desc(self) -> str:
        return """generic PE file processor"""

    def display_name(self) -> str:
        return "win_pe"

    def regex(self) -> str:
        """regex to identify this format"""
        return "^\x4d\x5a"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="encoding",
                type="str",
                desc="encoding to use",
                default_value="utf-8",
            ),
            GulpPluginCustomParameter(
                name="include_relocations",
                type="bool",
                desc="include base relocations information",
                default_value=False,
            ),
            GulpPluginCustomParameter(
                name="entropy_checks",
                type="bool",
                desc="include entropy checks (is_suspicious, is_probably_packed)",
                default_value=True,
            ),
            GulpPluginCustomParameter(
                name="keep_files",
                type="bool",
                desc="if True, event.original will keep the whole PE file",
                default_value=False,
            ),
            GulpPluginCustomParameter(
                name="keep_warnings",
                type="bool",
                desc="do not discard pefile parsing warnings from document",
                default_value=False,
            ),
        ]

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:

        record: pefile.PE = record
        relos: str = kwargs.get("include_relocations")
        entropy_checks: str = kwargs.get("entropy_checks")
        keep_files: bool = kwargs.get("keep_files")
        keep_warnings: bool = kwargs.get("keep_warnings")
        encoding: str = kwargs.get("encoding")

        d = record.dump_dict()
        if entropy_checks:
            d["peutils.is_suspicious"] = peutils.is_suspicious(record)
            d["peutils.is_probably_packed"] = peutils.is_probably_packed(record)
            d["peutils.is_valid"] = peutils.is_valid(record)

        if not relos:
            del d["Base relocations"]

        if not keep_warnings:
            del d["Parsing Warnings"]

        event_original = str(d)
        if keep_files:
            event_original = memoryview(record.__data__).hex()

        def pretty(s):
            return s.lower().replace(" ", "_")

        # apply mappings
        final = {}
        rec: dict = muty.dict.flatten(
            d, normalize=pretty, expand_lists=False
        )
        for k, v in rec.items():
            if isinstance(v, bytes):
                v = v.encode(encoding)
            mapped = await self._process_key(str(k), str(v), final, **kwargs)
            final.update(mapped)

        if record.is_dll():
            event_code = muty.crypto.hash_xxh64("dll")
        elif record.is_driver():
            event_code = muty.crypto.hash_xxh64("driver")
        elif record.is_exe():
            event_code = muty.crypto.hash_xxh64("exe")
        else:
            event_code = muty.crypto.hash_xxh64("pe")

        timestamp = d["FILE_HEADER"]["TimeDateStamp"]["Value"].split(" ")[0]
        timestamp = int(timestamp, 0)
        timestamp = datetime.datetime.fromtimestamp(
            timestamp, tz=datetime.timezone.utc
        ).isoformat()

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_code=event_code,
            timestamp=timestamp,
            event_original=event_original,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(self._file_path),
            **final,
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
        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        relos = self._plugin_params.custom_parameters.get("include_relocations")
        entropy_check = self._plugin_params.custom_parameters.get("entropy_checks")
        keep_files: bool = self._plugin_params.custom_parameters.get("keep_files")
        keep_warnings: bool = self._plugin_params.custom_parameters.get("keep_warnings")
        encoding: str = self._plugin_params.custom_parameters.get("encoding")
        
        doc_idx = 0
        try:
            with pefile.PE(file_path) as pe:
                try:
                    await self.process_record(
                        pe,
                        doc_idx,
                        flt=flt,
                        include_relocations=relos,
                        entropy_checks=entropy_check,
                        keep_files=keep_files,
                        keep_warnings=keep_warnings,
                        encoding=encoding
                    )
                except (RequestCanceledError, SourceCanceledError) as ex:
                    MutyLogger.get_instance().exception(ex)
                    await self._source_failed(ex)
                except PreviewDone:
                    pass
        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
        return self._stats_status()
