"""
A plugin for processing and ingesting ZIP files into GULP.

This plugin extracts files from ZIP archives, processes their metadata, and converts them into GulpDocument objects.
It supports password-protected ZIP files and hash calculation for extracted content.

Features:
- Extraction of files from ZIP archives
- Handling of file metadata (timestamps, permissions, etc.)
- MIME type detection for extracted files
- Support for password-protected archives
- Optional SHA1 hashing for extracted files
"""

import asyncio
import datetime
import hashlib
import mimetypes
import os
import stat
import zipfile
from typing import Any, override

from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters


class Plugin(GulpPluginBase):
    @override
    def desc(self) -> str:
        return """generic zip file processor"""

    def display_name(self) -> str:
        return "zip"

    def regex(self) -> str:
        """regex to identify this format"""
        return "^\x50\x4b\x03\x04|^\x50\x4b\x05\x06|^\x50\x4b\x07\x08"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="password",
                type="str",
                desc="password to decrypt the zip file",
            ),
            GulpPluginCustomParameter(
                name="calculate_hash",
                type="bool",
                desc="if true, calculate SHA1 for each extracted file",
                default_value=False,
            ),
        ]

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        d: dict = record
        event_original = d.pop("zip_info")
        event_code = "zip_entry"

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_code=event_code,
            # timestamp=timestamp,
            event_original=event_original,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(self._file_path),
            **d,
        )

    def _extract_zip_entry_sync(
        self,
        zip_path: str,
        info: zipfile.ZipInfo,
        password: str | None,
        calculate_hash: bool,
    ) -> dict:
        # turn info to dict and add some extra fields

        is_dir = info.is_dir()
        file_size = info.file_size
        attrs_mask = info.external_attr >> 16
        d: dict[str, Any] = {
            "file.name": info.filename,
            "file.is_dir": is_dir,
            "file.size": file_size,
            "hash.crc32": info.CRC,
        }
        if attrs_mask:
            d["file.attributes"]=stat.filemode(attrs_mask)
        d["zip_info"] = "%s" % (info)

        if info.comment:
            d["file.comment"] = info.comment.decode("utf-8", errors="replace")

        if not is_dir:
            guessed_mimetype = mimetypes.guess_type(info.filename)[0]
            d["file.mimetype"] = guessed_mimetype or "file/unknown"

        timestamp = datetime.datetime(
            *info.date_time, tzinfo=datetime.timezone.utc
        ).isoformat()
        d["@timestamp"] = timestamp

        if calculate_hash and file_size and not info.is_dir():
            pwd = password.encode("utf-8") if password else None
            sha1 = hashlib.sha1()
            with zipfile.ZipFile(zip_path) as zf:
                with zf.open(info, pwd=pwd) as extracted:
                    for chunk in iter(lambda: extracted.read(256 * 1024), b""):
                        sha1.update(chunk)
            d["hash.sha1"] = sha1.hexdigest()

        return d

    async def _extract_zip_entry(
        self,
        zip_path: str,
        info: zipfile.ZipInfo,
        password: str | None,
        calculate_hash: bool,
    ) -> dict:
        return await asyncio.to_thread(
            self._extract_zip_entry_sync,
            zip_path,
            info,
            password,
            calculate_hash,
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

        password: str | None = self._plugin_params.custom_parameters.get("password")
        calculate_hash: bool = self._plugin_params.custom_parameters.get(
            "calculate_hash"
        )

        doc_idx = 0

        with zipfile.ZipFile(file_path) as z:
            files = z.infolist()

        for info in files:
            record = await self._extract_zip_entry(
                file_path,
                info,
                password,
                calculate_hash,
            )

            if not await self.process_record(record, doc_idx, flt=flt):
                # stop processing (preview mode)
                break
            doc_idx += 1

        return stats.status
