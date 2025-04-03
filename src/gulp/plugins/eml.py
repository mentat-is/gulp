"""
EML (Email) Plugin for Gulp

This module provides the Plugin class which implements the GulpPluginBase interface
for processing EML (email) files during ingestion. The plugin parses email files,
extracts header fields and content parts, and converts them into GulpDocument objects
that can be indexed in search backends.

Features:
- Processes standard EML email files
- Extracts email headers as metadata fields
- Handles multipart email messages
- Attempts to decode message content based on charset
- Flattens email structure into searchable fields

The plugin normalizes field names and values for consistent indexing and provides
configurable decoding of message parts.
"""
import email
import os
from email.message import Message
from typing import Any, override

import aiofiles
import muty.crypto
import muty.dict
import muty.json
import muty.os
import muty.string
import muty.time
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
    @override
    def desc(self) -> str:
        return """generic EML file processor"""

    def display_name(self) -> str:
        return "eml"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="decode",
                type="bool",
                desc="attempt to decode messages wherever possible",
                default_value=True,
            )
        ]

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    def _normalize_field(self, name: str) -> str:
        name = name.lower()
        name = name.replace("-", "_")
        if name.endswith("."):
            name = name.rstrip(".")
        if name.startswith("."):
            name = name.lstrip(".")

        return name

    def _normalize_value(self, value, encoding=None) -> str:
        # attempt to decode based on the content type encoding provided or default to utf8

        if encoding is None or encoding == "":
            encoding = "utf-8"

        try:
            value = str(value, encoding=encoding)
        except:
            # we failed to encode
            if isinstance(value, bytes):
                value = value.hex()

        return value

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:

        event: Message = record
        email_dict = {}
        for k, v in event.items():
            k = f"email.{self._normalize_field(k)}"
            email_dict[k] = v

        email_dict["email.parts"] = []

        if event.is_multipart():
            email_dict["email.is_multipart"] = True
            for part in event.walk():
                msg = part.get_payload(
                    decode=self._plugin_params.custom_parameters.get("decode")
                )
                enc = part.get_content_charset()

                if msg is None:
                    msg = ""

                email_dict["email.parts"].append(self._normalize_value(msg, enc))
        else:
            msg = event.get_payload(decode=True)
            enc = event.get_content_charset()

            if msg is None:
                msg = ""
            email_dict["email.parts"].append(self._normalize_value(msg, enc))

        email_dict = muty.json.flatten_json(email_dict)

        d: dict = {}

        # map timestamp and event code manually
        d["@timestamp"] = event["Date"]
        d["event.code"] = str(muty.crypto.hash_xxh64_int(event["From"]))

        # map
        for k, v in muty.json.flatten_json(email_dict).items():
            mapped = self._process_key(k, v)
            d.update(mapped)

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=str(event),
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
                flt=flt,
                plugin_params=plugin_params,
                **kwargs,
            )
        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            async with aiofiles.open(file_path, mode="rb") as file:
                content = await file.read()
                message = email.message_from_bytes(content)
                try:
                    await self.process_record(message, doc_idx, flt=flt)
                except (RequestCanceledError, SourceCanceledError) as ex:
                    MutyLogger.get_instance().exception(ex)
                    await self._source_failed(ex)
                except PreviewDone:
                    # preview done, stop processing
                    pass
                doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
        return self._stats_status()
