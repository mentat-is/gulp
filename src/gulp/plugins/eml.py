import email
import os

import aiofiles
import muty.crypto
import muty.dict
import muty.json
import muty.os
import muty.string
import muty.time

from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.opensearch.structs import GulpDocument, GulpIngestionFilter
from gulp.api.mapping.models import GulpMappingField, GulpMapping
from gulp.defs import GulpPluginType
from gulp.plugin import GulpPluginBase
from gulp.plugin_internal import GulpPluginSpecificParam, GulpPluginGenericParams
from gulp.utils import GulpLogger


class Plugin(GulpPluginBase):
    def desc(self) -> str:
        return """generic EML file processor"""

    def display_name(self) -> str:
        return "eml"

    def version(self) -> str:
        return "1.0"

    def additional_parameters(self) -> list[GulpPluginSpecificParam]:
        return [
            GulpPluginSpecificParam(
                "decode", "bool", "attempt to decode messages wherever possible", True
            )
        ]

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

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

    async def _record_to_gulp_document(
        self,
        operation_id: int,
        client_id: int,
        context: str,
        source: str,
        fs: TmpIngestStats,
        record: any,
        record_idx: int,
        custom_mapping: GulpMapping = None,
        index_type_mapping: dict = None,
        plugin: str = None,
        plugin_params: GulpPluginGenericParams = None,
        extra: dict = None,
        **kwargs,
    ) -> list[GulpDocument]:

        event: email.message.Message = record
        d = {}
        for k, v in event.items():
            k = f"email.{self._normalize_field(k)}"
            d[k] = v

        d["email.parts"] = []

        if event.is_multipart():
            d["email.is_multipart"] = True
            for part in event.walk():
                msg = part.get_payload(decode=plugin_params.extra.get("decode", True))
                enc = part.get_content_charset()

                if msg is None:
                    msg = ""

                d["email.parts"].append(self._normalize_value(msg, enc))
        else:
            msg = event.get_payload(decode=True)
            enc = event.get_content_charset()

            if msg is None:
                msg = ""
            d["email.parts"].append(self._normalize_value(msg, enc))

        d = muty.json.flatten_json(d)

        fme: list[GulpMappingField] = []
        for k, v in d.items():
            e = self._map_source_key(
                plugin_params,
                custom_mapping,
                k,
                v,
                index_type_mapping=index_type_mapping,
                **kwargs,
            )
            fme.extend(e)

        timestamp_nsec = muty.time.string_to_epoch_nsec(event["Date"])
        timestamp = muty.time.nanos_to_millis(timestamp_nsec)
        docs = self._build_gulpdocuments(
            fme=fme,
            idx=record_idx,
            operation_id=operation_id,
            context=context,
            plugin=plugin,
            client_id=client_id,
            raw_event=str(event),
            event_code=str(muty.crypto.hash_crc24(event["From"])),
            original_id=event["Message-Id"],
            src_file=os.path.basename(source),
            timestamp=timestamp,
            timestamp_nsec=timestamp_nsec,
        )

        return docs

    async def ingest_file(
        self,
        index: str,
        req_id: str,
        client_id: int,
        operation_id: int,
        context: str,
        source: str | list[dict],
        ws_id: str,
        plugin_params: GulpPluginGenericParams = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> GulpRequestStatus:
        fs = TmpIngestStats(source)

        # initialize mapping
        index_type_mapping, custom_mapping = await self._initialize()(
            index, source, plugin_params=plugin_params
        )

        GulpLogger().debug("custom_mapping=%s" % (custom_mapping))

        if custom_mapping.options.agent_type is None:
            plugin = self.display_name()
        else:
            plugin = custom_mapping.options.agent_type
            GulpLogger().warning("using plugin name=%s" % (plugin))

        # get options
        # attempt_decode = plugin_params.extra.get("decode", True)

        ev_idx = 0
        try:
            async with aiofiles.open(source, mode="rb") as file:
                # update stats and check for failure due to errors threshold

                try:
                    content = await file.read()
                    message = email.message_from_bytes(content)
                    fs, _ = await self.process_record(
                        index,
                        message,
                        ev_idx,
                        self._record_to_gulp_document,
                        ws_id,
                        req_id,
                        operation_id,
                        client_id,
                        context,
                        source,
                        fs,
                        custom_mapping=custom_mapping,
                        index_type_mapping=index_type_mapping,
                        plugin=self.display_name(),
                        plugin_params=plugin_params,
                        flt=flt,
                        **kwargs,
                    )

                except Exception as ex:
                    self._record_failed(fs, message, source, ex)

        except Exception as ex:
            self._source_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(
            index, source, req_id, client_id, ws_id, fs, flt
        )
