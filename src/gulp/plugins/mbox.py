import email
import json
import mailbox
import os
from copy import deepcopy

import aiofiles
import muty.crypto
import muty.dict
import muty.json
import muty.os
import muty.string
import muty.time

import gulp.api.mapping.helpers as mappings_helper
import gulp.plugins.eml as eml
import gulp.utils as gulp_utils
from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import GulpStats, TmpIngestStats
from gulp.api.elastic.structs import GulpDocument, GulpIngestionFilter
from gulp.api.mapping.models import GulpMapping, GulpMappingOptions
from gulp.defs import GulpPluginType, InvalidArgument
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginOption, GulpPluginParams


class Plugin(PluginBase):
    def desc(self) -> str:
        return """generic MBOX file processor"""

    def name(self) -> str:
        return "mbox"

    def version(self) -> str:
        return "1.0"

    def options(self) -> list[GulpPluginOption]:
        return [
            GulpPluginOption(
                "decode", "bool", "attempt to decode messages wherever possible", True
            )
        ]

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    async def ingest(
        self,
        index: str,
        req_id: str,
        client_id: int,
        operation_id: int,
        context: str,
        source: str | list[dict],
        ws_id: str,
        plugin_params: GulpPluginParams = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> GulpRequestStatus:

        fs = TmpIngestStats(source)
        # initialize mapping
        index_type_mapping, custom_mapping = await self.ingest_plugin_initialize(
            index, source, plugin_params=plugin_params
        )

        self.logger().debug("custom_mapping=%s" % (custom_mapping))

        # get options
        # attempt_decode = plugin_params.extra.get("decode", True)

        # reuse the eml.py plugin record_to_gulp_document to parse the object
        # (this can be done because mailbox.Message is a subclass of email.Message)
        eml_parser = eml.Plugin(self.path, self.collab, self.elastic)

        ev_idx = 0
        try:
            async with aiofiles.open(source, mode="rb") as file:
                mbox = mailbox.mbox(source)

                for message in mbox.itervalues():
                    try:
                        fs, must_break = await self._process_record(
                            index,
                            message,
                            ev_idx,
                            eml_parser.record_to_gulp_document,
                            ws_id,
                            req_id,
                            operation_id,
                            client_id,
                            context,
                            source,
                            fs,
                            custom_mapping=custom_mapping,
                            index_type_mapping=index_type_mapping,
                            plugin=self.name(),
                            plugin_params=plugin_params,
                            flt=flt,
                            **kwargs,
                        )
                        ev_idx += 1
                        if must_break:
                            break

                    except Exception as ex:
                        fs = self._record_failed(fs, message, source, ex)

        except Exception as ex:
            fs = self._parser_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(
            index, source, req_id, client_id, ws_id, fs, flt
        )
