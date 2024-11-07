import os
import re

import aiofiles
import muty.dict
import muty.json
import muty.os
import muty.time
from typing_extensions import Match

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
        return """generic regex file processor"""

    def display_name(self) -> str:
        return "regex"

    def version(self) -> str:
        return "1.0"

    def additional_parameters(self) -> list[GulpPluginSpecificParam]:
        return [
            GulpPluginSpecificParam(
                "regex", "str", "regex to apply - must use named groups", default_value=None
            ),
            GulpPluginSpecificParam("flags", "int", "flags to apply to regex", default_value=0),
        ]

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
        **kwargs,
    ) -> GulpDocument:

        event: Match = record

        line = kwargs.get("line", None)

        d = {}
        fme: list[GulpMappingField] = []
        for k, v in event.groupdict().items():
            d[k] = v
            e = self._map_source_key(
                plugin_params,
                custom_mapping,
                k,
                v,
                index_type_mapping=index_type_mapping,
                **kwargs,
            )
            fme.extend(e)

        # TODO: find a better solution(?) to parse timestamp so that it is usable by GulpDocument/stackable plugins
        timestamp = d.get("timestamp", 0)
        if timestamp.isnumeric():
            # FIXME: timestamp, both millis and nanos, should be an integer, not float
            # also, are we assuming here that the timestamp is in nanos ? millis ?
            # timestamp = float(timestamp)
            timestamp = int(timestamp)
            timestamp_nsec = int(timestamp)
        else:
            timestamp_nsec = muty.time.string_to_epoch_nsec(timestamp)
            timestamp = muty.time.nanos_to_millis(timestamp_nsec)

        docs = self._build_gulpdocuments(
            fme=fme,
            idx=record_idx,
            timestamp=timestamp,
            timestamp_nsec=timestamp_nsec,
            operation_id=operation_id,
            context=context,
            plugin=plugin,
            original_id=str(record_idx),
            client_id=client_id,
            raw_event=line,
            src_file=os.path.basename(source),
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

        # get options
        regex = plugin_params.extra.get("regex", None)
        flags = plugin_params.extra.get("flags", 0)

        try:
            regex = re.compile(regex, flags)

            # make sure we have at least 1 named group
            if regex.groups == 0:
                GulpLogger().error("no named groups provided, invalid regex")
                fs = self._source_failed(fs, source, "no named groups provided")
                return await self._finish_ingestion(
                    index, source, req_id, client_id, ws_id, fs=fs, flt=flt
                )

            # check if we have at least one field named timestamp
            valid = False
            for k in regex.groupindex:
                if k.casefold() == "timestamp":
                    valid = True

            if not valid:
                GulpLogger().error("no timestamp named group provided, invalid regex")
                fs = self._source_failed(
                    fs, source, "no timestamp named group provided"
                )
                return await self._finish_ingestion(
                    index, source, req_id, client_id, ws_id, fs=fs, flt=flt
                )

        except Exception as ex:
            fs = self._source_failed(fs, source, ex)
            return await self._finish_ingestion(
                index, source, req_id, client_id, ws_id, fs=fs, flt=flt
            )

        ev_idx = 0
        try:
            async with aiofiles.open(source, mode="r") as file:
                # update stats and check for failure due to errors threshold

                async for line in file:
                    try:
                        m = regex.match(line)
                        if m is not None:
                            fs, must_break = await self.process_record(
                                index,
                                m,
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
                                original_id=ev_idx,
                                line=line,
                                **kwargs,
                            )
                        else:
                            fs = self._record_failed(
                                fs, line, source, "pattern doesn't match"
                            )

                        ev_idx += 1
                        if must_break:
                            break
                    except Exception as ex:
                        fs = self._record_failed(fs, line, source, ex)
        except Exception as ex:
            fs = self._source_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(
            index, source, req_id, client_id, ws_id, fs=fs, flt=flt
        )

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION
