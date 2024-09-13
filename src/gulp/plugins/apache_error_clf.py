import datetime
import os
import re
from urllib.parse import parse_qs, urlparse

import aiofiles
import muty.dict
import muty.jsend
import muty.log
import muty.string
import muty.time
import muty.xml

from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import GulpStats, TmpIngestStats
from gulp.api.elastic.structs import GulpDocument, GulpIngestionFilter
from gulp.api.mapping.models import FieldMappingEntry, GulpMapping
from gulp.defs import GulpLogLevel, GulpPluginType
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginParams


class Plugin(PluginBase):
    """
    common error.log format file processor.
    """

    # ^\[(?P<timestamp>[^\]]+)\]\s\[(?P<loglevel>[^\]]+)\]\s\[(?P<pid>[^\]:]+)((?=:):(?P<tid>[^\]]+)|)\]\s((?=\[)(\[(?P<client>[^\]]+)\s(?P<ip>[^\]]+)\]\s(?P<message>.*))|(?!=\[)(?P<message_x>.*))$
    _parts = [
        r"^\[(?P<date>[^\]]+)\]",
        r"\[(?P<loglevel>[^\]]+)\]",
        r"\[(?P<pid>[^\]]+)((?=:):(?P<tid>[^\]]+)|)\]",
        r"((?=\[)(\[(?P<client>[^\]]+)\s(?P<ip>[^\]]+)\]\s(?P<message>.*))|(?!=\[)(?P<message_only>.*))$",
    ]
    _pattern = re.compile(r"\s+".join(_parts))

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return "Apache error.log CLF file processor."

    def name(self) -> str:
        return "apache_error_clf"

    def version(self) -> str:
        return "1.0"

    async def record_to_gulp_document(
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
        plugin_params: GulpPluginParams = None,
        **kwargs,
    ) -> list[GulpDocument]:

        # [Fri Sep 09 10:42:29.902022 2011] [core:error] [pid 35708:tid 4328636416] [client 72.15.99.187] File does not exist: /usr/local/apache2/htdocs/favicon.ico
        matches = self._pattern.match(record.strip("\n"))
        if matches is None:
            print(matches)
            print(record)
            print("*" * 1000)
        client = matches.groupdict().get("client", "")
        ip = matches.groupdict().get("ip", "")
        message = matches.groupdict().get("message", None)
        message_only = matches.groupdict().get("message_only", "")

        if message is None:
            message = message_only

        event = {
            "date": matches["date"],
            "loglevel": matches["loglevel"],
            "pid": matches["pid"],
            "tid": matches["tid"],
            "client": client,
            "ip": ip,
            "message": message,
        }

        time_str = event["date"]
        time_nanosec = muty.time.string_to_epoch_nsec(time_str)
        time_msec = muty.time.nanos_to_millis(time_nanosec)

        raw_text = record

        # map
        fme: list[FieldMappingEntry] = []
        for k, v in event.items():
            # each event item is a list[str]
            e = self._map_source_key(plugin_params, custom_mapping, k, v, **kwargs)
            for f in e:
                fme.append(f)

        # create event
        docs = self._build_gulpdocuments(
            fme,
            idx=record_idx,
            operation_id=operation_id,
            context=context,
            plugin=self.name(),
            client_id=client_id,
            raw_event=raw_text,
            # we do not have an original id from the logs, so we reuse the index
            original_id=record_idx,
            src_file=os.path.basename(source),
            timestamp=time_msec,
            timestamp_nsec=time_nanosec,
            original_log_level=event["loglevel"],
        )

        return docs

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

        await super().ingest(
            index=index,
            req_id=req_id,
            client_id=client_id,
            operation_id=operation_id,
            context=context,
            source=source,
            ws_id=ws_id,
            plugin_params=plugin_params,
            flt=flt,
            **kwargs,
        )

        fs = TmpIngestStats(source)
        ev_idx = 0

        # initialize mapping
        try:
            index_type_mapping, custom_mapping = await self.ingest_plugin_initialize(
                index, source, mapping_file="apache_error_clf.json", plugin_params=plugin_params
            )
        except Exception as ex:
            fs = self._parser_failed(fs, source, ex)
            return await self._finish_ingestion(index, source, req_id, client_id, ws_id, fs=fs, flt=flt)

        try:
            async with aiofiles.open(source, "r", encoding="utf8") as log_src:
                async for l in log_src:
                    try:
                        if l.strip() == "":
                            continue

                        # process (ingest + update stats)
                        fs, must_break = await self._process_record(index, l, ev_idx,
                                                                    self.record_to_gulp_document,
                                                                    ws_id, req_id, operation_id, client_id,
                                                                    context, source, fs,
                                                                    custom_mapping=custom_mapping,
                                                                    index_type_mapping=index_type_mapping,
                                                                    plugin_params=plugin_params,
                                                                    flt=flt,
                                                                    **kwargs)
                        if must_break:
                            break


                        # next
                        ev_idx += 1
                    except Exception as ex:
                        fs = self._record_failed(fs, l, source, ex)

        except Exception as ex:
            fs = self._parser_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(index, source, req_id, client_id, ws_id, fs, flt)
