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
from muty.log import MutyLogger

from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.plugin_internal import GulpPluginParameters, GulpPluginSpecificParam
from gulp.structs import GulpLogLevel

# TODO support gzipped logs from rotated configurations, same for error logs


class Plugin(GulpPluginBase):
    """
    common access.log format file processor.
    """

    _parts = [
        r"(?P<host>\S+)",  # host %h
        r"\S+",  # indent %l (unused)
        r"(?P<user>\S+)",  # user %u
        # date and timezone %t
        r"\[(?P<datetime>(?P<date>.*?)(?= ) (?P<timezone>.*?))\]",  # TODO: group timezone sould be optional
        # request "%r"
        r"\"(?P<request_method>.*?) (?P<path>.*?)(?P<request_version> HTTP\/.*)?\"",
        r"(?P<status>[0-9]+)",  # status %>s
        r"(?P<size>\S+)",  # size %b (might be '-')
        r"\"(?P<referrer>.*)\"",  # referrer "%{Referer}i"
        r"\"(?P<agent>.*)\"",  # user agent "%{User-agent}i"
    ]
    _pattern = re.compile(r"\s+".join(_parts) + r".*\Z")

    def _normalize_loglevel(self, l: int | str) -> GulpLogLevel:
        ll = int(l)

        if ll >= 100 and ll <= 199:
            return GulpLogLevel.VERBOSE
        elif ll >= 200 and ll <= 299:
            return GulpLogLevel.INFO
        elif ll >= 300 and ll <= 399:
            return GulpLogLevel.WARNING
        elif ll == 400 and ll <= 499:
            return GulpLogLevel.ERROR
        elif ll >= 500 and ll <= 599:
            return GulpLogLevel.CRITICAL
        else:
            return GulpLogLevel.ALWAYS

        # shouldnt happen
        return GulpLogLevel.UNEXPECTED

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return "Apache access.log CLF file processor."

    def display_name(self) -> str:
        return "apache_access_clf"

    def version(self) -> str:
        return "1.0"

    def additional_parameters(self) -> list[GulpPluginSpecificParam]:
        return [
            GulpPluginSpecificParam(
                "locale", "str", "original server's locale", default_value=None
            ),  # TODO
            GulpPluginSpecificParam(
                "date_format",
                "str",
                "server date log format",
                default_value="%d/%b/%Y:%H:%M:%S %z",
            ),
        ]

    async def _record_to_gulp_document(
        self,
        operation_id: int,
        client_id: int,
        context_id: str,
        source: str,
        fs: TmpIngestStats,
        record: any,
        record_idx: int,
        custom_mapping: GulpMapping = None,
        index_type_mapping: dict = None,
        plugin: str = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> list[GulpDocument]:

        matches = self._pattern.match(record.strip("\n"))
        event = {
            "host": matches["host"],
            "user": matches["user"],
            "datetime": matches["datetime"],
            "date": matches["date"],
            "timezone": matches["timezone"],
            "request_method": matches["request_method"],
            "path": matches["path"],
            "request_version": matches["request_version"],
            "status": matches["status"],
            "size": matches["size"],
            "referrer": matches["referrer"],
            "agent": matches["agent"],
        }

        url = urlparse(event["path"])
        query = parse_qs(url.query)

        # TODO: split netloc into user, pass, port and assign to event
        time_str = event["datetime"]
        time = datetime.datetime.strptime(
            time_str, plugin_params.extra.get("date_format", "%d/%b/%Y:%H:%M:%S %z")
        )
        time_nanosec = muty.time.datetime_to_epoch_nsec(time)
        time_msec = muty.time.nanos_to_millis(time_nanosec)

        raw_text = record
        original_log_level = event["status"]
        gulp_log_level = self._normalize_loglevel(original_log_level)
        evt_code = event["status"] or "unknown"

        # map
        fme: list[GulpMappingField] = []
        for k, v in event.items():
            # each event item is a list[str]

            # since we are treating the timestamp ourselves, do not attempt convertion automatically from _map_source_key
            if k == "datetime":
                continue
            e = self._map_source_key(
                plugin_params,
                custom_mapping,
                k,
                v,
                index_type_mapping=index_type_mapping,
                **kwargs,
            )
            for f in e:
                fme.append(f)

        # also add extra gulp specific mapping:
        for pk, pv in query.items():
            k = "gulp.http.query.params.%s" % (pk)
            e = self._map_source_key(
                plugin_params,
                custom_mapping,
                k,
                pv,
                ignore_custom_mapping=True,
                **kwargs,
            )
            for f in e:
                fme.append(f)

        # create event
        docs = self._build_gulpdocuments(
            fme,
            idx=record_idx,
            operation_id=operation_id,
            context=context_id,
            plugin=self.display_name(),
            client_id=client_id,
            raw_event=raw_text,
            # we do not have an original id from the logs, so we reuse the index
            original_id=record_idx,
            event_code=evt_code,
            src_file=os.path.basename(source),
            timestamp=time_msec,
            timestamp_nsec=time_nanosec,
            gulp_log_level=gulp_log_level,
            original_log_level=original_log_level,
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
        plugin_params: GulpPluginParameters = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> GulpRequestStatus:

        await super().ingest_file(
            index=index,
            req_id=req_id,
            client_id=client_id,
            operation_id=operation_id,
            context_id=context,
            source=source,
            ws_id=ws_id,
            plugin_params=plugin_params,
            flt=flt,
            **kwargs,
        )
        MutyLogger.get_instance().debug("ingesting file: %s" % source)
        print("REGEX IS:", r"\s+".join(self._parts) + r".*\Z")
        fs = TmpIngestStats(source)

        ev_idx = 0

        # initialize mapping
        try:
            index_type_mapping, custom_mapping = await self._initialize()(
                index,
                source,
                mapping_file="apache_access_clf.json",
                plugin_params=plugin_params,
            )
        except Exception as ex:
            fs = self._source_failed(fs, source, ex)
            return await self._finish_ingestion(
                index, source, req_id, client_id, ws_id, fs=fs, flt=flt
            )

        try:
            async with aiofiles.open(source, "r", encoding="utf8") as log_src:
                async for l in log_src:
                    try:
                        if l.strip() == "":
                            fs = self._record_failed(fs, l, source, "empty record")
                            ev_idx += 1
                            continue

                        # process (ingest + update stats)
                        fs, must_break = await self.process_record(
                            index,
                            l,
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
                            plugin_params=plugin_params,
                            flt=flt,
                            **kwargs,
                        )

                        ev_idx += 1
                        if must_break:
                            break
                    except Exception as ex:
                        fs = self._record_failed(fs, l, source, ex)

        except Exception as ex:
            fs = self._source_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(
            index, source, req_id, client_id, ws_id, fs, flt
        )
