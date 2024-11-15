import datetime
import os

import muty.dict
import muty.jsend
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml

from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.mapping.models import GulpMappingField, GulpMapping
from gulp.structs import GulpLogLevel, GulpPluginType
from gulp.plugin import GulpPluginBase
from gulp.plugin_internal import GulpPluginParameters

# not available on macos, will throw exception
muty.os.check_os(exclude=["windows", "darwin"])
try:
    from systemd import journal
except Exception:
    muty.os.install_package("systemd-python")
    from systemd import journal


class Plugin(GulpPluginBase):
    """
    common log format file processor.
    """

    def _normalize_loglevel(self, l: int | str) -> str:
        """
        int to str mapping
        :param l:
        :return:
        """

        ll = int(l)
        if ll == journal.LOG_DEBUG:
            return GulpLogLevel.VERBOSE
        elif ll == journal.LOG_INFO:
            return GulpLogLevel.INFO
        elif ll == journal.LOG_WARNING:
            return GulpLogLevel.WARNING
        elif ll == journal.LOG_ERR:
            return GulpLogLevel.ERROR
        elif ll == journal.LOG_CRIT:
            return GulpLogLevel.CRITICAL
        else:
            # shouldnt happen
            return GulpLogLevel.ALWAYS

    def _to_str_dict(self, rec: dict) -> dict:
        str_dict = {}
        for k, v in rec.items():
            if isinstance(v, datetime.datetime):
                # TODO: keep data as is or to utc?
                str_dict[k] = str(v.astimezone(datetime.timezone.utc))
            # elif type(v) == jounral.Monotonic:
            # m = journal.Monotonic((datetime.timedelta(seconds=2823, microseconds=692301), uuid.uuid1()))
            # str(m) #"journal.Monotonic(timestamp=datetime.timedelta(seconds=2823, microseconds=692301), bootid=UUID('e542849c-4b8b-11ed-aaf9-00216be45548'))"
            # TODO: convert to timestamp?
            else:
                str_dict[k] = str(v)

        return str_dict

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return "Systemd journal log file processor."

    def display_name(self) -> str:
        return "systemd_journal"

    def version(self) -> str:
        return "1.0"

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
        plugin_params: GulpPluginParameters = None,
        extra: dict = None,
        **kwargs,
    ) -> GulpDocument:

        event = self._to_str_dict(record)
        if extra is None:
            extra = {}

        time_str = event["__REALTIME_TIMESTAMP"]
        time_nanosec = muty.time.string_to_epoch_nsec(time_str)
        time_msec = muty.time.nanos_to_millis(time_nanosec)

        raw_text = str(record)
        original_log_level = record["PRIORITY"]
        gulp_log_level = self._normalize_loglevel(original_log_level)  # TODO check

        # map
        # TODO: consider mapping also to syslog.msgid, priority, procid, etc...
        fme: list[GulpMappingField] = []
        for k, v in event.items():
            # each event item is a list[str]
            e = self._map_source_key(plugin_params, custom_mapping, k, v, **kwargs)
            for f in e:
                fme.append(f)

        docs = self._build_gulpdocuments(
            fme,
            idx=record_idx,
            timestamp_nsec=time_nanosec,
            timestamp=time_msec,
            operation_id=operation_id,
            context=context,
            plugin=self.display_name(),
            client_id=client_id,
            raw_event=raw_text,
            original_id=record_idx,
            event_code=str(gulp_log_level.value),
            src_file=os.path.basename(source),
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
        fs = TmpIngestStats(source)

        ev_idx = 0

        # initialize mapping
        try:
            index_type_mapping, custom_mapping = await self._initialize()(
                index,
                source,
                mapping_file="systemd_journal.json",
                plugin_params=plugin_params,
            )

        except Exception as ex:
            fs = self._source_failed(fs, source, ex)
            return await self._finish_ingestion(
                index, source, req_id, client_id, ws_id, fs=fs, flt=flt
            )

        try:
            with journal.Reader(None, files=[source]) as log_file:
                log_file.log_level(journal.LOG_DEBUG)
                for rr in log_file:
                    try:
                        fs, must_break = await self.process_record(
                            index,
                            rr,
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
                        fs = self._record_failed(fs, rr, source, ex)

        except Exception as ex:
            fs = self._source_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(
            index, source, req_id, client_id, ws_id, fs=fs, flt=flt
        )
