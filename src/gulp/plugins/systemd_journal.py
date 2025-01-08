import datetime
import os
from typing import Any, override

import muty.dict
import muty.jsend
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.stats import (
    GulpRequestStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginParameters

# not available on macos, will throw exception
muty.os.check_os(exclude=["windows", "darwin"])
muty.os.check_and_install_package("systemd-python", ">=235,<300")
from systemd import journal


class Plugin(GulpPluginBase):
    """
    common log format file processor.
    """

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

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def desc(self) -> str:
        return "Systemd journal log file processor."

    def display_name(self) -> str:
        return "systemd_journal"

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        event = self._to_str_dict(record)
        d: dict = {}

        # map timestamp manually
        time_str = event["__REALTIME_TIMESTAMP"]
        d["@timestamp"] = time_str
        d["event.code"] = str(record["PRIORITY"])

        # map
        # TODO: consider mapping also to syslog.msgid, priority, procid, etc...
        for k, v in event.items():
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
        plugin_params: GulpPluginParameters = None,
        flt: GulpIngestionFilter = None,
    ) -> GulpRequestStatus:
        try:
            # initialize plugin
            if not plugin_params or plugin_params.is_empty():
                plugin_params = GulpPluginParameters(
                    mapping_file="systemd_journal.json"
                )

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
            )
        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            with journal.Reader(None, files=[file_path]) as log_file:
                log_file.log_level(journal.LOG_DEBUG)
                for rr in log_file:
                    try:
                        await self.process_record(rr, doc_idx, flt=flt)
                    except (RequestCanceledError, SourceCanceledError) as ex:
                        MutyLogger.get_instance().exception(ex)
                        await self._source_failed(ex)
                        break
                    doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
            return self._stats_status()
