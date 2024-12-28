import os
import re
from typing import Any, override

import aiofiles
import muty.dict
import muty.jsend
import muty.log
import muty.string
import muty.time
import muty.xml
from sqlalchemy.ext.asyncio import AsyncSession
from muty.log import MutyLogger
from gulp.api.collab.stats import (
    GulpRequestStats,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginType
from gulp.plugin import GulpPluginBase
from gulp.structs import GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    common error.log format file processor.
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return "Apache error.log CLF file processor."

    def display_name(self) -> str:
        return "apache_error_clf"

    def version(self) -> str:
        return "1.0"

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, data: Any = None
    ) -> GulpDocument:
        # ^\[(?P<timestamp>[^\]]+)\]\s\[(?P<loglevel>[^\]]+)\]\s\[(?P<pid>[^\]:]+)((?=:):(?P<tid>[^\]]+)|)\]\s((?=\[)(\[(?P<client>[^\]]+)\s(?P<ip>[^\]]+)\]\s(?P<message>.*))|(?!=\[)(?P<message_x>.*))$
        # [Fri Sep 09 10:42:29.902022 2011] [core:error] [pid 35708:tid 4328636416] [client 72.15.99.187] File does not exist: /usr/local/apache2/htdocs/favicon.ico
        parts = [
            r"^\[(?P<date>[^\]]+)\]",
            r"\[(?P<loglevel>[^\]]+)\]",
            r"\[(?P<pid>[^\]]+)((?=:):(?P<tid>[^\]]+)|)\]",
            r"((?=\[)(\[(?P<client>[^\]]+)\s(?P<ip>[^\]]+)\]\s(?P<message>.*))|(?!=\[)(?P<message_only>.*))$",
        ]
        pattern = re.compile(r"\s+".join(parts))
        matches = pattern.match(record.strip("\n"))

        """if matches is None:
            print(matches)
            print(record)
            print("*" * 1000)"""

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

        d: dict = {}

        # map timestamp manually
        time_str = event.pop("date")
        time_str = str(muty.time.string_to_nanos_from_unix_epoch(time_str))
        d["@timestamp"] = time_str

        # map
        for k, v in event.items():
            mapped = self._process_key(k, v)
            d.update(mapped)

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=record,
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
        try:
            # initialize plugin
            if not plugin_params or plugin_params.is_empty():
                plugin_params = GulpPluginParameters(
                    mapping_file="apache_error_clf.json"
                )
            await self._initialize(plugin_params)

        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            async with aiofiles.open(file_path, "r", encoding="utf8") as log_src:
                async for l in log_src:
                    l = l.strip()
                    if not l:
                        continue

                    try:
                        await self.process_record(l, doc_idx, flt=flt)
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
