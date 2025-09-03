"""
A Gulp plugin for processing files using regular expressions.

This plugin processes files by applying a regex pattern with named groups
to each line. It extracts the matched groups into a GulpDocument.

Key features:
- Uses regex with named capture groups to extract data
- Requires a 'timestamp' named group in the regex pattern
- Normalizes timestamps to nanoseconds from Unix epoch
- Maps extracted fields according to configured mappings
"""

import os
import re
from typing import Any, override
from datetime import datetime

import aiofiles
import re._constants
import muty.dict
import muty.os
import muty.time
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession
from typing_extensions import Match

from gulp.api.collab.stats import (
    GulpRequestStats,
    PreviewDone,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.mapping.models import GulpMapping, GulpMappingField
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters


class Plugin(GulpPluginBase):
    @override
    def desc(self) -> str:
        return """generic regex file processor"""

    def display_name(self) -> str:
        return "regex"

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="encoding",
                type="str",
                desc="encoding to use",
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="date_format",
                type="str",
                desc="format string to parse the timestamp field, if null try autoparse",
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="regex",
                type="str",
                desc="regex to apply - must use named groups",
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="flags",
                type="int",
                desc="flags to apply to regex",
                default_value=re.NOFLAG,
            ),
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        event: Match = record
        line = kwargs.get("line")
        date_format = kwargs.get("date_format")

        d: dict = {}
        # print("---> mapping = %s" % (self.selected_mapping().model_dump_json(indent=2)))

        # map
        rec: dict = event.groupdict()
        for k, v in rec.items():
            mapped = await self._process_key(k, v, d, **kwargs)
            d.update(mapped)

        timestamp: str = None
        if date_format:
            timestamp = datetime.strptime(d["@timestamp"], date_format).isoformat()

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=line,
            event_sequence=record_idx,
            timestamp=timestamp,
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
            plugin_params = self._ensure_plugin_params(
                plugin_params,
                mappings={
                    "default": GulpMapping(
                        fields={"@timestamp": GulpMappingField(ecs="@timestamp")}
                    )
                },
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
                **kwargs,
            )

        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        encoding = self._plugin_params.custom_parameters.get("encoding")
        date_format = self._plugin_params.custom_parameters.get("date_format")
        flags = self._plugin_params.custom_parameters.get("flags")
        regex = self._plugin_params.custom_parameters.get("regex")
        regex = re.compile(regex, flags)

        # make sure we have at least 1 named group
        if regex.groups == 0:
            await self._source_failed("no named groups provided, invalid regex")
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        # make sure we have at least one field named timestamp
        valid = False
        for k in regex.groupindex:
            if k.casefold() == "timestamp":
                valid = True

        if not valid:
            await self._source_failed(
                "no timestamp named group provided, invalid regex"
            )
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        # we can process!
        doc_idx = 0
        try:
            async with aiofiles.open(file_path, mode="r", encoding=encoding) as file:
                async for line in file:
                    m = regex.match(line)
                    if m:
                        try:
                            await self.process_record(m, doc_idx, flt=flt, line=line)
                        except (RequestCanceledError, SourceCanceledError) as ex:
                            MutyLogger.get_instance().exception(ex)
                            await self._source_failed(ex)
                        except PreviewDone:
                            # preview done, stop processing
                            pass
                    else:
                        # no match
                        MutyLogger.get_instance().warning(
                            f"regex did not match: {line}"
                        )
                        self._record_failed()
                    doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
        return self._stats_status()
