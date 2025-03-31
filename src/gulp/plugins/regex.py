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

import aiofiles
import muty.dict
import muty.json
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
                name="regex",
                type="str",
                desc="regex to apply - must use named groups",
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="flags",
                type="int",
                desc="flags to apply to regex",
                default_value=0,
            ),
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        event: Match = record
        line = kwargs["line"]

        d: dict = {}

        # map
        for k, v in event.groupdict().items():
            mapped = self._process_key(k, v)
            d.update(mapped)

        # TODO: find a better solution(?)
        # currently we assume the following:
        # - timestamp is nanoseconds from unix epoch, if numeric
        timestamp: str = d.get("@timestamp", "0")
        if not timestamp.isnumeric():
            timestamp = muty.time.string_to_nanos_from_unix_epoch(timestamp)
        d["@timestamp"] = timestamp

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=line,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(
                self._file_path),
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
        **kwargs
    ) -> GulpRequestStatus:
        try:
            if not plugin_params:
                plugin_params = GulpPluginParameters()

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
            if not plugin_params.mappings:
                plugin_params.mappings = {}

            mappings = plugin_params.mappings.get("default")
            if not mappings:
                mappings = {
                    "default": GulpMapping(
                        fields={"timestamp": GulpMappingField(
                            ecs="@timestamp")}
                    )
                }
                plugin_params.mappings = mappings

        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        regex = self._plugin_params.custom_parameters["regex"]
        regex = re.compile(
            regex, self._plugin_params.custom_parameters["flags"])

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
            async with aiofiles.open(file_path, mode="r") as file:
                async for line in file:
                    m = regex.match(line)
                    if m:
                        try:
                            await self.process_record(
                                m, doc_idx, flt=flt, line=line
                            )
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
