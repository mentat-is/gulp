"""
JSON plugin for GULP - a generic json file processor.

This module provides a plugin to process and ingest JSON data in jsonline format (one JSON object per line),

NOTE: Used as standalone, it is mandatory to provide a mapping that defines how the JSON keys should be mapped to GULP fields.

It can also be used as base for stacked plugins dealing with specific JSON formats.
"""

from datetime import datetime
import json
import os
from typing import Any, override

import aiofiles
import dateutil
import muty.dict
import muty.json
import muty.os
import muty.time
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

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
import dateutil.parser

# pylint: disable=C0411
muty.os.check_and_install_package("json-stream", ">=2.3.3,<3.0.0")
import json_stream as json_s


class Plugin(GulpPluginBase):
    @override
    def desc(self) -> str:
        return """generic json file processor"""

    def display_name(self) -> str:
        return "json"

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="encoding",
                type="str",
                desc="encoding to use",
                default_value="utf-8",
            )
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        line: str = kwargs.get("__line__")
        d: dict = muty.json.flatten_json(record)

        # map
        final: dict = {}
        for k, v in d.items():
            mapped = self._process_key(k, v)
            final.update(mapped)

        # MutyLogger.get_instance().debug("final mapped record:\n%s" % (json.dumps(final, indent=2)))
        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=line,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(self._file_path),
            **final,
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
            encoding = self._plugin_params.custom_parameters.get("encoding")

        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        # we can process!
        doc_idx = 0
        try:
            # one record per line:
            # {"a": "b"}\n
            # {"b": "c"}\n
            async with aiofiles.open(file_path, mode="r", encoding=encoding) as file:
                async for line in file:
                    try:
                        parsed = json.loads(line)

                        await self.process_record(
                            parsed,
                            doc_idx,
                            flt=flt,
                            __line__=line,
                        )
                    except (RequestCanceledError, SourceCanceledError) as ex:
                        MutyLogger.get_instance().exception(ex)
                        await self._source_failed(ex)
                    except PreviewDone:
                        # preview done, stop processing
                        pass

                    doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
        return self._stats_status()
