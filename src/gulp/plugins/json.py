import datetime
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

muty.os.check_and_install_package(
    "json-stream", ">=2.3.3,<3.0.0")
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
                name="timestamp_field",
                type="str",
                desc="field containing the timestamp (e.g. some.nested.timestamp)",
                default_value="create_datetime",
            ),
            GulpPluginCustomParameter(
                name="date_format",
                type="str",
                desc="format string to parse the timestamp field, if null try autoparse",
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="format",
                type="str",
                desc="'line' one object per line, 'dict' standard json object, 'list' list of json objects",
                default_value="list",
            )
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        json_format = kwargs.get("json_format")
        timestamp_field = kwargs.get("timestamp_field")
        date_format = kwargs.get("date_format")
        line = kwargs.get("line")

        d: dict = {}
        if json_format in ["line", "list"]:
            # print(json_format, timestamp_filed, date_format)
            # print(record)
            # print("-"*20)
            for k, v in record.items():
                d[k] = v
        else:
            # TODO: dict
            pass

        if date_format:
            timestamp = datetime.datetime.strptime(
                time_str, date_format).isoformat()
        else:
            # TODO: find a better solution(?)
            # currently we assume the following:
            # - timestamp is nanoseconds from unix epoch, if numeric

            # timestamp: str = d.get(timestamp_filed, "0")
            # if timestamp.isnumeric():
            #    timestamp = muty.time.string_to_nanos_from_unix_epoch(timestamp)
            # else:
            timestamp: str = dateutil.parser.parse(
                d.get(timestamp_field)).isoformat()

        # map
        final: dict = {}
        for k, v in muty.json.flatten_json(d).items():
            mapped = self._process_key(k, v)
            final.update(mapped)

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            timestamp=timestamp,
            source_id=self._source_id,
            event_original=line,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(
                self._file_path),
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
        plugin_params: GulpPluginParameters = None,
        flt: GulpIngestionFilter = None,
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

        timestamp_field = self._plugin_params.custom_parameters.get(
            "timestamp_field", "create_datetime")
        date_format = self._plugin_params.custom_parameters.get(
            "date_format", None)
        json_format = self._plugin_params.custom_parameters.get(
            "format", "list").lower()

        if not timestamp_field:
            return GulpRequestStatus.FAILED

        # we can process!
        doc_idx = 0
        try:
            if json_format == "list":
                with open(file_path) as file:
                    # list of objects:
                    # [ {"a":"b"}, {"b":"c"}]
                    events = json_s.load(file)
                    if not isinstance(events, json_s.base.TransientStreamingJSONList):
                        MutyLogger.get_instance().exception(
                            f"wrong json format, expected '{json_format}' got {type(events)}")
                        return GulpRequestStatus.FAILED

                    for event in events:
                        try:
                            await self.process_record(
                                event, doc_idx, flt=flt, json_format=json_format, timestamp_field=timestamp_field, date_format=date_format
                            )
                        except (RequestCanceledError, SourceCanceledError) as ex:
                            MutyLogger.get_instance().exception(ex)
                            await self._source_failed(ex)
                        except PreviewDone:
                            # preview done, stop processing
                            pass
                        doc_idx += 1
            elif json_format == "dict":
                # json file:
                # {"a":"b", "c":"d"}
                # this may be memory heavy for big files
                with open(file_path) as file:
                    if not isinstance(events, json_stream.base.TransientStreamingJSON):
                        MutyLogger.get_instance().exception(
                            f"wrong json format, expected '{json_format}' got {type(events)}")
                        return GulpRequestStatus.FAILED

                    for k, v in json_s.load(file).items():
                        try:
                            await self.process_record(
                                {k: v}, doc_idx, flt=flt, json_format=json_format, timestamp_filed=timestamp_field, date_format=date_format
                            )
                        except (RequestCanceledError, SourceCanceledError) as ex:
                            MutyLogger.get_instance().exception(ex)
                            await self._source_failed(ex)
                        except PreviewDone:
                            # preview done, stop processing
                            pass

                        doc_idx += 1
            elif json_format == "line":
                # one record per line:
                # {"a": "b"}\n
                # {"b": "c"}\n
                async with aiofiles.open(file_path, mode="r") as file:
                    async for line in file:
                        try:
                            parsed = json.loads(line)

                            await self.process_record(
                                parsed, doc_idx, flt=flt, line=line, json_format=json_format, timestamp_filed=timestamp_field, date_format=date_format
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
