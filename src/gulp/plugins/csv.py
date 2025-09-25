"""
CSV generic file processor

the csv plugin may ingest any CSV file itself, but it is also used as a base plugin for other plugins (in "stacked" mode).

NOTE: since each document must have a "@timestamp", a mapping file with "@timestamp" field mapped is advised.

### standalone mode

when used standalone, it is enough to ingest a CSV file with the default settings (no extra parameters needed).

### stacked mode

in stacked mode, we simply run the stacked plugin, which in turn use the CSV plugin to parse the data.

### parameters

CSV plugin support the following custom parameters in the plugin_params.extra dictionary:

- `encoding`: encoding to use when opening the file (default="utf-8")
- `delimiter`: set the delimiter for the CSV file (default=",")
- `dialect`: python's csv supported dialect to use ('excel', 'excel-tab', 'unix')
"""

import os
from typing import override
from datetime import datetime
import orjson
import aiofiles
import muty.dict
import muty.os
import muty.string
import muty.xml
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import (
    GulpRequestStats,
    PreviewDone,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters

muty.os.check_and_install_package("aiocsv")
from aiocsv import AsyncDictReader


class Plugin(GulpPluginBase):
    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def display_name(self) -> str:
        return "csv"

    @override
    def desc(self) -> str:
        return """generic CSV file processor"""

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="encoding",
                type="str",
                desc="encoding to use",
                default_value="utf-8",
            ),
            GulpPluginCustomParameter(
                name="date_format",
                type="str",
                desc="format string to parse the timestamp field, if null try autoparse",
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="delimiter",
                type="str",
                desc="delimiter for the CSV file",
                default_value=",",
            ),
            GulpPluginCustomParameter(
                name="dialect",
                type="str",
                desc="python's csv supported dialect to use ('excel', 'excel-tab', 'unix')",
                default_value="excel",
            ),
        ]

    @override
    async def _record_to_gulp_document(
        self, record: dict, record_idx: int, **kwargs
    ) -> GulpDocument:
        date_format = kwargs.get("date_format")

        # MutyLogger.get_instance().debug("processing record:\n%s" % (orjson.dumps(record, option=orjson.OPT_INDENT_2).decode()))

        # get raw csv line (then remove it)
        event_original: str = record["__line__"]
        del record["__line__"]

        # map all keys for this record
        d = {}
        for k, v in record.items():
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
            event_original=event_original,
            event_sequence=record_idx,
            timestamp=timestamp,
            log_file_path=self._original_file_path or os.path.basename(self._file_path),
            **d,
        )

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
                flt=flt,
                plugin_params=plugin_params,
                **kwargs,
            )

        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        date_format = self._plugin_params.custom_parameters.get("date_format")
        delimiter = self._plugin_params.custom_parameters.get("delimiter")
        encoding = self._plugin_params.custom_parameters.get("encoding")
        dialect = self._plugin_params.custom_parameters.get("dialect")

        doc_idx = 0
        try:
            async with aiofiles.open(
                file_path, mode="r", encoding=encoding, newline=""
            ) as f:
                async for line_dict in AsyncDictReader(
                    f, dialect=dialect, delimiter=delimiter
                ):
                    # fix dict on first line (remove unicode BOM from keys, if present)
                    fixed_dict = {
                        muty.string.remove_unicode_bom(k, unenclose=True): v
                        for k, v in line_dict.items()
                        if v
                    }

                    # print("*****************")
                    # print(fixed_dict)
                    # rebuild line
                    line = delimiter.join(fixed_dict.values())
                    # add original line as __line__
                    fixed_dict["__line__"] = line[:-1]

                    try:
                        await self.process_record(
                            fixed_dict, doc_idx, flt, date_format=date_format
                        )
                    except (RequestCanceledError, SourceCanceledError) as ex:
                        MutyLogger.get_instance().exception(ex)
                        await self._source_failed(ex)
                        break
                    except PreviewDone:
                        # preview done, stop processing
                        break

                    doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
        return self._stats_status()
