import os
from typing import Any, override

import aiofiles
import muty.dict
import muty.os
import muty.string
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
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters

try:
    from aiocsv import AsyncDictReader
except Exception:
    muty.os.install_package("aiocsv")
    from aiocsv import AsyncDictReader


class Plugin(GulpPluginBase):
    """
    CSV generic file processor

    the csv plugin may ingest any CSV file itself, but it is also used as a base plugin for other plugins (in "stacked" mode).

    NOTE: since it is mandatory that for each document to have a `@timestamp`, a `GulpMapping` must be set with either `timestamp_field` or
    a field directly set to "@timestamp".


    ### standalone mode

    when used by itself, it is enough to ingest a CSV file with the default settings (no extra parameters needed).

    ### stacked mode

    in stacked mode, we simply run the stacked plugin, which in turn use the CSV plugin to parse the data.

    ### parameters

    CSV plugin support the following custom parameters in the plugin_params.extra dictionary:

    - `delimiter`: set the delimiter for the CSV file (default=",")

    ~~~
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def display_name(self) -> str:
        return "csv"

    @override
    def desc(self) -> str:
        return """generic CSV file processor"""

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="delimiter",
                type="str",
                desc="delimiter for the CSV file",
                default_value=",",
            )
        ]

    @override
    def version(self) -> str:
        return "1.0"

    @override
    async def _record_to_gulp_document(
        self, record: dict, record_idx: int, data: Any = None
    ) -> GulpDocument:

        # MutyLogger.get_instance().debug("processing record:\n%s" % (json.dumps(record,indent=2)))

        # get raw csv line (then remove it)
        event_original: str = record["__line__"]
        del record["__line__"]

        # map all keys for this record
        d = {}
        for k, v in record.items():
            mapped = self._process_key(k, v)
            d.update(mapped)

        timestamp = d.get("@timestamp")
        if not timestamp:
            # not mapped, last resort is to use the timestamp field, if set
            timestamp = record.get(self.selected_mapping().timestamp_field)

        return GulpDocument(
            self,
            timestamp=timestamp,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=event_original,
            event_sequence=record_idx,
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

        # stats must be created by the caller, get it
        stats: GulpRequestStats = await GulpRequestStats.get_by_id(sess, id=req_id)
        try:
            # initialize plugin
            if not plugin_params:
                plugin_params = GulpPluginParameters()

            # initialize plugin
            await self._initialize(plugin_params)

            # csv plugin needs a mapping or a timestamp field
            selected_mapping = self.selected_mapping()
            if not selected_mapping.fields and not selected_mapping.timestamp_field:
                raise ValueError(
                    "if no mapping_file or mappings specified, timestamp_field must be set in GulpMapping !"
                )
        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        delimiter = self._custom_params.get("delimiter", ",")
        doc_idx = 0
        try:
            async with aiofiles.open(
                file_path, mode="r", encoding="utf-8", newline=""
            ) as f:
                async for line_dict in AsyncDictReader(f, delimiter=delimiter):
                    # fix dict (remove BOM from keys, if present)
                    fixed_dict = {
                        muty.string.remove_unicode_bom(k): v
                        for k, v in line_dict.items()
                        if v
                    }
                    # rebuild line
                    line = delimiter.join(fixed_dict.values())
                    # add original line as __line__
                    fixed_dict["__line__"] = line[:-1]

                    try:
                        await self.process_record(fixed_dict, doc_idx, flt)
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
