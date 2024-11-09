from typing import override

import aiofiles
import muty.dict
import muty.os
import muty.string
import muty.xml

from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.collab.stats import GulpIngestionStats, RequestCanceledError
from gulp.api.opensearch.structs import GulpDocument, GulpIngestionFilter
from gulp.defs import GulpPluginType
from gulp.plugin import GulpPluginBase
from gulp.plugin_params import GulpPluginAdditionalParameter, GulpPluginGenericParameters

try:
    from aiocsv import AsyncDictReader
except Exception:
    muty.os.install_package("aiocsv")
    from aiocsv import AsyncDictReader


class Plugin(GulpPluginBase):
    """
    CSV generic file processor

    the csv plugin may ingest any CSV file itself, but it is also used as a base plugin for other plugins (in "stacked" mode).

    ### standalone mode

    when used by itself, it is enough to ingest a CSV file with the default settings (no extra parameters needed).

    NOTE: since each document must have a "@timestamp", a GulpMapping must be set with a "timestamp_field" set in the plugin_params.

    ~~~bash
    # all CSV field will result in "gulp.unmapped.*" fields, timestamp will be set from "UpdateTimestamp" field
    TEST_PLUGIN_PARAMS='{"timestamp_field": "UpdateTimestamp"}' TEST_PLUGIN=csv ./test_scripts/test_ingest.sh -p ./samples/mftecmd/sample_j.csv

    # use a mapping file
    # a mapping file may hold more than one mapping definition with its own options (as defined in helpers.get_mapping_from_file())
    TEST_PLUGIN_PARAMS='{"mapping_file": "mftecmd_csv.json", "mapping_id": "j"}' TEST_PLUGIN=csv ./test_scripts/test_ingest.sh -p ./samples/mftecmd/sample_j.csv
    ~~~

    ### stacked mode

    in stacked mode, we simply run the stacked plugin, which in turn use the CSV plugin to parse the data.

    ~~~bash
    TEST_PLUGIN=stacked_example ./test_scripts/test_ingest.sh -p ./samples/mftecmd/sample_j.csv
    ~~~

    see the example in [stacked_example.py](stacked_example.py)

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
    def additional_parameters(self) -> list[GulpPluginAdditionalParameter]:
        return [
            GulpPluginAdditionalParameter(
               name="delimiter", type="str", desc="delimiter for the CSV file", default_value=","
            )
        ]

    @override
    def version(self) -> str:
        return "1.0"

    @override
    async def _record_to_gulp_document(
        self, record: dict, record_idx: int
    ) -> GulpDocument:

        # GulpLogger.get_instance().debug("record: %s" % record)

        # get raw csv line (then remove it)
        event_original: str = record["__line__"]
        del record["__line__"]

        # map all keys for this record
        d = {}
        for k, v in record.items():
            mapped = self._process_key(k, v)
            d.update(mapped)

        timestamp = d.get('@timestamp')
        if not timestamp:
            # not mapped, last resort is to use the timestamp field, if set
            timestamp = record.get(self.selected_mapping().timestamp_field)

        return GulpDocument(
            self,
            timestamp=timestamp,
            operation=self._operation,
            context=self._context,
            event_original=event_original,
            event_sequence=record_idx,
            log_file_path=self._log_file_path,
            **d,
        )

    async def ingest_file(
        self,
        req_id: str,
        ws_id: str,
        user: str,
        index: str,
        operation: str,
        context: str,
        log_file_path: str,
        plugin_params: GulpPluginGenericParameters = None,
        flt: GulpIngestionFilter = None,
    ) -> GulpRequestStatus:
        await super().ingest_file(
            req_id,
            ws_id,
            user,
            index,
            operation,
            context,
            log_file_path,
            plugin_params,
            flt,
        )

        # initialize stats
        stats: GulpIngestionStats = await GulpIngestionStats.create_or_get(
            req_id, operation=operation, context=context
        )
        try:
            # initialize plugin
            if plugin_params is None:
                plugin_params = GulpPluginGenericParameters()

            # initialize plugin
            await self._initialize(plugin_params)
            
            # csv plugin needs a mapping or a timestamp field
            selected_mapping = self.selected_mapping()
            if not selected_mapping.fields and not selected_mapping.timestamp_field:
                    raise ValueError(
                        "if no mapping_file or mappings specified, timestamp_field must be set in GulpMapping !"
                    )
        except Exception as ex:
            await self._source_failed(stats, ex)
            return GulpRequestStatus.FAILED

        delimiter = plugin_params.model_extra.get("delimiter", ",")        
        doc_idx = 0
        try:
            async with aiofiles.open(
                log_file_path, mode="r", encoding="utf-8", newline=""
            ) as f:
                async for line_dict in AsyncDictReader(f, delimiter=delimiter):
                    # fix dict (remove BOM from keys, if present)
                    fixed_dict = {muty.string.remove_unicode_bom(k): v for k, v in line_dict.items() if v}
                    # rebuild line
                    line = delimiter.join(fixed_dict.values())
                    # add original line as __line__
                    fixed_dict["__line__"] = line[:-1] 

                    try:
                        await self.process_record(stats, fixed_dict, doc_idx, flt)
                    except RequestCanceledError as ex:
                        break
        except Exception as ex:
            await self._source_failed(stats, ex)

        finally:
            await self._source_done(stats, flt)

        return stats.status
