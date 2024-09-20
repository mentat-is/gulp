import os

import aiofiles
import muty.dict
import muty.os
import muty.string
import muty.xml

from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.elastic.structs import GulpDocument, GulpIngestionFilter
from gulp.api.mapping.models import FieldMappingEntry, GulpMapping
from gulp.defs import GulpPluginType, InvalidArgument
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginOption, GulpPluginParams
from gulp.utils import logger
try:
    from aiocsv import AsyncDictReader
except Exception:
    muty.os.install_package("aiocsv")
    from aiocsv import AsyncDictReader


class Plugin(PluginBase):
    """
    CSV generic file processor

    the csv plugin may ingest any CSV file itself, but it is also used as a base plugin for other plugins (in "stacked" mode).

    ### standalone mode

    when used by itself, it is sufficient to ingest a CSV file with the default settings (no extra parameters needed).

    NOTE: since each document stored on elasticsearch must have a "@timestamp", either a mapping file is provided, or "timestamp_field" is set to a field name in the CSV file.

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

    def desc(self) -> str:
        return """generic CSV file processor"""

    def options(self) -> list[GulpPluginOption]:
        return [
            GulpPluginOption(
                "delimiter", "str", "delimiter for the CSV file", default=","
            )
        ]

    def name(self) -> str:
        return "csv"

    def version(self) -> str:
        return "1.0"

    async def record_to_gulp_document(
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
        plugin_params: GulpPluginParams = None,
        **kwargs,
    ) -> list[GulpDocument]:

        # logger().debug("record: %s" % record)
        event: dict = record

        # get raw csv line (then remove it)
        raw_text: str = event["__line__"]
        del event["__line__"]

        # map all keys for this record
        fme: list[FieldMappingEntry] = []
        for k, v in event.items():
            e = self._map_source_key(
                plugin_params,
                custom_mapping,
                k,
                v,
                index_type_mapping=index_type_mapping,
                **kwargs,
            )
            fme.extend(e)

        # logger().debug("processed extra=%s" % (json.dumps(extra, indent=2)))

        # this is the global event code for this mapping, but it may be overridden
        # at field level, anyway
        event_code = (
            custom_mapping.options.default_event_code if custom_mapping is not None else None
        )
        #logger().error(f"**** SET FROM PLUGIN ev_code: {event_code}, custom_mapping: {custom_mapping}")
        events = self._build_gulpdocuments(
            fme,
            idx=record_idx,
            operation_id=operation_id,
            context=context,
            plugin=plugin,
            client_id=client_id,
            raw_event=raw_text,
            event_code=event_code,
            original_id=record_idx,
            src_file=os.path.basename(source),
        )
        return events

    async def ingest(
        self,
        index: str,
        req_id: str,
        client_id: int,
        operation_id: int,
        context: str,
        source: str | list[dict],
        ws_id: str,
        plugin_params: GulpPluginParams = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> GulpRequestStatus:

        await super().ingest(
            index=index,
            req_id=req_id,
            client_id=client_id,
            operation_id=operation_id,
            context=context,
            source=source,
            ws_id=ws_id,
            plugin_params=plugin_params,
            flt=flt,
            **kwargs,
        )

        fs = TmpIngestStats(source)

        # initialize mapping
        index_type_mapping, custom_mapping = await self.ingest_plugin_initialize(
            index, source, plugin_params=plugin_params
        )

        # check plugin_params
        try:
            custom_mapping, plugin_params = self._process_plugin_params(
                custom_mapping, plugin_params
            )
        except InvalidArgument as ex:
            fs = self._parser_failed(fs, source, ex)
            return await self._finish_ingestion(index, source, req_id, client_id, ws_id, fs=fs, flt=flt)

        logger().debug("custom_mapping=%s" % (custom_mapping))

        delimiter = plugin_params.extra.get("delimiter", ",")

        if custom_mapping.options.agent_type is None:
            plugin = self.name()
        else:
            plugin = custom_mapping.options.agent_type
            logger().warning("using plugin name=%s" % (plugin))

        ev_idx = 0
        try:
            async with aiofiles.open(
                source, mode="r", encoding="utf-8", newline=""
            ) as f:
                async for line_dict in AsyncDictReader(f, delimiter=delimiter):
                    # rebuild original line and fix dict (remove BOM, if present)
                    line = ""
                    fixed_dict = {}
                    for k, v in line_dict.items():
                        if v is not None and len(v) > 0:
                            k = muty.string.remove_unicode_bom(k)
                            fixed_dict[k] = v

                        if v is None:
                            v = ""

                        line += v + delimiter

                    # add original line as __line__
                    line = line[:-1]
                    fixed_dict["__line__"] = line

                    # convert record to gulp document
                    try:
                        fs, must_break = await self._process_record(index, fixed_dict, ev_idx,
                                                                    self.record_to_gulp_document,
                                                                    ws_id, req_id, operation_id, client_id,
                                                                    context, source, fs,
                                                                    custom_mapping=custom_mapping,
                                                                    index_type_mapping=index_type_mapping,
                                                                    plugin=self.name(),
                                                                    plugin_params=plugin_params,
                                                                    flt=flt,
                                                                    **kwargs)
                        ev_idx += 1
                        if must_break:
                            break

                    except Exception as ex:
                        fs = self._record_failed(fs, fixed_dict, source, ex)

        except Exception as ex:
            # add an error
            fs = self._parser_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(index, source, req_id, client_id, ws_id, fs, flt)
