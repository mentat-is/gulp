import os
import json
from copy import deepcopy

import muty.crypto
import muty.dict
import muty.json
import muty.os
import muty.string
import muty.time

from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import GulpStats, TmpIngestStats
from gulp.api.elastic.structs import GulpDocument, GulpIngestionFilter
from gulp.api.mapping.models import FieldMappingEntry, GulpMapping
from gulp.defs import GulpPluginType, InvalidArgument
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginOption, GulpPluginParams

try:
    from regipy.registry import RegistryHive, Subkey
except Exception:
    muty.os.install_package("regipy")
    from regipy.registry import RegistryHive, Subkey

from construct.core import EnumInteger

class Plugin(PluginBase):
    """
    Windows Registry

    the win_reg plugin ingests windows registry files

    ### standalone mode

    when used by itself, it is sufficient to ingest a registry hive with the default settings (no extra parameters needed).

    ~~~bash
    TEST_PLUGIN=win_reg ./test_scripts/test_ingest.sh -p ./samples/win_reg/NTUSER.DAT
    ~~~
    ### parameters

    win_reg supports the following custom parameters in the plugin_params.extra dictionary:

    - `path`: registry path to start traversing the hive from (default=None - start of the hive)
    - `partial_hive_path`: the path from which the partial hive actually starts (default=None)
    - `partial_hive_type`: the hive type can be specified if this is a partial hive, or if auto-detection fails (default=None)
    ~~~
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return """Windows registry file processor"""

    def name(self) -> str:
        return "win_reg"

    def version(self) -> str:
        return "1.0"

    def options(self) -> list[GulpPluginOption]:
        return [
            GulpPluginOption(
                "path",
                "str",
                "registry path to start traversing the hive from",
                default=None,
            ),
            GulpPluginOption(
                "partial_hive_path",
                "str",
                "the path from which the partial hive actually starts",
                default=None,
            ),
            GulpPluginOption(
                "partial_hive_type",
                "str",
                "the hive type can be specified if this is a partial hive, or if auto-detection fails",
                default=None,
            ),
        ]

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
        # Plugin.logger().debug(custom_mapping"record: %s" % record)
        event: Subkey = record


        regkey = {
            "path": event.path,
            "subkey_name": event.subkey_name,
            "actual_path": event.actual_path,
            "values_count": event.values_count,
        }

        values = []
        for val in event.values:
            try:
                name = val.get("name", "(Default)")
                val_type = val.get("value_type", None)

                # for unknown/invalid types regipy returns an EnumInteger instead, make sure it is treatable
                # as string
                if isinstance(val_type, EnumInteger):
                    val_type = str(val_type)

                # if it's a REG_MULTI_SZ we are gonna flatten "value" into multiple values
                if val_type.upper() == "REG_MULTI_SZ":
                    data = val.get("value", [])
                else:
                    data = val.get("value", None)

                values.append(json.dumps({name: data}))
            except Exception as e:
                self.logger().error(e)

        regkey["values"] = values

        # self.logger().debug("VALUES: ",values)
        # apply mappings for each value
        fme: list[FieldMappingEntry] = []
        for k, v in muty.json.flatten_json(regkey).items():
            # self.logger().debug(f"MAPPING: path:{d['path']} subkey_name:{d['subkey_name']} type:{d['type']} {k}={v}")
            e = self._map_source_key(plugin_params, custom_mapping, k, v, **kwargs)
            for f in e:
                fme.append(f)

        time_str = event.timestamp
        time_nanosec = muty.time.string_to_epoch_nsec(time_str)
        time_msec = muty.time.nanos_to_millis(time_nanosec)
        event_code = str(muty.crypto.hash_crc24(str(regkey["path"])))

        # self.logger().debug("processed extra=%s" % (json.dumps(extra, indent=2)))
        # event_code = custom_mapping.options.event_code if custom_mapping.options.event_code is not None else str(muty.crypto.hash_crc24(d["type"]))

        docs = self._build_gulpdocuments(
            fme,
            idx=record_idx,
            timestamp=time_msec,
            timestamp_nsec=time_nanosec,
            operation_id=operation_id,
            context=context,
            plugin=plugin,
            client_id=client_id,
            raw_event=str(record),
            #event_code=event_code,
            original_id=str(record_idx),
            src_file=os.path.basename(source),
        )
        return docs

    async def ingest(
        self,
        index: str,
        req_id: str,
        client_id: int,
        operation_id: int,
        context: str,
        source: str | list,
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
            index, source=source, plugin_params=plugin_params
        )

        plugin_params.timestamp_field = "timestamp"
        # check plugin_params
        try:
            custom_mapping, plugin_params = self._process_plugin_params(
                custom_mapping, plugin_params
            )
        except InvalidArgument as ex:
            fs=self._parser_failed(fs, source, ex)
            return await self._finish_ingestion(index, source, req_id, client_id, ws_id, fs=fs, flt=flt)

        self.logger().debug("custom_mapping=%s" % (custom_mapping))
        self.logger().debug(plugin_params)

        path = plugin_params.extra.get("path", None)
        partial_hive_type = plugin_params.extra.get("partial_hive_type", None)
        partial_hive_path = plugin_params.extra.get("partial_hive_path", None)

        extra: dict = {}

        if custom_mapping.options.agent_type is None:
            plugin = self.name()
        else:
            plugin = custom_mapping.options.agent_type
            self.logger().warning("using plugin name=%s" % (plugin))

        record_to_gulp_document_fun = plugin_params.record_to_gulp_document_fun
        if record_to_gulp_document_fun is None:
            # no record_to_gulp_document() function defined in params, use the default one
            record_to_gulp_document_fun = self.record_to_gulp_document
            self.logger().warning("using win_reg plugin record_to_gulp_document().")

        ev_idx = 0
        try:
            hive = RegistryHive(
                source, hive_type=partial_hive_type, partial_hive_path=partial_hive_path
            )
            for entry in hive.recurse_subkeys(as_json=True, path_root=path):

                if len(entry.values) < 1:
                    continue
                try:
                    fs, must_break = await self._process_record(index, entry, ev_idx,
                                                                self.record_to_gulp_document,
                                                                ws_id, req_id, operation_id, client_id,
                                                                context, source, fs,
                                                                custom_mapping=custom_mapping,
                                                                index_type_mapping=index_type_mapping,
                                                                plugin_params=plugin_params,
                                                                plugin=plugin,
                                                                flt=flt,
                                                                extra=deepcopy(extra),
                                                                **kwargs)

                    ev_idx += 1
                    if must_break:
                        break

                except Exception as ex:
                    fs = self._record_failed(fs, entry, source, ex)

        except Exception as ex:
            fs=self._parser_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(index, source, req_id, client_id, ws_id, fs=fs, flt=flt)
