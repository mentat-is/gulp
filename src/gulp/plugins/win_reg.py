"""
Windows Registry File Processor Plugin for Gulp.

This module provides functionality to ingest and process Windows registry files,
extracting registry keys, values, and other metadata for analysis.

The plugin supports parsing both complete registry hives and partial hives,
with customizable starting paths for traversal.
"""

import os
from typing import Any, override

import muty.crypto
import muty.dict
import muty.os
from construct.core import EnumInteger
from muty.log import MutyLogger
from regipy.registry import RegistryHive, Subkey
from sqlalchemy.ext.asyncio import AsyncSession
import orjson
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

muty.os.check_and_install_package("regipy", ">=5.1.0,<6")


class Plugin(GulpPluginBase):
    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def desc(self) -> str:
        return """Windows registry file processor"""

    def display_name(self) -> str:
        return "win_reg"

    def regex(self) -> str:
        """regex to identify this format"""
        return "^\x72\x65\x67\x66"

    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="path",
                type="str",
                desc="registry path to start traversing the hive from",
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="partial_hive_path",
                type="str",
                desc="the path from which the partial hive actually starts",
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="partial_hive_type",
                type="str",
                desc="the hive type can be specified if this is a partial hive, or if auto-detection fails",
                default_value=None,
            ),
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:

        # MutyLogger.get_instance().debug(custom_mapping"record: %s" % record)
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

                values.append(orjson.dumps({name: data}).decode())
            except Exception as e:
                MutyLogger.get_instance().error(e)

        regkey["values"] = values

        d: dict = {}

        # map timestamp and event code manually
        d["@timestamp"] = event.timestamp
        d["event.code"] = str(muty.crypto.hash_xxh64_int(str(regkey["path"])))

        # map
        rec: dict = muty.dict.flatten(regkey)
        for k, v in rec.items():
            mapped = await self._process_key(k, v, d, **kwargs)
            d.update(mapped)

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=str(record),
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
                plugin_params=plugin_params,
                flt=flt,
                **kwargs,
            )
        except Exception as ex:
            await self._source_failed(ex)
            await self.source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0
        try:
            hive = RegistryHive(
                file_path,
                hive_type=self._plugin_params.custom_parameters.get(
                    "partial_hive_type"
                ),
                partial_hive_path=self._plugin_params.custom_parameters.get(
                    "partial_hive_path"
                ),
            )
            for entry in hive.recurse_subkeys(
                as_json=True,
                path_root=self._plugin_params.custom_parameters.get("path"),
            ):

                if len(entry.values) < 1:
                    continue
                try:
                    await self.process_record(entry, doc_idx, flt=flt)
                except (RequestCanceledError, SourceCanceledError) as ex:
                    MutyLogger.get_instance().exception(ex)
                    await self._source_failed(ex)
                    break
                except PreviewDone:
                    break
                doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self.source_done(flt)
        return self._stats_status()
