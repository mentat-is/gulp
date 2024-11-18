import os
import string
from copy import deepcopy

import muty.dict
import muty.os
import muty.string
import muty.xml
from muty.log import MutyLogger

import gulp.api.mapping.helpers as mappings_helper
import gulp.config as gulp_utils
from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.mapping.models import GulpMapping, GulpMappingField, GulpMappingOptions
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.plugin_internal import GulpPluginParameters, GulpPluginSpecificParam
from gulp.structs import InvalidArgument

try:
    import aiosqlite
except Exception:
    muty.os.install_package("aiosqlite")
    import aiosqlite


class Plugin(GulpPluginBase):
    """
    SQLITE generic file processor

    the sqlite plugin may ingest any SQLITE db file, but it is also used as a base plugin for other plugins (in "stacked" mode).

    ### standalone mode

    when used by itself, it is sufficient to ingest a SQLITE file with the default settings (no extra parameters needed).

    NOTE: since each document stored on elasticsearch must have a "@timestamp", either a mapping file is provided,  or "timestamp_field" is set to a field name in the SQLITE file.

    ### stacked mode

    in stacked mode, we simply run the stacked plugin, which in turn use the SQLITE plugin to parse the data.

    ~~~bash
    TEST_PLUGIN=stacked_example ./test_scripts/test_ingest.sh -p ./samples/chrome/sample_j.db
    ~~~

    see the example in [stacked_example.py](stacked_example.py)

    ### parameters

    SQLITE plugin support the following custom parameters in the plugin_params.extra dictionary:

    - `delimiter`: set the delimiter for the CSV file (default=",")

    ~~~


    chorme: cookie, file downloaded, autofill forms, history, searches, account and usernames.
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return """generic SQLITE file processor"""

    def display_name(self) -> str:
        return "sqlite"

    def version(self) -> str:
        return "1.0"

    def additional_parameters(self) -> list[GulpPluginSpecificParam]:
        return [
            GulpPluginSpecificParam(
                "encryption_key", "str", "DB encryption key", default_value=None
            ),
            GulpPluginSpecificParam(
                "key_type", "str", "DB encryption key type", default_value=None
            ),
            GulpPluginSpecificParam(
                "queries", "dict", "query to run for each table", default_value={}
            ),
        ]

    async def _record_to_gulp_document(
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
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> list[GulpDocument]:

        # MutyLogger.get_instance().debug(custom_mapping"record: %s" % record)
        event: dict = record
        extra = kwargs.get("extra", {})
        original_id = kwargs.get("original_id", record_idx)

        # we probably are dealing with a table which has no mappings, make an empty one
        if custom_mapping is None:
            custom_mapping = GulpMapping()
            custom_mapping.options = GulpMappingOptions()

            custom_mapping, _ = self._process_plugin_params(
                custom_mapping, plugin_params
            )

        fme: list[GulpMappingField] = []

        # add passed extras if any
        e = GulpMappingField(result=extra)
        fme.append(e)

        # map
        for k, v in event.items():
            e = self._map_source_key(
                plugin_params,
                custom_mapping,
                k,
                v,
                index_type_mapping=index_type_mapping,
            )
            for f in e:
                fme.append(f)

        # MutyLogger.get_instance().debug("processed extra=%s" % (json.dumps(extra, indent=2)))
        event_code = str(
            muty.crypto.hash_crc24(
                f"{extra["gulp.sqlite.db.name"]}.{extra["gulp.sqlite.db.table.name"]}"
            )
        )

        docs = self._build_gulpdocuments(
            fme,
            idx=record_idx,
            operation_id=operation_id,
            context=context,
            plugin=plugin,
            client_id=client_id,
            raw_event=str(record),
            event_code=event_code,
            original_id=record_idx,
            src_file=os.path.basename(source),
        )

        return docs

    @staticmethod
    def dict_factory(cursor: aiosqlite.Cursor, row: aiosqlite.Row):
        """Helper function to convert a sqlite row to dict

        Args:
            cursor (aiosqlite.Cursor): SQLite cursor
            row (aiosqlite.Row): row to convert

        Returns:
            d (dict): row converted to dictionary
        """
        d = {}
        for idx, col in enumerate(cursor.description):
            if isinstance(row[idx], bytes):
                d[col[0]] = row[idx].hex()
            else:
                d[col[0]] = row[idx]

        return d

    def table_exists(self, db: aiosqlite.Connection, name: str):
        """Checks if a given table exists

        Args:
            db (aiosqlite.Connection): database connection
            name (str): name of table to check

        Returns:
            _type_: _description_
        """
        query = "SELECT 1 FROM sqlite_master WHERE type='table' and name = ?"
        return db.execute(query, (name,)).fetchone() is not None

    def sanitize_value(self, value: str) -> str:
        # allowed charset: A-Za-z0-9_-
        charset = string.ascii_lowercase + string.ascii_uppercase + string.digits + "_-"
        for c in value:
            if c not in charset:
                value = value.replace(c, "")

        return value.strip()

    async def ingest_file(
        self,
        index: str,
        req_id: str,
        client_id: int,
        operation_id: int,
        context: str,
        source: str | list,
        ws_id: str,
        plugin_params: GulpPluginParameters = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> GulpRequestStatus:

        await super().ingest_file(
            index=index,
            req_id=req_id,
            client_id=client_id,
            operation_id=operation_id,
            context_id=context,
            source=source,
            ws_id=ws_id,
            plugin_params=plugin_params,
            flt=flt,
            **kwargs,
        )
        db_name = os.path.basename(source)

        fs = TmpIngestStats(source)

        # initialize mapping
        index_type_mapping, custom_mapping = await self._initialize()(
            index, source, plugin_params=plugin_params
        )

        # check plugin_params
        try:
            custom_mapping, plugin_params = self._process_plugin_params(
                custom_mapping, plugin_params
            )
        except InvalidArgument as ex:
            fs = self._source_failed(fs, source, ex)
            return await self._finish_ingestion(
                index, source, req_id, client_id, ws_id, fs=fs, flt=flt
            )

        MutyLogger.get_instance().debug("custom_mapping=%s" % (custom_mapping))

        try:
            custom_mappings: list[GulpMapping] = (
                await mappings_helper.get_mappings_from_file(
                    GulpConfig.build_mapping_file_path(plugin_params.mapping_file)
                )
            )
        except Exception as e:
            fs = self._source_failed(fs, source, ex)
            return await self._finish_ingestion(
                index, source, req_id, client_id, ws_id, fs=fs, flt=flt
            )

        tables_to_map = []
        for mapping in custom_mappings:
            tables_to_map.append(mapping.to_dict()["options"]["mapping_id"])

        MutyLogger.get_instance().debug(plugin_params)
        MutyLogger.get_instance().debug(plugin_params.extra)

        encryption_key = plugin_params.extra.get("encryption_key", None)
        key_type = plugin_params.extra.get("key_type", "key")
        queries = plugin_params.extra.get("queries", {})

        # check if key_type is supported
        if key_type.lower() not in ["key", "textkey", "hexkey"]:
            MutyLogger.get_instance().warning(
                "unsupported key type %s, defaulting to 'key'" % (key_type,)
            )
            key_type = "key"

        tables_mappings = {}
        for table in tables_to_map:
            mapping_id = table
            mapping_file = GulpConfig.build_mapping_file_path(
                plugin_params.mapping_file
            )
            tables_mappings[mapping_id] = None
            if mapping_file is not None:
                try:
                    tables_mappings[mapping_id] = (
                        await mappings_helper.get_mapping_from_file(
                            mapping_file, mapping_id
                        )
                    )
                    MutyLogger.get_instance().info(
                        "custom mapping for table %s found" % (table,)
                    )
                except ValueError:
                    MutyLogger.get_instance().error(
                        "custom mapping for table %s NOT found" % (table,)
                    )

        if custom_mapping.options.agent_type is None:
            plugin = self.display_name()
        else:
            plugin = custom_mapping.options.agent_type
            MutyLogger.get_instance().warning("using plugin name=%s" % (plugin))

        ev_idx = 0
        try:
            async with aiosqlite.connect(source) as db:
                db.row_factory = Plugin.dict_factory

                if encryption_key is not None:
                    async with db.execute(
                        "PRAGMA ?='?'", (key_type, encryption_key)
                    ) as cur:
                        MutyLogger.get_instance().info(
                            "attempting database decryption with provided key: %s"
                            % (await cur.fetchall())
                        )

                all_tables = []
                # get tables from both sqlite_master and sqlite_temp_master (TODO: should we 'split' tables and tmp_tables instead?)
                async with db.execute(
                    """SELECT name FROM sqlite_master WHERE type='table'
                                        UNION
                                        SELECT name FROM sqlite_temp_master WHERE type='table'"""
                ) as cur:
                    for table in await cur.fetchall():
                        if (
                            table["name"] in tables_mappings
                        ):  # ONLY MAP TABLES THAT HAVE A MAPPING
                            all_tables.append(table["name"])  #

                # no tables were provided, default to ALL tables in the DB
                if len(tables_to_map) < 1:
                    tables_to_map = all_tables
                    for t in all_tables:
                        tables_mappings[t] = None

                for table in all_tables:
                    if table not in tables_to_map:
                        # table is not marked for mapping, skip
                        continue

                    # Parametrized queries are not supported for "FROM {}",
                    table = self.sanitize_value(table)

                    data_query: str = queries.get(table, None)
                    metadata_query = (
                        None  # TODO: which metadata to get and what to map it to
                    )

                    if data_query is None:
                        data_query = f"SELECT * FROM {table}"
                    if metadata_query is None:
                        metadata_query = (
                            f'SELECT name FROM pragma_table_info("{table}") WHERE pk=1'
                        )

                    data_query = str(data_query).format(table=table)

                    async with db.execute(data_query) as cur:
                        for row in await cur.fetchall():
                            # print(f"gulp.sqlite.{db_name}.{table}.{column} = {value}")
                            extra = {}
                            extra["gulp.sqlite.db.name"] = db_name
                            extra["gulp.sqlite.db.table.name"] = table

                            original_id = None
                            async with db.execute(metadata_query) as cur_tmp:
                                r = await cur_tmp.fetchone()
                                # print(r)
                                for c, v in r.items():
                                    if v is not None:
                                        original_id = v
                                        break

                            # MutyLogger.get_instance().debug("Mapping: %s" % mapping)
                            try:
                                fs, must_break = await self.process_record(
                                    index,
                                    row,
                                    ev_idx,
                                    self._record_to_gulp_document,
                                    ws_id,
                                    req_id,
                                    operation_id,
                                    client_id,
                                    context,
                                    source,
                                    fs,
                                    custom_mapping=tables_mappings[table],
                                    index_type_mapping=index_type_mapping,
                                    plugin=plugin,
                                    plugin_params=plugin_params,
                                    flt=flt,
                                    original_id=row[original_id],
                                    extra=deepcopy(extra),
                                    **kwargs,
                                )
                                ev_idx += 1
                                if must_break:
                                    break

                            except Exception as ex:
                                fs = self._record_failed(fs, row, source, ex)

        except Exception as ex:
            fs = self._source_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(
            index, source, req_id, client_id, ws_id, fs=fs, flt=flt
        )
