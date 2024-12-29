import os
import string
from typing import Any, override

import muty.dict
import muty.os
import muty.string
import muty.xml
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession
import muty.crypto
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

muty.os.check_and_install_package("aiosqlite", ">=0.20.0")
import aiosqlite


class Plugin(GulpPluginBase):
    """
    SQLITE generic file processor

    the sqlite plugin may ingest any SQLITE db file, but it is also used as a base plugin for other plugins (in "stacked" mode).

    ### standalone mode

    when used by itself, it is sufficient to ingest a SQLITE file with the default settings (no extra parameters needed).

    NOTE: since each document must have a "@timestamp", a mapping file with "@timestamp" field mapped is advised.

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

    @override
    def desc(self) -> str:
        return """generic SQLITE file processor"""

    def display_name(self) -> str:
        return "sqlite"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="encryption_key",
                type="str",
                desc="DB encryption key",
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="key_type",
                type="str",
                desc="DB encryption key type",
                default_value="key",
            ),
            GulpPluginCustomParameter(
                name="queries",
                type="dict",
                desc="query to run for each table",
                default_value={},
            ),
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, data: Any = None
    ) -> GulpDocument:

        # MutyLogger.get_instance().debug(custom_mapping"record: %s" % record)
        event: dict = record
        data: dict

        # use original id as record_idx, if any
        record_idx = data.pop("original_id", record_idx)
        d: dict = {}
        for k, v in event.items():
            mapped = self._process_key(k, v)
            d.update(mapped)

        # add data
        d.update(data)

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

    @staticmethod
    def _dict_factory(cursor: aiosqlite.Cursor, row: aiosqlite.Row):
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

    def _table_exists(self, db: aiosqlite.Connection, name: str):
        """Checks if a given table exists

        Args:
            db (aiosqlite.Connection): database connection
            name (str): name of table to check

        Returns:
            _type_: _description_
        """
        query = "SELECT 1 FROM sqlite_master WHERE type='table' and name = ?"
        return db.execute(query, (name,)).fetchone() is not None

    def _sanitize_value(self, value: str) -> str:
        # allowed charset: A-Za-z0-9_-
        charset = string.ascii_lowercase + string.ascii_uppercase + string.digits + "_-"
        for c in value:
            if c not in charset:
                value = value.replace(c, "")

        return value.strip()

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
        try:
            # initialize plugin
            await self._initialize(plugin_params)

            # get tables to map
            tables_to_map: list[str] = []
            d = list(str(key) for key in self._mappings.keys())
            for m in d:
                tables_to_map.append(m)

            # get custom parameters
            encryption_key: str = self._custom_params.get("encryption_key")
            key_type: str = self._custom_params.get("key_type")
            queries: dict = self._custom_params.get("queries")

            # check if key_type is supported
            if key_type.lower() not in ["key", "textkey", "hexkey"]:
                MutyLogger.get_instance().warning(
                    "unsupported key type %s, defaulting to 'key'" % (key_type,)
                )
                key_type = "key"

        except Exception as ex:
            await self._source_failed(ex)
            await self._source_done(flt)
            return GulpRequestStatus.FAILED

        doc_idx = 0

        # these are the tables we are going to map, table names are our mapping_ids
        mapping_ids = list(str(key) for key in self._mappings.keys())

        try:
            async with aiosqlite.connect(file_path) as db:
                db.row_factory = Plugin._dict_factory
                if encryption_key is not None:
                    # unlock the database
                    async with db.execute(
                        "PRAGMA ?='?'", (key_type, encryption_key)
                    ) as cur:
                        MutyLogger.get_instance().info(
                            "attempting database decryption with provided key: %s"
                            % (await cur.fetchall())
                        )

                # these are the tables found effectively matching the mapping_ids
                tables_to_process = []

                # get tables from both sqlite_master and sqlite_temp_master
                # (TODO: should we 'split' tables and tmp_tables instead?)
                async with db.execute(
                    """SELECT name FROM sqlite_master WHERE type='table'
                                        UNION
                                        SELECT name FROM sqlite_temp_master WHERE type='table'"""
                ) as cur:
                    for table in await cur.fetchall():
                        if (
                            table["name"] in mapping_ids
                        ):  # ONLY MAP TABLES THAT HAVE A MAPPING
                            tables_to_process.append(table["name"])

                for table in tables_to_process:
                    # parametrized queries are not supported for "FROM {}",
                    table = self._sanitize_value(table)

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

                    # process this table
                    async with db.execute(data_query) as cur:
                        for row in await cur.fetchall():
                            # print(f"gulp.sqlite.{db_name}.{table}.{column} = {value}")
                            d: dict = {}
                            d["gulp.sqlite.db.name"] = os.path.basename(file_path)
                            d["gulp.sqlite.db.table.name"] = table

                            # use this mapping id
                            self._mapping_id = table

                            """
                            # get record's original id
                            original_id = None
                            async with db.execute(metadata_query) as cur_tmp:
                                r = await cur_tmp.fetchone()
                                for _, v in r.items():
                                    if v:
                                        original_id = v
                                        break
                            d["original_id"] = row[original_id]
                            """
                            try:
                                await self.process_record(row, doc_idx, flt=flt, data=d)
                            except (RequestCanceledError, SourceCanceledError) as ex:
                                MutyLogger.get_instance().exception(ex)
                                await self._source_failed(ex)
                                break
                            doc_idx += 1

                    if self._is_source_failed:
                        break

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
            return self._stats_status()
