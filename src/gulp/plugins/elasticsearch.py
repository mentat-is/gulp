from typing import Any, override

import muty.dict
import muty.jsend
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml

try:
    from elasticsearch import AsyncElasticsearch
except ImportError:
    muty.os.check_and_install_package("elasticsearch", ">=8.1.5, <9")
    from elasticsearch import AsyncElasticsearch

from muty.log import MutyLogger
from opensearchpy import AsyncOpenSearch
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.opensearch.query import GulpQueryHelpers, GulpQueryParameters
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    query plugin for opensearch/elasticsearch.
    """

    def type(self) -> GulpPluginType:
        return [GulpPluginType.EXTERNAL]

    @override
    def desc(self) -> str:
        return "Query data from elasticsearch."

    def display_name(self) -> str:
        return "query_elasticsearch"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="uri",
                type="str",
                desc="""
                    Elasticsearch/opensearch URI.
                    """,
                required=True,
            ),
            GulpPluginCustomParameter(
                name="username",
                type="str",
                desc="""
                    username
                    """,
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="password",
                type="str",
                desc="""
                    password
                    """,
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="index",
                type="str",
                desc="""
                    the index to query.
                    """,
                required=True,
            ),
            GulpPluginCustomParameter(
                name="offset_msec",
                type="int",
                desc="""
                    offset in milliseconds to be added to documents timestamp.

                    - to subtract, use a negative offset.
                    """,
                default_value=0,
            ),
            GulpPluginCustomParameter(
                name="timestamp_field",
                type="str",
                desc="the main timestamp field in the documents.",
                default_value="@timestamp",
            ),
            GulpPluginCustomParameter(
                name="is_elasticsearch",
                type="bool",
                desc="if True, connect to elasticsearch, otherwise connect to opensearch.",
                default_value=True,
            ),
            GulpPluginCustomParameter(
                name="context_field",
                type="str",
                desc="the field representing a GulpContext.",
                default_value="host",
            ),
            GulpPluginCustomParameter(
                name="source_field",
                type="str",
                desc="the field representing a GulpSource.",
                default_value=None,
            ),
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        # record is a dict
        doc: dict = record

        offset_msec: int = self._plugin_params.custom_parameters.get("offset_msec", 0)
        timestamp_field: str = self._plugin_params.custom_parameters.get(
            "timestamp_field", "@timestamp"
        )

        # get context and source
        self._context_id, self._source_id = await self._add_context_and_source_from_doc(
            doc
        )
        # MutyLogger.get_instance().debug(f"ctx_id={self._context_id}, src_id={self._source_id}")

        # convert timestamp to nanoseconds
        mapping = self.selected_mapping()
        _, ts_nsec, _ = GulpDocument.ensure_timestamp(
            doc[timestamp_field],
            dayfirst=mapping.timestamp_dayfirst if mapping else None,
            yearfirst=mapping.timestamp_yearfirst if mapping else None,
            fuzzy=mapping.timestamp_fuzzy if mapping else None,
        )
        # strip timestamp
        doc.pop(timestamp_field, None)

        # map any other field
        d = {}
        for k, v in doc.items():
            # do not
            mapped = self._process_key(k, v)
            d.update(mapped)

        """
        MutyLogger.get_instance().debug(
            "operation_id=%s, context_id=%s, source_id=%s, doc=\n%s"
            % (
                self._operation_id,
                self._context_id,
                self._source_id,
                json.dumps(d, indent=2),
            )
        )
        """

        # create a gulp document
        d = GulpDocument(
            self,
            timestamp=str(
                ts_nsec + (offset_msec * muty.time.MILLISECONDS_TO_NANOSECONDS)
            ),
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=str(doc),
            event_sequence=record_idx,
            **d,
        )
        # MutyLogger.get_instance().debug(d)
        return d

    @override
    async def query_external(
        self,
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        operation_id: str,
        q: Any,
        plugin_params: GulpPluginParameters,
        q_options: GulpQueryParameters,
        index: str = None,
    ) -> tuple[int, int, str]:
        await super().query_external(
            sess,
            user_id,
            req_id,
            ws_id,
            operation_id,
            q,
            plugin_params,
            q_options,
            index,
        )

        # connect
        is_elasticsearch = self._plugin_params.custom_parameters.get("is_elasticsearch")
        uri = self._plugin_params.custom_parameters["uri"]
        user = self._plugin_params.custom_parameters["username"]
        password = self._plugin_params.custom_parameters["password"]
        query_index = self._plugin_params.custom_parameters["index"]

        MutyLogger.get_instance().info(
            "connecting to %s, is_elasticsearch=%r, user=%s"
            % (uri, is_elasticsearch, user)
        )
        try:
            if is_elasticsearch:
                # elastic
                cl: AsyncElasticsearch = AsyncElasticsearch(
                    uri,
                    basic_auth=(user, password),
                    verify_certs=False,
                )
            else:
                # opensearch
                cl: AsyncOpenSearch = AsyncOpenSearch(
                    uri,
                    http_auth=(user, password),
                    verify_certs=False,
                )
        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
            return 0, 0, q_options.name

        # query
        q_options.fields = "*"
        total_count = 0
        try:
            total_count, processed, _ = await GulpQueryHelpers.query_raw(
                sess=sess,
                user_id=user_id,
                req_id=req_id,
                ws_id=ws_id,
                q=q,
                index=query_index,
                q_options=q_options,
                el=cl,
                callback=self.process_record,
            )
            if total_count == 0:
                MutyLogger.get_instance().warning("no results!")
                return 0, 0

            MutyLogger.get_instance().debug(
                "elasticsearch/opensearch query done, total=%d, processed=%d!"
                % (total_count, processed)
            )
            return total_count, processed, q_options.name

        except Exception as ex:
            # error during query
            MutyLogger.get_instance().exception(ex)
            return 0, 0, q_options.name

        finally:
            # last flush
            await self._source_done()
            MutyLogger.get_instance().debug("closing client ...")
            await cl.close()
