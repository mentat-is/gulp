"""
A Gulp external plugin for querying Elasticsearch or OpenSearch.

This plugin allows querying and/or extracting data from Elasticsearch/OpenSearch indices,
transforming the results into GulpDocument objects with proper timestamp
handling, context, and source information.

Supports authentication, custom field mapping, timestamp adjustments,
and works with both Elasticsearch and OpenSearch backends.

Example command line:
./test_scripts/query_external.py \                                                                                                                    gulp 19:08:53
    --preview-mode \
    --q '{ "query": {"match_all": {}} }' \
    --plugin query_elasticsearch --operation_id test_operation \
    --plugin_params '{
        "custom_parameters":  {
            "uri": "http://localhost:9200",
            "username": "admin",
            "password": "Gulp1234!",
            "index": "test_operation",
            "is_elasticsearch": false
        },
        "override_chunk_size": 200
}'
"""

from typing import Any, override

import muty.os
import orjson
from muty.log import MutyLogger
from opensearchpy import AsyncOpenSearch
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import GulpRequestStats, PreviewDone
from gulp.api.mapping.models import GulpMappingField
from gulp.api.opensearch.query import GulpQueryHelpers, GulpQueryParameters
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import (
    GulpMappingParameters,
    GulpPluginCustomParameter,
    GulpPluginParameters,
)

muty.os.check_and_install_package("elasticsearch", ">=8.1.5, <9")
muty.os.check_and_install_package("pysigma-backend-elasticsearch", ">=1.1.5")
from elasticsearch import AsyncElasticsearch
from sigma.backends.elasticsearch import LuceneBackend
from sigma.backends.opensearch import OpensearchLuceneBackend


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
                name="is_elasticsearch",
                type="bool",
                desc="if True, connect to elasticsearch, otherwise connect to opensearch.",
                default_value=True,
            ),
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        # record is a dict
        doc: dict = record

        # map any other field
        d = {}
        for k, v in doc.items():
            # do not
            mapped = await self._process_key(k, v, d, **kwargs)
            d.update(mapped)

        # MutyLogger.get_instance().debug(
        #     "operation_id=%s, doc=\n%s"
        #     % (
        #         self._operation_id,
        #         orjson.dumps(d, option=orjson.OPT_INDENT_2).decode(),
        #     )
        # )

        # create a gulp document
        d = GulpDocument(
            self,
            operation_id=self._operation_id,
            event_original=str(doc),
            event_sequence=record_idx,
            **d,
        )
        import json

        # MutyLogger.get_instance().debug(d)
        return d

    @override
    async def query_external(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        operation_id: str,
        q: Any,
        index: str,
        plugin_params: GulpPluginParameters,
        q_options: GulpQueryParameters = None,
        **kwargs,
    ) -> tuple[int, int, str] | tuple[int, list[dict]]:
        await super().query_external(
            sess,
            stats,
            user_id,
            req_id,
            ws_id,
            operation_id,
            q,
            index,
            plugin_params,
            q_options,
            **kwargs,
        )

        # check @timestamp mapping
        m = self.selected_mapping()
        if not "@timestamp" in m.fields:
            # set default
            m.fields["@timestamp"] = GulpMappingField(
                ecs="@timestamp",
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
            if self._preview_mode:
                # in preview mode, we want to raise the exception
                raise ex

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
                if self._preview_mode:
                    return 0, []

                return 0, 0, q_options.name

            MutyLogger.get_instance().debug(
                "elasticsearch/opensearch query done, total=%d, processed=%d!"
                % (total_count, processed)
            )
            if self._preview_mode:
                return total_count, self.preview_chunk()

            return total_count, processed, q_options.name

        except PreviewDone:
            # preview done before finishing current chunk processing
            pr: list[dict] = self.preview_chunk()
            return len(pr), pr

        except Exception as ex:
            # error during query
            MutyLogger.get_instance().exception(ex)
            if self._preview_mode:
                raise ex

            return total_count, processed, q_options.name

        finally:
            # last flush
            await self._source_done()
            MutyLogger.get_instance().debug("closing client ...")
            await cl.close()
