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

import json
from typing import Any, override

import muty.os
import orjson
from muty.log import MutyLogger
from opensearchpy import AsyncOpenSearch
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import GulpRequestStats, RequestCanceledError
from gulp.api.mapping.models import GulpMappingField
from gulp.api.opensearch.filters import QUERY_DEFAULT_FIELDS
from gulp.api.opensearch.structs import GulpDocument, GulpQueryParameters
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import (
    GulpDocumentsChunkCallback,
    GulpPluginCustomParameter,
    GulpPluginParameters,
    GulpSortOrder,
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
        return GulpPluginType.EXTERNAL

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
                desc="""Elasticsearch/opensearch URI.""",
                required=True,
            ),
            GulpPluginCustomParameter(
                name="username",
                type="str",
                desc="""username""",
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="password",
                type="str",
                desc="""password""",
                default_value=None,
            ),
            GulpPluginCustomParameter(
                name="index",
                type="str",
                desc="""the index to query.""",
                required=True,
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
                desc="""name of the field representing the context.""",
                required=True,
            ),
            GulpPluginCustomParameter(
                name="context_type",
                type="str",
                desc="""the field type for context (check documentation for is_gulp_type`)""",
                default_value="context_id",
                values=["context_id", "context_name"],
            ),
            GulpPluginCustomParameter(
                name="source_field",
                type="str",
                desc="""name of the field representing the source.""",
                required=True,
            ),
            GulpPluginCustomParameter(
                name="source_type",
                type="str",
                desc="""the field type for source (check documentation for is_gulp_type`)""",
                default_value="source_id",
                values=["source_id", "source_name"],
            ),
        ]

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:

        # record is a dict
        doc: dict = muty.dict.flatten(record)

        # map any other field
        d = {}

        for k, v in doc.items():
            # do not
            mapped = await self._process_key(k, v, d, **kwargs)
            d.update(mapped)

        # if preview mode is on we must valued context and source with "preview"
        if self._preview_mode:
            ctx_id = "preview"
            src_id = "preview"
        else:

            src_id = d.get("gulp.source_id")
            ctx_id = d.get("gulp.source_id")
            # MutyLogger.get_instance().warning(f"src_id: {src_id} -- ctx_id:{ctx_id}")
            # if source and context are not managed via mapping check gulp plugin parameters
            # and try to create or get from cache the id for context and source
            if not src_id and not ctx_id:
                # MutyLogger.get_instance().warning(f"try to create context and source from custom parameters")
                source_field_key = kwargs.get("source_field")
                source_field_value = record.get(source_field_key)
                source_type = kwargs.get("source_type")
                context_field_key = kwargs.get("context_field")
                context_field_value = record.get(context_field_key)
                context_type = kwargs.get("context_type")

                if not source_field_value or not context_field_value:
                    raise Exception("missing source and context field value")
                ctx_id = await self._context_id_from_doc_value(
                    context_field_key, context_field_value, context_type == "context_id"
                )
                src_id = await self._source_id_from_doc_value(
                    ctx_id,
                    source_field_key,
                    source_field_value,
                    source_type == "source_id",
                )

        d = GulpDocument(
            self,
            operation_id=self._operation_id,
            event_original=str(record),
            event_sequence=record_idx,
            context_id=ctx_id,
            source_id=src_id,
            **d,
        )

        # MutyLogger.get_instance().debug(d)
        return d

    async def _process_record_callback(
        self,
        sess: AsyncSession,
        chunk: list[dict],
        chunk_num: int = 0,
        total_hits: int = 0,
        index: str = None,
        last: bool = False,
        req_id: str = None,
        q_name: str = None,
        q_group: str = None,
        **kwargs,
    ) -> list[dict]:
        for iter in range(len(chunk)):
            await self.process_record(chunk[iter], iter, **kwargs)

    def parse(self, q_options) -> dict:
        """
        Parse the additional options to a dictionary for the OpenSearch/Elasticsearch search api.

        Returns:
            dict: The parsed dictionary.
        """
        n = {}

        # sorting
        n["sort"] = []
        if not q_options.sort:
            # default sort
            sort = {
                "@timestamp": GulpSortOrder.ASC.value,
                "_doc": GulpSortOrder.ASC.value,
            }

        else:
            # use provided
            sort = q_options.sort

        if "_doc" not in sort:
            sort["_doc"] = GulpSortOrder.ASC.value

        for k, v in sort.items():
            n["sort"].append({k: {"order": v}})

        # fields to be returned
        if not q_options.fields:
            # default, if not set
            fields = QUERY_DEFAULT_FIELDS
        else:
            # use the given set
            fields = q_options.fields

        n["_source"] = None
        if fields != "*":
            # if "*", return all (so we do not set "_source"). either, only return these fields
            if q_options.ensure_default_fields:
                # ensure default fields are included
                for f in QUERY_DEFAULT_FIELDS:
                    if f not in fields:
                        fields.append(f)
            n["_source"] = fields

        # pagination: doc limit
        n["size"] = None
        if q_options.limit:
            # use provided
            n["size"] = q_options.limit

        # pagination: start from
        if q_options.search_after:
            # next chunk from this point
            n["search_after"] = q_options.search_after
        else:
            n["search_after"] = None

        # wether to highlight results for the query (warning: may take a lot of memory)
        if q_options.highlight_results:
            n["highlight"] = {"fields": {"*": {}}}
        # MutyLogger.get_instance().debug("query options: %s" % (orjson.dumps(n, option=orjson.OPT_INDENT_2).decode()))
        return n

    async def search_dsl(
        self,
        sess: AsyncSession,
        index: str,
        q: dict,
        req_id: str,
        q_options: "GulpQueryParameters" = None,
        el: AsyncElasticsearch | AsyncOpenSearch = None,
        callback: GulpDocumentsChunkCallback = None,
        check_canceled: bool = True,
        **kwargs,
    ) -> tuple[int, int]:
        from gulp.api.opensearch.structs import GulpQueryParameters

        if not q_options:
            # use defaults
            q_options = GulpQueryParameters()

        if el:
            # force use_elasticsearch_api if el is provided
            MutyLogger.get_instance().debug(
                "search_dsl: using provided ElasticSearch/OpenSearch client %s, class=%s",
                el,
                el.__class__,
            )

        q_options.limit = 100
        parsed_options = self.parse(q_options)
        processed: int = 0
        chunk_num: int = 0
        check_canceled_count: int = 0
        total_hits: int = 0
        canceled: bool = False
        last: bool = False

        while True:
            docs: list[dict] = []
            (
                total_hits,
                docs,
                search_after,
                _,
            ) = await GulpOpenSearch.get_instance()._search_dsl_internal(
                index, parsed_options, q, el
            )

            processed += len(docs)
            MutyLogger.get_instance().debug(
                "_search_dsl_internal returned total_hits=%d, len(docs)=%d",
                total_hits,
                len(docs),
            )
            if (
                not total_hits
                or processed >= total_hits
                or (q_options.total_limit and processed >= q_options.total_limit)
            ):
                # this is the last chunk
                MutyLogger.get_instance().warning("this is the last chunk")
                last = True

            if check_canceled_count % 10 == 0 and check_canceled and sess:
                # every 10 chunk, call callback and check for request cancelation
                canceled = (
                    await GulpRequestStats.is_canceled(sess, req_id) if sess else False
                )

            if callback:
                # call the callback at every chunk
                # MutyLogger.get_instance().warning(
                #     f"Call callback\nchunk_num={chunk_num}\nprocessed={processed}\ntotal_hits={total_hits}"
                # )
                await callback(
                    sess,
                    docs,
                    chunk_num=chunk_num,
                    total_hits=total_hits,
                    index=index,
                    last=True if last or canceled else False,
                    req_id=req_id,
                    q_name=q_options.name,
                    q_group_by=q_options.group,
                    **kwargs,
                )

            if last or canceled:
                if canceled:
                    MutyLogger.get_instance().warning(
                        "search_dsl: request %s canceled!", req_id
                    )
                    raise RequestCanceledError()
                break

            # next chunk
            chunk_num += 1
            check_canceled_count += 1
            parsed_options["search_after"] = search_after

        MutyLogger.get_instance().info(
            "***FINISHED search_dsl***: processed=%d, total_hits=%d, chunk_num=%d",
            processed,
            total_hits,
            chunk_num,
        )
        return processed, total_hits

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
    ) -> tuple[int, int]:
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
        source_field = self._plugin_params.custom_parameters["source_field"]
        source_type = self._plugin_params.custom_parameters["source_type"]
        context_field = self._plugin_params.custom_parameters.get("context_field", None)
        context_type = self._plugin_params.custom_parameters["context_type"]

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
                raise

            return 0, 0

        # query
        q_options.fields = "*"

        q_dict = json.loads(q.replace("'", '"'))
        total_hits = 0
        processed = 0
        try:
            cb_context: dict = {
                "user_id": user_id,
                "operation_id": operation_id,
                "ws_id": ws_id,
                "q_options": q_options,
                "q": q_dict,
                "total_hits": 0,
            }
            processed, total_hits = await self.search_dsl(
                sess=sess,
                index=query_index,
                q=q_dict,
                req_id=req_id,
                q_options=q_options,
                el=cl,
                callback=self._process_record_callback,
                cb_context=cb_context,
                context_field=context_field,
                source_field=source_field,
                source_type=source_type,
                context_type=context_type,
            )
            if total_hits == 0:
                MutyLogger.get_instance().warning("no results!")
                if self._preview_mode:
                    return 0, []

                return 0, 0

            MutyLogger.get_instance().debug(
                "elasticsearch/opensearch query done, total=%d, processed=%d!"
                % (total_hits, processed)
            )
            if self._preview_mode:
                return total_hits, self.preview_chunk()

            return processed, total_hits

        except Exception as ex:
            # error during query
            MutyLogger.get_instance().exception(ex)
            if self._preview_mode:
                raise

            return processed, total_hits

        finally:
            # last flush
            MutyLogger.get_instance().debug("closing client ...")
            await cl.close()
