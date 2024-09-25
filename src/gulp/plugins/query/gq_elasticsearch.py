
import muty.dict
import muty.jsend
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml

from gulp.api import collab_api, elastic_api
from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import GulpStats, TmpQueryStats
from gulp.api.elastic.query import QueryResult
from gulp.api.elastic.query_utils import build_elasticsearch_generic_query, check_canceled_or_failed
from gulp.api.elastic.structs import (
    GulpQueryFilter,
    GulpQueryOptions,
    gulpqueryflt_to_elastic_dsl,
)
from gulp.defs import GulpPluginType, InvalidArgument, ObjectNotFound
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginOption, GulpPluginParams
from gulp.utils import logger
from gulp.api.rest import ws as ws_api
from elasticsearch import AsyncElasticsearch
from opensearchpy import AsyncOpenSearch

"""
Query plugins

Query plugins are used to query data from external sources, such as databases, APIs, etc.

The plugin must implement the following methods:
- type() -> GulpPluginType: return the plugin type.
- desc() -> str: return a description of the plugin.
- name() -> str: return the plugin name.
- version() -> str: return the plugin version.
- options() -> list[GulpPluginOption]: for the UI, this is usually the options to be put into GulpPluginParams.extra when calling query().
- query(client_id: int, ws_id: str, flt: GulpQueryFilter, params: GulpPluginParams) -> int: query data from the external source.

"""


class Plugin(PluginBase):
    """
    query plugin for Wazuh.
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.QUERY

    def desc(self) -> str:
        return "Query data from elasticsearch."

    def name(self) -> str:
        return "query_elasticsearch"

    def version(self) -> str:
        return "1.0"

    def options(self) -> list[GulpPluginOption]:
        return [
            GulpPluginOption(
                "url", "str", "opensearch/elasticsearch server URL, i.e. http://localhost:9200.", default=None
            ),  # TODO
            GulpPluginOption(
                "is_elasticsearch",
                "bool",
                "True if the server is an ElasticSearch server, False if is an OpenSearch server.",
                default=True,
            ),
            GulpPluginOption(
                "username",
                "str",
                "Username.",
                default=None,
            ),
            GulpPluginOption(
                "password",
                "str",
                "Password.",
                default=None,
            ),
            GulpPluginOption(
                "index",
                "str",
                "Index name.",
                default=None,
            ),
        ]

    async def query(
        self,
        operation_id: int,
        client_id: int,
        user_id: int,
        username: str,
        ws_id: str,
        req_id: str,
        plugin_params: GulpPluginParams,
        flt: GulpQueryFilter,
        options: GulpQueryOptions = None,
    ) -> tuple[int, GulpRequestStatus]:
        logger().debug(
            "querying elasticsearch, params=%s, filter: %s" % (plugin_params, flt)
        )

        # get options
        url: str = plugin_params.extra.get("url")
        is_elasticsearch: bool = plugin_params.extra.get("is_elasticsearch")
        elastic_user: str = plugin_params.extra.get("username")
        password: str = plugin_params.extra.get("password")
        index:str = plugin_params.extra.get("index")

        # TODO: add support for client and CA certificates, i.e. dumping the certificates to temporary files and using them
        if not url or not index:
            raise InvalidArgument("missing required parameters (url, index)")

        # convert basic filter and options to a raw query, ensure only start_msec, end_msec, extra is present
        q, o = build_elasticsearch_generic_query(flt, options)
        raw_query_dict = q['query']
        if is_elasticsearch:
            # connect to elastic
            cl: AsyncElasticsearch = AsyncElasticsearch(
                url,
                basic_auth=(elastic_user, password),
                verify_certs=False,
            )
            api = elastic_api.query_raw_elastic
            logger().debug("connected to elasticsearch at %s, instance=%s" % (url, cl))
        else:
            # opensearch
            cl: AsyncOpenSearch = AsyncOpenSearch(
                url,
                http_auth=(elastic_user, password),
                verify_certs=False,
            )
            api = elastic_api.query_raw
            logger().debug("connected to opensearch at %s, instance=%s" % (url, cl))
            
        # initialize result
        query_res = QueryResult()
        query_res.query_name = "%s_%s" % (req_id, muty.string.generate_unique())
        query_res.req_id = req_id
        if o.include_query_in_result:
            query_res.query_raw = raw_query_dict
        processed: int=0
        chunk: int=0
        status = GulpRequestStatus.DONE
        while True:            
            logger().debug("querying, query=%s, options=%s" % (q, o))
            try:
                r = await api(cl,index, raw_query_dict, o)
            except ObjectNotFound as ex:
                logger().error('no more data found!')
                break
            except Exception as ex:
                raise ex

            # get data
            evts = r.get("results", [])
            aggs = r.get("aggregations", None)
            len_evts = len(evts)

            # fill query result
            query_res.search_after = r.get("search_after", None)
            query_res.total_hits = r.get("total", 0)
            logger().debug(
                "%d results (TOTAL), this chunk=%d"
                % (query_res.total_hits, len_evts)
            )

            query_res.events = evts
            query_res.aggregations = aggs
            if len_evts == 0 or len_evts < options.limit:
                query_res.last_chunk = True

            # send QueryResult over websocket
            ws_api.shared_queue_add_data(
                ws_api.WsQueueDataType.QUERY_RESULT,
                req_id,
                query_res.to_dict(),
                client_id=client_id,
                operation_id=operation_id,
                username=username,
                ws_id=ws_id,
            )

            # processed an event chunk (evts)
            processed += len_evts
            chunk += 1
            logger().error(
                "sent %d events to ws, num processed events=%d, chunk=%d ..."
                % (len(evts), processed, chunk)
            )
            if await check_canceled_or_failed(req_id):
                status = GulpRequestStatus.CANCELED
                break                

            query_res.chunk += 1
            if query_res.search_after is None:
                logger().debug(
                    "search_after=None or search_after_loop=False, query done!"
                )
                break

            o.search_after = query_res.search_after
            logger().debug(
                "search_after=%s, total_hits=%d, running another query to get more results ...."
                % (query_res.search_after, query_res.total_hits)
            )
            
        await cl.close()
        logger().debug("elasticsearch connection instance=%s closed!" % (cl))

        return query_res.total_hits, status
