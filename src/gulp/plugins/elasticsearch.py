import muty.dict
import muty.jsend
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml

from gulp.api import elastic_api
from gulp.api.collab.base import GulpRequestStatus
from gulp.api.elastic.query import QueryResult
from gulp.api.elastic.query_utils import (
    adjust_fields_filter,
    check_canceled_or_failed,
    process_event_timestamp,
)
from gulp.api.elastic.structs import (
    QUERY_DEFAULT_FIELDS,
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
    query plugin for opensearch/elasticsearch.

    TODO: outdated. must be reworked, based on the splunk plugin

    example flt and plugin_params:

     {
        "flt": { "start_msec": 1475730263242, "end_msec": 1475830263242},
        "plugin_params": {
            "extra": {
                "url": "localhost:9200",
                "username": "admin",
                "password": "Gulp1234!",
                "index": "testidx",
                "timestamp_is_string": false,
                "timestamp_unit": "ms",
                "is_elasticsearch": false
            }
        }
    }
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.QUERY

    def desc(self) -> str:
        return "Query data from elasticsearch."

    def display_name(self) -> str:
        return "query_elasticsearch"

    def version(self) -> str:
        return "1.0"

    def options(self) -> list[GulpPluginOption]:
        return [
            GulpPluginOption(
                "url",
                "str",
                "opensearch/elasticsearch server URL, i.e. http://localhost:9200.",
                default=None,
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
            GulpPluginOption(
                "timestamp_offset_msec",
                "int",
                'if set, this is used to rebase "@timestamp" (and "gulp.timestamp.nsec") in the query results.',
                default="_time",
            ),
            GulpPluginOption(
                "timestamp_field",
                "str",
                'timestamp field to be used for querying external sources in "query_external" API, if different from the default.',
                default="@timestamp",
            ),
            GulpPluginOption(
                "timestamp_is_string",
                "bool",
                "if set, timestamp is a string (not numeric).",
                default=None,
            ),
            GulpPluginOption(
                "timestamp_format_string",
                "str",
                'if set, the format string used to parse the timestamp if "timestamp_is_string" is True.',
                default=None,
            ),
            GulpPluginOption(
                "timestamp_day_first",
                "bool",
                "if set and timestamp is a string, parse the timestamp using dateutil.parser.parse() with dayfirst=True.",
                default=False,
            ),
            GulpPluginOption(
                "timestamp_year_first",
                "bool",
                "if set and timestamp is a string, parse the timestamp using dateutil.parser.parse() with yearfirst=True.",
                default=True,
            ),
            GulpPluginOption(
                "timestamp_unit",
                "str",
                'if timestamp is a number, this is the unit: can be "s" (seconds from epoch) or "ms" (milliseconds from epoch).',
                default="ms",
            ),
        ]

    def _get_parameters(
        self, plugin_params: GulpPluginParams
    ) -> tuple[
        str, bool, str, str, str, bool, str, bool, str, str, str, str, int, dict
    ]:
        """
        get elasticsearch parameters from plugin_params

        Args:
        plugin_params (GulpPluginParams): plugin parameters

        Returns:
        tuple: url, is_elasticsearch, elastic_user, password, index, timestamp_is_string, timestamp_format_string, timestamp_day_first, timestamp_year_first, timestamp_unit, timestamp_field, timestamp_offset_msec, mapping
        """
        if plugin_params is None or plugin_params.extra is None:
            raise InvalidArgument("invalid plugin parameters (extra is None)")

        url: str = plugin_params.extra.get("url")
        is_elasticsearch: bool = plugin_params.extra.get("is_elasticsearch", True)
        elastic_user: str = plugin_params.extra.get("username")
        password: str = plugin_params.extra.get("password")
        index: str = plugin_params.extra.get("index")
        timestamp_is_string: bool = plugin_params.extra.get("timestamp_is_string", True)
        timestamp_format_string: str = plugin_params.extra.get(
            "timestamp_format_string", None
        )
        timestamp_day_first: bool = plugin_params.extra.get(
            "timestamp_day_first", False
        )
        timestamp_year_first: bool = plugin_params.extra.get(
            "timestamp_year_first", True
        )
        timestamp_unit: str = plugin_params.extra.get("timestamp_unit", "ms")
        timestamp_field: str = plugin_params.extra.get("timestamp_field", "@timestamp")
        timestamp_offset_msec: int = plugin_params.extra.get("timestamp_offset_msec", 0)
        mapping: dict = plugin_params.extra.get("mapping", None)

        # TODO: add support for client and CA certificates, i.e. dumping the certificates to temporary files and using them
        if not url or not index or not elastic_user:
            raise InvalidArgument(
                "missing required parameters (url=%s, username=%s, index=%s)"
                % (url, elastic_user, index)
            )
        if not url.startswith("http"):
            # default to http
            logger().warning(
                "url does not start with http, adding default http:// prefix!"
            )
            url = "http://" + url

        return (
            url,
            is_elasticsearch,
            elastic_user,
            password,
            index,
            timestamp_is_string,
            timestamp_format_string,
            timestamp_day_first,
            timestamp_year_first,
            timestamp_unit,
            timestamp_field,
            timestamp_offset_msec,
            mapping,
        )

    def _gulpqueryflt_to_elastic_dsl_generic(
        self,
        flt: GulpQueryFilter,
        options: GulpQueryOptions = None,
        timestamp_field: str = "@timestamp",
    ) -> tuple[dict, GulpQueryOptions]:
        """
        Build a generic Elasticsearch query based on the provided filter and options.
        Args:
            flt (GulpQueryFilter): The filter criteria for the query: "start_msec", "end_msec", "extra" (everything else is ignored).
            options (GulpQueryOptions, optional): The options for the query: "limit", "sort", "fields_filter" (everything else is ignored).
        Returns:
            tuple[dict, GulpQueryOptions]: A tuple containing the Elasticsearch query dictionary and the modified query options dictionary.
        """
        if options is None:
            options = GulpQueryOptions()

        f = GulpQueryFilter()
        o = GulpQueryOptions()

        # only these filters are supported
        f.start_msec = flt.start_msec
        f.end_msec = flt.end_msec
        f.extra = flt.extra

        # only these options are supported
        o.limit = options.limit
        o.sort = options.sort
        o.fields_filter = adjust_fields_filter(
            QUERY_DEFAULT_FIELDS, options.fields_filter
        )

        q = gulpqueryflt_to_elastic_dsl(f, options, timestamp_field)
        return q, o

    def _process_event(
        self,
        e: dict,
        timestamp_offset_msec: int,
        timestamp_is_string: bool,
        timestamp_format_string: str,
        timestamp_day_first: bool,
        timestamp_year_first: bool,
        timestamp_unit: str,
        timestamp_field: str,
        mapping: dict,
    ) -> dict:
        """
        process an event from elasticsearch: adjust timestamp, map fields using the mapping.

        Args:
            e (dict): the event to be processed.
            timestamp_offset_msec (int): timestamp offset in milliseconds.
            timestamp_is_string (bool): if True, timestamp is a string.
            timestamp_format_string (str): if timestamp is a string, this is the format string.
            timestamp_day_first (bool): if True, timestamp is a string and day is first.
            timestamp_year_first (bool): if True, timestamp is a string and year is first.
            timestamp_unit (str): if timestamp is a number, this is the unit: can be "s" (seconds from epoch) or "ms" (milliseconds from epoch).
            timestamp_field (str): the timestamp field name.
            mapping (dict): the mapping of the source keys to the destination keys.

        Returns:
            dict: the processed event.
        """
        ts = process_event_timestamp(
            e,
            timestamp_offset_msec,
            timestamp_is_string,
            timestamp_format_string,
            timestamp_day_first,
            timestamp_year_first,
            timestamp_unit,
            timestamp_field,
        )
        e["gulp.timestamp.nsec"] = ts
        e["@timestamp"] = ts / muty.time.MILLISECONDS_TO_NANOSECONDS
        e["_source_plugin"] = self.filename()
        if mapping is not None:
            # map source keys using the mapping
            self._remap_event_fields(e, mapping)

        if "event.duration" not in e:
            # default
            e["event.duration"] = 1
        return e

    async def query_single(
        self,
        plugin_params: GulpPluginParams,
        event: dict,
    ) -> dict:
        # get parameters
        (
            url,
            is_elasticsearch,
            elastic_user,
            password,
            index,
            timestamp_is_string,
            timestamp_format_string,
            timestamp_day_first,
            timestamp_year_first,
            timestamp_unit,
            timestamp_field,
            timestamp_offset_msec,
            mapping,
        ) = self._get_parameters(plugin_params)
        if is_elasticsearch:
            # connect to elastic
            cl: AsyncElasticsearch = AsyncElasticsearch(
                url,
                basic_auth=(elastic_user, password),
                verify_certs=False,
            )
            logger().debug("connected to elasticsearch at %s, instance=%s" % (url, cl))
        else:
            # opensearch
            cl: AsyncOpenSearch = AsyncOpenSearch(
                url,
                http_auth=(elastic_user, password),
                verify_certs=False,
            )
            logger().debug("connected to opensearch at %s, instance=%s" % (url, cl))

        # get the event from elasticsearch
        e = await elastic_api.query_single_event(cl, index, event["_id"])
        e = self._process_event(
            e,
            timestamp_offset_msec,
            timestamp_is_string,
            timestamp_format_string,
            timestamp_day_first,
            timestamp_year_first,
            timestamp_unit,
            timestamp_field,
            mapping,
        )
        return e

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

        # get parameters
        (
            url,
            is_elasticsearch,
            elastic_user,
            password,
            index,
            timestamp_is_string,
            timestamp_format_string,
            timestamp_day_first,
            timestamp_year_first,
            timestamp_unit,
            timestamp_field,
            timestamp_offset_msec,
            mapping,
        ) = self._get_parameters(plugin_params)

        # convert basic filter and options to a raw query, ensure only start_msec, end_msec, extra is present
        q, o = self._gulpqueryflt_to_elastic_dsl_generic(flt, options, timestamp_field)
        raw_query_dict = q["query"]
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
        processed: int = 0
        chunk: int = 0
        status = GulpRequestStatus.DONE

        try:
            while True:
                logger().debug("querying, query=%s, options=%s" % (q, o))
                try:
                    r = await api(cl, index, raw_query_dict, o)
                except ObjectNotFound as ex:
                    logger().error("no more data found!")
                    break
                except Exception as ex:
                    raise ex

                # get data
                evts = r.get("results", [])
                aggs = r.get("aggregations", None)
                len_evts = len(evts)

                for e in evts:
                    # process the event (timestamp, mapping, ...)
                    e = self._process_event(
                        e,
                        timestamp_offset_msec,
                        timestamp_is_string,
                        timestamp_format_string,
                        timestamp_day_first,
                        timestamp_year_first,
                        timestamp_unit,
                        timestamp_field,
                        mapping,
                    )

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

            return query_res.total_hits, status
        finally:
            await cl.close()
            logger().debug("elasticsearch connection instance=%s closed!" % (cl))
