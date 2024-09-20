
import muty.dict
import muty.jsend
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml

from gulp.api.collab.base import GulpRequestStatus
from gulp.api.elastic.structs import (
    GulpQueryFilter,
    GulpQueryOptions,
)
from gulp.defs import GulpPluginType, InvalidArgument
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginOption, GulpPluginParams
from gulp.utils import logger

try:
    from elasticsearch import AsyncElasticsearch
except ImportError:
    muty.os.install_package("elasticsearch[async]")
    from elasticsearch import AsyncElasticsearch

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
                "url", "str", "opensearch/elasticsearch server URL.", default=None
            ),  # TODO
            GulpPluginOption(
                "is_opensearch",
                "bool",
                "True if the server is an OpenSearch server, False if it's an Elasticsearch server.",
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

        status = GulpRequestStatus.FAILED
        num_results = 0

        # get options
        url = plugin_params.extra.get("url")
        is_opensearch = plugin_params.extra.get("is_opensearch")
        username = plugin_params.extra.get("username")
        password = plugin_params.extra.get("password")
        index = plugin_params.extra.get("index")

        if not url or not index:
            raise InvalidArgument("missing required parameters (url, index)")

        # connect to elastic

        # loop until there's data, in chunks of 1000 events, or until we reach the limit
        # - update stats
        # - check request canceled
        # - write results on the websocket
        return num_results, status
