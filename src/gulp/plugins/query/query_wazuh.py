import datetime
import os
import re
from urllib.parse import parse_qs, urlparse

import aiofiles
import muty.dict
import muty.jsend
import muty.log
import muty.string
import muty.time
import muty.xml

from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.elastic.structs import (
    GulpDocument,
    GulpIngestionFilter,
    GulpQueryFilter,
    GulpQueryOptions,
)
from gulp.api.mapping.models import FieldMappingEntry, GulpMapping
from gulp.defs import GulpLogLevel, GulpPluginType
from gulp.plugin import PluginBase
from gulp.plugin_internal import GulpPluginOption, GulpPluginParams
from gulp.utils import logger

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
        return "Query data from Wazuh."

    def name(self) -> str:
        return "query_wazuh"

    def version(self) -> str:
        return "1.0"

    def options(self) -> list[GulpPluginOption]:
        return [
            GulpPluginOption(
                "locale", "str", "original server's locale", default=None
            ),  # TODO
            GulpPluginOption(
                "date_format",
                "str",
                "server date log format",
                default="%d/%b/%Y:%H:%M:%S %z",
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
        logger().debug("querying Wazuh, params=%s, filter: %s" % (plugin_params, flt))

        # loop until there's data, in chunks of 1000 events, or until we reach the limit
        # - update stats
        # - check request canceled
        # - write results on the websocket
        status = GulpRequestStatus.FAILED
        num_results = 0
        return num_results, status
