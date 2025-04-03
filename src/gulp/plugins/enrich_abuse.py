"""
An abuse.ch URL enrichment plugin for GULP.

This plugin queries the abuse.ch API to enrich documents containing URLs with
information about whether those URLs are known to be malicious or have been
reported to abuse.ch (in the last 30 days).

The plugin supports:
- Checking URLs against the abuse.ch URLhaus database
- Enriching documents with abuse.ch data for matching URLs
- Configurable URL field targeting

Authentication is required through an abuse.ch auth key, which can be provided
either as a plugin parameter or in the GULP configuration.

TODO: abuse.ch DB enrichment plugin
"""

from typing import Optional, override
from urllib.parse import urlparse

import aiohttp
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    abuse.ch API enrichment plugin

    """

    class MissingAuthKey(Exception):
        def __init__(self, message):
            # Call the base class constructor with the parameters it needs
            super().__init__(message)

    def __init__(
        self,
        path: str,
        pickled: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(path, pickled=pickled, **kwargs)
        self._whois_cache = {}

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.ENRICHMENT]

    def display_name(self) -> str:
        return "enrich_abuse"

    @override
    def desc(self) -> str:
        return "abuse.ch url enrichment plugin"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="url_fields",
                type="list",
                desc="a list of url fields to enrich.",
                default_value=["url.full", "url.original", "http.request.referrer"],
            ),
            GulpPluginCustomParameter(
                name="auth_key",
                type="str",
                desc="abuse.ch auth-key (if not provided, the config file is checked for it)",
                default_value=None,
                required=False,
            ),
        ]

    async def _get_abuse(self, url: str, auth_key: str) -> Optional[dict]:
        """
        Given an url checks it against abuse.ch APIs
        """
        if not self._is_valid_url(url):
            return None

        data = {"url": url}
        headers = {"Auth-Key": auth_key}

        async with aiohttp.ClientSession(headers=headers) as sess:
            async with sess.post(
                "https://urlhaus-api.abuse.ch/v1/url", data=data
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    return None

    def _is_valid_url(self, u: str):
        try:
            # TODO: c optimization ?
            _ = urlparse(u)
            return u
        except AttributeError:
            return False

    async def _enrich_documents_chunk(self, docs: list[dict], **kwargs) -> list[dict]:
        auth_key = self._plugin_params.custom_parameters.get("auth_key")
        dd = []
        url_fields = self._plugin_params.custom_parameters.get("url_fields", [])
        for doc in docs:
            for url_field in url_fields:
                f = doc.get(url_field)
                if not f:
                    continue

                # check if the url is a valid url
                url = self._is_valid_url(f)
                if not url:
                    continue

                # append flattened data to the document
                abuse_data = await self._get_abuse(url, auth_key)
                if abuse_data:
                    for key, value in abuse_data.items():
                        if value:
                            doc["gulp.%s.%s" % (self.name, key)] = value
                    dd.append(doc)

        return dd

    def _get_auth_key(self):
        # get abuse.ch auth key from either the params or from config

        auth_key = self._plugin_params.custom_parameters.get("auth_key")
        if not auth_key:
            # TODO: attempt reading it from config
            plugin_config = GulpConfig.get_instance().get("enrich_abuse", {})
            auth_key = plugin_config.get("auth_key", None)

            if not auth_key:
                raise self.MissingAuthKey(
                    "no auth_key provided for %s" % (self.display_name())
                )

        self._plugin_params.custom_parameters["auth_key"] = auth_key

    @override
    async def enrich_documents(
        self,
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        operation_id: str,
        index: str,
        flt: GulpQueryFilter = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> int:
        # parse custom parameters
        await self._initialize(plugin_params)
        self._get_auth_key()

        url_fields = self._plugin_params.custom_parameters.get("url_fields", [])
        qq = {
            "query": {
                "bool": {
                    "should": [],
                    "minimum_should_match": 1,
                }
            }
        }

        # select all non-empty url fields
        for url_field in url_fields:
            qq["query"]["bool"]["should"].append(
                {
                    "bool": {
                        "must": [
                            {"exists": {"field": url_field}},
                        ]
                    }
                }
            )

        # enrich
        return await super().enrich_documents(
            sess, user_id, req_id, ws_id, operation_id, index, flt, plugin_params, rq=qq
        )

    @override
    async def enrich_single_document(
        self,
        sess: AsyncSession,
        doc_id: str,
        operation_id: str,
        index: str,
        plugin_params: GulpPluginParameters,
    ) -> dict:
        # parse custom parameters
        await self._initialize(plugin_params)
        self._get_auth_key()

        return await super().enrich_single_document(
            sess, doc_id, operation_id, index, plugin_params
        )
