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

import asyncio
from typing import Optional, override
from urllib.parse import urlparse

import aiohttp
import muty.dict
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    abuse.ch API enrichment plugin

    """
    def __init__(
        self,
        path: str,
        module_name: str,
        pickled: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(path, module_name, pickled=pickled, **kwargs)

    def type(self) -> GulpPluginType:
        return GulpPluginType.ENRICHMENT

    def display_name(self) -> str:
        return "enrich_abuse"

    @override
    def desc(self) -> str:
        return "abuse.ch url enrichment plugin"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="query_type",
                type="str",
                desc="`url` or `hash`",
                default_value="url",
                values=["url", "hash"],
            ),
            GulpPluginCustomParameter(
                name="hash_type",
                type="str",
                desc="`md5` or `sha256`, or None to autodetect from hash length. only used if `query_type` is set to `hash`, ignored either.",
                default_value=None,
                values=["md5", "sha256", None],
            ),
            GulpPluginCustomParameter(
                name="auth_key",
                type="str",
                desc="abuse.ch auth-key (if not provided, the config file is checked for it)",
                required=False,
                default_value=None,
            ),
        ]

    def _get_auth_key(self) -> str:
        # get abuse.ch auth key from either the params or from config
        auth_key = self._plugin_params.custom_parameters.get("auth_key")
        if not auth_key:
            # TODO: attempt reading it from config
            plugin_config = GulpConfig.get_instance().get("enrich_abuse", {})
            auth_key = plugin_config.get("auth_key", None)

            if not auth_key:
                raise Exception(
                    "no auth_key provided for %s" % (self.display_name())
                )

        return auth_key

    def _check_query_status(self, js: dict) -> bool:
        qs: str = js.get("query_status", "error")
        if qs != "ok":
            MutyLogger.get_instance().warning(
                f"abuse.ch query returned non-ok status: {qs}"
            )
            return False
        return True
    async def _get_abuse_url(self, sess: aiohttp.ClientSession, url: str, auth_key: str) -> Optional[dict]:
        """
        Given an url checks it against abuse.ch APIs
        """
        if not self._is_valid_url(url):
            return None

        data = {"url": url}
        headers = {"Auth-Key": auth_key}

        async with aiohttp.ClientSession(headers=headers) as sess:
            async with sess.post("https://urlhaus-api.abuse.ch/v1/url", data=data) as resp:
                if resp.status == 200:
                    js = await resp.json()
                    if not self._check_query_status(js):                        
                        return None
                    MutyLogger.get_instance().debug(
                        f"url found for url='{url}': {js}"
                    )
                    return js

                MutyLogger.get_instance().warning(
                    f"url not found for url='{url}', status={resp.status}"
                )
                return None

    async def _get_abuse_hash(self, sess: aiohttp.ClientSession, hash_algo: str, h: str, auth_key: str) -> Optional[dict]:
        """
        Given an hash checks it against abuse.ch APIs
        """
        data = {f"{hash_algo}_hash": h}
        headers = {"Auth-Key": auth_key}

        async with aiohttp.ClientSession(headers=headers) as sess:
            async with sess.post("https://urlhaus-api.abuse.ch/v1/payload", data=data) as resp:
                if resp.status == 200:
                    js = await resp.json()
                    if not self._check_query_status(js):
                        return None
                    MutyLogger.get_instance().debug(
                        f"hash found for hash='{h}' and hash_type='{hash_algo}': {js}"
                    )
                    return js
                
                MutyLogger.get_instance().warning(
                    f"url not found for url='{h}', status={resp.status}"
                )
                return None

    def _is_valid_url(self, u: str):
        try:
            # TODO: c optimization ?
            _ = urlparse(u)
            return u
        except AttributeError:
            return False

    async def _enrich_documents_chunk(
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
        auth_key: str = self._get_auth_key()
        fields: dict = kwargs["fields"]
        hash_type: str = self._plugin_params.custom_parameters.get("hash_type")
        query_type: str = self._plugin_params.custom_parameters.get("query_type")        
        dd: list[dict] = []
        abuse_data: dict = None

        async with aiohttp.ClientSession() as http_sess:
            for doc in chunk:
                for field,field_value in fields.items():
                    if field_value:
                        # value provided
                        f = field_value
                    else:
                        # get from document
                        f = muty.dict.get_value_nested(doc, field)
                    if not f:
                        await asyncio.sleep(0.1)  # let other tasks run
                        continue
                    
                    if query_type == "url":
                        # get url
                        abuse_data = await self._get_abuse_url(http_sess, f, auth_key)                        
                    elif query_type == "hash":
                        h_to_use: str = hash_type
                        if not h_to_use:
                            # autodetect hash type from length
                            hash_len_map = {
                                "md5": 32,
                                "sha256": 64,
                            }
                            if len(f) not in hash_len_map.values():
                                MutyLogger.get_instance().warning(
                                    f"unable to autodetect hash type for field='{field}' with value='{f}' (len={len(f)}), skipping"
                                )
                                await asyncio.sleep(0.1)  # let other tasks run, check next field
                                continue
                            for h, l in hash_len_map.items():
                                if len(f) == l:
                                    h_to_use = h
                                    MutyLogger.get_instance().debug(
                                        f"autodetected hash type='{h_to_use}' for field='{field}' with value='{f}'"
                                    )
                                    break
                        else:
                            MutyLogger.get_instance().debug(
                                f"using provided hash type='{h_to_use}' for field='{field}' with value='{f}'"
                            )
                        # get hash
                        abuse_data = await self._get_abuse_hash(http_sess, h_to_use, f, auth_key)

                    if abuse_data:
                        # remove unneeded fields
                        abuse_data.pop("query_status", None) 
                        
                        # set data
                        doc.update(self._build_or_update_enriched_obj(doc, field, abuse_data))
                        dd.append(doc)

                        # add to cache
                        if query_type == "url":
                            cache_key: str = f"{self.name}:url:{f}"
                        elif query_type == "hash":
                            cache_key: str = f"{self.name}:{h_to_use}:{f}"
                        self.doc_value_cache.set_value(cache_key, abuse_data)
                
                    # do not hammer the server ...
                    await asyncio.sleep(0.5)


        return dd

    @override
    async def enrich_documents(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        operation_id: str,
        index: str,
        fields: dict,
        q: dict = None,
        flt: GulpQueryFilter = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> tuple[int, int, list[str], bool]:
        # parse custom parameters
        await self._initialize(plugin_params)

        qq = {
            "query": {
                "bool": {
                    "should": [],
                    "minimum_should_match": 1,
                }
            }
        }

        # enrich
        return await super().enrich_documents(
            sess,
            stats,
            user_id,
            req_id,
            ws_id,
            operation_id,
            index,
            fields,
            qq,
            flt,
            plugin_params,
        )

    @override
    async def enrich_single_document(
        self,
        sess: AsyncSession,
        doc_id: str,
        operation_id: str,
        index: str,
        fields: str,
        plugin_params: GulpPluginParameters,
    ) -> dict:
        # parse custom parameters
        await self._initialize(plugin_params)
        return await super().enrich_single_document(
            sess, doc_id, operation_id, index, fields, plugin_params
        )
