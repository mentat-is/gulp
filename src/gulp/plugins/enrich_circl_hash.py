import json
import socket
import aiohttp
from typing import Any, Optional, override
import muty.file
import muty.json
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml
import itertools
from muty.log import MutyLogger
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQueryHelpers, GulpQueryParameters
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters
from gulp.config import GulpConfig
from sqlalchemy.ext.asyncio import AsyncSession
from urllib.parse import urlparse

class Plugin(GulpPluginBase):
    """
    enric a file hash using circl.lu hash lookup API
    """
    class MissingAuthKey(Exception):
        def __init__(self, message, errors):            
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
        return "enrich_circl_hash"

    @override
    def desc(self) -> str:
        return "circl.lu hash lookup enrichment plugin"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="hash_fields",
                type="list",
                desc="a list of url fields to enrich.",
                default_value=["".join(r) for r in itertools.product(["file.hash.", "hash."], ["md5", "sha1", "sha256", "sha512"])],
            ),
            GulpPluginCustomParameter(
                name="hash_type",
                type="str",
                desc="type of hash (md5 sha1, sha256, etc.) to lookup, if not set auto-detect from field name ",
                default_value=None,
            )
        ]

    async def _get_hash(self, hash: str, hash_type:str) -> Optional[dict]:
        """
            Given a hash get info from circl.lu's db
        """

        async with aiohttp.ClientSession(headers=headers) as sess:
            async with sess.get(f"https://hashlookup.circl.lu/lookup/{hash_type}/{hash}") as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    return None

    async def _enrich_documents_chunk(self, docs: list[dict], **kwargs) -> list[dict]:
        hash_type = self._custom_params.get("hash_type")
        
        dd = []
        hash_fields = self._custom_params.get("hash_fields", [])
        for doc in docs:
            for hash_field in hash_fields:
                f = doc.get(hash_field)
                if not f:
                    continue

                # no hash type was provided, attempt autodetection from field name
                if not hash_type:
                    supported_hashes = ["md5", "sha1", "sha256", "sha512"] #TODO check actual supported ones from circl.lu
                    for s in supported_hashes: #TODO: this is prone to error, actually unpack fields "."s and check
                        if s in hash_field.lower():
                            hash_type = s
                            break

                # append flattened data to the document                
                hash_data = await self._get_hash(f, hash_type)
                if hash_data:
                    for key, value in hash_data.items():
                        if value:
                            doc["gulp.%s.%s.%s" % (self.name, f, key)] = value
                    dd.append(doc)

        return dd

    @override
    async def enrich_documents(
        self,
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        q: dict = None,
        q_options: GulpQueryParameters = None,
        plugin_params: GulpPluginParameters = None,
    ) -> None:
        # parse custom parameters
        self._parse_custom_parameters(plugin_params)

        hash_fields = self._custom_params.get("hash_fields", [])
        qq = {
            "query": {
                "bool": {
                    "should": [],
                    "minimum_should_match": 1,
                }
            }
        }

        # select all non-empty url fields
        for hash_field in hash_fields:
            qq["query"]["bool"]["should"].append(
                {
                    "bool": {
                        "must": [
                            {"exists": {"field": hash_field}},
                        ]
                    }
                }
            )

        if q:
            # merge with provided query
            qq = GulpQueryHelpers.merge_queries(q, qq)

        await super().enrich_documents(
            sess, user_id, req_id, ws_id, index, qq, q_options, plugin_params
        )

    @ override
    async def enrich_single_document(
        self,
        sess: AsyncSession,
        doc_id: str,
        index: str,
        plugin_params: GulpPluginParameters,
    ) -> dict:
        # parse custom parameters
        self._parse_custom_parameters(plugin_params)
        return await super().enrich_single_document(sess, doc_id, index, plugin_params)
