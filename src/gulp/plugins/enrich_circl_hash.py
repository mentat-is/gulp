import itertools
import json
import socket
import hashlib
from typing import Any, Optional, override
from urllib.parse import urlparse

import aiohttp
import muty.jsend
from sqlalchemy.ext.asyncio import AsyncSession

import muty
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.structs import GulpQueryHelpers
from gulp.api.opensearch.structs import GulpQueryParameters
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    enric a file hash using circl.lu hash lookup API
    """

    class MissingAuthKey(Exception):
        """API Authentication error"""

        def __init__(self, message):
            # Call the base class constructor with the parameters it needs
            super().__init__(message)

    def __init__(
        self,
        path: str,
        module_name: str,
        pickled: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(path, module_name, pickled=pickled, **kwargs)
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
                desc="a list of fields containing hashes to lookup.",
                default_value=[
                    "".join(r)
                    for r in itertools.product(
                        ["file.hash.", "hash."], ["md5", "sha1", "sha256", "sha512"]
                    )
                ],
            ),
            GulpPluginCustomParameter(
                name="compute",
                type="bool",
                desc="treat the hash_fields as fields containing raw data(must be in hexstring format) and lookup the calculated hash instead",
                default_value=False,
            ),
            GulpPluginCustomParameter(
                name="hash_type",
                type="str",
                desc="type of hash (md5 sha1, sha256, etc.) to lookup, if not set auto-detect from field name ",
                default_value=None,
            ),
        ]

    async def _get_hash(self, hash: str, hash_type: str) -> Optional[dict]:
        """
        Given a hash get info from circl.lu's db
        """

        async with aiohttp.ClientSession() as sess:
            async with sess.get(
                f"https://hashlookup.circl.lu/lookup/{hash_type}/{hash}"
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    return None

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
        hash_type = self._plugin_params.custom_parameters.get("hash_type")
        compute = self._plugin_params.custom_parameters.get("compute")

        dd = []
        hash_fields = self._plugin_params.custom_parameters.get("hash_fields", [])
        for doc in chunk:
            for hash_field in hash_fields:
                f = doc.get(hash_field)
                if not f:
                    continue

                # no hash type was provided, attempt autodetection from field name
                # (only do this when not computing)
                if not compute and not hash_type:
                    # TODO check actual supported ones from circl.lu
                    supported_hashes = ["md5", "sha1", "sha256", "sha512"]
                    for (
                        s
                    ) in (
                        supported_hashes
                    ):  # TODO: this is prone to error, actually unpack fields "."s and check
                        if s in hash_field.lower():
                            hash_type = s
                            break

                if compute:
                    f = hashlib.new(hash_type, bytes.fromhex(f)).hexdigest()

                # append flattened data to the document
                hash_data = await self._get_hash(f, hash_type)
                if hash_data:
                    for key, value in hash_data.items():
                        if value:
                            # TODO: normalize using normalizing field name helper from muty
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
        operation_id: str,
        index: str,
        flt: GulpQueryFilter = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> tuple[int, int, list[str], bool]:
        # parse custom parameters
        self._initialize(plugin_params)

        hash_fields = self._plugin_params.custom_parameters.get("hash_fields", [])
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
        self._initialize(plugin_params)
        return await super().enrich_single_document(
            sess, doc_id, operation_id, index, plugin_params
        )
