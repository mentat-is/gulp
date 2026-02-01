import asyncio
import hashlib
import itertools
import json
import socket
from typing import Any, Optional, override
from urllib.parse import urlparse

import aiohttp
import muty.dict
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.structs import GulpQueryHelpers, GulpQueryParameters
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

    def type(self) -> GulpPluginType:
        return GulpPluginType.ENRICHMENT

    def display_name(self) -> str:
        return "enrich_circl_hash"

    @override
    def desc(self) -> str:
        return "circl.lu hash lookup enrichment plugin"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="compute",
                type="bool",
                desc="if set, hash value will be computed from raw data (i.e. for ssdeep). If not set, hash value is expected to be already computed and present in the document field.",
                default_value=False,
            ),
            GulpPluginCustomParameter(
                name="hash_type",
                type="str",
                desc="if not set (default) will autodetect hash type from hash length, if `compute` is not set. if `compute` is set, it is the hash type to be calculated from raw data. Supported values are `md5`, `sha1` and `sha256`.",
                default_value=None,
                values=["md5", "sha1", "sha256"],
            ),
        ]

    async def _get_hash(self, sess: aiohttp.ClientSession, hash: str, hash_type: str) -> Optional[dict]:
        """
        Given a hash get info from circl.lu's db
        """
        # check cache first
        cache_key: str = f"{self.name}:{hash_type}:{hash}"
        cached: dict = self.doc_value_cache.get_value(cache_key)
        if cached is not None:
            # found cached!
            MutyLogger.get_instance().debug(
                f"hash found in cache for hash='{hash}' and hash_type='{hash_type}': {cached}"
            )
            return cached

        async with sess.get(f"https://hashlookup.circl.lu/lookup/{hash_type}/{hash}") as resp:
            if resp.status == 200:
                # found!
                js = await resp.json()
                MutyLogger.get_instance().debug(
                    f"hash found for hash='{hash}' and hash_type='{hash_type}': {js}"
                )
                return js
            
            # not found
            MutyLogger.get_instance().warning(
                f"hash NOT found for hash='{hash}' and hash_type='{hash_type}': status={resp.status}"
            )
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
        fields: dict = kwargs["fields"]
        if compute and not h_to_use:
            raise ValueError("when 'compute' is set to True, 'hash_type' must be provided")
        
        dd: list[dict] = []
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

                    h_to_use: str = hash_type
                    if not h_to_use and not compute:
                        # autodetect hash type from length
                        hash_len_map = {
                            "md5": 32,
                            "sha1": 40,
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
                    elif h_to_use and not compute:
                        MutyLogger.get_instance().debug(
                            f"using provided hash type='{h_to_use}' for field='{field}' with value='{f}'"
                        )

                    elif h_to_use and compute:
                        # compute hash from raw data
                        MutyLogger.get_instance().debug(
                            f"computing hash for field='{field}' using hash_type='{h_to_use}'"
                        )
                        f = hashlib.new(h_to_use, bytes.fromhex(f)).hexdigest()

                    # check hash on circl
                    hash_data = await self._get_hash(http_sess, f, h_to_use)
                    if hash_data:
                        # found!                        
                        doc.update(self._build_or_update_enriched_obj(doc, field, hash_data))
                        dd.append(doc)

                        # add to cache
                        cache_key: str = f"{self.name}:{h_to_use}:{f}"
                        self.doc_value_cache.set_value(cache_key, hash_data)
                    
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
