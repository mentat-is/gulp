"""
this is an example enrichment plugin.

it processes every provided documents and adds a bunch of fields, including "enriched": true
"""

from typing import override

import muty.file
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch_api import GulpOpenSearch
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginParameters


class Plugin(GulpPluginBase):
    """
    example enrichment plugin.
    """

    def type(self) -> GulpPluginType:
        return GulpPluginType.ENRICHMENT

    def display_name(self) -> str:
        return "enrich_example"

    @override
    def desc(self) -> str:
        return "Example enrichment plugin."

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
        for doc in chunk:
            doc["enriched"] = True
            doc[f"{GulpOpenSearch.ENRICHED_PREFIX}.new_field"] = muty.string.generate_unique()
            doc[f"{GulpOpenSearch.ENRICHED_PREFIX}.nested"] = {
                "field1": muty.string.generate_unique(),
                "field2": muty.string.generate_unique(),
                "field3": {"field4": muty.string.generate_unique()},
            }

        return await super()._enrich_documents_chunk(
            sess,
            chunk,
            chunk_num=chunk_num,
            total_hits=total_hits,
            index=index,
            last=last,
            req_id=req_id,
            q_name=q_name,
            q_group=q_group,
            **kwargs,
        )

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
        flt: GulpQueryFilter = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> tuple[int, int, list[str], bool]:
        await self._initialize(plugin_params)
        return await super().enrich_documents(
            sess,
            stats,
            user_id,
            req_id,
            ws_id,
            operation_id,
            index,
            fields,
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
        fields: dict,
        plugin_params: GulpPluginParameters,
    ) -> dict:
        await self._initialize(plugin_params)
        return await super().enrich_single_document(
            sess, doc_id, operation_id, index, fields, plugin_params
        )
