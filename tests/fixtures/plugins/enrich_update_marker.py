"""Deterministic enrichment plugin used by duplicate-request integration tests."""

from typing import override

from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginParameters


class Plugin(GulpPluginBase):
    """Mark every provided document with a deterministic enrichment field."""

    def type(self) -> GulpPluginType:
        return GulpPluginType.ENRICHMENT

    def display_name(self) -> str:
        return "enrich_update_marker"

    @override
    def desc(self) -> str:
        return "Duplicate-request integration test enrichment plugin."

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
            doc.update(
                self._build_or_update_enriched_obj(
                    doc, "marker", f"applied:{req_id}"
                )
            )

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
            flt=flt,
            plugin_params=plugin_params,
        )
