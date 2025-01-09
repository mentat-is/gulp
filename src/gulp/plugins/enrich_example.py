from typing import Any, override

import muty.file
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import (
    GulpQueryParameters,
)
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import (
    GulpPluginParameters,
)


class Plugin(GulpPluginBase):
    """
    example enrichment plugin.
    """

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.ENRICHMENT]

    def display_name(self) -> str:
        return "enrich_example"

    @override
    def desc(self) -> str:
        return "Example enrichment plugin."

    async def _enrich_documents_chunk(self, docs: list[dict], **kwargs) -> list[dict]:
        for doc in docs:
            doc["enriched"] = True
            doc["gulp.enriched.new_field"] = muty.string.generate_unique()
            doc["gulp.enriched.nested"] = {
                "field1": muty.string.generate_unique(),
                "field2": muty.string.generate_unique(),
                "field3": {"field4": muty.string.generate_unique()},
            }

        return docs

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
        """
        example query:
        {
            "q": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "wildcard": {
                                    "gulp.enriched.nested.field3.field4": "a0ddcac7-862d-4071-86d5-246fbc096af1"
                                }
                            },
                            {
                                "wildcard": {
                                    "gulp.enriched.nested.field2": "d8d7b20f-8d98-4a79-87f5-b999d870791b"
                                }
                            }
                        ]
                    }
                }
            }
        }
        """
        await super().enrich_documents(
            sess, user_id, req_id, ws_id, index, q, q_options, plugin_params
        )

    @override
    async def enrich_single_document(
        self,
        sess: AsyncSession,
        doc_id: str,
        index: str,
        plugin_params: GulpPluginParameters,
    ) -> dict:
        return await super().enrich_single_document(sess, doc_id, index, plugin_params)
