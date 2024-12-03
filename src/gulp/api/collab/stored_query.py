from typing import Optional, override

from sigma.rule import SigmaRule
from sqlalchemy import ARRAY, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabType,
    T,
)


class GulpStoredQuery(GulpCollabBase, type=GulpCollabType.STORED_QUERY):
    """
    a stored query in the gulp collaboration system
    """

    name: Mapped[str] = mapped_column(
        String,
        doc="The query display name.",
    )
    q: Mapped[str] = mapped_column(
        String,
        doc="The query as string: it is intended to be a JSON object for gulp queries or an arbitrary string to be interpreted by the target plugin for external queries.",
    )
    text: Mapped[Optional[str]] = mapped_column(
        String,
        doc="The query in its original format (just for reference), as string.",
    )
    tags: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String),
        doc="The tags associated with the query.",
    )
    description: Mapped[Optional[str]] = mapped_column(
        String,
        doc="The description of the query.",
    )
    glyph_id: Mapped[Optional[str]] = mapped_column(
        String,
        doc="ID of a glyph to associate with the query.",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["name"] = "query_name"
        d["text"] = "the_original_query_as_string"
        d["tags"] = ["tag1", "tag2"]
        d["description"] = "query_description"
        d["glyph_id"] = "glyph_id"
        d["q"] = "the_query_ready_to_be_used_as_string"
        return d
