"""
gulp source representation module.

this module provides the `GulpSource` class, which represents a source of data
being processed by the gulp system. a source is always associated with an operation
and a context, forming a unique tuple.

the source entity is a fundamental part of the collaboration data model, linking
operations and contexts with the actual datasource, and providing additional metadata like color.
"""

from typing import Optional

from sqlalchemy import ForeignKey, String, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import COLLABTYPE_SOURCE, GulpCollabBase


class GulpSource(GulpCollabBase, type=COLLABTYPE_SOURCE):
    """
    Represents a source of data being processed by the gulp system.

    it has always associated a context and an operation, and the tuple composed by the three is unique.
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        doc="The ID of the operation associated with the context.",
        primary_key=True,
    )
    context_id: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The ID of the context associated with this source.",
        primary_key=True,
    )
    color: Mapped[Optional[str]] = mapped_column(
        String, doc="The color of the context."
    )
    plugin: Mapped[Optional[str]] = mapped_column(
        String,
        default=None,
        doc="plugin used for ingestion.",
    )
    mapping_parameters: Mapped[Optional[dict]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        default_factory=dict,
        doc="mapping used for ingestion.",
    )

    @classmethod
    async def get_by_ids_ordered(
        cls, sess: AsyncSession, source_ids: list[str]
    ) -> list["GulpSource"]:
        """
        get sources by a list of ids, in the same order as requested source_ids.

        Args:
            sess (AsyncSession): the database session
            source_ids (list[str]): list of source ids

        Returns:
            list[GulpSource]: list of sources (may contain None for not found sources)
        """
        stmt = select(cls).where(cls.id.in_(source_ids))
        result = await sess.execute(stmt)
        sources = result.scalars().all()

        # create a mapping for fast lookup
        source_map = {s.id: s for s in sources}

        # return sources in the same order as requested, with None for missing ones
        return [source_map.get(src_id) for src_id in source_ids]
