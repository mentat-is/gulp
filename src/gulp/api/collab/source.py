"""
gulp source representation module.

this module provides the `GulpSource` class, which represents a source of data
being processed by the gulp system. a source is always associated with an operation
and a context, forming a unique tuple.

the source entity is a fundamental part of the collaboration data model, linking
operations and contexts with the actual datasource, and providing additional metadata like color.
"""

from typing import Optional, override

from sqlalchemy import ForeignKey, String, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.structs import COLLABTYPE_SOURCE, GulpCollabBase


class GulpSource(GulpCollabBase, type=COLLABTYPE_SOURCE):
    """
    Represents a source of data being processed by the gulp system.

    it has always associated a context and an operation, and the tuple composed by the three is unique.
    """
    context_id: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The ID of the context associated with this source.",
        primary_key=True,
    )
    # relationship to load mapping parameters when needed
    mapping_parameters: Mapped[Optional["GulpMappingParametersEntry"]] = relationship(
        "GulpMappingParametersEntry",
        lazy="selectin",
        uselist=False,
        doc="The mapping parameters entry referenced by this source.",
    )

    # reference to mapping parameters stored in a dedicated table
    mapping_parameters_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("mapping_parameters.id", ondelete="SET NULL"),
        default=None,
        doc="Reference to an entry in the mapping_parameters table.",
    )

    plugin: Mapped[Optional[str]] = mapped_column(
        String,
        default=None,
        doc="plugin used for ingestion.",
    )


    async def set_mapping_parameters(
        self, sess: AsyncSession, mapping: dict, user_id: str
    ) -> str:
        """
        Ensure the mapping exists in the dedicated table and set the reference on this source.
        Returns the mapping id used.
        """
        from gulp.api.collab.mapping_parameters import GulpMappingParametersEntry

        mp, created = await GulpMappingParametersEntry.create_if_not_exists(
            sess, mapping, user_id
        )
        await self.update(sess, mapping_parameters_id=mp.id)
        return mp.id

    async def get_mapping(self, sess: AsyncSession) -> dict | None:
        """Return the mapping dict for this source, or None if not set."""
        if self.mapping_parameters:
            return self.mapping_parameters.mapping
        if not self.mapping_parameters_id:
            return None
        from gulp.api.collab.mapping_parameters import GulpMappingParametersEntry

        mp = await GulpMappingParametersEntry.get_by_id(
            sess, self.mapping_parameters_id, throw_if_not_found=False
        )
        return mp.mapping if mp else None

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

    @override
    async def delete(self, sess: AsyncSession, *args, **kwargs) -> None:
        """Delete the source and cleanup any orphaned mapping parameters."""
        # call base delete (which commits)
        await super().delete(sess, *args, **kwargs)
        # remove orphaned mapping params
        from gulp.api.collab.mapping_parameters import GulpMappingParametersEntry

        await GulpMappingParametersEntry.delete_orphaned(sess)
