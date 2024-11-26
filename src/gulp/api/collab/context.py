from typing import Optional, override

import muty.crypto
from muty.log import MutyLogger
import muty.string
from sqlalchemy import ForeignKey, PrimaryKeyConstraint, String, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.source import GulpSource
from gulp.api.collab.structs import GulpCollabBase, GulpCollabFilter, GulpCollabType, T


class GulpContext(GulpCollabBase, type=GulpCollabType.CONTEXT):
    """
    Represents a context object

    in gulp terms, a context is used to group a set of data coming from the same host.

    it has always associated an operation, and the tuple composed by the two is unique.
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        doc="The ID of the operation associated with the context.",
        primary_key=True,
    )
    # multiple sources can be associated with a context
    sources: Mapped[Optional[list[GulpSource]]] = relationship(
        "GulpSource",
        cascade="all, delete-orphan",
        uselist=True,
        lazy="selectin",
        foreign_keys=[GulpSource.context_id],
        doc="The source/s associated with the context.",
    )

    name: Mapped[Optional[str]] = mapped_column(
        String, doc="The name of the context.", default=None
    )
    color: Mapped[Optional[str]] = mapped_column(
        String, default="white", doc="The color of the context."
    )
    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        default=None,
        doc="The glyph associated with the context.",
    )

    # composite primary key
    __table_args__ = (PrimaryKeyConstraint("operation_id", "id"),)

    async def add_source(
        self,
        sess: AsyncSession,
        user_id: str,
        name: str,
    ) -> GulpSource:
        """
        Add a source to the context.

        Args:
            sess (AsyncSession): The session to use.
            user_id (str): The id of the user adding the source.
            name (str): The name of the source.
        Returns:
            GulpSource: the source added (or already existing), eager loaded
        """

        # acquire lock first
        lock_id = muty.crypto.hash_xxh64_int(f"{self.id}-{name}")
        await sess.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"), {"lock_id": lock_id}
        )
        sess.add(self)

        # check if source exists
        src: GulpSource = await GulpSource.get_by_filter(
            sess,
            flt=GulpCollabFilter(
                name=[name],
                operation_id=[self.operation_id],
                context_id=[self.id],
            ),
            throw_if_not_found=False,
            with_for_update=True,
        )
        if src:
            MutyLogger.get_instance().info(
                f"source {src.id}, name={name} already exists in context {self.id}."
            )
            return src

        # create new source
        src = await GulpSource.create(
            sess,
            user_id=user_id,
            operation_id=self.operation_id,
            context_id=self.id,
            name=name,
        )
        await sess.refresh(self)

        MutyLogger.get_instance().info(
            f"source {src.id}, name={name} added to context {self.id}."
        )
        return src

    @classmethod
    async def create(
        cls,
        sess: AsyncSession,
        user_id: str,
        operation_id: str,
        name: str,
        color: str = None,
        glyph_id: str = None,
    ) -> T:
        """
        Create a new context object on the collab database.

        Args:
            sess (AsyncSession): The database session to use.
            user_id (str): The id of the user creating the context.
            operation_id (str): The id of the operation associated with the context.
            name (str, optional): The display name of the context.
            color (str, optional): The color of the context. Defaults to white.
            glyph_id (str, optional): The id of the glyph associated with the context. Defaults to None.
            **kwargs: Arbitrary keyword arguments.
        Returns:
            T: The created context object
        """
        object_data = {
            "operation_id": operation_id,
            "name": name,
            "color": color or "white",
            "glyph_id": glyph_id,
        }
        return await super()._create(
            sess,
            object_data,
            id=muty.string.ensure_no_space_no_special(name),
            user_id=user_id,
        )
