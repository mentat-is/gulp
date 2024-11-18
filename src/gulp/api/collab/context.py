from typing import Optional, override

from muty.log import MutyLogger
from sqlalchemy import ForeignKey, PrimaryKeyConstraint, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.source import GulpSource
from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabFilter,
    GulpCollabType,
    GulpUserPermission,
    T,
)
from gulp.api.collab_api import GulpCollab


class GulpContext(GulpCollabBase, type=GulpCollabType.CONTEXT):
    """
    Represents a context object

    in gulp terms, a context is used to group a set of data coming from the same host.

    it has always associated an operation, and the tuple composed by the two is unique.
    """

    operation: Mapped["GulpOperation"] = relationship(
        "GulpOperation",
        back_populates="contexts",
        doc="The operation associated with the context.",
    )
    operation_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        doc="The ID of the operation associated with the context.",
        primary_key=True,
    )

    # multiple sources can be associated with a context
    sources: Mapped[Optional[list[GulpSource]]] = relationship(
        "GulpSource",
        back_populates="context",
        cascade="all, delete-orphan",
        doc="The source/s associated with the contextt.",
    )

    title: Mapped[Optional[str]] = mapped_column(
        String, doc="The title of the context."
    )
    color: Mapped[Optional[str]] = mapped_column(
        String, default="#ffffff", doc="The color of the context."
    )
    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        default=None,
        doc="The glyph associated with the context.",
    )

    # composite primary key
    __table_args__ = (PrimaryKeyConstraint("operation_id", "id"),)

    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.CONTEXT, **kwargs)

    async def add_source(
        self,
        title: str,
        create_if_not_exist: bool = True,
        sess: AsyncSession = False,
    ) -> bool:
        """
        Add a source to the context.

        Args:
            source_id (str): The id of the source.
            title (str): The title of the source.
            create_if_not_exist (bool, optional): If True, the source is created if it does not exist. Defaults to True.
            sess (AsyncSession, optional): The session to use. Defaults to None.
        Returns:
            bool: True if the source was added, False otherwise
        """
        if sess is None:
            sess = GulpCollab.get_instance().session()
        async with sess:
            # check if source exists
            src: GulpSource = await GulpSource.get_one(
                GulpCollabFilter(
                    title=[title], operation_id=[self.operation_id], context_id=[self.id]
                ),
                sess=sess,
            )
            if not src:
                if not create_if_not_exist:
                    raise ValueError(f"source title={title} not found.")

                # create new source
                src = await GulpSource.create(
                    token=None,
                    id=None,
                    operation_id=self.operation_id,
                    context_id=self.id,
                    title=title,
                    sess=sess,
                )

            # link source to context
            await self.awaitable_attrs.sources
            if src not in self.sources:
                self.sources.append(src)
                sess.add(self)
                await sess.commit()
                MutyLogger.get_instance().info(
                    f"source {src.id}, title={title} added to context {self.id}."
                )
                return True

    @staticmethod
    async def add_source_to_id(
        operation_id: str, context_id: str, title: str, create_if_not_exist: bool = True
    ) -> bool:
        """operation
        Add a source to a context.

        Args:
            context_id (str): The id of the context.
            operation_id (str): The id of the operation.
            title (str): The title of the source (i.e. file name or path)
            create_if_not_exist (bool, optional): If True, the source is created if it does not exist. Defaults to True.

        Returns:
            bool: True if the source was added, False otherwise
        """
        async with GulpCollab.get_instance().session() as sess:
            ctx: GulpContext = await GulpContext.get_one(
                GulpCollabFilter(id=[context_id], operation_id=[operation_id]),
                sess=sess,
                ensure_eager_load=False,
            )
            if not ctx:
                raise ValueError(f"context id={context_id}, {operation_id} not found.")
            return await ctx.add_source(title=title, sess=sess)

    async def remove_source(self, source_id: str, sess: AsyncSession = None) -> bool:
        """
        Remove a source from the context.

        Args:
            source_id (str): The id of the source.
            sess (AsyncSession, optional): The session to use. Defaults to None.

        Returns:
            bool: True if the source was removed, False otherwise
        """
        if sess is None:
            sess = GulpCollab.get_instance().session()
        async with sess:
            src: GulpSource = await GulpSource.get_one(
                GulpCollabFilter(
                    id=[source_id], context_id=[self.id], operation_id=[self.operation_id]
                ),
                sess=sess,
            )
            if not src:
                raise ValueError(
                    f"source id={source_id}, {self.id}, {self.operation_id} not found."
                )

            # unlink
            await self.awaitable_attrs.sources
            if src in self.sources:
                self.sources.remove(src)
                await sess.commit()
                MutyLogger.get_instance().info(
                    f"source id={source_id} removed from context {self.id}."
                )
                return True

            MutyLogger.get_instance().info(
                f"source id={source_id} not found in context {self.id}."
            )
            return False
        
    @staticmethod
    async def remove_source_from_id(context_id: str, source_id: str, operation_id: str) -> bool:
        """
        Remove a source from a context.

        Args:
            context_id (str): The id of the context.
            source_id (str): The id of the source.
            operation_id (str): The id of the operation.

        Returns:
            bool: True if the source was removed, False otherwise

        """
        async with GulpCollab.get_instance().session() as sess:
            ctx: GulpContext = await GulpContext.get_one(
                GulpCollabFilter(id=[context_id], operation_id=[operation_id]),
                sess=sess,
                ensure_eager_load=False,
            )
            if not ctx:
                raise ValueError(f"context id={context_id}, {operation_id} not found.")
            return await ctx.remove_source(source_id=source_id, sess=sess)

    @classmethod
    async def create(
        cls,
        token: str,
        id: str,
        operation_id: str,
        title: str = None,
        color: str = None,
        glyph: str = None,
        **kwargs,
    ) -> T:
        """
        Create a new context object on the collab database.

        Args:
            token (str): The authentication token (must have INGEST permission).
            id (str): The name of the context
            operation_id (str): The id of the operation associated with the context.
            title (str, optional): The display name of the context. Defaults to id.
            color (str, optional): The color of the context. Defaults to white.
            glyph_id (str, optional): The id of the glyph associated with the context. Defaults to None.
            **kwargs: Arbitrary keyword arguments.
        Returns:
            T: The created context object
        """
        args = {
            "color": color or "white",
            "glyph_id": glyph,
            "title": title or id,
            "operation_id": operation_id,
            **kwargs,
        }
        return await super()._create(
            id=id,
            token=token,
            required_permission=[GulpUserPermission.INGEST],
            **args,
        )
