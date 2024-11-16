import json
from typing import Optional, Union, override

from sqlalchemy import ForeignKey, PrimaryKeyConstraint, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.source import GulpSource
from gulp.api.collab.structs import GulpCollabBase, GulpCollabFilter, GulpCollabType, T, GulpUserPermission
from gulp.api.collab_api import GulpCollab
from muty.log import MutyLogger


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
        doc="The ID of the operation associated with the context.", primary_key=True
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
    
    @staticmethod
    async def add_source(context_id: str, source_id: str, operation_id: str) -> None:
        """operation
        Add a source to a context.

        Args:
            context_id (str): The id of the context.
            source_id (str): The id of the source.
            operation_id (str): The id of the operation.
        """
        async with GulpCollab.get_instance().session() as sess:            
            ctx:GulpContext = await GulpContext.get_one(GulpCollabFilter(id=[context_id], operation_id=[operation_id]), sess=sess, ensure_eager_load=False)
            if not ctx:
                raise ValueError(f"context id={context_id}, {operation_id} not found.")
            src:GulpSource = await GulpSource.get_one(GulpCollabFilter(id=[source_id], context_id=[context_id], operation_id=[operation_id]), sess=sess, ensure_eager_load=False)
            if not src:
                raise ValueError(f"source id={source_id}, {context_id}, {operation_id} not found.")
    
            # link
            await ctx.awaitable_attrs.sources
            ctx.sources.append(src)
            await sess.commit()
            #print(json.dumps(ctx.to_dict(nested=True), indent=4))
            MutyLogger.get_logger().info(f"source id={source_id} added to context {context_id}.")

    @staticmethod
    async def remove_source(context_id: str, source_id: str, operation_id: str) -> None:
        """
        Remove a source from a context.

        Args:
            context_id (str): The id of the context.
            source_id (str): The id of the source.
            operation_id (str): The id of the operation.
        """
        async with GulpCollab.get_instance().session() as sess:
            ctx:GulpContext = await GulpContext.get_one(GulpCollabFilter(id=[context_id], operation_id=[operation_id]), sess=sess, ensure_eager_load=False)
            if not ctx:
                raise ValueError(f"context id={context_id}, {operation_id} not found.")
            src:GulpSource = await GulpSource.get_one(GulpCollabFilter(id=[source_id], context_id=[context_id], operation_id=[operation_id]), sess=sess, ensure_eager_load=False)
            if not src:
                raise ValueError(f"source id={source_id}, {context_id}, {operation_id} not found.")
                
            # unlink
            await ctx.awaitable_attrs.sources
            ctx.sources.remove(src)
            await sess.commit()
            MutyLogger.get_logger().info(f"source id={source_id} removed from context {context_id}.")

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
        args = {"color": color or 'white',
                "glyph_id": glyph, 
                "title": title or id,
                "operation_id": operation_id,
                **kwargs}
        return await super()._create(
            id=id,
            token=token,
            required_permission=[GulpUserPermission.INGEST],
            **args,
        )
