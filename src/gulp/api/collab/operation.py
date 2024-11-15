from typing import Optional, Union, override

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.source import GulpSource
from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T, GulpUserPermission
from gulp.api.collab_api import GulpCollab
from gulp.api.collab.context import GulpContext
from gulp.utils import GulpLogger


class GulpOperation(GulpCollabBase, type=GulpCollabType.OPERATION):
    """
    Represents an operation in the gulp system.
    """
    title: Mapped[Optional[str]] = mapped_column(
        String, doc="The title of the operation."
    )
    
    index: Mapped[Optional[str]] = mapped_column(
        String,
        doc="The gulp opensearch index to associate the operation with.",
    )
    description: Mapped[Optional[str]] = mapped_column(
        String, doc="The description of the operation."
    )

    # multiple sources can be associated with an operation
    source: Mapped[Optional[list[GulpSource]]] = relationship(
        "GulpSource",
        back_populates="operation",
        cascade="all, delete-orphan",
        doc="The source/s associated with the operation.",
    )

    # multiple contexts can be associated with an operation
    context: Mapped[Optional[list[GulpContext]]] = relationship(
        "GulpContext",
        back_populates="operation",
        cascade="all, delete-orphan",
        doc="The context/s associated with the operation.",
    )

    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        doc="The glyph associated with the operation.",
    )

    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.OPERATION, **kwargs)

    @staticmethod
    async def add_context(operation_id: str, context_id: str) -> None:
        """
        Add a context to an operation.

        Args:
            operation_id (str): The id of the operation.
            context_id (str): The id of the context.
        """
        async with GulpCollab.get_instance().session() as sess:
            op = await sess.get(GulpOperation, operation_id)
            if not op:
                raise ValueError(f"operation id={operation_id} not found.")
            ctx = await sess.get(GulpContext, context_id)
            if not ctx:
                raise ValueError(f"context id={context_id} not found.")
            
            # link
            await op.awaitable_attrs.context
            op.context.append(ctx)
            await sess.commit()
            GulpLogger.get_logger().info(f"context {context_id} added to operation {operation_id}.")

    @staticmethod
    async def remove_context(operation_id: str, context_id: str) -> None:
        """
        Remove a context from an operation.

        Args:
            operation (str): The id of the operation.
            context (str): The id of the context.
        """
        async with GulpCollab.get_instance().session() as sess:
            async with sess:
                op = await sess.get(GulpOperation, operation_id)
                if not op:
                    raise ValueError(f"operation id={operation_id} not found.")
                ctx = await sess.get(GulpContext, context_id)
                if not ctx:
                    raise ValueError(f"context id={context_id} not found.")
            
                # unlink
                await op.awaitable_attrs.context        
                op.context.remove(ctx)
                await sess.commit()
                GulpLogger.get_logger().info(f"context id={context_id} removed from operation {operation_id}.")                

    @override
    @classmethod
    async def create(
        cls,
        token: str,
        id: str,
        index: str = None,
        title: str = None,
        description: str = None,
        glyph_id: str = None,
        **kwargs,
    ) -> T:
        """
        Create a new operation object on the collab database.

        Args:
            token (str): The authentication token (must have INGEST permission).
            id (str, optional): The name of the operation (must be unique)
            index (str, optional): The gulp opensearch index to associate the operation with.
            title (str, optional): The display name for the operation. Defaults to id.
            description (str, optional): The description of the operation. Defaults to None.
            glyph_id (str, optional): The id of the glyph associated with the operation. Defaults to None.
            kwargs: Arbitrary keyword arguments.

        Returns:
            The created operation object.
        """
        args = {
            "index": index,
            "title": title or id,
            "description": description,
            "glyph_id": glyph_id,
            "context": [],
            **kwargs,
        }
        return await super()._create(
            token=token,
            id=id,
            required_permission=[GulpUserPermission.INGEST],
            **args,
        )
