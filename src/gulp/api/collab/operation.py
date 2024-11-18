from typing import Optional, Union, override

from muty.log import MutyLogger
from sqlalchemy import ForeignKey, String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.context import GulpContext
from gulp.api.collab.source import GulpSource
from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabFilter,
    GulpCollabType,
    GulpUserPermission,
    T,
)
from gulp.api.collab_api import GulpCollab


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
    sources: Mapped[Optional[list[GulpSource]]] = relationship(
        "GulpSource",
        back_populates="operation",
        cascade="all, delete-orphan",
        doc="The source/s associated with the operation.",
    )

    # multiple contexts can be associated with an operation
    contexts: Mapped[Optional[list[GulpContext]]] = relationship(
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

    async def add_context(
        self,
        context_id: str,
        create_if_not_exist: bool = True,
        sess: AsyncSession = None,
    ) -> bool:
        """
        Add a context to the operation.

        Args:
            context_id (str): The id of the context.
            create_if_not_exist (bool, optional): Whether to create the context if not found. Defaults to True.
            sess (AsyncSession, optional): The session to use. Defaults to None.
        Returns:
            bool: True if the context was added, False otherwise.
        """
        if not sess:
            sess = GulpCollab.get_instance().session()
        async with sess:
            # check if context exists
            ctx: GulpContext = await GulpContext.get_one(
                GulpCollabFilter(id=[context_id], operation_id=[self.id]),
                sess=sess,
                ensure_eager_load=False,
            )
            if not ctx:
                # create context if not found
                if not create_if_not_exist:
                    raise ValueError(
                        f"context id={context_id}, {self.id} not found."
                    )
                ctx = await GulpContext.create(
                    token=None, id=None, operation_id=self.id
                )
                sess.add(ctx)
                await sess.commit()

            # check if context is already added
            await self.awaitable_attrs.contexts
            if ctx not in self.contexts:
                self.contexts.append(ctx)
                sess.add(self)
                await sess.commit()
                MutyLogger.get_instance().info(
                    f"context {context_id} added to operation {self.id}."
                )
                return True
            MutyLogger.get_instance().warning(
                f"context {context_id} already added to operation {self.id}."
            )
            return False

    @staticmethod
    async def add_context_to_id(
        operation_id: str, context_id: str, create_if_not_exist: bool = True
    ) -> bool:
        """
        Add a context to an operation.

        Args:
            operation_id (str): The id of the operation.
            context_id (str): The id of the context.
            create_if_not_exist (bool, optional): Whether to create the context if not found. Defaults to True.

        Returns:
            bool: True if the context was added, False otherwise
        """
        async with GulpCollab.get_instance().session() as sess:
            op = await sess.get(GulpOperation, operation_id)
            if not op:
                raise ValueError(f"operation id={operation_id} not found.")
            return await op.add_context(
                context_id=context_id,
                create_if_not_exist=create_if_not_exist,
                sess=sess,
            )

    async def remove_context(self, context_id: str, sess: AsyncSession=None) -> bool:
        """
        Remove a context from the operation.

        Args:
            context_id (str): The id of the context.
            sess (AsyncSession, optional): The session to use. Defaults to None.

        Returns:
            bool: True if the context was removed, False otherwise.
        """
        if not sess:
            sess = GulpCollab.get_instance().session()
        async with sess:
            ctx: GulpContext = await GulpContext.get_one(
                GulpCollabFilter(id=[context_id], operation_id=[self.id]),
                sess=sess,
                ensure_eager_load=False,
            )
            if not ctx:
                raise ValueError(f"context id={context_id}, {self.id} not found.")

            # unlink
            await self.awaitable_attrs.contexts
            if ctx in self.contexts:
                self.contexts.remove(ctx)
                sess.add(self)
                await sess.commit()
                MutyLogger.get_instance().info(
                    f"context id={context_id} removed from operation {self.id}."
                )
                return True

            MutyLogger.get_instance().warning(
                f"context id={context_id} not found in operation {self.id}."
            )
            return False

    @staticmethod
    async def remove_context_from_id(operation_id: str, context_id: str) -> bool:
        """
        Remove a context from an operation.

        Args:
            operation (str): The id of the operation.
            context (str): The id of the context.

        Returns:
            bool: True if the context was removed, False otherwise.
        """
        async with GulpCollab.get_instance().session() as sess:
            op = await sess.get(GulpOperation, operation_id)
            if not op:
                raise ValueError(f"operation id={operation_id} not found.")
            return await op.remove_context(context_id=context_id, sess=sess)

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
