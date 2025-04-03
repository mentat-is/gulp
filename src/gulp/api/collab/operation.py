"""
This module defines the `GulpOperation` class, which represents operations in the gulp system.

An operation is a core entity in the gulp collaboration framework that manages contexts and
their associated sources. Operations provide a hierarchical structure to organize data and
control access permissions across multiple contexts.

Key features:
- Each operation is associated with an opensearch index
- Operations can contain multiple contexts (GulpContext objects)
- Operations enforce permission inheritance to all child contexts and sources
- Support for adding and managing user and group grants throughout the hierarchy

The module provides methods for creating, retrieving, and manipulating operations,
including the ability to add contexts and manage access permissions.
"""

from typing import Optional, override

import muty.crypto
from muty.log import MutyLogger
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.context import GulpContext
from gulp.api.collab.structs import GulpCollabBase, GulpCollabType
from gulp.api.ws_api import GulpWsQueueDataType


class GulpOperation(GulpCollabBase, type=GulpCollabType.OPERATION):
    """
    Represents an operation in the gulp system.
    """

    index: Mapped[str] = mapped_column(
        String,
        doc="The gulp opensearch index to associate the operation with.",
    )
    # multiple contexts can be associated with an operation
    contexts: Mapped[Optional[list[GulpContext]]] = relationship(
        "GulpContext",
        cascade="all, delete-orphan",
        lazy="selectin",
        uselist=True,
        doc="The context/s associated with the operation.",
    )
    operation_data: Mapped[Optional[dict]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        default_factory=dict,
        doc="Arbitrary operation data.",
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["index"] = "operation_index"
        d["operation_data"] = {"key": "value"}
        return d

    @override
    # pylint: disable=arguments-differ
    def to_dict(self, nested=False, **kwargs) -> dict:
        d = super().to_dict(nested=nested, **kwargs)
        if nested:
            # add nested contexts
            d["contexts"] = (
                [ctx.to_dict(nested=True) for ctx in self.contexts]
                if self.contexts
                else []
            )
        return d

    async def add_context(
        self,
        sess: AsyncSession,
        user_id: str,
        name: str,
        ws_id: str = None,
        req_id: str = None,
    ) -> tuple[GulpContext, bool]:
        """
        Add a context to the operation, or return the context if already added.

        Args:
            sess (AsyncSession): The session to use.
            user_id (str): The id of the user adding the context.
            name (str): The name of the context.
            ws_id (str, optional): The websocket id to stream NEW_CONTEXT to. Defaults to None.
            req_id (str, optional): The request id. Defaults to None.

        Returns:
            tuple(GulpContext, bool): The context added (or already existing) and a flag indicating if the context was added
        """
        obj_id = GulpContext.make_context_id_key(self.id, name)

        try:
            await GulpContext.acquire_advisory_lock(sess, obj_id)

            # check if context exists
            ctx: GulpContext = await GulpContext.get_by_id(
                sess, obj_id=obj_id, throw_if_not_found=False
            )
            if ctx:
                MutyLogger.get_instance().debug(
                    f"context {name} already added to operation {self.id}."
                )
                return ctx, False

            # create new context and link it to operation
            object_data = {
                "operation_id": self.id,
                "name": name,
                "color": "white",
            }
            # pylint: disable=protected-access
            ctx = await GulpContext._create_internal(
                sess,
                object_data,
                obj_id=obj_id,
                owner_id=user_id,
                ws_queue_datatype=GulpWsQueueDataType.NEW_CONTEXT if ws_id else None,
                ws_id=ws_id,
                req_id=req_id,
            )

            # add same grants to the context as the operation
            for u in self.granted_user_ids:
                await ctx.add_user_grant(sess, u)
            for g in self.granted_user_group_ids:
                await ctx.add_group_grant(sess, g)

            await sess.refresh(self)            
            MutyLogger.get_instance().info(f"context {name} added to operation {self.id}.")
            return ctx, True
        finally:
            await GulpContext.release_advisory_lock(sess, obj_id)

    @override
    async def add_user_grant(self, sess: AsyncSession, user_id: str) -> None:
        # add grant to the operation
        await super().add_user_grant(sess, user_id)
        if not self.contexts:
            return

        # add grant to all contexts and sources
        for ctx in self.contexts:
            await ctx.add_user_grant(sess, user_id)
            if ctx.sources:
                for src in ctx.sources:
                    await src.add_user_grant(sess, user_id)
        # await sess.refresh(self)

    @override
    async def remove_user_grant(self, sess: AsyncSession, user_id: str) -> None:
        # remove grant from the operation
        await super().remove_user_grant(sess, user_id)
        if not self.contexts:
            return

        # remove grant from all contexts and sources
        for ctx in self.contexts:
            await ctx.remove_user_grant(sess, user_id)
            if ctx.sources:
                for src in ctx.sources:
                    await src.remove_user_grant(sess, user_id)

    @override
    async def add_group_grant(self, sess: AsyncSession, group_id: str):
        # add grant to the operation
        await super().add_group_grant(sess, group_id)
        if not self.contexts:
            return

        # add grant to all contexts and sources
        for ctx in self.contexts:
            await ctx.add_group_grant(sess, group_id)
            if ctx.sources:
                for src in ctx.sources:
                    await src.add_group_grant(sess, group_id)

    @override
    async def remove_group_grant(self, sess: AsyncSession, group_id: str):
        # remove grant from the operation
        await super().remove_group_grant(sess, group_id)
        if not self.contexts:
            return

        # remove grant from all contexts and sources
        for ctx in self.contexts:
            await ctx.remove_group_grant(sess, group_id)
            if ctx.sources:
                for src in ctx.sources:
                    await src.remove_group_grant(sess, group_id)
