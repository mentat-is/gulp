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

from muty.log import MutyLogger
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship
from gulp.api.collab.context import GulpContext
from gulp.api.collab.structs import COLLABTYPE_OPERATION, GulpCollabBase, GulpUserPermission
from gulp.api.collab_api import GulpCollab
from gulp.api.ws_api import WSDATA_NEW_CONTEXT

# every operation have a default context and source which are used to associate data when no specific context or source is provided.
DEFAULT_CONTEXT_ID = "default_context"
DEFAULT_SOURCE_ID = "default_source"

class GulpOperation(GulpCollabBase, type=COLLABTYPE_OPERATION):
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
    
    async def create_default_source_and_context(
        self,
        sess: AsyncSession,
        user_id: str,
        ws_id: str = None,
        req_id: str = None,
    ) -> None:
        """
        Create the default context and source for the operation.

        Args:
            sess (AsyncSession): The session to use.
            user_id (str): The id of the user creating the context and source.
            ws_id (str, optional): The websocket id to stream NEW_CONTEXT to. Defaults to None.
            req_id (str, optional): The request id. Defaults to None.
        """

        # add default context to the operation
        ctx: GulpContext
        ctx, _ = await self.add_context(
            sess,
            user_id=user_id,
            name=DEFAULT_CONTEXT_ID,
            ctx_id='%s_%s' % (self.id, DEFAULT_CONTEXT_ID),
            ws_id=ws_id,
            req_id=req_id,
        )

        await ctx.add_source(
            sess,
            user_id=user_id,
            name=DEFAULT_SOURCE_ID,
            src_id='%s_%s' % (self.id, DEFAULT_SOURCE_ID),
            ws_id=ws_id,
            req_id=req_id,
        )

    @classmethod
    @override
    async def create(
        cls,
        token: str,
        ws_id: str,
        req_id: str,
        object_data: dict,
        permission: list[GulpUserPermission] = None,
        obj_id: str = None,
        private: bool = True,
        operation_id: str = None,
        **kwargs,
    ) -> dict:
        # create operation
        d: dict = await super().create(
            token=token,
            ws_id=ws_id,
            req_id=req_id,
            object_data=object_data,
            permission=permission,
            obj_id=obj_id,
            private=private,
            operation_id=operation_id,
            **kwargs,
        )

        # create default source and context
        async with GulpCollab.get_instance().session() as sess:
            op: GulpOperation = await GulpOperation.get_by_id(
                sess, obj_id=d["id"]
            )
            await op.create_default_source_and_context(
                sess,
                user_id=d["owner_user_id"],
                ws_id=ws_id,
                req_id=req_id,
            )

        return d


    async def add_context(
        self,
        sess: AsyncSession,
        user_id: str,
        name: str,
        ws_id: str = None,
        req_id: str = None,
        ctx_id: str=None,
        color: str = None,
    ) -> tuple[GulpContext, bool]:
        """
        Add a context to the operation, or return the context if already added.

        Args:
            sess (AsyncSession): The session to use.
            user_id (str): The id of the user adding the context.
            name (str): The name of the context.
            ws_id (str, optional): The websocket id to stream NEW_CONTEXT to. Defaults to None.
            req_id (str, optional): The request id. Defaults to None.
            src_id (str, optional): The id of the context. If not provided, a new id will be generated.
            color (str, optional): The color of the context. Defaults to "purple".

        Returns:
            tuple(GulpContext, bool): The context added (or already existing) and a flag indicating if the context was added
        """
        if not ctx_id:
            ctx_id = GulpContext.make_context_id_key(self.id, name)

        try:
            await GulpContext.acquire_advisory_lock(sess, self.id)

            # check if context exists
            ctx: GulpContext = await GulpContext.get_by_id(
                sess, obj_id=ctx_id, throw_if_not_found=False
            )
            if ctx:
                MutyLogger.get_instance().debug(f"context {name} already added to operation {self.id}.")
                return ctx, False
            
            # MutyLogger.get_instance().warning("creating new context: %s, id=%s", name, obj_id)

            # create new context and link it to operation
            object_data = {
                "operation_id": self.id,
                "name": name,
                "color": color or "white",
                "glyph_id": "box",

            }
            # pylint: disable=protected-access
            ctx = await GulpContext._create_internal(
                sess,
                object_data,
                obj_id=ctx_id,
                owner_id=user_id,
                ws_queue_datatype=WSDATA_NEW_CONTEXT if ws_id else None,
                ws_id=ws_id,
                req_id=req_id,
                commit=False,
            )

            # add same grants to the context as the operation
            for u in self.granted_user_ids:
                await ctx.add_user_grant(sess, u, commit=False)
            for g in self.granted_user_group_ids:
                await ctx.add_group_grant(sess, g, commit=False)

            # finally commit the session
            await sess.commit()
            await sess.refresh(self)

            MutyLogger.get_instance().debug(f"context {name} added to operation {self.id}: {self}")
            return ctx, True
        finally:
            await GulpContext.release_advisory_lock(sess, self.id)

    @override
    async def add_user_grant(self, sess: AsyncSession, user_id: str, commit: bool=True) -> None:
        # add grant to the operation
        await super().add_user_grant(sess, user_id, commit=False)
        if not self.contexts:
            await sess.commit()
            return

        # add grant to all contexts and sources
        for ctx in self.contexts:
            await ctx.add_user_grant(sess, user_id, commit=False)
            if ctx.sources:
                for src in ctx.sources:
                    await src.add_user_grant(sess, user_id, commit=False)
        await sess.commit()

    @override
    async def remove_user_grant(self, sess: AsyncSession, user_id: str, commit: bool=True) -> None:
        # remove grant from the operation
        await super().remove_user_grant(sess, user_id, commit=False)
        if not self.contexts:
            await sess.commit()
            return

        # remove grant from all contexts and sources
        for ctx in self.contexts:
            await ctx.remove_user_grant(sess, user_id, commit=False)
            if ctx.sources:
                for src in ctx.sources:
                    await src.remove_user_grant(sess, user_id, commit=False)
        await sess.commit()

    @override
    async def add_group_grant(self, sess: AsyncSession, group_id: str, commit: bool=True) -> None:
        # add grant to the operation
        await super().add_group_grant(sess, group_id, commit=False)
        if not self.contexts:
            await sess.commit()
            return

        # add grant to all contexts and sources
        for ctx in self.contexts:
            await ctx.add_group_grant(sess, group_id, commit=False)
            if ctx.sources:
                for src in ctx.sources:
                    await src.add_group_grant(sess, group_id, commit=False)

        await sess.commit()

    @override
    async def remove_group_grant(self, sess: AsyncSession, group_id: str, commit: bool=True) -> None:
        # remove grant from the operation
        await super().remove_group_grant(sess, group_id, commit=False)
        if not self.contexts:
            await sess.commit()
            return

        # remove grant from all contexts and sources
        for ctx in self.contexts:
            await ctx.remove_group_grant(sess, group_id, commit=False)
            if ctx.sources:
                for src in ctx.sources:
                    await src.remove_group_grant(sess, group_id,commit=False)
        await sess.commit()
