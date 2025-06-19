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
import muty.string
from muty.log import MutyLogger
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship
from gulp.api.collab.context import GulpContext
from gulp.api.collab.structs import (
    COLLABTYPE_OPERATION,
    GulpCollabBase,
    GulpUserPermission,
)
from gulp.api.collab.user_group import ADMINISTRATORS_GROUP_ID
from gulp.api.collab_api import GulpCollab
from gulp.api.ws_api import WSDATA_NEW_CONTEXT
from gulp.structs import ObjectAlreadyExists

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
            ctx_id="%s_%s" % (self.id, DEFAULT_CONTEXT_ID),
            ws_id=ws_id,
            req_id=req_id,
        )

        await ctx.add_source(
            sess,
            user_id=user_id,
            name=DEFAULT_SOURCE_ID,
            src_id="%s_%s" % (self.id, DEFAULT_SOURCE_ID),
            ws_id=ws_id,
            req_id=req_id,
        )

    @staticmethod
    async def create_wrapper(
        name: str,
        user_id: str,
        index: str = None,
        description: str = None,
        glyph_id: str = None,
        create_index: bool = True,
        grant_to_users: list[str] = None,
        grant_to_groups: list[str] = None,
        set_default_grants: bool = False,
        fail_if_exists: bool = True,
        index_template: dict = None,
        ws_id: str = None,
        req_id: str = None,
        keep_data: bool = False,
    ) -> dict:
        """
        creates a new operation with the given name and index.

        if the operation already exists, it will either raise an error or delete the existing operation before creating a new one,

        Args:
            name (str): The name of the operation.
            user_id (str): The id of the user creating the operation.
            index (str, optional): The index to associate with the operation. If not provided, it will be derived from the name.
            description (str, optional): A description for the operation.
            glyph_id (str, optional): The glyph id for the operation.
            create_index (bool, optional): Whether to create the index for the operation. Defaults to True.
            grant_to_users (list[str], optional): List of user ids to grant access to the operation. Defaults to None.
            grant_to_groups (list[str], optional): List of group ids to grant access to the operation. Defaults to None.
            set_default_grants (bool, optional): Whether to set default grants for default users for the operation. Defaults to False.
            fail_if_exists (bool, optional): Whether to raise an error if the operation already exists. Defaults to True.
            index_template (dict, optional): The index template to use for creating the index. Defaults to None.
            ws_id (str, optional): The websocket id to stream creation of default context/default source to. Defaults to None.
            req_id (str, optional): The request id. Defaults to None.

        Returns:
            dict: The created operation as a dictionary.

        Raises:
            ObjectAlreadyExists: If the operation already exists and `fail_if_exists` is True.
        """
        operation_id = muty.string.ensure_no_space_no_special(name.lower())
        if not index:
            index = operation_id

            async with GulpCollab.get_instance().session() as sess:
                op: GulpOperation = await GulpOperation.get_by_id(
                    sess, operation_id, throw_if_not_found=False
                )
                if op:
                    if fail_if_exists:
                        # fail if the operation already exists
                        raise ObjectAlreadyExists(
                            f"operation_id={operation_id} already exists."
                        )
                    # delete
                    await op.delete(sess)

        if not keep_data and create_index:
            # re/create the index
            from gulp.api.opensearch_api import GulpOpenSearch

            await GulpOpenSearch.get_instance().datastream_create_from_raw_dict(
                index, index_template=index_template
            )

        # either we keep the data by do not deleting the index

        # create the operation
        d = {
            "index": index,
            "name": name,
            "description": description,
            "glyph_id": glyph_id or "box",
            "operation_data": {},
        }
        if set_default_grants:
            MutyLogger.get_instance().info(
                "setting default grants for operation=%s" % (name)
            )
            d["granted_user_ids"] = ["admin", "guest", "ingest", "power", "editor"]
            if grant_to_users:
                d["granted_user_ids"].extend(grant_to_users)
            d["granted_user_group_ids"] = [ADMINISTRATORS_GROUP_ID]
            if grant_to_groups:
                d["granted_user_group_ids"].extend(grant_to_groups)

        try:
            async with GulpCollab.get_instance().session() as sess:
                op = await GulpOperation._create_internal(
                    sess, d, obj_id=operation_id, owner_id=user_id
                )

                # create default source and context
                # await op.create_default_source_and_context(
                #     sess,
                #     user_id=user_id,
                #     ws_id=ws_id,
                #     req_id=req_id,
                # )

                # done
                return op.to_dict(exclude_none=True)

        except Exception as exx:
            if create_index:
                # fail, delete the previously created index
                await GulpOpenSearch.get_instance().datastream_delete(index)
            raise exx


    async def add_context(
        self,
        sess: AsyncSession,
        user_id: str,
        name: str,
        ws_id: str = None,
        req_id: str = None,
        ctx_id: str = None,
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
                MutyLogger.get_instance().debug(
                    f"context {name} already added to operation {self.id}."
                )
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

            MutyLogger.get_instance().debug(
                f"context {name} added to operation {self.id}: {self}"
            )
            return ctx, True
        finally:
            await GulpContext.release_advisory_lock(sess, self.id)

    @override
    async def add_user_grant(
        self, sess: AsyncSession, user_id: str, commit: bool = True
    ) -> None:
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
    async def remove_user_grant(
        self, sess: AsyncSession, user_id: str, commit: bool = True
    ) -> None:
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
    async def add_group_grant(
        self, sess: AsyncSession, group_id: str, commit: bool = True
    ) -> None:
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
    async def remove_group_grant(
        self, sess: AsyncSession, group_id: str, commit: bool = True
    ) -> None:
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
                    await src.remove_group_grant(sess, group_id, commit=False)
        await sess.commit()
