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

    @override
    @classmethod
    async def create(clr, *args, **kwargs):
        raise TypeError("use create_operation instead")

    @classmethod
    async def create_operation(
        cls,
        name: str,
        user_id: str,
        index: str = None,
        description: str = None,
        glyph_id: str = None,
        create_index: bool = True,
        set_default_grants: bool = False,
        fail_if_exists: bool = True,
        index_template: dict = None,
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
            set_default_grants (bool, optional): Whether to set grants for default users (guest, admin) for the operation. Defaults to False.
            fail_if_exists (bool, optional): Whether to raise an error if the operation already exists. Defaults to True.
            index_template (dict, optional): The index template to use for creating the index. Defaults to None.

        Returns:
            dict: The created operation as a dictionary.

        Raises:
            ObjectAlreadyExists: If the operation already exists and `fail_if_exists` is True.
        """
        operation_id = muty.string.ensure_no_space_no_special(name.lower())
        if not index:
            # use the operation_id as the index
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
                # delete the operation if exists
                await op.delete(sess)

            if create_index:
                # re/create the index
                from gulp.api.opensearch_api import GulpOpenSearch

                await GulpOpenSearch.get_instance().datastream_create_from_raw_dict(
                    index, index_template=index_template
                )

            # create the operation
            d = {
                "index": index,
                "operation_data": {},
            }
            granted_user_ids: list[str] = None
            granted_user_group_ids: list[str] = None
            if set_default_grants:
                MutyLogger.get_instance().info(
                    "setting default grants for operation=%s" % (name)
                )
                granted_user_ids = ["admin", "guest"]
                granted_user_group_ids = [ADMINISTRATORS_GROUP_ID]
            try:
                op = await GulpOperation.create_internal(
                    sess,
                    user_id,
                    name=name,
                    description=description,
                    glyph_id=glyph_id or "box",
                    granted_user_ids=granted_user_ids,
                    granted_user_group_ids=granted_user_group_ids,
                    **d,
                )
            except Exception as exx:
                if create_index:
                    # fail, delete the previously created index
                    await GulpOpenSearch.get_instance().datastream_delete(index)
                raise exx

            # done
            return op.to_dict(exclude_none=True)

    async def add_context(
        self,
        sess: AsyncSession,
        user_id: str,
        name: str,
        ws_id: str = None,
        req_id: str = None,
        ctx_id: str = None,
        color: str = None,
        glyph_id: str = None,
    ) -> tuple[GulpContext, bool]:
        """
        Add a context to the operation, or return the context if already added.

        Args:
            sess (AsyncSession): The session to use.
            user_id (str): The id of the user adding the context.
            name (str): The name of the context.
            ws_id (str, optional): The websocket id to stream NEW_CONTEXT to. Defaults to None.
            req_id (str, optional): The request id. Defaults to None.
            ctx_id (str, optional): The id of the context. If not provided, a new id will be generated from name.
            color (str, optional): The color of the context
            glyph_id (str, optional): The glyph id for the context. Defaults to None ("box").

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
                "color": color,
                "glyph_id": glyph_id or "box",
            }
            # pylint: disable=protected-access
            ctx = await GulpContext.create_internal(
                sess,
                object_data,
                obj_id=ctx_id,
                user_id=user_id,
                ws_data_type=WSDATA_NEW_CONTEXT if ws_id else None,
                ws_id=ws_id,
                req_id=req_id,
                private=False,
            )

            # add same grants to the context as the operation
            # TODO: at the moment, keep contexts public (ACL checks are only done operation-wide)
            # for u in self.granted_user_ids:
            #     await ctx.add_user_grant(sess, u, commit=False)
            # for g in self.granted_user_group_ids:
            #     await ctx.add_group_grant(sess, g, commit=False)
            await sess.commit()
            await sess.refresh(self)

            MutyLogger.get_instance().debug(
                f"context {name} added to operation {self.id}: {self}"
            )
            return ctx, True
        except Exception as e:
            await sess.rollback()
            raise e

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
