import json
from typing import Optional, Union, override

import muty.crypto
from muty.log import MutyLogger
from sqlalchemy import ForeignKey, String, text
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

    name: Mapped[Optional[str]] = mapped_column(
        String, doc="The name of the operation."
    )

    index: Mapped[Optional[str]] = mapped_column(
        String,
        doc="The gulp opensearch index to associate the operation with.",
    )
    description: Mapped[Optional[str]] = mapped_column(
        String, doc="The description of the operation."
    )

    # multiple contexts can be associated with an operation
    contexts: Mapped[Optional[list[GulpContext]]] = relationship(
        "GulpContext",
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
        sess: AsyncSession = None,
    ) -> GulpContext:
        """
        Add a context to the operation, or return the context if already added.

        Args:
            context_id (str): The id of the context.
            sess (AsyncSession, optional): The session to use. Defaults to None.
        Returns:
            GulpContext: the context added (or already existing), eager loaded
        """

        async def _add_context_internal():
            # check if context exists
            ctx: GulpContext = await GulpContext.get_one(
                GulpCollabFilter(id=[context_id], operation_id=[self.id]),
                throw_if_not_found=False,
                sess=sess,
                ensure_eager_load=False,
            )
            if ctx:
                MutyLogger.get_instance().info(
                    f"context {context_id} already added to operation {self.id}."
                )
                return ctx

            # create new context and link it to operation
            # we are calling this internally only, so we can use token=None to skip
            # token check.
            ctx = await GulpContext.create(
                token=None, id=context_id, operation_id=self.id
            )
            sess.add(ctx)
            await sess.commit()
            await sess.refresh(ctx)
            MutyLogger.get_instance().info(
                f"context {context_id} added to operation {self.id}."
            )
            return ctx

        created = False
        if not sess:
            sess = GulpCollab.get_instance().session()
            created = True

        # acquire lock first
        lock_id = muty.crypto.hash_xxh64_int(f"{self.id}-{context_id}")
        await sess.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"), {"lock_id": lock_id}
        )
        sess.add(self)

        if created:
            async with sess:
                return await _add_context_internal()
        else:
            return await _add_context_internal()

    @staticmethod
    async def add_context_to_id(operation_id: str, context_id: str) -> GulpContext:
        """
        Add a context to an operation.

        Args:
            operation_id (str): The id of the operation.
            context_id (str): The id of the context.

        Returns:
            GulContext: The context added (or already existing), eager loaded.
        """
        async with GulpCollab.get_instance().session() as sess:
            op = await sess.get(GulpOperation, operation_id)
            if not op:
                raise ValueError(f"operation id={operation_id} not found.")
            return await op.add_context(context_id=context_id, sess=sess)

    @override
    @classmethod
    async def create(
        cls,
        token: str,
        id: str,
        index: str = None,
        name: str = None,
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
            name (str, optional): The display name for the operation. Defaults to id.
            description (str, optional): The description of the operation. Defaults to None.
            glyph_id (str, optional): The id of the glyph associated with the operation. Defaults to None.
            kwargs: Arbitrary keyword arguments.

        Returns:
            The created operation object.
        """
        args = {
            "index": index,
            "name": name or id,
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
