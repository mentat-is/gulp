from typing import Optional, override

import muty.crypto
from muty.log import MutyLogger
import muty.string
from sqlalchemy import ForeignKey, String, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.context import GulpContext
from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T


class GulpOperation(GulpCollabBase, type=GulpCollabType.OPERATION):
    """
    Represents an operation in the gulp system.
    """

    name: Mapped[str] = mapped_column(String, doc="The name of the operation.")

    index: Mapped[str] = mapped_column(
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
        lazy="selectin",
        uselist=True,
        doc="The context/s associated with the operation.",
    )

    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        doc="The glyph associated with the operation.",
    )

    @override
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
        context_id: str,
    ) -> GulpContext:
        """
        Add a context to the operation, or return the context if already added.

        Args:
            sess (AsyncSession): The session to use.
            user_id (str): The id of the user adding the context.
            context_id (str): The id of the context.
        Returns:
            GulpContext: the context added (or already existing), eager loaded
        """
        # acquire lock first
        lock_id = muty.crypto.hash_xxh64_int(f"{self.id}-{context_id}")
        await sess.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"), {"lock_id": lock_id}
        )

        # check if context exists
        ctx: GulpContext = await GulpContext.get_by_id(
            sess, id=context_id, throw_if_not_found=False
        )
        if ctx:
            MutyLogger.get_instance().info(
                f"context {context_id} already added to operation {self.id}."
            )
            return ctx

        # create new context and link it to operation
        ctx = await GulpContext.create(
            sess,
            user_id,
            operation_id=self.id,
            name=context_id,
        )
        await sess.refresh(self)
        MutyLogger.get_instance().info(
            f"context {context_id} added to operation {self.id}."
        )
        return ctx

    @override
    @classmethod
    async def create(
        cls,
        sess: AsyncSession,
        user_id: str,
        name: str,
        index: str,
        description: str = None,
        glyph_id: str = None,
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
        object_data = {
            "index": index,
            "name": name,
            "description": description,
            "glyph_id": glyph_id,
        }

        return await super()._create(
            sess,
            object_data,
            id=muty.string.ensure_no_space_no_special(name),
            owner_id=user_id,
        )
