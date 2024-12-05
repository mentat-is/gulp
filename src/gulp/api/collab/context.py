from typing import Optional, override

import muty.crypto
from muty.log import MutyLogger
import muty.string
from sqlalchemy import ForeignKey, PrimaryKeyConstraint, String, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.source import GulpSource
from gulp.api.collab.structs import GulpCollabBase, GulpCollabFilter, GulpCollabType, T


class GulpContext(GulpCollabBase, type=GulpCollabType.CONTEXT):
    """
    Represents a context object

    in gulp terms, a context is used to group a set of data coming from the same host.

    it has always associated an operation, and the tuple composed by the two is unique.
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        doc="The ID of the operation associated with the context.",
        primary_key=True,
    )
    # multiple sources can be associated with a context
    sources: Mapped[Optional[list[GulpSource]]] = relationship(
        "GulpSource",
        cascade="all, delete-orphan",
        uselist=True,
        lazy="selectin",
        foreign_keys=[GulpSource.context_id],
        doc="The source/s associated with the context.",
    )

    color: Mapped[Optional[str]] = mapped_column(
        String, default="white", doc="The color of the context."
    )

    @staticmethod
    def make_context_id_key(operation_id: str, context_name: str) -> str:
        """
        Make a key for the context_id.

        Args:
            operation_id (str): The operation id.
            context_id (str): The context name.

        Returns:
            str: The key.
        """
        return muty.crypto.hash_sha1("%s%s" % (operation_id, context_name.lower()))

    @staticmethod
    def make_source_id_key(operation_id: str, context_id: str, source_name: str) -> str:
        """
        Make a key for the source_id.

        Args:
            operation_id (str): The operation id.
            context_id (str): The context id.
            source_name (str): The source name.

        Returns:
            str: The key.
        """
        return muty.crypto.hash_sha1(
            "%s%s%s" % (operation_id, context_id, source_name.lower())
        )

    async def add_source(
        self,
        sess: AsyncSession,
        user_id: str,
        name: str,
    ) -> GulpSource:
        """
        Add a source to the context.

        Args:
            sess (AsyncSession): The session to use.
            user_id (str): The id of the user adding the source.
            name (str): The name of the source (may be file name, path, etc...)
        Returns:
            GulpSource: the source added (or already existing), eager loaded
        """
        src_id = GulpContext.make_source_id_key(self.operation_id, self.id, name)

        # acquire lock first
        lock_id = muty.crypto.hash_xxh64_int(src_id)
        await GulpCollabBase.acquire_advisory_lock(sess, lock_id)
        sess.add(self)

        # check if source exists
        src: GulpSource = await GulpSource.get_first_by_filter(
            sess,
            flt=GulpCollabFilter(
                names=[name],
                operation_ids=[self.operation_id],
                context_ids=[self.id],
            ),
            throw_if_not_found=False,
        )
        if src:
            MutyLogger.get_instance().info(
                f"source {src.id}, name={name} already exists in context {self.id}."
            )
            return src

        # create new source and link it to context
        object_data = {
            "operation_id": self.operation_id,
            "context_id": self.id,
            "name": name,
            "color": "purple",
        }
        src = await GulpSource._create(
            sess,
            object_data,
            id=src_id,
            owner_id=user_id,
        )
        await sess.refresh(self)

        MutyLogger.get_instance().info(
            f"source {src.id}, name={name} added to context {self.id}."
        )
        return src
