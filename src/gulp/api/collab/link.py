from typing import Union, override
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType, T
from gulp.api.elastic.structs import GulpAssociatedDocument, GulpDocument
from gulp.utils import logger


class GulpLink(GulpCollabObject):
    """
    a link in the gulp collaboration system
    """

    __tablename__ = GulpCollabType.LINK.value

    # the source event
    document_from: Mapped[str] = mapped_column(String, doc="The source document.")
    # target events
    documents: Mapped[list[GulpAssociatedDocument]] = mapped_column(
        JSONB, doc="One or more target documents."
    )

    __mapper_args__ = {
        "polymorphic_identity": GulpCollabType.LINK.value,
    }

    @override
    @classmethod
    async def create(
        cls,
        id: str,
        owner: str,
        operation: str,
        document_from: str,
        documents: list[GulpAssociatedDocument],
        glyph: str = None,
        tags: list[str] = None,
        title: str = None,
        private: bool = False,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        commit: bool = True,
        **kwargs,
    ) -> T:
        args = {
            "operation": operation,
            "document_from": document_from,
            "documents": documents,
            "glyph": glyph,
            "tags": tags,
            "title": title,
            "private": private,
        }
        return await super()._create(
            id,
            owner,
            ws_id,
            req_id,
            sess,
            commit,
            **args,
        )
