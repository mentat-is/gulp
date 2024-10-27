from typing import Optional, override
from sqlalchemy import ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType, T
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.elastic.structs import GulpAssociatedDocument, GulpDocument
from gulp.utils import logger


class GulpNote(GulpCollabObject):
    """
    a note in the gulp collaboration system
    """

    __tablename__ = GulpCollabType.NOTE

    context: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The context associated with the note.",
    )
    log_file_path: Mapped[str] = mapped_column(
        String, doc="The log file path associated with the note."
    )
    documents: Mapped[Optional[list[GulpAssociatedDocument]]] = mapped_column(
        JSONB, doc="One or more documents associated with the note."
    )
    text: Mapped[str] = mapped_column(String, doc="The text of the note.")

    __mapper_args__ = {
        f"polymorphic_identity": {GulpCollabType.NOTE},
    }

    @override
    @classmethod
    async def create(
        cls,
        id: str,
        user: str | "GulpUser",
        operation: str,
        context: str,
        log_file_path: str,
        documents: list[GulpAssociatedDocument],
        text: str,
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
            "context": context,
            "log_file_path": log_file_path,
            "documents": documents,
            "glyph": glyph,
            "tags": tags,
            "title": title,
            "text": text,
            "private": private,
        }
        return await super()._create(
            id,
            user,
            ws_id,
            req_id,
            sess,
            commit,
            **args,
        )
