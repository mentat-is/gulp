from typing import Optional, Union, override
from sqlalchemy import ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType, T
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab_api import session
from gulp.api.elastic.structs import GulpAssociatedDocument, GulpDocument
from gulp.utils import logger


class GulpNote(GulpCollabObject):
    """
    a note in the gulp collaboration system
    """

    __tablename__ = GulpCollabType.NOTE.value

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
        "polymorphic_identity": GulpCollabType.NOTE.value,
    }
    __table_args__ = (Index("idx_note_operation", "operation"),)

    @override
    @classmethod
    async def update_by_id(
        cls,
        id: str,
        d: dict,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
    ) -> T:
        sess = await session()
        async with sess:
            # get note first
            note: GulpNote = await cls.get_one_by_id(
                id,
                ws_id=ws_id,
                req_id=req_id,
                sess=sess,
                throw_if_not_found=throw_if_not_found,
            )

            # save old text
            old_text = note.text

            # update note, websocket will also receive the old text
            obj = await note.update(
                d,
                ws_id=ws_id,
                req_id=req_id,
                sess=sess,
                throw_if_not_found=throw_if_not_found,
                old_text=old_text,
            )

            # commit in the end
            await sess.commit()
            return obj

    @override
    @classmethod
    async def create(
        cls,
        id: str,
        owner: str,
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
            GulpCollabType.NOTE,
            owner,
            ws_id,
            req_id,
            **args,
        )
