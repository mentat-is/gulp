from typing import Optional, Union, override
from sqlalchemy import ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import GulpCollabObject, GulpCollabType, T, GulpUserPermission
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.opensearch.structs import GulpAssociatedDocument, GulpDocument
from gulp.utils import GulpLogger
from gulp.api.collab_api import GulpCollab

class GulpNote(GulpCollabObject, type=GulpCollabType.NOTE):
    """
    a note in the gulp collaboration system
    """

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

    __table_args__ = (Index("idx_note_operation", "operation"),)

    @override
    @classmethod
    async def update_by_id(
        cls,
        token: str,
        id: str,
        d: dict,
        permission: list[GulpUserPermission] = [GulpUserPermission.EDIT],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
        **kwargs,
    ) -> T:
        sess = GulpCollab.get_instance().session()
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
                token=token,
                d=d,
                permission=permission,
                ws_id=ws_id,
                req_id=req_id,
                sess=sess,
                throw_if_not_found=throw_if_not_found,
                old_text=old_text,
                **kwargs,
            )

            # commit in the end
            await sess.commit()
            return obj

    @classmethod
    async def create(
        cls,
        token: str,
        id: str,
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
        """
        Create a new note object

        Args:
            token: the token of the user who is creating the object, for access check
            id: the id of the note
            operation: the operation associated with the note
            context: the context associated with the note
            log_file_path: the log file path associated with the note
            documents: the target documents
            text: the text of the note
            glyph: the glyph associated with the note
            tags: the tags associated with the note
            title: the title of the note
            private: whether the note is private
            ws_id: the websocket id
            req_id: the request id
            kwargs: additional arguments
        
        Returns:
            the created note object
        """
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
            **kwargs,
        }
        return await super()._create(
            id,
            token=token,
            ws_id=ws_id,
            req_id=req_id,
            **args,
        )
