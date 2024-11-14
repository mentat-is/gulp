from typing import Optional, Union, override
import muty.string
from sqlalchemy import ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from gulp.api.collab.structs import (
    GulpCollabObject,
    GulpCollabType,
    T,
    GulpUserPermission,
)
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.opensearch.structs import GulpBasicDocument, GulpDocument
from gulp.api.ws_api import GulpSharedWsQueue, WsQueueDataType
from gulp.utils import GulpLogger
from gulp.api.collab_api import GulpCollab
from sqlalchemy import insert
import muty.string

class GulpNote(GulpCollabObject, type=GulpCollabType.NOTE):
    """
    a note in the gulp collaboration system
    """
    context: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The context associated with the note.",
    )
    log_file_path: Mapped[Optional[str]] = mapped_column(
        String, doc="The log file path associated with the note."
    )
    documents: Mapped[Optional[list[GulpBasicDocument]]] = mapped_column(
        JSONB, doc="One or more GulpBasicDocument associated with the note."
    )
    text: Mapped[Optional[str]] = mapped_column(String, doc="The text of the note.")

    __table_args__ = (Index("idx_note_operation", "operation"),)

    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.NOTE, **kwargs)

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

    @staticmethod
    async def bulk_create_from_documents(
        req_id: str,
        ws_id: str,
        user_id: str,
        docs: list[dict],
        title: str,
        tags: list[str] = None,
        color: str = None,
        glyph: str = None,
    ) -> int:
        """
        create a note for each document in the list, using bulk insert

        Args:
            req_id(str): the request id
            ws_id(str): the websocket id
            user_id(str): the requestor user id
            docs(list[dict]): the list of GulpDocument dictionaries to be added to the note
            title(str): the title of the note
            tags(list[str], optional): the tags of the note: if not set, ["auto"] is automatically set here.
            color(str, optional): the color of the note
            glyph(str, optional): the id of the glyph of the note

        Returns:
            the number of notes created

        """
        default_tags = ["auto"]
        if tags:
            # add the default tags if not already present
            tags = list(default_tags.union(tag.lower() for tag in tags))
        else:
            tags = list(default_tags)

        async with GulpCollab.get_instance().session() as sess:
            color = color or "yellow"
            
            # create a note for each document
            notes = []
            for doc in docs:
                associated_doc = GulpBasicDocument(
                    id=doc.get('_id'),
                    timestamp=doc.get('@timestamp'),
                    gulp_timestamp=doc.get('gulp.timestamp'),
                    invalid_timestamp=doc.get('gulp.invalid.timestamp', False),
                    operation=doc.get('gulp.operation'),
                    context=doc.get('gulp.context'),
                    log_file_path=doc.get('log.file.path'),
                )
                args = {
                    "operation": associated_doc.operation,
                    "context": associated_doc.context,
                    "log_file_path": associated_doc.log_file_path,
                    "documents": [associated_doc.model_dump(by_alias=True, exclude_none=True, exclude_defaults=True)],
                    "glyph": glyph,
                    "color": color,
                    "title": title,
                    "tags": tags,
                }                
                note = GulpNote(id=None, owner=user_id, **args)
                notes.append(note.to_dict(exclude_none=True))

            # bulk insert
            GulpLogger.get_logger().debug("creating %d notes" % len(notes))
            await sess.execute(insert(GulpNote).values(notes))
            await sess.commit()

            GulpLogger.get_logger().info(
                "created %d notes" % len(notes)
            )

            if ws_id:
                # send over the websocket
                GulpLogger.get_logger().debug("sending %d notes on the websocket %s " % (len(notes), ws_id))
                
                # operation is always the same
                operation = notes[0].get('operation')
                await GulpSharedWsQueue.get_instance().put(
                    WsQueueDataType.COLLAB_UPDATE,
                    ws_id=ws_id,
                    user_id=user_id,
                    operation = operation,
                    req_id=req_id,
                    data=notes,
                )
                GulpLogger.get_logger().debug("sent %d notes on the websocket %s " % (len(notes), ws_id)) 

            return len(notes)

    @classmethod
    async def create(
        cls,
        token: str,
        title: str,
        operation: str,
        context: str,
        log_file_path: str,
        documents: list[GulpBasicDocument],
        text: str,
        description: str = None,
        glyph: str = None,
        color: str = None,
        tags: list[str] = None,
        private: bool = False,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> T:
        """
        Create a new note object on the collab database.

        Args:
            token(str): the token of the user creating the object, for access check
            title(str): the title of the note
            operation(str): the id of the operation associated with the note
            context(str): the id of the context associated with the note
            log_file_path(str): the log file path (or source) associated with the note
            documents(list[GulpBasicDocument]): the list of documents associated with the note
            text(str): the text of the note
            description(str, optional): the description of the note
            glyph(str, optional): id of the glyph associated with the note
            color(str, optional): the color associated with the note (default: yellow)
            tags(list[str], optional): the tags associated with the note
            private(bool, optional): whether the note is private (default: False)
            ws_id(str, optional): the websocket id
            req_id(str, optional): the request id
        Returns:
            the created note object
        """
        args = {
            "operation": operation,
            "context": context,
            "log_file_path": log_file_path,
            "documents": documents,
            "glyph": glyph,
            "color": color or "yellow",
            "tags": tags,
            "title": title,
            "text": text,
            "description": description,
            "private": private,
            **kwargs,
        }
        # id is automatically generated
        return await super()._create(
            token=token,
            ws_id=ws_id,
            req_id=req_id,
            **args,
        )
