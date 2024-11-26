from typing import Optional, Union, override

import muty.string
from muty.log import MutyLogger
from sqlalchemy import ForeignKey, Index, String, insert
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import (
    GulpCollabBase,
    GulpCollabObject,
    GulpCollabType,
    GulpUserPermission,
    T,
)
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.structs import GulpBasicDocument, GulpDocument
from gulp.api.ws_api import GulpSharedWsQueue, GulpWsQueueDataType


class GulpNote(GulpCollabObject, type=GulpCollabType.NOTE):
    """
    a note in the gulp collaboration system
    """

    context_id: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The context associated with the note.",
    )
    source_id: Mapped[Optional[str]] = mapped_column(
        String, doc="The log file path (source) associated with the note."
    )
    docs: Mapped[Optional[list[GulpBasicDocument]]] = mapped_column(
        JSONB, doc="One or more GulpBasicDocument associated with the note."
    )
    text: Mapped[Optional[str]] = mapped_column(String, doc="The text of the note.")

    __table_args__ = (Index("idx_note_operation", "operation_id"),)

    @override
    async def update(
        self,
        sess: AsyncSession,
        d: dict,
        ws_id: str = None,
        user_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> None:
        # save old text
        old_text = self.text
        await super().update(
            sess,
            d,
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
            old_text=old_text,
            **kwargs,
        )

    @staticmethod
    async def bulk_create_from_documents(
        sess: AsyncSession,
        user_id: str,
        ws_id: str,
        req_id: str,
        docs: list[dict],
        name: str,
        tags: list[str] = None,
        color: str = None,
        glyph_id: str = None,
    ) -> int:
        """
        creates a note for each document in the list, using bulk insert

        Args:
            sess (AsyncSession): the database session
            user_id (str): the user id creating the notes
            ws_id (str): the websocket id
            req_id (str): the request id
            docs (list[dict]): the list of documents to create notes for
            name (str): the name of the notes
            tags (list[str], optional): the tags to add to the notes. Defaults to None.
            color (str, optional): the color of the notes. Defaults to "yellow".
            glyph_id (str, optional): the glyph id of the notes. Defaults to None.

        Returns:
            the number of notes created

        """
        default_tags = ["auto"]
        if tags:
            # add the default tags if not already present
            tags = list(default_tags.union(tag.lower() for tag in tags))
        else:
            tags = list(default_tags)

        color = color or "yellow"

        # create a note for each document
        notes = []
        for doc in docs:
            associated_doc = GulpBasicDocument(
                id=doc.get("_id"),
                timestamp=doc.get("@timestamp"),
                gulp_timestamp=doc.get("gulp.timestamp"),
                invalid_timestamp=doc.get("gulp.timestamp_invalid", False),
                operation_id=doc.get("gulp.operation_id"),
                context_id=doc.get("gulp.context_id"),
                source_id=doc.get("gulp.source_id"),
            )
            object_data = {
                "operation_id": associated_doc.operation_id,
                "context_id": associated_doc.context_id,
                "source_id": associated_doc.source_id,
                "docs": [
                    associated_doc.model_dump(
                        by_alias=True, exclude_none=True, exclude_defaults=True
                    )
                ],
                "glyph_id": glyph_id,
                "color": color,
                "name": name,
                "tags": tags,
            }
            note_dict = GulpCollabBase.build_object_dict(
                object_data=object_data,
                type=GulpCollabType.NOTE,
                user_id=user_id,
            )
            notes.append(note_dict)

            # bulk insert
            MutyLogger.get_instance().debug("creating %d notes" % len(notes))
            await sess.execute(insert(GulpNote).values(notes))
            await sess.commit()

            MutyLogger.get_instance().info("created %d notes" % len(notes))

            # send over the websocket
            MutyLogger.get_instance().debug(
                "sending %d notes on the websocket %s " % (len(notes), ws_id)
            )

            # operation is always the same
            operation = notes[0].get("operation")
            GulpSharedWsQueue.get_instance().put(
                GulpWsQueueDataType.COLLAB_UPDATE,
                ws_id=ws_id,
                user_id=user_id,
                operation_id=operation,
                req_id=req_id,
                data=notes,
            )
            MutyLogger.get_instance().debug(
                "sent %d notes on the websocket %s " % (len(notes), ws_id)
            )

            return len(notes)

    @classmethod
    async def create(
        cls,
        sess: AsyncSession,
        user_id: str,
        operation_id: str,
        ws_id: str,
        req_id: str,
        name: str,
        context_id: str,
        source_id: str,
        documents: list[GulpBasicDocument],
        text: str,
        description: str = None,
        glyph_id: str = None,
        color: str = None,
        tags: list[str] = None,
        private: bool = False,
    ) -> T:
        """
        Create a new note object on the collab database.

        Args:
            sess (AsyncSession): The database session to use.
            user_id (str): The id of the user creating the note.
            operation_id (str): The id of the operation associated with the note.
            ws_id (str): The id of the workspace associated with the note.
            req_id (str): The id of the request associated with the note.
            name (str): The display name of the note.
            context_id (str): The id of the context associated with the note.
            source_id (str): The id of the source associated with the note.
            documents (list[GulpBasicDocument]): The list of GulpBasicDocument associated with the note.
            text (str): The text of the note.
            description (str, optional): The description of the note. Defaults to None.
            glyph_id (str, optional): The id of the glyph associated with the note. Defaults to None.
            color (str, optional): The color of the note. Defaults to yellow.
            tags (list[str], optional): The tags associated with the note. Defaults to None.
            private (bool, optional): Whether the note is private or not. Defaults to False.
        Returns:
            the created note object
        """
        object_data = {
            "operation_id": operation_id,
            "context_id": context_id,
            "source_id": source_id,
            "documents": documents,
            "glyph_id": glyph_id,
            "color": color or "yellow",
            "tags": tags,
            "name": name,
            "text": text,
            "description": description,
            "private": private,
        }

        return await super()._create(
            sess,
            object_data,
            operation_id=operation_id,
            user_id=user_id,
            ws_id=ws_id,
            req_id=req_id,
            private=private,
        )
