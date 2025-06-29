"""
This module defines the GulpNote class, which represents a note in the gulp collaboration system.

The GulpNote class inherits from GulpCollabObject and implements note-specific functionality.
It includes methods for creating and updating notes, particularly in bulk operations.

Classes:
    GulpNoteEdit: Represents an edit made to a note, tracking the editor, timestamp, and text.
    GulpNote: Represents a note in the collaboration system with methods for creation and manipulation.

Main functionalities:
- Creating notes from documents
- Bulk creation of notes
- Bulk updating of note tags
- Associating notes with contexts, sources, and documents
"""

import json
from typing import List, Optional, override

from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
from opensearchpy import Field
import muty.crypto
from pydantic import BaseModel, ConfigDict
from sqlalchemy import ARRAY, BIGINT, ForeignKey, Index, Insert, String
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.structs import COLLABTYPE_NOTE, GulpCollabFilter, GulpCollabObject
from gulp.api.opensearch.structs import GulpBasicDocument
from gulp.api.ws_api import (
    WSDATA_COLLAB_UPDATE,
    GulpCollabCreateUpdatePacket,
    GulpWsSharedQueue,
)


class GulpNoteEdit(BaseModel):
    """
    a note edit represent an edit made to a note
    it tracks the editor, timestamp and text
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "editor_id": "editor_id",
                    "timestamp": 1234567890,
                    "text": "previous note text",
                }
            ]
        }
    )

    user_id: str = Field(..., description="The user ID of the editor.")
    timestamp: int = Field(
        ..., description="The timestamp of the edit, in milliseconds from unix epoch."
    )
    text: str = Field(..., description="The note text.")


class GulpNote(GulpCollabObject, type=COLLABTYPE_NOTE):
    """
    a note in the gulp collaboration system
    """

    context_id: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The context associated with the note.",
    )
    source_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("source.id", ondelete="CASCADE"),
        doc="The log file path (source) associated with the note.",
    )
    doc: Mapped[Optional[GulpBasicDocument]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        doc="a GulpBasicDocument associated with the note.",
    )
    time_pin: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        doc="To pin the note to a specific time, in nanoseconds from the unix epoch.",
    )
    last_editor_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("user.id", ondelete="SET NULL"),
        doc="The ID of the last user who edited the note.",
    )
    text: Mapped[Optional[str]] = mapped_column(String, doc="The text of the note.")

    edits: Mapped[Optional[list[GulpNoteEdit]]] = mapped_column(
        MutableList.as_mutable(ARRAY(JSONB)),
        doc="The edits made to the note.",
        default_factory=list,
    )

    # add an index on the operation_id for faster queries
    __table_args__ = (Index("idx_note_operation", "operation_id"),)

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d.update(
            {
                "context_id": "context_id",
                "source_id": "source_id",
                "doc": autogenerate_model_example_by_class(GulpBasicDocument),
                "time_pin": 1234567890,
                "last_editor_id": "last_editor_id",
                "text": "note text",
                "edits": [
                    autogenerate_model_example_by_class(GulpNoteEdit),
                ],
            }
        )
        return d

    @override
    @classmethod
    # pylint: disable=arguments-renamed,arguments-differ
    def build_dict(
        cls,
        operation_id: str,
        context_id: str,
        source_id: str,
        glyph_id: str = None,
        tags: list[str] = None,
        color: str = None,
        name: str = None,
        description: str = None,
        doc: GulpBasicDocument = None,
        time_pin: int = None,
        text: str = None,
    ) -> dict:
        """
        builds a note dictionary, taking care of converting the documents to dictionaries

        Args:
            operation_id (str): the operation id
            context_id (str): the context id
            source_id (str): the source id
            glyph_id (str, optional): the glyph id. Defaults to None.
            tags (list[str], optional): the tags. Defaults to None.
            color (str, optional): the color. Defaults to None.
            name (str, optional): the name. Defaults to None.
            description (str, optional): the description. Defaults to None.
            doc (GulpBasicDocument, optional): the document the note is associated with. Defaults to None.
            time_pin (int, optional): the time pin. Defaults to None.
            text (str, optional): the text. Defaults to None.

        Returns:
            the note dictionary
        """
        dd: dict = None
        if doc:
            # if a document is provided, convert it to a dictionary
            dd = doc.model_dump(by_alias=True, exclude_none=True, exclude_defaults=True)
        return super().build_dict(
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            glyph_id=glyph_id,
            tags=tags,
            color=color,
            name=name,
            description=description,
            doc=dd,
            time_pin=time_pin,
            text=text,
            edits=[],
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
        source_q: str = None,
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
            tags (list[str], optional): the tags to add to the notes. Defaults to None (set to ["auto"]).
            color (str, optional): the color of the notes. Defaults to None (use default).
            glyph_id (str, optional): the glyph id of the notes. Defaults to None (use default).
            source_q (str, optional): the original query to be set as text, if any. Defaults to None.
        Returns:
            the number of notes created
        """
        tt: list[str] = tags
        if not tt:
            tt = []

        if "auto" not in tt:
            tt.append("auto")

        # creates a list of notes, one for each document
        notes = []
        MutyLogger.get_instance().info("creating a bulk of %d notes ..." % len(docs))
        for doc in docs:
            highlights = doc.pop("highlight", None)

            # associate the document with the note by creating a GulpBasicDocument object
            associated_doc = GulpBasicDocument(
                id=doc.get("_id"),
                timestamp=doc.get("@timestamp"),
                gulp_timestamp=doc.get("gulp.timestamp"),
                invalid_timestamp=doc.get("gulp.timestamp_invalid", False),
                operation_id=doc.get("gulp.operation_id"),
                context_id=doc.get("gulp.context_id"),
                source_id=doc.get("gulp.source_id"),
            )

            # build the note text
            text = ""
            if highlights:
                # if highlights are present, add highlights to the text
                text += "### matches\n\n"
                text += "````json" + "\n" + json.dumps(highlights, indent=2) + "\n````"

            text += "\n\n### query:\n\n"
            text += f"````text\n{str(source_q)}````"

            # add the note object dictionary
            object_data = GulpNote.build_dict(
                operation_id=associated_doc.operation_id,
                context_id=associated_doc.context_id,
                source_id=associated_doc.source_id,
                glyph_id=glyph_id,
                tags=tt,
                color=color,
                name=name,
                text=text,
                doc=associated_doc,
            )
            obj_id: str = muty.crypto.hash_xxh128(str(object_data))
            note_dict = GulpNote.build_base_object_dict(
                object_data=object_data, owner_id=user_id, private=False, obj_id=obj_id
            )
            notes.append(note_dict)

        # bulk insert (handles duplicates)
        stmt: Insert = insert(GulpNote).values(notes)
        stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
        stmt = stmt.returning(GulpNote.id)
        res = await sess.execute(stmt)
        await sess.commit()
        inserted_ids = [row[0] for row in res]
        MutyLogger.get_instance().info(
            "written %d notes on collab db" % len(inserted_ids)
        )

        # in notes to be sent on ws, only keep the ones inserted
        notes = [note for note in notes if note["id"] in inserted_ids]
        if notes:
            # operation is always the same
            operation_id = notes[0].get("operation_id")
            data: GulpCollabCreateUpdatePacket = GulpCollabCreateUpdatePacket(
                data=notes,
                bulk=True,
                type=COLLABTYPE_NOTE,
                created=True,
                bulk_size=len(inserted_ids),
            )
            wsq = GulpWsSharedQueue.get_instance()
            await wsq.put(
                WSDATA_COLLAB_UPDATE,
                ws_id=ws_id,
                user_id=user_id,
                operation_id=operation_id,
                req_id=req_id,
                data=data,
            )
            MutyLogger.get_instance().debug(
                "sent %d notes on the websocket %s " % (len(notes), ws_id)
            )
        else:
            MutyLogger.get_instance().warning(
                "no notes to send on the websocket %s " % (ws_id)
            )
        return len(notes)

    @staticmethod
    async def bulk_update_tags(
        sess: AsyncSession,
        tags: list[str],
        new_tags: list[str],
        operation_id: str,
        user_id: str,
    ) -> None:
        """
        get tags from notes and update them with new tags

        Args:
            sess (AsyncSession): the database session
            tags (list[str]): the tags to match
            new_tags (list[str]): the new tags to add
            operation_id (str): the operation id
            user_id (str): the user id making the request
            user_id_is_admin (bool): whether the user is an admin
            user_group_ids (list[str], optional): the user groups ids. Defaults to None.
        """
        MutyLogger.get_instance().debug(
            f"updating notes tags from {tags} to {new_tags} ..."
        )
        offset = 0
        chunk_size = 1000

        # build filter
        flt = GulpCollabFilter(
            tags=tags,
            limit=chunk_size,
            offset=offset,
        )

        # restrict to operation_id and user_id
        if operation_id:
            flt.operation_ids = [operation_id]
        # if user_id:
        #   flt.owner_user_ids = [user_id]

        updated = 0

        while True:
            # get all notes matching "tags"
            notes: list[GulpNote] = await GulpNote.get_by_filter(
                sess,
                flt,
                user_id=user_id,
                throw_if_not_found=False,
            )
            if not notes:
                break

            # for each note, update tags
            for n in notes:
                for t in new_tags:
                    if t not in n.tags:
                        n.tags.append(t)

            await sess.commit()
            rows_updated = len(notes)
            MutyLogger.get_instance().debug(f"updated {rows_updated} notes")

            # next chunk
            offset += rows_updated
            flt.offset = offset
            updated += rows_updated

        MutyLogger.get_instance().info("updated %d notes tags" % (updated))
