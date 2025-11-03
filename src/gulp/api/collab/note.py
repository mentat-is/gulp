"""
This module defines the GulpNote class, which represents a note in the gulp collaboration system.

The GulpNote class inherits from GulpCollabBase and implements note-specific functionality.
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

import orjson
from typing import Optional, override

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

from gulp.api.collab.structs import COLLABTYPE_NOTE, GulpCollabFilter, GulpCollabBase
from gulp.api.opensearch.structs import GulpBasicDocument, GulpQueryParameters
from gulp.api.ws_api import (
    WSDATA_COLLAB_CREATE,
    GulpCollabCreatePacket,
    GulpRedisBroker,
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


class GulpNote(GulpCollabBase, type=COLLABTYPE_NOTE):
    """
    a note in the gulp collaboration system
    """

    context_id: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The context associated with the note.",
    )
    text: Mapped[str] = mapped_column(String, doc="The text of the note.")

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

    @staticmethod
    async def bulk_create_for_documents_and_send_to_ws(
        sess: AsyncSession,
        operation_id: str,
        user_id: str,
        ws_id: str,
        req_id: str,
        docs: list[dict],
        name: str,
        q: str,
        sigma_yml: str = None,
        tags: list[str] = None,
        color: str = None,
        glyph_id: str = None,
        last: bool = False,
    ) -> int:
        """
        creates a note on collab for each document in the list, using bulk insert, then streams the chunk of notes to the websocket

        NOTE: this is meant for internal usage, access checks should be done before calling this method

        Args:
            sess (AsyncSession): the database session
            operation_id (str): the operation id the notes belong to
            user_id (str): the user id creating the notes
            ws_id (str): the websocket id
            req_id (str): the request id
            docs (list[dict]): the list of GulpDocument dictionaries to create notes for (must belong to the operation_id, no checks!)
            name (str): the name (title) to set on the notes
            q (str): the original query to be set as text
            sigma_yml (str, optional): the originating sigma rule in yml format, if any. Defaults to None.
            tags (list[str], optional): the tags to add to the notes. Defaults to None (will set tags=["auto"]).
            color (str, optional): the color of the notes. Defaults to None (use default).
            glyph_id (str, optional): the glyph id of the notes. Defaults to None (use default).
            last (bool, optional): whether this is the last batch of notes to be created. Defaults to False.
        Returns:
            the number of notes created
        """
        if not docs:
            MutyLogger.get_instance().warning("no documents provided, no notes created")
            return 0

        tt: list[str] = tags
        if not tt:
            # empty tags
            tt = []

        if "auto" not in tt:
            # add "auto" tag if not present
            tt.append("auto")
        if not name in tt:
            # add the name as tag if not present
            tt.append(name)

        # creates a list of notes, one for each document
        notes: list[dict] = []
        MutyLogger.get_instance().info("creating a bulk of %d notes ..." % len(docs))
        for doc in docs:
            # remove highlights from the document, if any
            highlights: dict = doc.pop("highlight", {})

            # use basic fields from the document and builds an associated doc for the note
            associated_doc: dict = {
                k: v
                for k, v in doc.items()
                if k
                in [
                    "_id",
                    "@timestamp",
                    "gulp.operation_id",
                    "gulp.context_id",
                    "gulp.source_id",
                    "gulp.timestamp",
                    "gulp.timestamp_invalid",
                ]
            }

            # build the note text
            text: str = ""
            if highlights:
                # if highlights are present, add highlights to the text
                text += "### matches\n\n"
                text += (
                    "````json"
                    + "\n"
                    + orjson.dumps(highlights, option=orjson.OPT_INDENT_2).decode()
                    + "\n````"
                )

            if sigma_yml:
                # if a sigma rule is provided, add it to the text
                text += "\n\n### sigma rule:\n\n"
                text += f"````yaml\n{sigma_yml}````"

            # always add the query
            text += "\n\n### query:\n\n"
            text += f"````json\n{str(q)}````"

            # build the object for the note
            object_data: dict = GulpNote.build_object_dict(
                user_id,
                name=name,
                operation_id=associated_doc["gulp.operation_id"],
                glyph_id=glyph_id,
                tags=tt,
                color=color,
                private=False,
                doc=associated_doc,
                text=text,
                context_id=associated_doc["gulp.context_id"],
                source_id=associated_doc["gulp.source_id"],
            )
            obj_id=muty.crypto.hash_xxh128(str(object_data))
            object_data["id"]=obj_id
            notes.append(object_data)

        # bulk insert (handles duplicates)
        stmt: Insert = insert(GulpNote).values(notes)
        stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
        stmt = stmt.returning(GulpNote.id)
        res = await sess.execute(stmt)
        await sess.commit()
        inserted_ids = [row[0] for row in res]
        MutyLogger.get_instance().info(
            "written %d notes on collab db", len(inserted_ids)
        )

        # send on ws, only keep the ones inserted (to avoid sending duplicates, but we always send something if "last" is set)
        inserted_notes: list[dict] = [
            note for note in notes if note["id"] in inserted_ids
        ]
        if inserted_notes or last:
            p: GulpCollabCreatePacket = GulpCollabCreatePacket(
                obj=inserted_notes,
                bulk=True,
                last=last,
                bulk_size=len(inserted_notes),
                total_size=len(notes),
            )
            wsq = GulpRedisBroker.get_instance()
            await wsq.put(
                WSDATA_COLLAB_CREATE,
                user_id,
                ws_id=ws_id,
                operation_id=operation_id,
                req_id=req_id,
                d=p.model_dump(exclude_none=True),
            )
            MutyLogger.get_instance().debug(
                "sent (inserted) notes on the websocket %s: notes=%d,inserted=%d,last=%r (if 'notes' > 'inserted', duplicate notes were skipped!)"
                % (ws_id, len(notes), len(inserted_notes),last)
            )
        return len(inserted_notes)

    @staticmethod
    async def bulk_update_for_group_match(
        sess: AsyncSession,
        operation_id: str,
        tags: list[str],
        q_options: GulpQueryParameters,
        user_id: str = None,
    ) -> None:
        """
        update all notes matching "tags" to also include q_options.group as tag, and also update color and glyph_id with group_color and group_glyph_id if any

        NOTE: this is meant for internal usage, access checks should be done before calling this method

        Args:
            sess (AsyncSession): the database session, use None to create a new session internally (only valid within this function scope)
            operation_id (str): the operation id the notes belong to
            tags (list[str]): the list of tags to match to trigger update: if a note have any of these tags, it will be updated
            q_options (GulpQueryParameters): the query options containing group, group_color, group_glyph_id to update the notes with
            user_id (str, optional): the user id to perform the update with. Defaults to None (no access check).
        """

        async def _internal(sess: AsyncSession) -> None:
            MutyLogger.get_instance().debug("updating notes with %s tags ...", tags)
            offset = 0
            chunk_size = 1000

            # build filter
            flt = GulpCollabFilter(
                tags=tags,
                limit=chunk_size,
                offset=offset,
                operation_ids=[operation_id],
            )

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

                # for each note, add the group tag, color and glyph_id
                for n in notes:
                    if q_options.group and q_options.group not in n.tags:
                        n.tags.append(q_options.group)
                    if q_options.group_color:
                        n.color = q_options.group_color
                    if q_options.group_glyph_id:
                        n.glyph_id = q_options.group_glyph_id

                await sess.commit()
                rows_updated = len(notes)
                MutyLogger.get_instance().debug(
                    "updated %d notes tags/colors/glyphs", rows_updated
                )

                # next chunk
                offset += rows_updated
                flt.offset = offset
                updated += rows_updated

            MutyLogger.get_instance().info(
                "updated %d notes tags/colors/glyphs (total)", updated
            )

        if not sess:
            async with GulpCollab.get_instance().session() as sess:
                await _internal(sess)
        else:
            await _internal(sess)
