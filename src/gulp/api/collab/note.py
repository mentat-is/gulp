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

import asyncio
from typing import Optional, override

import muty.crypto
import muty.string
import orjson
from muty.pydantic import autogenerate_model_example_by_class
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import ARRAY, BIGINT, ForeignKey, Index, Insert, String
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy.orm import Mapped, mapped_column, relationship

from gulp.api.collab.structs import (
    COLLABTYPE_NOTE,
    COLLABTYPE_NOTE_TEXT,
    GulpCollabBase,
    GulpCollabFilter,
)
from gulp.api.opensearch.structs import GulpBasicDocument, GulpQueryParameters
from gulp.api.ws_api import (
    WSDATA_COLLAB_CREATE,
    GulpCollabCreatePacket,
    GulpRedisBroker,
)
from gulp.glug import Glug


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


class GulpNoteText(GulpCollabBase, type=COLLABTYPE_NOTE_TEXT):
    """Text shared by automatic notes generated for the same query."""

    text: Mapped[str] = mapped_column(String, doc="The shared note text.")

    __table_args__ = (Index("idx_note_text_operation", "operation_id"),)


@GulpCollabBase.registry.mapped
class GulpNoteAttachment:
    """Metadata for one binary object attached to a note."""

    __tablename__ = "note_attachment"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    note_id: Mapped[str] = mapped_column(
        ForeignKey("note.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[str] = mapped_column(
        ForeignKey("user.id", ondelete="CASCADE"), nullable=False
    )
    operation_id: Mapped[str] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"), nullable=False
    )
    title: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False, default="")
    mime_type: Mapped[str] = mapped_column(String, nullable=False)

    __table_args__ = (
        Index("idx_note_attachment_note", "note_id"),
        Index("idx_note_attachment_user", "user_id"),
        Index("idx_note_attachment_operation", "operation_id"),
    )

    def to_dict(self) -> dict:
        """Serialize public attachment metadata."""
        return {
            "id": self.id,
            "note_id": self.note_id,
            "user_id": self.user_id,
            "operation_id": self.operation_id,
            "title": self.title,
            "description": self.description,
            "mime_type": self.mime_type,
        }

    @classmethod
    def from_upload(
        cls,
        note: "GulpNote",
        user_id: str,
        filename: str | None,
        upload_mime_type: str | None,
        title: str | None = None,
        mime_type: str | None = None,
    ) -> "GulpNoteAttachment":
        """Build attachment metadata using upload defaults where needed."""
        attachment_id = muty.string.generate_unique()
        return cls(
            id=attachment_id,
            note_id=note.id,
            user_id=user_id,
            operation_id=note.operation_id,
            title=title if title is not None else (filename or attachment_id),
            description="",
            mime_type=mime_type or upload_mime_type or "application/octet-stream",
        )

    @staticmethod
    async def delete_objects(attachment_ids: list[str]) -> None:
        """Delete attachment objects by their exact storage keys."""
        if not attachment_ids:
            return
        from gulp.api.s3_api import GulpS3

        for attachment_id in attachment_ids:
            await GulpS3.get_instance().delete_file(attachment_id)

    @staticmethod
    async def delete_objects_by_tags(tags: dict[str, str]) -> None:
        """Delete attachment objects matching parent cascade tags."""
        from gulp.api.s3_api import GulpS3

        await GulpS3.get_instance().delete_by_tags({**tags, "kind": "note_attachment"})

    @classmethod
    async def create_from_upload(
        cls,
        sess: AsyncSession,
        note: "GulpNote",
        user_id: str,
        data: bytes,
        filename: str | None,
        upload_mime_type: str | None,
        title: str | None = None,
        mime_type: str | None = None,
    ) -> "GulpNoteAttachment":
        """Upload bytes and persist metadata, compensating failed inserts."""
        from gulp.api.s3_api import GulpS3

        attachment = cls.from_upload(
            note,
            user_id,
            filename,
            upload_mime_type,
            title=title,
            mime_type=mime_type,
        )
        await GulpS3.get_instance().upload_data(
            data,
            attachment.id,
            tags={
                "note_id": note.id,
                "user_id": user_id,
                "operation_id": note.operation_id,
                "context_id": note.context_id,
                "source_id": note.source_id or "",
                "kind": "note_attachment",
            },
        )
        note.attachments.append(attachment)
        try:
            await sess.commit()
        except (Exception, asyncio.CancelledError):
            try:
                await asyncio.shield(sess.rollback())
            except (Exception, asyncio.CancelledError) as rollback_error:
                Glug.get_instance().exception(
                    f"failed to roll back note attachment insert: {rollback_error}"
                )
            try:
                await asyncio.shield(GulpS3.get_instance().delete_file(attachment.id))
            except (Exception, asyncio.CancelledError) as cleanup_error:
                Glug.get_instance().exception(
                    f"failed to compensate note attachment upload {attachment.id}: {cleanup_error}"
                )
            raise
        return attachment


class GulpNote(GulpCollabBase, type=COLLABTYPE_NOTE):
    """
    a note in the gulp collaboration system
    """

    context_id: Mapped[str] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The context associated with the note.",
    )
    text: Mapped[str] = mapped_column(String, doc="The text of the note.")
    text_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("note_text.id", ondelete="SET NULL"),
        doc="Shared automatic note text.",
    )
    shared_text: Mapped[Optional[GulpNoteText]] = relationship(
        GulpNoteText,
        lazy="selectin",
        init=False,
    )
    attachments: Mapped[list[GulpNoteAttachment]] = relationship(
        GulpNoteAttachment,
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="selectin",
        order_by=GulpNoteAttachment.id,
        init=False,
    )

    source_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("source.id", ondelete="CASCADE"),
        doc="The log file path (source) associated with the note.",
    )
    doc: Mapped[Optional[GulpBasicDocument]] = mapped_column(
        MutableDict.as_mutable(JSONB),
        doc="a GulpBasicDocument associated with the note.",
    )
    doc_id: Mapped[Optional[str]] = mapped_column(
        String,
        doc="Indexed ID of the associated document.",
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

    __table_args__ = (
        Index("idx_note_operation", "operation_id"),
        Index("idx_note_context", "context_id"),
        Index("idx_note_source", "source_id"),
        Index("idx_note_doc", "doc_id"),
        Index("idx_note_text_id", "text_id"),
        Index("idx_note_user", "user_id"),
    )

    @override
    def to_dict(self, *args, **kwargs) -> dict:
        """Serialize shared automatic text through the existing text field."""
        data = super().to_dict(*args, **kwargs)
        excluded = kwargs.get("exclude") or []
        if "text" not in excluded:
            data["text"] = (self.text or "") + (
                self.shared_text.text if self.shared_text else ""
            )
        data.pop("text_id", None)
        data.pop("shared_text", None)
        data.pop("attachments", None)
        data["attachment_ids"] = [a.id for a in self.attachments]
        return data

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
    def _normalise_auto_note_tags(tags: list[str] | None, name: str) -> list[str]:
        """Return deterministic query-note tags without mutating caller state."""
        normalised: list[str] = []
        for tag in tags or []:
            if tag not in normalised:
                normalised.append(tag)
        for tag in ("auto", name):
            if tag and tag not in normalised:
                normalised.append(tag)
        return normalised

    @staticmethod
    def _deterministic_query_note_id(
        associated_doc: dict,
        name: str,
        q: str,
        tags: list[str],
        req_id: str = None,
        query_ordinal: int = None,
        color: str = None,
        glyph_id: str = None,
        sigma_yml: str = None,
        match_ordinal: int = None,
    ) -> str:
        """Return a stable note id for one query-created note."""
        key = {
            "type": COLLABTYPE_NOTE,
            "operation_id": associated_doc.get("gulp.operation_id"),
            "context_id": associated_doc.get("gulp.context_id"),
            "source_id": associated_doc.get("gulp.source_id"),
            "doc_id": associated_doc.get("_id"),
            "doc_timestamp": associated_doc.get("@timestamp"),
            "doc_gulp_timestamp": associated_doc.get("gulp.timestamp"),
            "name": name,
            "req_id": req_id,
            "query_ordinal": query_ordinal,
            "q": q,
            "tags": sorted(set(tags or [])),
            "color": color,
            "glyph_id": glyph_id,
            "sigma_yml": sigma_yml,
            "match_ordinal": match_ordinal,
        }
        encoded = orjson.dumps(key, option=orjson.OPT_SORT_KEYS).decode()
        return muty.crypto.hash_xxh128(encoded)

    @staticmethod
    def _strip_text_for_ws_payload(note: dict) -> dict:
        """Return a lightweight note object for automatic query-created websocket events."""
        payload = dict(note)
        payload["text"] = ""
        payload["attachment_ids"] = []
        return payload

    @override
    async def delete(self, sess: AsyncSession, *args, **kwargs) -> None:
        """Delete attachment objects before deleting their note metadata."""
        await GulpNoteAttachment.delete_objects([a.id for a in self.attachments])
        await super().delete(sess, *args, **kwargs)

    @override
    @classmethod
    async def delete_by_filter(
        cls,
        sess: AsyncSession,
        flt=None,
        user_id: str = None,
        throw_if_not_found: bool = True,
        ws_id: str = None,
        req_id: str = None,
        operation_id: str = None,
    ) -> int:
        """Delete attachments and notes, optionally publishing one bulk event."""
        if operation_id:
            flt = flt or GulpCollabFilter()
            flt.operation_ids = [operation_id]

        notes = await cls.get_by_filter(
            sess,
            flt=flt,
            user_id=user_id,
            throw_if_not_found=throw_if_not_found,
        )
        await GulpNoteAttachment.delete_objects(
            [attachment.id for note in notes for attachment in note.attachments]
        )
        return await super().delete_by_filter(
            sess,
            flt=flt,
            user_id=user_id,
            throw_if_not_found=throw_if_not_found,
            ws_id=ws_id,
            req_id=req_id,
            operation_id=operation_id,
        )

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
        q_options: GulpQueryParameters,
        sigma_yml: str = None,
        tags: list[str] = None,
        color: str = None,
        glyph_id: str = None,
        query_ordinal: int = None,
        hit_offset: int = 0,
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
            q_options (GulpQueryParameters): the query options containing notes settings
            sigma_yml (str, optional): the originating sigma rule in yml format, if any. Defaults to None.
            tags (list[str], optional): the tags to add to the notes. Defaults to None (will set tags=["auto"]).
            color (str, optional): the color of the notes. Defaults to None (use default).
            glyph_id (str, optional): the glyph id of the notes. Defaults to None (use default).
            query_ordinal (int, optional): zero-based query occurrence inside the request.
            hit_offset (int, optional): zero-based offset of this chunk in the query result stream.
            last (bool, optional): whether this is the last batch of notes to be created. Defaults to False.
        Returns:
            the number of notes created
        """
        if not docs:
            Glug.get_instance().warning("no documents provided, no notes created")
            return 0

        tt = GulpNote._normalise_auto_note_tags(tags, name)

        shared_text = ""
        if sigma_yml:
            shared_text += f"\n\n### sigma rule:\n\n````yaml\n{sigma_yml}````"
        shared_text += f"\n\n### query:\n\n````json\n{str(q)}````"
        text_id = muty.crypto.hash_xxh128(f"{operation_id}\0{shared_text}")
        text_data = GulpNoteText.build_object_dict(
            user_id,
            name=name,
            operation_id=operation_id,
            obj_id=text_id,
            private=False,
            text=shared_text,
        )
        await sess.execute(
            insert(GulpNoteText)
            .values(text_data)
            .on_conflict_do_nothing(index_elements=["id"])
        )

        # creates a list of notes, one for each document
        notes: list[dict] = []
        Glug.get_instance().info(f"creating a bulk of {len(docs)} notes")
        for idx, doc in enumerate(docs):
            # remove highlights from the document, if any
            highlights: dict = doc.get("_highlight", {})

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

            # Keep only per-hit highlights inline; query/rule text is shared.
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

            obj_id = GulpNote._deterministic_query_note_id(
                associated_doc,
                name=name,
                q=str(q),
                tags=tt,
                req_id=req_id,
                query_ordinal=query_ordinal,
                color=color,
                glyph_id=glyph_id,
                sigma_yml=sigma_yml,
                match_ordinal=hit_offset + idx,
            )

            # build the object for the note
            object_data: dict = GulpNote.build_object_dict(
                user_id,
                name=name,
                operation_id=associated_doc["gulp.operation_id"],
                glyph_id=glyph_id,
                tags=tt,
                color=color,
                obj_id=obj_id,
                private=False,
                doc=associated_doc,
                doc_id=associated_doc.get("_id", "").lower() or None,
                text=text,
                text_id=text_id,
                context_id=associated_doc["gulp.context_id"],
                source_id=associated_doc["gulp.source_id"],
            )
            notes.append(object_data)

        # bulk insert (handles duplicates)
        stmt: Insert = insert(GulpNote).values(notes)
        stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
        stmt = stmt.returning(GulpNote.id)
        res = await sess.execute(stmt)
        await sess.commit()
        inserted_ids = [row[0] for row in res]
        Glug.get_instance().info(f"written {len(inserted_ids)} notes on collab db")

        # send on ws, only keep the ones inserted (to avoid sending duplicates, but we always send something if "last" is set)
        inserted_id_set = set(inserted_ids)
        inserted_notes: list[dict] = [
            GulpNote._strip_text_for_ws_payload(
                note
            )  # strip text to reduce pressure on the websocket, the text is not used on the client side just to receive the note creation event
            for note in notes
            if note["id"] in inserted_id_set
        ]
        if inserted_notes or last:
            p: GulpCollabCreatePacket = GulpCollabCreatePacket(
                obj=inserted_notes,
                bulk=True,
                last=last,
                bulk_size=len(inserted_notes),
                total_size=len(notes),
            )
            redis_broker = GulpRedisBroker.get_instance()
            await redis_broker.put(
                WSDATA_COLLAB_CREATE,
                user_id,
                ws_id=ws_id,
                operation_id=operation_id,
                req_id=req_id,
                d=p.model_dump(exclude_none=True),
            )
            Glug.get_instance().debug(
                f"sent (inserted) notes on the websocket {ws_id}: "
                f"notes={len(notes)},inserted={len(inserted_notes)},last={last!r} "
                "(if 'notes' > 'inserted', duplicate notes were skipped!)"
            )
        return len(inserted_notes)
