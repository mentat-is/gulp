"""
Module for handling story objects in the GULP collaborative system.

A story in GULP represents a collection of document references, providing
a way to group and organize related documents within the collaboration system.

This module defines the GulpStory class which inherits from GulpCollabObject
and is specifically typed as a STORY in the collaboration type system.

"""
from typing import Optional, override

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import ARRAY, BIGINT
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB
from gulp.api.collab.structs import COLLABTYPE_STORY, GulpCollabObject
from gulp.api.rest.test_values import TEST_CONTEXT_ID, TEST_OPERATION_ID, TEST_SOURCE_ID
import muty.pydantic

class GulpStoryNoteEntry(BaseModel):
    """
    represents a single note entry in a story, containing metadata about the note
    """
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": "1234567890abcdef1234567890abcdef",
                    "name": "note name",
                    "text": "note text",
                }
            ]
        },
    )

    id: str = Field(
        description='the unique identifier of the note.',
    )
    name: Optional[str] = Field(
        None,
        description='the name/title of the note.',
    )
    text: str = Field(
        description=' the text of the note.',
    )

class GulpStoryLinkEntry(BaseModel):
    """
    represents a single link entry in a story, containing metadata about the link
    """
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": "1234567890abcdef1234567890abcdef",
                    "name": "link name",
                    "doc_ids": ["_id1", "_id2"],
                }
            ]
        },
    )

    id: str = Field(
        description='the unique identifier of the link.',
    )
    name: Optional[str] = Field(
        None,
        description='the name/title of the link.',
    )
    doc_ids: list[str] = Field(
        description='the document IDs the link points to.',
    )

class GulpStoryHighlightEntry(BaseModel):
    """
    represents a single highlight entry in a story, containing metadata about the highlight
    """
    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "id": "1234567890abcdef1234567890abcdef",
                    "name": "highlight name",
                    "time_range": [1609459200000000000, 1609462800000000000],
                    "source_id": "source_id",

                }
            ]
        },
    )

    id: str = Field(
        description='the unique identifier of the highlight.',
    )
    name: Optional[str] = Field(
        None,
        description='the name/title of the highlight.',
    )
    time_range: tuple[int, int] = Field(
        description="The time range of the highlight, in nanoseconds from unix epoch.",
    )
    source_id: Optional[str] = Field(
        default=None,
        description="The associated GulpSource id.",
        alias="gulp.source_id",
    )
    source: Optional[str] = Field(
        default=None,
        description="The name of the source identified by gulp.source_id.")

class GulpStoryEntry(BaseModel):
    """
    represents a single entry in a story, containing metadata about the document
    """
    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "_id": "1234567890abcdef1234567890abcdef",
                    "@timestamp": "2021-01-01T00:00:00Z",
                    "gulp.timestamp": 1609459200000000000,
                    "gulp.operation_id": TEST_OPERATION_ID,
                    "gulp.context_id": TEST_CONTEXT_ID,
                    "gulp.source_id": TEST_SOURCE_ID,
                    "notes": [muty.pydantic.autogenerate_model_example_by_class(GulpStoryNoteEntry)],
                    "event.code": "example_event_code",
                    "agent.type": "example_agent_type",
                    "event.duration": 1000000000,
                    "links": [muty.pydantic.autogenerate_model_example_by_class(GulpStoryLinkEntry)],
                }
            ]
        },
    )

    id: str = Field(
        description='"_id": the unique identifier of the document.',
        alias="_id",
    )

    operation_id: str = Field(
        description='"gulp.operation_id": the operation ID the document is associated with.',
        alias="gulp.operation_id",
    )
    context_id: str = Field(
        description='"gulp.context_id": the context (i.e. an host name) the document is associated with.',
        alias="gulp.context_id",
    )
    context: str = Field(
        description='name of the context identified by "gulp.context_id".')

    source_id: str = Field(
        description='"gulp.source_id": the source the document is associated with.',
        alias="gulp.source_id",
    )

    source: str = Field(
        description='name of the source identified by "gulp.source_id".')
    
    event_code: str = Field(
        description='"event.code": the event code the document is associated with.',
        alias="event.code",
    )
    agent_type: str = Field(
        description='"agent.type": the plugin which created the document.',
        alias="agent.type",
    )
    timestamp: str = Field(
        description='"@timestamp": document timestamp, in iso8601 format.',
        alias="@timestamp",
    )
    duration: Optional[int] = Field(
        default=None,
        description='"event.duration": optional duration of the event in nanoseconds.',
        alias="event.duration",
    )
    notes: list[GulpStoryNoteEntry] = Field(
        description='one or more notes associated with the document.',        
    )
    links: Optional[list[GulpStoryLinkEntry]] = Field(
        default=None,
        description='if the document is the source of a link (=points to other documents), these are the document `_id`s it points to.',
    )

class GulpStory(GulpCollabObject, type=COLLABTYPE_STORY):
    """
    a story in the gulp collaboration system
    """

    entries: Mapped[list[GulpStoryEntry]] = mapped_column(
        MutableList.as_mutable(ARRAY(JSONB)),
        default_factory=list,
        doc="one or more GulpStoryEntry associated with the story.",
    )
    highlights: Optional[list[GulpStoryHighlightEntry]] = mapped_column(
        MutableList.as_mutable(ARRAY(JSONB)),
        default_factory=list,
        doc='optional highlighted time ranges.',
    )
    
    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["entries"] = [
            muty.pydantic.autogenerate_model_example_by_class(GulpStoryEntry)
        ]
        d["highlights"] = [
            muty.pydantic.autogenerate_model_example_by_class(GulpStoryHighlightEntry)
        ]
        return d
