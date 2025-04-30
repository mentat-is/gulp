"""
Module for handling story objects in the GULP collaborative system.

A story in GULP represents a collection of document references, providing
a way to group and organize related documents within the collaboration system.

This module defines the GulpStory class which inherits from GulpCollabObject
and is specifically typed as a STORY in the collaboration type system.

"""
from typing import Optional, override

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import ARRAY
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSONB
from gulp.api.collab.structs import COLLABTYPE_STORY, GulpCollabObject
from gulp.api.rest.test_values import TEST_CONTEXT_ID, TEST_OPERATION_ID, TEST_SOURCE_ID
import muty.pydantic

class GulpStoryEntry(BaseModel):
    """
    represents a single entry in a story, containing metadata about the document
    """
    model_config = ConfigDict(
        populate_by_name=True,
        json_schema_extra={
            "examples": [
                {
                    "_id": "1234567890abcdef1234567890abcdef",
                    "@timestamp": "2021-01-01T00:00:00Z",
                    "gulp.timestamp": 1609459200000000000,
                    "gulp.operation_id": TEST_OPERATION_ID,
                    "gulp.context_id": TEST_CONTEXT_ID,
                    "gulp.source_id": TEST_SOURCE_ID,
                    "context_name": "test_context",
                    "source_name": "test_source",
                    "notes": ["note1", "note2"],
                }
            ]
        },
    )

    id: str = Field(
        description='"_id": the unique identifier of the document.',
        alias="_id",
    )
    gulp_timestamp: int = Field(
        description='"@timestamp": document timestamp in nanoseconds from unix epoch',
        alias="gulp.timestamp",
    )
    operation_id: str = Field(
        description='"gulp.operation_id": the operation ID the document is associated with.',
        alias="gulp.operation_id",
    )
    context_id: str = Field(
        description='"gulp.context_id": the context (i.e. an host name) the document is associated with.',
        alias="gulp.context_id",
    )
    source_id: str = Field(
        description='"gulp.source_id": the source the document is associated with.',
        alias="gulp.source_id",
    )
    timestamp: Optional[str] = Field(
        None,
        description='"@timestamp": document timestamp, in iso8601 format.',
        alias="@timestamp",
    )
    context_name: Optional[str] = Field(
        None,
        description='name of the context the document is associated with.',
    )
    source_name: Optional[str] = Field(
        None,
        description='name of the source the document is associated with.',
    )
    notes: list[str] = Field(
        default_factory=list,
        description='one or more notes associated with the document.',
    )

class GulpStory(GulpCollabObject, type=COLLABTYPE_STORY):
    """
    a story in the gulp collaboration system
    """

    entries: Mapped[list[dict]] = mapped_column(
        MutableList.as_mutable(ARRAY(JSONB)),
        default_factory=list,
        doc="one or more GulpStoryEntry associated with the story.",
    )
    notes: Mapped[list[str]] = mapped_column(
        MutableList.as_mutable(ARRAY(str)),
        default_factory=list,
        doc="one or more story-specific notes.",
    )
    
    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d["docs"] = [
            muty.pydantic.autogenerate_model_example_by_class(GulpStoryEntry)
        ]
        return d
