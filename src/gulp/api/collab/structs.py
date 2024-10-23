from enum import StrEnum
import json
from typing import Optional

from pydantic import BaseModel, Field, model_validator


class SessionExpired(Exception):
    """if the user session has expired"""


class WrongUsernameOrPassword(Exception):
    """if the user provides wrong username or password"""


class MissingPermission(Exception):
    """if the user does not have the required permission"""


COLLAB_MAX_NAME_LENGTH = 128


class GulpRequestStatus(StrEnum):
    """Gulp request status codes (used by the stats API)."""

    ONGOING = "ongoing"
    DONE = "done"
    FAILED = "failed"
    CANCELED = "canceled"
    DONE_WITH_ERRORS = "done_with_errors"


class GulpUserPermission(StrEnum):
    """represent the permission of a user in the Gulp platform."""

    # can read only
    READ = "read"
    # can edit own highlights, notes, stories, links
    EDIT = "edit"
    # can delete highlights, notes, stories, links
    DELETE = "delete"
    # can ingest files
    INGEST = "ingest"
    # can do anything, including creating new users and change permissions
    ADMIN = "admin"
    # can monitor the system (READ + monitor)
    MONITOR = "monitor"


PERMISSION_EDIT = [GulpUserPermission.READ, GulpUserPermission.EDIT]
PERMISSION_DELETE = [
    GulpUserPermission.READ,
    GulpUserPermission.EDIT,
    GulpUserPermission.DELETE,
]
PERMISSION_INGEST = [
    GulpUserPermission.READ,
    GulpUserPermission.INGEST,
    GulpUserPermission.EDIT,
]
PERMISSION_CLIENT = [
    GulpUserPermission.READ,
    GulpUserPermission.INGEST,
    GulpUserPermission.EDIT,
    GulpUserPermission.DELETE,
]
PERMISSION_MONITOR = [GulpUserPermission.READ, GulpUserPermission.MONITOR]


class GulpCollabType(StrEnum):
    """
    defines the type of collaboration object
    """

    NOTE = "note"
    HIGHLIGHT = "highlight"
    STORY = "story"
    LINK = "link"
    STORED_QUERY = "query"
    STATS_INGESTION = "stats_ingestion"
    STATS_QUERY = "stats_query"
    SIGMA_FILTER = "sigma_filter"
    SHARED_DATA_GENERIC = "shared_data_generic"
    CONTEXT = "context"
    USER = "user"
    GLYPH = "glyph"
    OPERATION = "operation"
    CLIENT = "client"
    SESSION = "session"


class GulpCollabFilter(BaseModel):
    """
    defines filter to be applied to all collaboration objects

    each filter specified as list, i.e. *operation_id*, matches as OR (one or more may match).<br>
    combining multiple filters (i.e. id + operation + ...) matches as AND (all must match).<br><br>

    for string fields, where not specified otherwise, the match is case-insensitive and supports SQL-like wildcards (% and _)<br>

    an empty filter returns all objects.<br>

    **NOTE**: not all fields are applicable to all types.
    """
    """
    {
        "id": "bla",
        "events": [
            {
                # if id is null, note is pinned at timestamp
                "id": null,
                # pinned note
                "timestamp": 1234567890,
            },
            {
                # normal note associated with an event
                "id": "kkkk",
                "timestamp": 1234567890,
            },
            {                
                "id": null,
                # pinned note
                "timestamp": 1234567890,
            }
        ]

    }
    """
    id: list[str] = Field(None, description="filter by the given id/s.")
    type: list[GulpCollabType] = Field(
        None, unique=True, description="filter by the given type/s."
    )
    operation: list[str] = Field(None, description="filter by the given operation/s.")
    context: list[str] = Field(None, description="filter by the given context/s.")
    source: list[str] = Field(
        None, unique=True, description="filter by the given source file/s."
    )
    user: list[str] = Field(None, description="filter by the given user(owner) id/s.")
    tags: list[str] = Field(None, unique=True, description="filter by the given tag/s.")
    title:list[str] = Field(None, description="filter by the given title/s.")
    text: list[str] = Field(
        None, unique=True, description="filter by the given object text."
    )
    events: list[str] = Field(
        None,
        unique=True,
        description="filter by the given event ID/s in CollabObj.events list of GulpAssociatedEvents.",
    )
    time_range: tuple[int, int] = Field(
        None,
        description="timestamp range relative to CollabObj.events (milliseconds from unix epoch).",
    )
    opt_private: bool = Field(
        False,
        description="if True, return only private objects. Default=False (return all).",
    )
    opt_limit: int = Field(
        None,
        description='to be used together with "offset", maximum number of results to return. default=return all.',
    )
    opt_offset: int = Field(
        None,
        description='to be used together with "limit", number of results to skip from the beginning. default=0 (from start).',
    )
    opt_tags_and: bool = Field(
        False,
        description="if True, all tags must match. Default=False (at least one tag must match).",
    )

    @model_validator(mode="before")
    @classmethod
    def to_py_dict(cls, data: str | dict):
        if data is None or len(data) == 0:
            return {}

        if isinstance(data, dict):
            return data
        return json.loads(data)


class GulpAssociatedEvent(BaseModel):
    # TODO: remove
    """
    Represents an event in the collab API needing an "events" field.

    when used for filtering with a GulpCollabFilter, only "id" and "@timestamp" (if GulpCollabFilter.opt_time_start_end_events) are taken into account.
    """

    id: str = Field(None, description="the event ID")
    timestamp: int = Field(None, description="the event timestamp")
    operation_id: Optional[int] = Field(
        None, description="operation ID the event is associated with."
    )
    context: Optional[str] = Field(
        None, description="context the event is associated with."
    )
    src_file: Optional[str] = Field(
        None, description="source file the event is associated with."
    )

    def to_dict(self) -> dict:
        return {
            "_id": self.id,
            "@timestamp": self.timestamp,
            "gulp.operation.id": self.operation_id,
            "gulp.context": self.context,
            "gulp.source.file": self.src_file,
        }

    @staticmethod
    def from_dict(d: dict) -> "GulpAssociatedEvent":
        return GulpAssociatedEvent(
            id=d["id"],
            timestamp=d["@timestamp"],
            operation_id=d.get("gulp.operation.id"),
            context=d.get("gulp.context"),
            src_file=d.get("gulp.source.file"),
        )

    @model_validator(mode="before")
    @classmethod
    def to_py_dict(cls, data: str | dict):
        # print('*********** events: %s, %s' % (data, type(data)))
        if data is None or len(data) == 0:
            return {}

        if isinstance(data, dict):
            # print('****** events (after): %s, %s' % (data, type(data)))
            return data
        return json.loads(data)