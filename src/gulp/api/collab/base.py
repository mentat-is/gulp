import json
from enum import IntEnum, IntFlag
from typing import Optional

from pydantic import BaseModel, Field, model_validator
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass


class SessionExpired(Exception):
    """if the user session has expired"""


class WrongUsernameOrPassword(Exception):
    """if the user provides wrong username or password"""


class MissingPermission(Exception):
    """if the user does not have the required permission"""


class GulpClientType(IntEnum):
    """Defines the type of client."""

    SLURP = 0
    UI = 1
    OTHER = 2


class GulpCollabLevel(IntEnum):
    """Defines the level of collaboration object, mainly used for notes."""

    DEFAULT = 0
    WARNING = 1
    ERROR = 2


class GulpCollabType(IntEnum):
    """
    defines the type of collaboration object
    """

    NOTE = 0
    HIGHLIGHT = 1
    STORY = 2
    LINK = 3
    STORED_QUERY = 4
    STATS_INGESTION = 5
    STATS_QUERY = 6
    SHARED_DATA_SIGMA_GROUP_FILTER = 7
    SHARED_DATA_WORKFLOW = 8
    SHARED_DATA_GENERIC = 99


class CollabBase(MappedAsDataclass, AsyncAttrs, DeclarativeBase):
    """
    base class for all collaboration objects
    """

    pass


class GulpAssociatedEvent(BaseModel):
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


class GulpRequestStatus(IntEnum):
    """Gulp request status codes (used by the stats API)."""

    ONGOING = 0
    DONE = 1
    FAILED = 2
    CANCELED = 3
    DONE_WITH_ERRORS = 4


class GulpCollabFilter(BaseModel):
    """
    defines filter to be applied to all collaboration objects

    each filter specified as list, i.e. *operation_id*, matches as OR (one or more may match).<br>
    combining multiple filters (i.e. id + operation + ...) matches as AND (all must match).<br><br>

    for string fields, where not specified otherwise, the match is case-insensitive and supports SQL-like wildcards (% and _)<br>

    an empty filter returns all objects.<br>

    **NOTE**: not all fields are applicable to all types.
    """

    id: list[int] = Field(
        None, unique=True, description="filter by the given db row ID/s."
    )
    level: list[GulpCollabLevel] = Field(
        None, unique=True, description="filter by the given level/s."
    )
    owner_id: list[int] = Field(
        None, unique=True, description="filter by the given owner(user) ID/s."
    )
    name: list[str] = Field(
        None, unique=True, description="filter by the given object name (exact match)."
    )
    index: list[str] = Field(
        None,
        unique=True,
        description="filter by the given (elasticsearch) index name/s.",
    )
    text: list[str] = Field(
        None, unique=True, description="filter by the given object text."
    )
    operation_id: list[int] = Field(
        None, unique=True, description="filter by the given operation/s."
    )
    context: list[str] = Field(
        None, unique=True, description="filter by the given context/s."
    )
    src_file: list[str] = Field(
        None, unique=True, description="filter by the given source file/s."
    )
    type: list[GulpCollabType] = Field(
        None, unique=True, description="filter by the given type/s."
    )
    client_id: list[int] = Field(
        None, unique=True, description="filter by the given client id/s."
    )
    client_type: list["GulpClientType"] = Field(
        None, unique=True, description="filter by the given client type."
    )
    client_version: list[str] = Field(
        None, unique=True, description="filter by the given client version."
    )
    req_id: list[str] = Field(
        None, unique=True, description="filter by the given request id/s (exact match)."
    )
    status: list[GulpRequestStatus] = Field(
        None, unique=True, description="filter by the given status/es."
    )
    time_created_start: int = Field(
        None,
        description="creation timestamp range start (matches only AFTER a certain timestamp, inclusive, milliseconds from unix epoch).",
    )
    time_created_end: int = Field(
        None,
        description="creation timestamp range end  (matches only BEFORE a certain timestamp, inclusive, milliseconds from unix epoch).",
    )
    time_start: int = Field(
        None,
        description="timestamp range start (matches only AFTER a certain timestamp, inclusive, milliseconds from unix epoch).",
    )
    time_end: int = Field(
        None,
        description="timestamp range end  (matches only BEFORE a certain timestamp, inclusive, milliseconds from unix epoch).",
    )
    events: list[str] = Field(
        None,
        unique=True,
        description="filter by the given event ID/s in CollabObj.events list of CollabEvents.",
    )
    data: dict = Field(
        None,
        unique=True,
        description='filter by the given { "key": "value" } in data (exact match).',
    )
    tags: list[str] = Field(None, unique=True, description="filter by the given tag/s.")
    limit: int = Field(
        None,
        description='to be used together with "offset", maximum number of results to return. default=return all.',
    )
    offset: int = Field(
        None,
        description='to be used together with "limit", number of results to skip from the beginning. default=0 (from start).',
    )
    private_only: bool = Field(
        False,
        description="if True, return only private objects. Default=False (return all).",
    )
    opt_time_start_end_events: bool = Field(
        False,
        description="if True, time_start and time_end are relative to the CollabObj.events list of CollabEvents. Either, they refer to CollabObj.time_start and CollabObj.time_end as usual.",
    )
    opt_basic_fields_only: bool = Field(
        False,
        description="return only the basic fields (id, name, ...) for the collaboration object. Default=False (return all).",
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


class GulpUserPermission(IntFlag):
    """represent the permission of a user in the Gulp platform."""

    # can read only
    READ = 1
    # can edit own highlights, notes, stories, links
    EDIT = 2
    # can delete highlights, notes, stories, links
    DELETE = 4
    # can ingest files
    INGEST = 8
    # can do anything, including creating new users and change permissions
    ADMIN = 16
    # can monitor the system (READ + monitor)
    MONITOR = 32


PERMISSION_READ = GulpUserPermission.READ
PERMISSION_EDIT = GulpUserPermission.READ | GulpUserPermission.EDIT
PERMISSION_DELETE = (
    GulpUserPermission.READ | GulpUserPermission.EDIT | GulpUserPermission.DELETE
)
PERMISSION_INGEST = (
    GulpUserPermission.READ | GulpUserPermission.INGEST | GulpUserPermission.EDIT
)
PERMISSION_CLIENT = (
    GulpUserPermission.READ
    | GulpUserPermission.INGEST
    | GulpUserPermission.EDIT
    | GulpUserPermission.DELETE
)
PERMISSION_MONITOR = GulpUserPermission.READ | GulpUserPermission.MONITOR
