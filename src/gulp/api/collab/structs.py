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
    defines filter to be applied to all objects in the collaboration system
    """

    id: list[str] = Field(None, description="filter by the given id/s.")
    type: list[GulpCollabType] = Field(None, description="filter by the given type/s.")
    operation: list[str] = Field(None, description="filter by the given operation/s.")
    context: list[str] = Field(None, description="filter by the given context/s.")
    log_file_path: list[str] = Field(
        None, description="filter by the given source path/s or name/s."
    )
    user: list[str] = Field(None, description="filter by the given user(owner) id/s.")
    tags: list[str] = Field(None, description="filter by the given tag/s.")
    title: list[str] = Field(None, description="filter by the given title/s.")
    text: list[str] = Field(None, description="filter by the given object text.")
    events: list[str] = Field(
        None,
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
