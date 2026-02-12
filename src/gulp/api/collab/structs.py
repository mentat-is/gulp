"""
Defines the core database models and structures for the collaborative features in Gulp.

This module includes the base classes for all database objects in the collaboration system,
as well as utility classes for filtering, permissions, and object management.

Key components:
- GulpCollabBase: Abstract base class for all database objects
- GulpCollabFilter: Filter model for querying database objects
- Permission enums and constants for access control

The module implements a comprehensive access control system where objects can be:
- Private (only accessible to the owner)
- Shared with specific users via grants
- Shared with user groups
- Public (no specific grants)

Each object type is derived from these base classes and implements specific functionality
while inheriting common persistence and access control capabilities.

"""

# pylint: disable=too-many-lines
from contextlib import asynccontextmanager
import re
from enum import StrEnum
from typing import Annotated, List, Optional, TypeVar, override

import muty.crypto
import muty.string
import muty.time
from muty.log import MutyLogger
from psycopg import OperationalError
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import (
    ARRAY,
    BIGINT,
    Boolean,
    ColumnElement,
    Delete,
    ForeignKey,
    Select,
    String,
    Tuple,
    and_,
    column,
    delete,
    exists,
    func,
    insert,
    inspect,
    literal,
    or_,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import (
    DeclarativeBase,
    Load,
    Mapped,
    MappedAsDataclass,
    mapped_column,
    selectinload,
)
from sqlalchemy_mixins.serialize import SerializeMixin

from gulp.structs import GulpSortOrder, ObjectNotFound


class SessionExpired(Exception):
    """if the user session has expired"""


class WrongUsernameOrPassword(Exception):
    """if the user provides wrong username or password"""


class MissingPermission(Exception):
    """if the user does not have the required permission"""


class GulpRequestStatus(StrEnum):
    """Gulp request status codes."""

    ONGOING = "ongoing"
    DONE = "done"
    FAILED = "failed"
    CANCELED = "canceled"


class GulpUserPermission(StrEnum):
    """represent the permission of a user in the Gulp platform.

    a user can always read/edit/delete their own objects, but can only read other users' objects unless EDIT or DELETE permission is granted.
    """

    # can read only
    READ = "read"
    # can edit highlights, notes, stories, links
    EDIT = "edit"
    # can delete highlights, notes, stories, links
    DELETE = "delete"
    # can ingest data
    INGEST = "ingest"
    # can do anything, including creating new users and change permissions
    ADMIN = "admin"


PERMISSION_MASK_EDIT = [GulpUserPermission.READ, GulpUserPermission.EDIT]
PERMISSION_MASK_DELETE = [
    GulpUserPermission.READ,
    GulpUserPermission.EDIT,
    GulpUserPermission.DELETE,
]
PERMISSION_MASK_INGEST = [
    GulpUserPermission.READ,
    GulpUserPermission.INGEST,
    GulpUserPermission.EDIT,
]

"""
collaboration types (may be extended by plugins)
"""
COLLABTYPE_GENERIC = "collab_obj"
COLLABTYPE_NOTE = "note"
COLLABTYPE_HIGHLIGHT = "highlight"
COLLABTYPE_STORY = "story"
COLLABTYPE_LINK = "link"
COLLABTYPE_REQUEST_STATS = "request_stats"
COLLABTYPE_USER_DATA = "user_data"
COLLABTYPE_USER_SESSION = "user_session"
COLLABTYPE_CONTEXT = "context"
COLLABTYPE_USER = "user"
COLLABTYPE_GLYPH = "glyph"
COLLABTYPE_OPERATION = "operation"
COLLABTYPE_SOURCE = "source"
COLLABTYPE_USER_GROUP = "user_group"
COLLABTYPE_SOURCE_FIELD_TYPES = "source_fields"
COLLABTYPE_MAPPING_PARAMETERS = "mapping_parameters"
COLLABTYPE_FIELD_TYPES = "field_types_entries"
COLLABTYPE_QUERY_HISTORY = "query_history"
COLLABTYPE_TASK = "task"
COLLABTYPE_QUERY_GROUP_MATCH = "query_group_match"

T = TypeVar("T", bound="GulpCollabBase")


class GulpCollabFilter(BaseModel):
    """
    defines filter to be applied to all objects in the collaboration system.

    - custom fields can be provided in addition through model_extra, i.e. GulpCollabFilter(custom_list_field=["val1","val2"], custom_field="myval").
      if a list is provided, it will match if any of the values match (OR).
      Only string value is allowed
    - wildcards ("*", escape to match literal "*") are supported for most of the strings field except noted.
    - if "grant_user_ids" and/or "grant_user_group_ids" are provided, only objects with the defined grants will be returned.
    - all matches are case insensitive
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "ids": ["id1", "id2"],
                    "types": ["note", "highlight"],
                    "operation_ids": ["op1", "op2"],
                    "context_ids": ["ctx1", "ctx2"],
                    "source_ids": ["src1", "src2"],
                    "user_ids": ["admin"],
                    "tags": ["tag1", "tag2"],
                    "names": ["name1", "name2"],
                    "texts": ["text1", "text2"],
                    "time_pin_range": (1620000000000000000, 1620000000000000001),
                    "doc_ids": ["18b6332595d82048e31963e6960031a1"],
                    "doc_time_range": (1620000000000000000, 1620000000000000001),
                    "limit": 10,
                    "offset": 100,
                    "tags_and": False,
                    "sort": [("time_created", "ASC"), ("id", "ASC")],
                },
            ]
        },
    )

    ids: Annotated[list[str], Field(description="filter by the given id/s.")] = None
    types: Annotated[
        list[str],
        Field(
            description="filter by the given collaboration type/s.",
        ),
    ] = None
    operation_ids: Annotated[
        list[str], Field(description="filter by the given operation/s.")
    ] = None
    context_ids: Annotated[
        list[str], Field(description="filter by the given context/s.")
    ] = None
    source_ids: Annotated[
        list[str],
        Field(
            description="filter by the given source path/s or name/s.",
        ),
    ] = None
    user_ids: Annotated[
        list[str], Field(description="filter by the given owner user id/s.")
    ] = None
    tags: Annotated[list[str], Field(description="filter by the given tag/s (no wildcards accepted).")] = None
    names: Annotated[list[str], Field(description="filter by the given name/s.")] = None
    texts: Annotated[
        list[str],
        Field(
            description="filter by the given object text (wildcard accepted).",
        ),
    ] = None
    time_created_range: Annotated[
        tuple[int, int],
        Field(
            description="""
            if set, matches objects in a `CollabObject.time_created` range [start, end], inclusive, in milliseconds from unix epoch.
        """,
        ),
    ] = None
    time_pin_range: Annotated[
        tuple[int, int],
        Field(
            description="""
if set, matches objects in a `CollabObject.time_pin` range [start, end], inclusive, in nanoseconds from unix epoch.

- cannot be used with `doc_ids` or `doc_time_range`.
""",
        ),
    ] = None
    doc_ids: Annotated[
        list[str],
        Field(
            description="""
filter by the given document ID/s in a `CollabObject.docs` list of `GulpBasicDocument` or in a `CollabObject.doc_ids` list of document IDs.

- cannot be used with `time_pin_range` or `doc_time_range`.
""",
        ),
    ] = None
    doc_time_range: Annotated[
        tuple[int, int],
        Field(
            description="""
if set, a `gulp.timestamp` range [start, end] to match documents in a `CollabObject.docs`, inclusive, in nanoseconds from unix epoch.

- cannot be used with `time_pin_range` or `doc_ids`.
- works with Notes, does not work with Links
""",
        ),
    ] = None
    limit: Annotated[
        int,
        Field(
            description='to be used together with "offset", maximum number of results to return per call. default=return all.',
        ),
    ] = 0
    offset: Annotated[
        int,
        Field(
            description='to be used together with "limit", number of results to skip from the beginning. default=0 (from start).',
        ),
    ] = 0
    tags_and: Annotated[
        bool,
        Field(
            description="if True and `tags` set, all tags must match. Default=False (any tag matches).",
        ),
    ] = False
    sort: Annotated[
        list[tuple[str, GulpSortOrder]],
        Field(
            description="sort fields and order. Default=sort by `time_created` ASC, `id` ASC.",
        ),
    ] = None

    def is_empty(self) -> bool:
        """
        check if the filter is empty (no filtering criteria set)

        Returns:
            bool: True if the filter is empty, False otherwise
        """
        if not self.ids and not self.types and not self.operation_ids and not self.context_ids and not self.source_ids and not self.user_ids and not self.tags and not self.names and not self.texts and not self.time_created_range and not self.time_pin_range and not self.doc_ids and not self.doc_time_range and not self.model_extra:
            return True
        return False
    
    @override
    def __str__(self) -> str:
        return self.model_dump_json(exclude_none=True)

    def _bool_case(self, col, value: str) -> ColumnElement[bool]:
        """
        Convert string filter value to boolean.
        Accepts: 'true', 'false', '1', '0' (case-insensitive).

        Args:
            value: The string value to convert.

        Returns:
            bool: The converted boolean value.

        Raises:
            ValueError: If the value cannot be converted to a boolean.
        """
        normalized_value = None
        if isinstance(value, bool):
            normalized_value = value
        if isinstance(value, str):
            if value.lower() in ("true", "1", "yes"):
                normalized_value = True
            elif value.lower() in ("false", "0", "no"):
                normalized_value = False
        if normalized_value is None:
            raise ValueError(f"Cannot convert '{value}' to boolean")
        return col == normalized_value

    def _case_insensitive_or_ilike(self, col, values: list) -> ColumnElement[bool]:
        """
        Create a case-insensitive OR query for the given column and values.

        Args:
            col: The column to apply the ilike condition.
            values: The list of values to match against the column.

        Returns:
            ColumnElement[bool]: The OR query.
        """
        # print("column=%s, values=%s" % (column, values))
        # check if values in values contains wildcards as *, if so, replace with % for SQL LIKE operator (but consider only non-escaped *)
        values = [re.sub(r"(?<!\\)\*", "%", val) for val in values]
        conditions = [col.ilike(value) for value in values]
        return or_(*conditions)

    def _array_contains_all(self, array_field, values):
        """
        array containment check (ALL must match)
        """
        lowered_values = [val.lower() for val in values]
        conditions = []
        for val in lowered_values:
            subq = (
                select(literal(1))
                .select_from(func.unnest(array_field).alias("elem"))
                .where(func.lower(column("elem")) == val)
            )
            conditions.append(exists(subq))
        return and_(*conditions)

    def _array_contains_any(self, array_field, values):
        """
        array overlap check (ANY must match)
        """
        lowered_values = [val.lower() for val in values]

        # Unnest the array, compare each element in a simple WHERE condition
        subq = (
            select(literal(1))
            .select_from(func.unnest(array_field).alias("elem"))
            .where(func.lower(column("elem")).in_(lowered_values))
        )
        return exists(subq)

    def _to_query_internal(
        self, q: Select | Delete, obj_type: T
    ) -> Select[Tuple] | Delete[Tuple]:
        """
        convert the filter to a select or delete query

        Args:
            q (Select|Delete): the query to apply the filter to
            obj_type (T): the class of the object (one derived from GulpCollabBase, i.e. GulpNote)
        Returns:
            Select[Tuple]|Delete[Tuple]: the select or delete query
        """
        # disable not-an-iterable and non-subscribtable:
        # checks are in place and the pydantic model enforces the type
        # pylint: disable=E1133,E1136

        if self.ids:
            q = q.filter(self._case_insensitive_or_ilike(obj_type.id, self.ids))
        if self.types:
            # match if equal to any in the list
            q = q.filter(obj_type.type.in_(self.types))

        if self.operation_ids and "operation_id" in obj_type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(
                    obj_type.operation_id, self.operation_ids
                )
            )
        if self.context_ids and "context_id" in obj_type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(obj_type.context_id, self.context_ids)
            )
        if self.source_ids and "source_id" in obj_type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(obj_type.source_id, self.source_ids)
            )
        if self.user_ids and "user_id" in obj_type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(obj_type.user_id, self.user_ids)
            )

        if self.tags and "tags" in obj_type.columns:
            lower_tags = [tag.lower() for tag in self.tags]
            if self.tags_and:
                # all tags must match (CONTAINS operator)
                q = q.filter(self._array_contains_all(obj_type.tags, lower_tags))
            else:
                # at least one tag must match (OVERLAP operator)
                q = q.filter(self._array_contains_any(obj_type.tags, lower_tags))

        if self.names and "name" in obj_type.columns:
            q = q.filter(self._case_insensitive_or_ilike(obj_type.name, self.names))
        if self.texts and "text" in obj_type.columns:
            q = q.filter(self._case_insensitive_or_ilike(obj_type.text, self.texts))

        if self.model_extra:
            # check for granted_user_ids and granted_user_group_ids in model_extra
            #
            # if an object has no granted_user_ids or granted_user_group_ids, it is considered public and accessible to all users
            # either, the object is accessible to the user if at least one of the granted_user_ids matches the user_id
            granted_user_ids = self.model_extra.pop("granted_user_ids", None)
            granted_group_ids = self.model_extra.pop("granted_user_group_ids", None)
            # print("************************************************ filter: granted_user_ids=%s, granted_group_ids=%s" % (granted_user_ids, granted_group_ids))
            if granted_user_ids or granted_group_ids:
                # match only objects with the defined granted_user_ids or granted_group_ids
                conditions = []
                if granted_user_ids:
                    conditions.append(
                        obj_type.granted_user_ids.op("&&")(granted_user_ids)
                    )

                    # append condition that the column is empty or an empty array, as OR
                    conditions.append(obj_type.granted_user_ids is None)
                    conditions.append(obj_type.granted_user_ids == [])
                if granted_group_ids:
                    conditions.append(
                        obj_type.granted_user_group_ids.op("&&")(granted_group_ids)
                    )

                    # append condition that the column is empty or an empty array, as OR
                    conditions.append(obj_type.granted_user_group_ids is None)
                    conditions.append(obj_type.granted_user_group_ids == [])

                # Combine with OR
                if conditions:
                    q = q.filter(or_(*conditions))

            # process remaining model_extra fields (custom fields other than the GulpCollabFilter defined ones)
            for k, v in self.model_extra.items():
                if k in obj_type.columns:
                    col = getattr(obj_type, k)
                    if isinstance(v, str):
                        # single string, convert to list
                        v = [v]

                    # check if column type is ARRAY using SQLAlchemy's inspection
                    is_array = isinstance(getattr(col, "type", None), ARRAY)
                    is_bool = isinstance(getattr(col, "type", None), Boolean)
                    if is_array:
                        # if it's an array, use array overlap (matches if any element matches)
                        q = q.filter(self._case_insensitive_or_ilike(k, v))
                    elif is_bool:
                        # if it's a boolean column, convert string value to boolean and apply filter
                        q = q.filter(self._bool_case(col, v))
                    else:
                        # either a simple column, use case insensitive OR ilike match
                        q = q.filter(self._case_insensitive_or_ilike(col, v))
                    continue
                else:
                    raise ValueError("invalid filter field: %s" % k)

        if self.doc_ids and "doc_ids" in obj_type.columns:
            # return all collab objects that have at least one of the associated document "_id" (obj.doc_ids) in the given doc_ids list
            q = q.filter(obj_type.doc_ids.op("&&")(self.doc_ids))

        if self.time_pin_range and "time_pin" in obj_type.columns:
            # returns all collab objects that have time_pin in time_pin_range
            if self.time_pin_range[0]:
                q = q.where(obj_type.time_pin >= self.time_pin_range[0])
            if self.time_pin_range[1]:
                q = q.where(obj_type.time_pin <= self.time_pin_range[1])

        if self.time_created_range and "time_created" in obj_type.columns:
            # returns all collab objects that have time_created in time_created_range
            if self.time_created_range[0]:
                q = q.where(obj_type.time_created >= self.time_created_range[0])
            if self.time_created_range[1]:
                q = q.where(obj_type.time_created <= self.time_created_range[1])

        if self.doc_ids and "doc" in obj_type.columns:
            # returns all collab objects that have the associated document (obj.doc) with "_id" in doc_ids
            conditions = []
            for doc_id in self.doc_ids:
                # check if the document has _id matching doc_id
                # using ->> to extract the _id field from the JSONB doc object
                conditions.append(obj_type.doc["_id"].astext == doc_id.lower())
            q = q.filter(or_(*conditions))
        if self.doc_time_range and "doc" in obj_type.columns:
            # returns all collab objects that have the associated document (obj.doc) with "gulp.timestamp" in doc_time_range
            conditions = []
            if self.doc_time_range[0]:
                conditions.append(
                    func.cast(obj_type.doc["gulp.timestamp"].astext, BIGINT)
                    >= self.doc_time_range[0]
                )
            if self.doc_time_range[1]:
                conditions.append(
                    func.cast(obj_type.doc["gulp.timestamp"].astext, BIGINT)
                    <= self.doc_time_range[1]
                )
            q = q.filter(and_(*conditions))

        # add sort
        if not self.sort:
            # default, time_created ASC, id ASC
            order_clauses = [
                obj_type.time_created.asc(),
                obj_type.id.asc(),
            ]
        else:
            order_clauses = []
            for field, direction in self.sort:
                # check if field is a valid column in the object
                if hasattr(obj_type, field):
                    if direction == GulpSortOrder.ASC:
                        order_clauses.append(getattr(obj_type, field).asc())
                    else:
                        order_clauses.append(getattr(obj_type, field).desc())
                else:
                    MutyLogger.get_instance().warning(
                        "invalid sort field: %s. skipping this field.", field
                    )
        if isinstance(q, Select):
            q = q.order_by(*order_clauses)

        if self.limit:
            q = q.limit(self.limit)
        if self.offset:
            q = q.offset(self.offset)

        # MutyLogger.get_instance().debug(f"to_select_query: {q}")
        return q

    def to_select_query(self, obj_type: T) -> Select[Tuple]:
        """
        convert the filter to a select query

        Args:
            type (T): the type of the object (one derived from GulpCollabBase)

        Returns:
            Select[Tuple]: the select query
        """
        return self._to_query_internal(select(obj_type), obj_type)

    def to_delete_query(self, obj_type: T) -> Delete[Tuple]:
        """
        convert the filter to a delete query

        Args:
            type (T): the type of the object (one derived from GulpCollabBase)

        Returns:
            Delete[Tuple]: the delete query
        """
        return self._to_query_internal(delete(obj_type), obj_type)


class GulpCollabBase(DeclarativeBase, MappedAsDataclass, AsyncAttrs, SerializeMixin):
    """
    base for everything on the collab database
    """

    __abstract__ = True

    id: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        unique=True,
        doc="The unque id/name of the object.",
    )
    type: Mapped[str] = mapped_column(
        String, doc="The collaboration type for the object."
    )
    user_id: Mapped[str] = mapped_column(
        ForeignKey("user.id", ondelete="CASCADE"),
        doc="The id of the user who created the object (=the owner).",
    )
    name: Mapped[str] = mapped_column(String, doc="The display name of the object.")
    time_created: Mapped[int] = mapped_column(
        BIGINT,
        doc="The time the object was created, in milliseconds from unix epoch.",
    )
    time_updated: Mapped[int] = mapped_column(
        BIGINT,
        doc="The time the object was last updated, in milliseconds from unix epoch.",
    )
    operation_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey(
            "operation.id",
            ondelete="CASCADE",
        ),
        doc="The id of the operation associated with the object.",
    )
    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        doc="The glyph ID associated with the object.",
    )
    description: Mapped[Optional[str]] = mapped_column(
        String, doc="The description of the object."
    )
    tags: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="The tags associated with the object.",
    )
    color: Mapped[Optional[str]] = mapped_column(
        String, doc="The color associated with the object."
    )
    granted_user_ids: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="The ids of the users who have been granted access to the object. if not set(default), all users have access. either, the object is private to the listed user_id/s",
    )
    granted_user_group_ids: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="The ids of the user groups who have been granted access to the object. if not set(default), all groups have access.",
    )

    __mapper_args__ = {
        "polymorphic_identity": "collab_base",
        "polymorphic_on": type,
    }

    @classmethod
    def example(cls) -> dict:
        """
        builds example of the model

        Returns:
            dict: the model example
        """
        return {
            "obj_id": "id1",
            "type": cls.__tablename__,
            "operation_id": "op1",
            "tags": ["tag1", "tag2"],
            "color": "#FF0000",
            "owner_user_id": "admin",
            "granted_user_ids": ["user1"],
            "granted_user_group_ids": ["group1"],
            "time_created": 1620000000000000000,
            "time_updated": 1620000000000000001,
            "glyph_id": "glyph_id",
            "name": "the object display name",
            "description": "object description",
        }

    def __init_subclass__(cls, /, abstract: bool = False, **kwargs) -> None:
        """
        this is called automatically when a subclass is created, before __init__ on the instance is called

        Args:
            obj_type (str): The type of the object.
            abstract (bool): If True, the class is abstract
            **kwargs: Additional keyword arguments.
        """
        # print(f"__init_subclass__: cls={cls}, abstract={abstract}, kwargs={kwargs}")
        obj_type = kwargs.pop("type", None)
        cls.__gulp_collab_type__ = obj_type

        if abstract:
            # this is an abstract class
            cls.__abstract__ = True
        else:
            # set table name based on type
            cls.__tablename__ = obj_type

        cls.__mapper_args__ = {
            "polymorphic_identity": obj_type,
        }

        # print("type=%s, cls.__name__=%s, abstract=%r, cls.__abstract__=%r, cls.__mapper_args__=%s" % (cls.__gulp_collab_type__, cls.__name__, abstract, cls.__abstract__, cls.__mapper_args__))
        super().__init_subclass__(**kwargs)

    def __init__(self, *args, **kwargs):
        """
        Initialize the object with the specified attributes.
        """
        # MutyLogger.get_instance().debug("**** GulpCollabBase __init__")
        if self.__class__ == GulpCollabBase:
            # cannot instantiate this class directly
            raise TypeError(
                "GulpCollabBase is an abstract class and cannot be instantiated directly."
            )

        # call the base class constructor
        # MutyLogger.get_instance().debug("---> GulpCollabBase self in __init__=%s" % self)
        super().__init__()

    @classmethod
    def from_dict(cls, data: dict) -> T:
        """
        create an instance of the class from a dictionary

        Args:
            data (dict): the dictionary to create the instance from

        Returns:
            T: the created instance
        """
        # same as super().from_dict() but handles exclude_none parameter
        cols = {c.key for c in cls.__table__.columns}
        kwargs = {k: data.get(k) for k in cols}  # missing values set to None
        # print("********* from_dict() kwargs: ", kwargs)
        return cls(**kwargs)

    @override
    def to_dict(
        self,
        nested: bool = False,
        hybrid_attributes: bool = False,
        exclude: list[str] | None = None,
        exclude_none: bool = True,
    ) -> dict:
        # same as super.to_dict() but with exclude_none parameter
        d = super().to_dict(
            nested=nested, hybrid_attributes=hybrid_attributes, exclude=exclude
        )
        if not exclude_none:
            return d

        return {k: v for k, v in d.items() if v is not None}

    @classmethod
    def _build_eager_loading_options(
        cls, recursive: bool = False, max_depth: int = 3
    ) -> list[Load]:
        """
        build query options for eager loading relationships.

        Args:
            recursive (bool, optional): whether to traverse nested relationships. Defaults to False.
            max_depth (int, optional): maximum relationship depth to load when recursive is true.
              Defaults to 3 when recursive is true, otherwise 1.

        Returns:
            list[Load]: the list of selectinload options to apply
        Throws:
            None
        """
        # decide traversal depth based on the recursive flag
        depth: int = max_depth if recursive else 1

        # track unique attribute-paths to avoid duplicates
        seen_paths: set[tuple[str, ...]] = set()

        # list of built load options
        options: list[Load] = []

        # recursive dfs to collect attribute paths up to 'depth'
        def _walk(
            cur_cls: type, cur_depth: int, path_attrs: list[tuple[type, str]]
        ) -> None:
            """
            walk relationships and collect attribute paths.

            args:
              cur_cls (type): current sqlalchemy mapped class
              cur_depth (int): current depth in traversal
              path_attrs (list[tuple[type, str]]): sequence of (owner_class, rel_name)
            """
            # stop when reaching the configured depth
            if cur_depth >= depth:
                return

            # iterate relationships of the current class
            for rel in inspect(cur_cls).relationships:
                rel_name: str = rel.key
                owner_cls: type = cur_cls
                target_cls: type = rel.mapper.class_

                # prevent immediate cycles by checking if target repeats in the path
                parent_classes: set[type] = {owner for owner, _ in path_attrs}
                if target_cls in parent_classes:
                    continue

                # extend the current path
                new_path: list[tuple[type, str]] = [*path_attrs, (owner_cls, rel_name)]

                # convert path to attribute-name tuple to deduplicate
                path_key: tuple[str, ...] = tuple(name for _, name in new_path)
                if path_key not in seen_paths:
                    seen_paths.add(path_key)

                    # build the chained selectinload option for this path
                    # start with the first segment relative to the root class
                    load_opt: Load = selectinload(
                        getattr(new_path[0][0], new_path[0][1])
                    )
                    # chain the remaining segments
                    for seg_owner_cls, seg_rel_name in new_path[1:]:
                        load_opt = load_opt.selectinload(
                            getattr(seg_owner_cls, seg_rel_name)
                        )
                    options.append(load_opt)

                # continue walking from the target class
                _walk(target_cls, cur_depth + 1, new_path)

        # start walking from the root model class (depth starts at 0, no path yet)
        _walk(cls, 0, [])

        return options

    @classmethod
    def build_object_dict(
        cls,
        user_id: str,
        name: str = None,
        operation_id: str = None,
        glyph_id: str = None,
        description: str = None,
        tags: list[str] = None,
        color: str = None,
        obj_id: str = None,
        private: bool = True,
        granted_user_ids: list[str] = None,
        granted_user_group_ids: list[str] = None,
        **kwargs,
    ) -> dict:
        """
        build a dictionary to create a new object

        Args:
            user_id (str): The user to be set as the owner of the object.
            name (str, optional): The name of the object to be created. Defaults to None (set to `obj_id`).
            operation_id (str, optional): The ID of the operation associated with the instance. Defaults to None.
            glyph_id (str, optional): The glyph ID associated with the object to be created.
            description (str, optional): The description of the object to be created. Defaults to None
            tags (list[str], optional): The tags associated with the object to be created. Defaults
            color (str, optional): The color associated with the object to be created. Defaults to None.
            obj_id (str, optional): The ID of the object to create. Defaults to None (autogenerate)
            private (bool, optional): If True, the object is private (streamed only to ws_id websocket). Defaults to True.
            granted_user_ids (list[str], optional): List of user IDs to grant access to the object. Defaults to None (no specific user grants), ignored if private.
            granted_user_group_ids (list[str], optional): List of user group IDs to grant access. Defaults to None (no specific group grants), ignored if private.
            **kwargs: Additional attributes to include in the object dictionary

        Returns:
            dict: The dictionary to create the object with
        """
        if not obj_id:
            # generate a unique ID if not provided or None
            obj_id = muty.string.generate_unique()
        else:
            # check id is a valid string for a primary key (not having spaces, ...)
            if " " in obj_id or not re.match(r"^[a-zA-Z0-9_\-@\.]+$", obj_id):
                raise ValueError(f"invalid id: {obj_id}")

        # set the time created
        time_created = muty.time.now_msec()
        obj = {}
        obj["type"] = cls.__gulp_collab_type__
        obj["id"] = obj_id
        obj["name"] = name or obj_id
        obj["glyph_id"] = glyph_id
        obj["description"] = description
        obj["tags"] = tags or []
        obj["color"] = color
        obj["operation_id"] = operation_id
        obj["time_created"] = time_created
        obj["time_updated"] = time_created
        obj["user_id"] = user_id

        # by default, the object is public (no user/group grants)
        obj["granted_user_ids"] = []
        obj["granted_user_group_ids"] = []

        if granted_user_ids or granted_user_group_ids:
            # if grants are specified, private is overridden
            if private:
                MutyLogger.get_instance().warning(
                    "granted_user_ids or granted_user_group_ids set for object type=%s, private set but overridden!", cls.__gulp_collab_type__
                )

            if granted_user_ids:
                obj["granted_user_ids"] = granted_user_ids
            if granted_user_group_ids:
                obj["granted_user_group_ids"] = granted_user_group_ids
        else:
            # no grants specified, make the object private if asked
            if private:
                obj["granted_user_ids"] = [user_id]

        # add object from kwargs if not None and not already set
        for k, v in kwargs.items():
            if k in obj:
                continue
            if v is not None:
                obj[k] = v

        return obj

    @classmethod
    async def create_internal(
        cls,
        sess: AsyncSession,
        user_id: str,
        name: str = None,
        operation_id: str = None,
        glyph_id: str = None,
        description: str = None,
        tags: list[str] = None,
        color: str = None,
        obj_id: str = None,
        private: bool = True,
        granted_user_ids: list[str] = None,
        granted_user_group_ids: list[str] = None,
        ws_id: str = None,
        ws_data_type: str = None,
        ws_data: dict = None,
        req_id: str = None,
        extra_object_data: dict = None,
        on_conflict: str | None = None,
        return_conflict_status: bool = False,
        **kwargs,
    ) -> T:
        """
        Asynchronously creates and stores an instance of the class, also updating the websocket if required.

        NOTE: session is committed, and the returned object is **eager loaded**

        Args:
            sess (AsyncSession): The database session to use.
            user_id (str): The user to be set as the owner of the object.
            name (str, optional): The name of the object to be created. Defaults to None (autogenerate).
            operation_id (str, optional): The ID of the operation associated with the instance. Defaults to None.
            glyph_id (str, optional): The glyph ID associated with the object to be created. Defaults to None.
            description (str, optional): The description of the object to be created. Defaults to None
            tags (list[str], optional): The tags associated with the object to be created. Defaults to None.
            color (str, optional): The color associated with the object to be created. Defaults to None.
            obj_id (str, optional): The ID of the object to create. Defaults to None (generate a unique ID).
            private (bool, optional): If True, the object is private (streamed only to ws_id websocket). Defaults to True.
            granted_user_ids (list[str], optional): List of user IDs to grant access to the object. Defaults to None (no specific user grants), ignored if private.
            granted_user_group_ids (list[str], optional): List of user group IDs to grant access. Defaults to None (no specific group grants), ignored if private.
            ws_id (str, optional): The websocket id to send notification to. Defaults to None (no websocket notification).
            req_id (str, optional): Ignored if ws_id is None, the request ID to include in the websocket notification. Defaults to None.
            ws_data_type (str, optional): this is the type in `GulpWsData.type` sent on the websocket. Defaults to WSDATA_COLLAB_CREATE. Ignored if ws_id is not provided (used only for websocket notification).
            ws_data (dict, optional): value of GulpWsData.payload sent on the websocket. Defaults to the created object.
            extra_object_data (dict, optional): Additional data to include in the object dictionary, to avoid clash with main parameters passed explicitly (i.e. ws_id, ...). Defaults to None.
            on_conflict (str, optional): conflict handling mode. Supported modes: "do_nothing" (if a conflict occurs, do not insert and return the existing object). Defaults to None (no conflict handling, raise on conflict).
            return_conflict_status (bool, optional): whether to return a boolean indicating if the object was created or if a conflict occurred (only applicable if `on_conflict` is set). Defaults to False (only return the object).
            **kwargs: Additional attributes to include in the object.
        Returns:
            T: The created instance of the class.
            or, if on_conflict=="do_nothing" and a conflict occurs (i.e. an object with the same ID already exists):
            T: The existing instance of the class with the conflicting ID.
            bool: True if the object was created, False if a conflict occurred and the existing object was returned (only if `return_conflict_status` is True). 
            
        Raises:
            Exception: If there is an error during the creation or storage process.
        """
        # build object dictionary with necessary attributes
        d: dict = cls.build_object_dict(
            user_id=user_id,
            name=name,
            operation_id=operation_id,
            glyph_id=glyph_id,
            description=description,
            tags=tags,
            color=color,
            obj_id=obj_id,
            private=private,
            granted_user_ids=granted_user_ids,
            granted_user_group_ids=granted_user_group_ids,
            **kwargs,
        )

        if extra_object_data:
            # add extra data to the object dictionary, overriding existing keys
            d.update(extra_object_data)

        MutyLogger.get_instance().debug(
            "creating object in table=%s, dict=%s", cls.__tablename__, muty.string.make_shorter(str(d),max_len=260)
        )

        async with cls.advisory_lock(sess, obj_id):
            # insert the row using core insert so we don't instantiate the mapped class for persistence

            # allow optional conflict-handling modes (only supported mode for now: "do_nothing")
            if on_conflict == "do_nothing":
                insert_stmt = (
                    pg_insert(cls)
                    .values(**d)
                    .on_conflict_do_nothing(index_elements=[cls.id.name])
                    .returning(*cls.__table__.c)
                )
            else:
                insert_stmt = insert(cls).values(**d).returning(*cls.__table__.c)

            # execute the insert and obtain the inserted row (row==None may mean conflict when on_conflict=="do_nothing")
            res = await sess.execute(insert_stmt)
            row = res.fetchone()

            if not row:
                if on_conflict == "do_nothing":
                    # conflict => fetch existing instance and return it (keep consistent commit behaviour)
                    loading_options = cls._build_eager_loading_options()
                    instance = (
                        await sess.execute(
                            select(cls).options(*loading_options).where(cls.id == d["id"])
                        )
                    ).scalar_one_or_none()
                    if not instance:
                        raise Exception("failed to create or fetch object %s" % d["id"])
                    await sess.commit()
                    if return_conflict_status:
                        return instance, False
                    return instance
                # otherwise raise as before
                raise Exception("failed to insert object into table %s" % cls.__tablename__)

            # inserted successfully — re-query as ORM instance so relationships are loaded
            loading_options = cls._build_eager_loading_options()
            instance = (
                await sess.execute(
                    select(cls).options(*loading_options).where(cls.id == d["id"])
                )
            ).scalar_one()

            # commit the transaction after successful insert + re-query
            await sess.commit()

        from gulp.api.ws_api import GulpCollabCreatePacket, GulpRedisBroker

        if not ws_data_type:
            # default websocket data type for object creation
            from gulp.api.ws_api import WSDATA_COLLAB_CREATE

            ws_data_type = WSDATA_COLLAB_CREATE

        if not ws_data:
            # serialize this object (default)
            data = instance.to_dict(nested=True, exclude_none=True)
        else:
            # use provided data
            data = ws_data

        # notify websocket
        p = GulpCollabCreatePacket(obj=data)
        redis_broker = GulpRedisBroker.get_instance()
        await redis_broker.put(
            ws_data_type,
            user_id=user_id,
            ws_id=ws_id,
            operation_id=operation_id,
            req_id=req_id,
            d=p.model_dump(exclude_none=True, exclude_defaults=True),
            private=private,
        )

        if return_conflict_status:
            return instance, True
        return instance

    @classmethod
    async def create(
        cls,
        token: str,
        name: str = None,
        operation_id: str = None,
        glyph_id: str = None,
        description: str = None,
        tags: list[str] = None,
        color: str = None,
        permission: list[GulpUserPermission] = None,
        obj_id: str = None,
        private: bool = True,
        granted_user_ids: list[str] = None,
        granted_user_group_ids: list[str] = None,
        ws_id: str = None,
        ws_data_type: str = None,
        req_id: str = None,
        return_nested: bool = False,
        **kwargs,
    ) -> dict:
        """
        helper to create a new object, handling veryfication of token and permission, notifying the websocket if `ws_id` is provided (WSDATA_COLLAB_CREATE).

        NOTE: for internal object creation (not requiring token check), use `create_internal`

        Args:
            token (str): The user token, may be None if user_id is provided (internal request, assumes permissions are already checked).
            name (str, optional): The name of the object to be created. Defaults to None (autogenerate).
            operation_id (str, optional): The ID of the operation associated with the object to be created: if set, it will be checked for permission.
            glyph_id (str, optional): The glyph ID associated with the object to be created. Defaults to None.
            description (str, optional): The description of the object to be created. Defaults to None
            tags (list[str], optional): The tags associated with the object to be created. Defaults to None.
            color (str, optional): The color associated with the object to be created. Defaults to None.
            permission (list[GulpUserPermission], optional): The list of permissions required to create the object. Defaults to [GulpUserPermission.EDIT].
            obj_id (str, optional): The ID of the object to create. Defaults to None (generate a unique ID).
            private (bool, optional): If True, the object will be private (can be seen only by the creator=owner or admin). Defaults to True.
            granted_user_ids (list[str], optional): List of user IDs to grant access to the object. Defaults to None (no specific user grants), ignored if private.
            granted_user_group_ids (list[str], optional): List of user group IDs to grant access. Defaults to None (no specific group grants), ignored if private.
            ws_id (str, optional): The websocket ID: pass None to not notify the websocket.
            ws_data_type (str, optional): this is the type in `GulpWsData.type` sent on the websocket. Defaults to WSDATA_COLLAB_CREATE. Ignored if ws_id is not provided (used only for websocket notification) or if token is None (internal request).
            req_id (str, optional): The request ID, may be None for internal requests, ignored if ws_id is None.
            return_nested (bool, optional): If True, pass nested=True when converting to dictionary before returning. Defaults to False.
            **kwargs: Additional field=value keypairs to be set on the object
        Returns:
            dict: The created object as a dictionary.

        Raises:
            MissingPermission: If the user does not have permission to create the object.
        """

        from gulp.api.collab.user_session import GulpUserSession
        from gulp.api.collab_api import GulpCollab

        async with GulpCollab.get_instance().session() as sess:
            if not permission:
                permission = [GulpUserPermission.EDIT]

            # check permission for creation
            if operation_id:
                # check permission on the operation
                from gulp.api.collab.operation import GulpOperation

                op: GulpOperation = await GulpOperation.get_by_id(sess, operation_id)
                s = await GulpUserSession.check_token(
                    sess, token, permission=permission, obj=op
                )
            else:
                # just check token permission
                s = await GulpUserSession.check_token(
                    sess, token, permission=permission
                )

            # create (and commit) the object
            user_id = s.user.id
            n: GulpCollabBase = await cls.create_internal(
                sess,
                user_id=user_id,
                name=name,
                operation_id=operation_id,
                glyph_id=glyph_id,
                description=description,
                tags=tags,
                color=color,
                obj_id=obj_id,
                ws_id=ws_id,
                ws_data_type=ws_data_type,
                req_id=req_id,
                private=private,
                granted_user_ids=granted_user_ids,
                granted_user_group_ids=granted_user_group_ids,
                **kwargs,
            )
            nn = n.to_dict(exclude_none=True, nested=return_nested)
            return nn

    async def update(
        self,
        sess: AsyncSession,
        ws_id: str = None,
        req_id: str = None,
        user_id: str = None,
        ws_data: dict = None,
        ws_data_type: str = None,
        **kwargs,
    ) -> dict:
        """
        updates the object, notifying the websocket if `ws_id` is provided (WSDATA_COLLAB_UPDATE).

        NOTE: session is committed

        Args:
            sess (AsyncSession): The database session to use: the session will be committed and refreshed after the update.
            ws_id (str, optional): The ID of the websocket connection to send update to the websocket. Defaults to None (no update will be sent)
            req_id (str, optional): The ID of the request to include in the websocket notification. Defaults to None (ignored if ws_id is None).
            user_id (str, optional): The ID of the user making the request. Defaults to None, ignored if ws_id is not provided (used only for websocket notification).
            ws_data (dict, optional): this is the data sent in `GulpWsData.payload` on the websocket after the object has been updated on database. Defaults to the (serialized) updated object itself. Ignored if ws_id is not provided.
            ws_data_type (str, optional): this is the type in `GulpWsData.type` sent on the websocket. Defaults to WSDATA_COLLAB_UPDATE. Ignored if ws_id is not provided (used only for websocket notification).
            **kwargs: additional fields to set on the object (existing values will be overwritten, None values will be ignored)
        Returns:
            dict: The updated object as a dictionary.
        """
        async with self.__class__.advisory_lock(sess, self.id):
            kwargs.pop("id", None)  # id cannot be updated

            # update vaues skipping None
            for k, v in kwargs.items():
                # only update if the value is not None and different from the current value
                if v is not None and getattr(self, k, None) != v:
                    # MutyLogger.get_instance().debug(f"setattr: {k}={v}")
                    setattr(self, k, v)

            # ensure time_updated is set
            self.time_updated = muty.time.now_msec()

            private = self.is_private()
            updated_dict = self.to_dict(nested=True, exclude_none=True)

            # commit
            await sess.commit()
            MutyLogger.get_instance().debug(
                "---> updated (type=%s, ws_id=%s): %s",
                self.type,
                ws_id,
                muty.string.make_shorter(str(updated_dict),max_len=260)
            )

        # send update to websocket
        from gulp.api.ws_api import GulpCollabUpdatePacket, GulpRedisBroker

        if not ws_data_type:
            # default to collab update
            from gulp.api.ws_api import WSDATA_COLLAB_UPDATE

            ws_data_type = WSDATA_COLLAB_UPDATE

        # notify the websocket of the collab object update
        if ws_data:
            # use provided dict
            data = ws_data
        else:
            # use the object itself
            data = updated_dict

        p = GulpCollabUpdatePacket(obj=data)
        redis_broker = GulpRedisBroker.get_instance()
        await redis_broker.put(
            t=ws_data_type,
            ws_id=ws_id,
            user_id=user_id,
            operation_id=data.get("operation_id", None),
            req_id=req_id,
            d=p.model_dump(exclude_none=True, exclude_defaults=True),
            private=private,
        )
        return updated_dict

    async def delete(
        self,
        sess: AsyncSession,
        ws_id: str = None,
        req_id: str = None,
        user_id: str = None,
        ws_data_type: str = None,
        ws_data: dict = None,
        raise_on_error: bool = True,
    ) -> None:
        """
        deletes the object, also updating the websocket if required.

        Args:
            sess (AsyncSession): The database session to use.
            user_id (str, optional): The ID of the user making the request. Defaults to None (ignored if ws_id is None)
            ws_id (str, optional): id of the websocket to send WS_COLLAB_DELETE notification to. Defaults to None (no websocket notification).
            req_id (str, optional): The ID of the request to include in the websocket notification. Defaults to None (ignored if ws_id is None).
            user_id (str, optional): The ID of the user making the request, only used if ws_id is set. Defaults to None.
            ws_data_type (str, optional): value of GulpWsData.type sent to the websocket: if not set, WSDATA_COLLAB_DELETE will be used.
            ws_data (dict, optional): payload to send to the websocket: if not set, a GulpDeleteCollabPacket with object id will be sent.
            raise_on_error (bool): Whether to raise an exception on error. Defaults to True.
        Returns:
            None
        """
        obj_id: str = self.id
        operation_id: str = self.operation_id
        try:
            async with self.__class__.advisory_lock(sess, obj_id):
                MutyLogger.get_instance().debug(
                    "deleting object %s.%s, operation=%s, ws_id=%s",
                    self.__class__,
                    obj_id,
                    operation_id,
                    ws_id
                )
                await sess.delete(self)
                await sess.commit()
        except Exception as e:
            MutyLogger.get_instance().error(e)
            if raise_on_error:
                raise e
            else:
                # swallow exception, manually rollback
                await sess.rollback()
                return None

        # notify the websocket of the deletion
        from gulp.api.ws_api import GulpCollabDeletePacket, GulpRedisBroker

        if not ws_data_type:
            from gulp.api.ws_api import WSDATA_COLLAB_DELETE

            ws_data_type = WSDATA_COLLAB_DELETE

        if ws_data:
            data = ws_data
        else:
            p: GulpCollabDeletePacket = GulpCollabDeletePacket(id=obj_id, type=self.type)
            data = p.model_dump()

        MutyLogger.get_instance().debug(
            "notifying websocket of deletion: type=%s, ws_id=%s, data=%s",
            ws_data_type,
            ws_id,
            muty.string.make_shorter(str(data),max_len=260)
        )
        redis_broker = GulpRedisBroker.get_instance()
        await redis_broker.put(
            t=ws_data_type,
            ws_id=ws_id,
            user_id=user_id,
            operation_id=operation_id,
            req_id=req_id,
            d=data,
        )

    @classmethod
    async def delete_by_filter(
        cls,
        sess: AsyncSession,
        flt: GulpCollabFilter = None,
        user_id: str = None,
        throw_if_not_found: bool = True,
    ) -> int:
        """
        Asynchronously deletes objects based on the provided filter.

        Args:
            sess (AsyncSession): The database session to use.
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None (all objects).
            user_id (str, optional): the caller user_id: if set, only return objects that the user has access to.
        Returns:
            int: The number of objects deleted.
        Raises:
            Exception: If there is an error during the query execution or result processing.
        """
        # build filter for the user
        flt: GulpCollabFilter = await GulpCollabBase._restrict_flt_to_user(
            sess, user_id, flt
        )
        q = flt.to_delete_query(cls)
        MutyLogger.get_instance().debug(
            "delete_by_filter, flt=%s, user_id=%s, query:\n%s", flt, user_id, q
        )
        res = await sess.execute(q)
        if not res or res.rowcount == 0:
            if throw_if_not_found:
                raise ObjectNotFound(
                    f"No {cls.__name__} found with filter {flt}", cls.__name__, str(flt)
                )
            return 0

        # commit the transaction
        deleted_count: int = res.rowcount
        await sess.commit()
        MutyLogger.get_instance().debug(
            "user_id=%s, deleted %d objects", user_id, deleted_count
        )
        return deleted_count

    @classmethod
    async def delete_by_id_internal(
        cls,
        sess: AsyncSession,
        obj_id: str,
        throw_on_error: bool=True
    ) -> None:
        """
        to delete an object by ID, without permission checks or websocket notification

        NOTE: internal usage only

        Args:
            sess (AsyncSession): The database session to use.
            obj_id (str): The ID of the object to delete.
            throw_on_error (bool): If True, raise an exception if the object is not found or deletion fails. Defaults to True.
        Raises:
            ObjectNotFoundError: If the object is not found.
            Exception: If there is an error during deletion.
        """
        obj: GulpCollabBase = await cls.get_by_id(sess, obj_id, throw_if_not_found=throw_on_error)
        if obj:
            await obj.delete(sess, raise_on_error=throw_on_error)

    @classmethod
    async def delete_by_id_wrapper(
        cls,
        token: str,
        obj_id: str,
        permission: list[GulpUserPermission] | GulpUserPermission = None,
        ws_id: str = None,
        req_id: str = None,
        ws_data_type: str = None,
        ws_data: dict = None,
        enforce_owner: bool = False,
    ) -> None:
        """
        helper to delete an object by ID, handling session and ACL check

        Args:
            token (str): The user token
            obj_id (str): The ID of the object to delete: if an `operation_id` is associated with the object, it will be checked for READ access by the user's token
            permission (list[GulpUserPermission]|GulpUserPermission, optional): The permission required to delete the object. Defaults to GulpUserPermission.DELETE.
            ws_id (str, optional): id of the websocket to send WS_COLLAB_DELETE notification to. Defaults to None (no websocket notification).
            req_id (str, optional): The ID of the request to include in the websocket notification. Defaults to None (ignored if ws_id is None).
            ws_data_type (str, optional): value of GulpWsData.type sent to the websocket: if not set, WSDATA_COLLAB_DELETE will be used.
            ws_data (dict, optional): data to send in GulpWsData.payload on the websocket: if not set, a GulpDeleteCollabPacket with object id will be sent.
            enforce_owner (bool, optional): If True, only the owner of the object (or admin) can delete it. Defaults to False.
        Raises:
            MissingPermission: If the user does not have permission to delete the object.
            ObjectNotFoundError: If the object is not found.
        """
        from gulp.api.collab.user_session import GulpUserSession
        from gulp.api.collab_api import GulpCollab

        if not permission:
            permission = [GulpUserPermission.DELETE]

        async with GulpCollab.get_instance().session() as sess:
            try:
                s: GulpUserSession
                obj: GulpCollabBase = await cls.get_by_id(sess, obj_id)
                if obj.operation_id:
                    # check operation access first
                    from gulp.api.collab.operation import GulpOperation

                    s, _, _ = await GulpOperation.get_by_id_wrapper(
                        sess,
                        token,
                        obj.operation_id,
                        permission=GulpUserPermission.READ,
                    )
                    # and object access
                    await s.check_permissions(
                        sess, permission, obj, enforce_owner=enforce_owner
                    )
                else:
                    # just check object access
                    s = await GulpUserSession.check_token(
                        sess,
                        token,
                        permission=permission,
                        obj=obj,
                        enforce_owner=enforce_owner,
                    )

                # delete
                await obj.delete(
                    sess,
                    ws_id=ws_id,
                    req_id=req_id,
                    user_id=s.user.id,
                    ws_data_type=ws_data_type,
                    ws_data=ws_data,
                )
            except:
                raise

    async def add_default_grants(self, sess: AsyncSession):
        """
        shortcut to add default user and groups grants to the object

        NOTE: should be used only when resetting the database.

        Args:
            sess (AsyncSession): The session to use.
        """
        # add user grants, admin and guest are guaranteed to exist (cannot be deleted)
        await self.add_user_grant(sess, "admin")
        await self.add_user_grant(sess, "guest")

        # add group grants
        from gulp.api.collab.user_group import ADMINISTRATORS_GROUP_ID

        await self.add_group_grant(sess, ADMINISTRATORS_GROUP_ID)

    async def add_group_grant(self, sess: AsyncSession, group_id: str) -> None:
        """
        grant a user group access to the object

        Args:
            sess (AsyncSession): The database session to use.
            group_id (str): The ID of the user group to add.
        Returns:
            None
        Raises:
            ObjectNotFound: If the user group does not exist.
        """
        # will except if the group do not exist!
        from gulp.api.collab.user_group import GulpUserGroup

        async with self.__class__.advisory_lock(sess, self.id):
            await GulpUserGroup.get_by_id(sess, group_id)
            if group_id in self.granted_user_group_ids:
                MutyLogger.get_instance().warning(
                    "user group %s already granted on object %s", group_id, self.id
                )
                return

            MutyLogger.get_instance().debug(
                "adding granted user group %s to object %s", group_id, self.id
            )
            self.granted_user_group_ids.append(group_id)
            await sess.commit()

    async def remove_group_grant(self, sess: AsyncSession, group_id: str) -> None:
        """
        remove a user group access to the object

        Args:
            sess (AsyncSession): The database session to use.
            group_id (str): The ID of the user group to remove.
        Returns:
            None
        Raises:
            ObjectNotFound: If the user group does not exist.
        """
        # will except if the group do not exist!
        from gulp.api.collab.user_group import GulpUserGroup

        async with self.__class__.advisory_lock(sess, self.id):
            await GulpUserGroup.get_by_id(sess, group_id)
            if group_id not in self.granted_user_group_ids:
                MutyLogger.get_instance().warning(
                    "user group %s not in granted list on object %s", group_id, self.id
                )
                return

            MutyLogger.get_instance().info(
                "removing granted user group %s from object %s", group_id, self.id
            )

            self.granted_user_group_ids.remove(group_id)
            await sess.commit()

    async def add_user_grant(self, sess: AsyncSession, user_id: str) -> None:
        """
        grant a user access to the object

        Args:
            sess (AsyncSession): The session to use for the query.
            user_id (str): The ID of the user to add.
        Returns:
            None
        Raises:
            ObjectNotFound: If the user does not exist.
        """
        # will except if the user do not exist!
        from gulp.api.collab.user import GulpUser

        async with self.__class__.advisory_lock(sess, self.id):
            await GulpUser.get_by_id(sess, user_id)
            if user_id in self.granted_user_ids:
                MutyLogger.get_instance().warning(
                    "user %s already granted on object %s", user_id, self.id
                )
                return

            MutyLogger.get_instance().debug(
                "adding granted user %s to object %s", user_id, self.id
            )

            self.granted_user_ids.append(user_id)
            await sess.commit()

    async def remove_user_grant(self, sess: AsyncSession, user_id: str) -> None:
        """
        remove a user access to the object

        Args:
            sess (AsyncSession): The session to use for the query.
            user_id (str): The ID of the user to remove.
        Returns:
            None
        Raises:
            ObjectNotFound: If the user does not exist.
        """

        # will except if the user do not exist!
        from gulp.api.collab.user import GulpUser

        async with self.__class__.advisory_lock(sess, self.id):
            await GulpUser.get_by_id(sess, user_id)

            if user_id not in self.granted_user_ids:
                MutyLogger.get_instance().warning(
                    "user %s not in granted list on object %s", user_id, self.id
                )
                return

            MutyLogger.get_instance().info(
                "removing granted user %s from object %s", user_id, self.id
            )
            self.granted_user_ids.remove(user_id)
            await sess.commit()

    def is_owner(self, user_id: str) -> bool:
        """
        check if the user is the owner of the object

        Args:
            user_id (str): The ID of the user to check.
        Returns:
            bool: True if the user is the owner, False otherwise.
        """
        return self.user_id == user_id

    def is_granted_user(self, user_id: str) -> bool:
        """
        check if the user is granted access to the object

        Args:
            user_id (str): The ID of the user to check.
        Returns:
            bool: True if the user is granted access, False otherwise.
        """
        return user_id in self.granted_user_ids

    def is_granted_group(self, group_id: str) -> bool:
        """
        check if the user group is granted access to the object

        Args:
            group_id (str): The ID of the user group to check.
        Returns:
            bool: True if the user group is granted access, False otherwise.
        """
        return group_id in self.granted_user_group_ids

    def is_private(self) -> bool:
        """
        check if the object is private (only the owner or admin can see it)

        Returns:
            bool: True if the object is private, False otherwise.
        """
        # private object = only owner or admin can see it
        if (
            self.granted_user_ids
            and len(self.granted_user_ids) == 1
            and self.granted_user_ids[0] == self.user_id
            and not self.granted_user_group_ids
        ):
            return True
        return False

    async def make_private(self, sess: AsyncSession) -> None:
        """
        make the object private (only the owner or admin can see it)

        Args:
            sess (AsyncSession): The database session to use.
            user_id (str): The ID of the user making the request.
        Returns:
            None
        """

        # private object = only owner or admin can see it
        async with self.__class__.advisory_lock(sess, self.id):
            self.granted_user_ids = [self.user_id]
            self.granted_user_group_ids = []
            await sess.commit()
            MutyLogger.get_instance().info(
                "object %s is now PRIVATE to user %s", self.id, self.user_id
            )

    async def make_public(self, sess: AsyncSession) -> None:
        """
        make the object public

        Args:
            sess (AsyncSession): The database session to use.
            user_id (str): The ID of the user making the request.
        Returns:
            None
        """
        async with self.__class__.advisory_lock(sess, self.id):
            # clear all granted users and groups
            self.granted_user_group_ids = []
            self.granted_user_ids = []
            await sess.commit()
            MutyLogger.get_instance().info("object %s is now PUBLIC", self.id)

    @staticmethod
    def object_type_to_class(collab_type: str) -> T:
        """
        get the class of the given type

        Args:
            collab_type (str): The type of the object.
        Returns:
            Type: The class of the object.

        Raises:
            ValueError: If the class is not found.
        """
        subclasses = GulpCollabBase.__subclasses__()
        for cls in subclasses:
            if cls.__gulp_collab_type__ == collab_type:
                return cls
        raise ValueError(f"no class found for collab type={collab_type}")

    @classmethod
    def is_model_registered(cls, model_name: str) -> bool:
        """
        Check whether a mapped model with the given class name is registered
        in the Declarative registry for GulpCollabBase.

        Returns:
            bool: True if the model is registered, False otherwise.
        """
        try:
            return model_name in cls.registry._class_registry
        except Exception:
            return False

    @classmethod
    async def acquire_advisory_lock(cls, sess: AsyncSession, obj_id: str) -> None:
        """
        Acquire an advisory lock

        NOTE: the lock is held until sess.commit() or sess.rollback() is called!

        Args:
            sess (AsyncSession): The database session to use.
            obj_id (str): The ID of the object to lock.
        """
        lock_id = muty.crypto.hash_xxh64_int(f"{cls.__name__}-{obj_id}")
        try:
            await sess.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"), {"lock_id": lock_id}
            )
        except OperationalError as e:
            MutyLogger.get_instance().error("failed to acquire advisory lock: %s", e)
            raise e

    @classmethod
    @asynccontextmanager
    async def advisory_lock(cls, sess: AsyncSession, obj_id: str):
        """Context manager that acquires an advisory lock and guarantees rollback
        on exception so the transaction-scoped advisory lock is always released.

        Usage:
            async with MyModel.advisory_lock(sess, obj_id):
                # protected work

        This preserves the existing `acquire_advisory_lock` API for callers that
        need the raw lock call.
        """
        await cls.acquire_advisory_lock(sess, obj_id)
        try:
            yield
        except Exception:
            # ensure transaction-scoped advisory lock is released on error
            await sess.rollback()
            raise

    @staticmethod
    async def get_obj_id_by_table(
        sess: AsyncSession,
        table_name: str,
        obj_id: str,
        throw_if_not_found: bool = True,
        recursive: bool = False,
    ) -> T:
        """
        asynchronously retrieves an object from specified table (eager loaded) with the given id.

        args:
            sess (AsyncSession): the database session to use
            table_name (str): the name of the table to query
            obj_id (str): the id of the object to retrieve
            throw_if_not_found (bool): if true, raises exception if not found (default=true)
            recursive (bool): load nested relationships recursively (default=false)

        returns:
            T: the object or none if not found

        raises:
            ObjectNotFound: if object not found and throw_if_not_found=true
            ValueError: if table name is invalid or class not found
        """
        # get model class from table name
        model_class = GulpCollabBase.object_type_to_class(table_name)
        c = await model_class.get_by_id(
            sess,
            obj_id,
            throw_if_not_found=throw_if_not_found,
            recursive=recursive,
        )
        return c

    @classmethod
    async def get_by_id(
        cls,
        sess: AsyncSession,
        obj_id: str,
        throw_if_not_found: bool = True,
        recursive: bool = False,
    ) -> T:
        """
        Asynchronously retrieves an object (eager loaded) of the class type with the specified ID.

        Args:
            sess (AsyncSession): The database session to use.
            obj_id (str): The ID of the object to retrieve.
            throw_if_not_found (bool, optional): If True, raises an exception if the object is not found. Defaults to True.
            recursive (bool, optional): If True, loads nested relationships recursively. Defaults to False.
        Returns:
            T: The object with the specified ID or None if not found
        Raises:
            ObjectNotFound: If the object with the specified ID is not found and throw_if_not_found is set
        """
        loading_options = cls._build_eager_loading_options(recursive=recursive)

        stmt = select(cls).options(*loading_options).filter(cls.id == obj_id)
        res = await sess.execute(stmt)
        c = res.scalar_one_or_none()
        if not c:
            if throw_if_not_found:
                raise ObjectNotFound(f'{cls.__name__} with id "{obj_id}" not found')

        return c

    @classmethod
    async def get_by_id_wrapper(
        cls,
        sess: AsyncSession,
        token: str,
        obj_id: str,
        permission: list[GulpUserPermission] | GulpUserPermission = None,
        enforce_owner: bool = False,
        recursive: bool = False,
    ) -> tuple["GulpUserSession", T, "GulpOperation":None]:
        """
        helper to get an object by ID and the GulpUserSession, after checking if the token has the required permission
        
        if `obj_id` have an `operation_id` set, the token will be checked for READ access on the operation first.
        
        Args:
            sess (AsyncSession): The database session to use.
            token (str): The user token.
            obj_id (str): The ID of the object to get.
            permission (list[GulpUserPermission]|GulpUserPermission, optional): The permission required to access `obj_id`. Defaults to GulpUserPermission.READ.
            enforce_owner (bool, optional): If True, enforce that the token belongs to the owner of the object (or the user is admin). Defaults to False.
            recursive (bool, optional): If True, loads nested relationships recursively. Defaults to False.
        Returns:
            tuple[GulpUserSession, T, GulpOperation]: The user session, the object, and the operation (if any, and ensuring token can access it) associated with the object.
        Raises:
            MissingPermission: If the user does not have permission to read the object.
            ObjectNotFound: If the object is not found.
        """
        from gulp.api.collab.user_session import GulpUserSession

        if not permission:
            permission = [GulpUserPermission.READ]

        # get session
        s: GulpUserSession = await GulpUserSession.get_by_id(
            sess,
            token,
        )
        # get object
        from gulp.api.collab.operation import GulpOperation

        obj: GulpCollabBase = await cls.get_by_id(sess, obj_id, recursive=recursive)
        op: GulpOperation = None
        if obj.operation_id:
            # get operation and check read access on it
            op: GulpOperation = await GulpOperation.get_by_id(sess, obj.operation_id)
            await s.check_permissions(sess, permission=GulpUserPermission.READ, obj=op)

        # check object access
        await s.check_permissions(
            sess, permission=permission, obj=obj, enforce_owner=enforce_owner
        )

        # done
        return s, obj, op

    @staticmethod
    async def _restrict_flt_to_user(
        sess: AsyncSession, user_id: str = None, flt: GulpCollabFilter = None
    ) -> GulpCollabFilter:
        """
        helper to restrict (set the granted_user_ids and granted_user_group_ids on) the filter based on the user_id provided (if any)

        with grants set, the filter will return only objects that the user has access to (or all objects if user is admin)

        Args:

            sess (AsyncSession): the database session to use
            user_id (str, optional): if set, only return objects that the user has access to. either, admin user is assumed (all objects access)
            flt (GulpCollabFilter, optional): the filter to modify, if None an empty filter is created. Defaults to None.
        Returns:
            GulpCollabFilter: the modified filter
        """

        # filter or empty filter
        flt = flt or GulpCollabFilter()

        is_admin = False
        group_ids = []
        if user_id:
            # get user info (its groups and admin status)
            from gulp.api.collab.user import GulpUser

            u: GulpUser = await GulpUser.get_by_id(sess, user_id)
            is_admin = u.is_admin()
            group_ids = [g.id for g in u.groups] if u.groups else []
            MutyLogger.get_instance().debug("building filter for user_id=%s, is admin=%r, group_ids=%s", user_id, is_admin, group_ids)
        else:
            # no user_id provided, assume admin
            MutyLogger.get_instance().debug("building filter for admin user (no user_id provided)")
            is_admin = True

        # build and run query (ensure eager loading)
        if is_admin:
            # admin must see all
            flt.granted_user_ids = None
            flt.granted_user_group_ids = None
        else:
            # user can see only objects he has access to
            flt.granted_user_ids = [user_id]
            flt.granted_user_group_ids = group_ids
        return flt

    @classmethod
    async def get_by_filter(
        cls,
        sess: AsyncSession,
        flt: GulpCollabFilter = None,
        throw_if_not_found: bool = True,
        user_id: str = None,
        recursive: bool = False,
        first_only: bool = False,
    ) -> list[T]:
        """
        retrieves objects based on the provided filter.

        NOTE: depending on the filter, this may return a large number of objects so using pagination is recommended.

        Args:
            sess (AsyncSession): The database session to use.
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None (all objects).
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to True.
            user_id (str, optional): if set, only return objects that the user has access to. defaults to None (no user filter, return all objects).
            recursive (bool, optional): If True, loads nested relationships recursively for the returned objects. Defaults to False.
            first_only (bool, optional): If True, returns a list with only the first object found (or empty list if none found). Defaults to False (return all matching objects).
        Returns:
            list[T]: A list of objects that match the filter criteria.
        Raises:
            Exception: If there is an error during the query execution or result processing.
        """
        # build filter for the user
        flt: GulpCollabFilter = await GulpCollabBase._restrict_flt_to_user(
            sess, user_id, flt
        )
        
        # MutyLogger.get_instance().debug("pre to_select_query: user=%s, admin=%r, groups=%s, flt=%s", u.to_dict(), is_admin, group_ids, flt)
        q = flt.to_select_query(cls)
        q = q.options(*cls._build_eager_loading_options(recursive=recursive))
        # MutyLogger.get_instance().debug(
        #     "get_by_filter, user_id=%s, is_admin=%r, flt=%s, query:\n%s"
        #     % (user_id, is_admin, flt, q)
        # )
        objects: list[T] = []
        try:
            res = await sess.execute(q)
            if first_only:
                obj = res.scalars().first()
                objects.append(obj)  # will get None if no object found
            else:
                objects = res.scalars().all()
            if not objects:
                if throw_if_not_found:
                    raise ObjectNotFound(
                        f"No {cls.__name__} found with filter {str(flt)}"
                    )

            # MutyLogger.get_instance().debug("user_id=%s, POST-filtered objects: %s", user_id, objects)
            return objects
        except Exception as e:
            MutyLogger.get_instance().error(
                "error in get_by_filter, user_id=%s, flt=%s: %s",
                user_id,
                str(flt),
                e,
            )
            if throw_if_not_found:
                raise e
            return objects

    @classmethod
    async def get_first_by_filter(
        cls,
        sess: AsyncSession,
        flt: GulpCollabFilter = None,
        throw_if_not_found: bool = True,
        user_id: str = None,
        recursive: bool = False,
    ) -> T:
        """
        shortcut to get the just the first object matching the filter

        Args:
            sess (AsyncSession): The database session to use.
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None (all objects).
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to True.
            user_id (str, optional): if set, only return objects that the user has access to.
            recursive (bool, optional): If True, loads nested relationships recursively. Defaults to False.

        Returns:
            T: The first object that matches the filter criteria or None if not found.
        """
        objs: list[T] = await cls.get_by_filter(
            sess,
            flt=flt,
            throw_if_not_found=throw_if_not_found,
            user_id=user_id,
            recursive=recursive,
            first_only=True,
        )
        if not objs:
            return None
        return objs[0]

    @classmethod
    async def get_by_filter_wrapper(
        cls,
        token: str,
        flt: GulpCollabFilter,
        operation_id: str = None,
        permission: list[GulpUserPermission] = None,
        throw_if_not_found: bool = False,
        recursive: bool = False,
    ) -> list[dict]:
        """
        same as get_by_filter, but handling session and ACL check, returning list of dicts

        NOTE: depending on the filter, this may return a large number of objects so using pagination is recommended.

        Args:
            token (str): The user token.
            operation_id (str, optional): The ID of the operation: if set, it will be checked for READ permission. Defaults to None.
            flt (GulpCollabFilter): The filter to apply to the query.
            permission (list[GulpUserPermission], optional): The permission required to read the object. Defaults to GulpUserPermission.READ.
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to False (return empty list).
            recursive (bool, optional): If True, loads nested relationships recursively for the returned objects. Defaults to False.
        Returns:
            list[dict]: The list of object dictionaries that match the filter criteria.
        """
        from gulp.api.collab.user_session import GulpUserSession
        from gulp.api.collab_api import GulpCollab

        async with GulpCollab.get_instance().session() as sess:
            if not permission:
                permission = [GulpUserPermission.READ]

            flt = flt or GulpCollabFilter()
            if operation_id:
                # check operation access
                from gulp.api.collab.operation import GulpOperation

                s, _, _ = await GulpOperation.get_by_id_wrapper(
                    sess, token, operation_id, permission=GulpUserPermission.READ
                )

                # ensure operation_id is set on the filter
                if not flt.operation_ids:
                    flt.operation_ids = []
                if operation_id not in flt.operation_ids:
                    flt.operation_ids.append(operation_id)
            else:
                # just check permission
                s = await GulpUserSession.check_token(
                    sess, token, permission=permission
                )

            # MutyLogger.get_instance().debug("get_by_filter_wrapper, user_id=%s" % (s.user.id))

            objs: list[GulpCollabBase] = await cls.get_by_filter(
                sess,
                flt,
                throw_if_not_found=throw_if_not_found,
                user_id=s.user.id,
                recursive=recursive,
            )
            if not objs:
                return []

            data = []
            for o in objs:
                data.append(o.to_dict(nested=recursive))

            MutyLogger.get_instance().debug(
                "get_by_filter_wrapper, user_id: %s, result: %s",
                s.user.id,
                muty.string.make_shorter(str(data), max_len=260),
            )

            return data
