"""
Defines the core database models and structures for the collaborative features in Gulp.

This module includes the base classes for all database objects in the collaboration system,
as well as utility classes for filtering, permissions, and object management.

Key components:
- GulpCollabBase: Abstract base class for all database objects
- GulpCollabObject: Base class for operation-related collaboration objects
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
import orjson
import re
from enum import StrEnum
from typing import TYPE_CHECKING, List, Optional, TypeVar, override

import muty.crypto
import muty.string
import muty.time
from muty.log import MutyLogger
from psycopg import OperationalError
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import (
    ARRAY,
    BIGINT,
    ColumnElement,
    ForeignKey,
    Select,
    Delete,
    String,
    Tuple,
    and_,
    column,
    exists,
    func,
    insert,
    inspect,
    literal,
    or_,
    select,
    delete,
    text,
)
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import (
    DeclarativeBase,
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
    PENDING = "pending"


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
COLLABTYPE_SOURCE_FIELDS = "source_fields"
COLLABTYPE_QUERY_HISTORY = "query_history"
COLLABTYPE_TASK = "task"


T = TypeVar("T", bound="GulpCollabBase")


class GulpCollabFilter(BaseModel):
    """
    defines filter to be applied to all objects in the collaboration system.

    - filtering by basic types in `GulpCollabBase` and `GulpCollabObject` (for collab objects) is always supported.
    - use % for wildcard instead of * (SQL LIKE operator).
    - custom fields are supported via `model_extra` as k: [v,v,v,...] pairs where v are strings to match against the column (case insensitive/OR match).
        i.e. `{"custom_field": ["val1", "val2"]}` will match all objects where `custom_field` is either "val1" or "val2".
        if "grant_user_ids" and/or "grant_user_group_ids" are provided, only objects with the defined (or empty, public) grants will be returned.
    """

    # allow extra fields to be interpreted as additional filters on the object columns as simple key-value pairs
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
                    "owner_user_ids": ["admin"],
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

    # the following fields are not part of the model, but are used to filter the objects internally: they're added to model.extra
    # granted_user_ids: Optional[list[str]] = Field(None)
    # granted_user_group_ids: Optional[list[str]] = Field(None)
    # owner_user_ids: Optional[list[str]] = Field(None)

    ids: Optional[list[str]] = Field(None, description="filter by the given id/s.")
    types: Optional[list[str]] = Field(
        None,
        description="filter by the given collaboration type/s.",
    )
    operation_ids: Optional[list[str]] = Field(
        None, description="filter by the given operation/s."
    )
    context_ids: Optional[list[str]] = Field(
        None, description="filter by the given context/s."
    )
    source_ids: Optional[list[str]] = Field(
        None,
        description="filter by the given source path/s or name/s.",
    )
    owner_user_ids: Optional[list[str]] = Field(
        None, description="filter by the given owner user id/s."
    )
    tags: Optional[list[str]] = Field(None, description="filter by the given tag/s.")
    names: Optional[list[str]] = Field(None, description="filter by the given name/s.")
    texts: Optional[list[str]] = Field(
        None,
        description="filter by the given object text (wildcard accepted).",
    )
    time_created_range: Optional[tuple[int, int]] = Field(
        None,
        description="""
            if set, matches objects in a `CollabObject.time_created` range [start, end], inclusive, in milliseconds from unix epoch.
        """,
    )
    time_pin_range: Optional[tuple[int, int]] = Field(
        None,
        description="""
if set, matches objects in a `CollabObject.time_pin` range [start, end], inclusive, in nanoseconds from unix epoch.

- cannot be used with `doc_ids` or `doc_time_range`.
""",
    )
    doc_ids: Optional[list[str]] = Field(
        None,
        description="""
filter by the given document ID/s in a `CollabObject.docs` list of `GulpBasicDocument` or in a `CollabObject.doc_ids` list of document IDs.

- cannot be used with `time_pin_range` or `doc_time_range`.
""",
    )
    doc_time_range: Optional[tuple[int, int]] = Field(
        None,
        description="""
if set, a `gulp.timestamp` range [start, end] to match documents in a `CollabObject.docs`, inclusive, in nanoseconds from unix epoch.

- cannot be used with `time_pin_range` or `doc_ids`.
- works with Notes, does not work with Links
""",
    )
    limit: Optional[int] = Field(
        None,
        description='to be used together with "offset", maximum number of results to return. default=return all.',
    )
    offset: Optional[int] = Field(
        None,
        description='to be used together with "limit", number of results to skip from the beginning. default=0 (from start).',
    )
    tags_and: Optional[bool] = Field(
        False,
        description="if True, all tags must match. Default=False (at least one tag must match).",
    )
    sort: Optional[list[tuple[str, GulpSortOrder]]] = Field(
        None,
        description="sort fields and order. Default=sort by `time_created` ASC, `id` ASC.",
    )

    @override
    def __str__(self) -> str:
        return self.model_dump_json(exclude_none=True)

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
        # check if values in values contains wildcards as *, if so, replace with % for SQL LIKE operator
        values = [val.replace("*", "%") for val in values]
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
        if self.owner_user_ids and "owner_user_id" in obj_type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(
                    obj_type.owner_user_id, self.owner_user_ids
                )
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
            # handle granted_user_ids and granted_group_ids as special case first:
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

            # Process remaining model_extra fields
            for k, v in self.model_extra.items():
                if k in obj_type.columns:
                    col = getattr(obj_type, k)
                    # check if column type is ARRAY using SQLAlchemy's inspection
                    is_array = isinstance(getattr(col, "type", None), ARRAY)
                    if is_array:
                        q = q.filter(self._array_contains_any(col, v))
                    else:
                        q = q.filter(self._case_insensitive_or_ilike(col, v))

                    continue

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
                    # field is not a valid column, log a warning
                    MutyLogger.get_instance().warning(
                        "invalid sort field: %s. skipping this field." % (field)
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
    owner_user_id: Mapped[str] = mapped_column(
        ForeignKey("user.id", ondelete="CASCADE"),
        doc="The id of the user who created the object.",
    )
    granted_user_ids: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="The ids of the users who have been granted access to the object. if not set(default), all objects have access.",
    )
    granted_user_group_ids: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="The ids of the user groups who have been granted access to the object. if not set(default), all groups have access.",
    )
    time_created: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        doc="The time the object was created, in milliseconds from unix epoch.",
    )
    time_updated: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        doc="The time the object was last updated, in milliseconds from unix epoch.",
    )
    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"), doc="The glyph ID."
    )
    name: Mapped[Optional[str]] = mapped_column(
        String, doc="The display name of the object."
    )
    description: Mapped[Optional[str]] = mapped_column(
        String, doc="The description of the object."
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
            raise Exception(
                "GulpCollabBase is an abstract class and cannot be instantiated directly."
            )

        # call the base class constructor
        # MutyLogger.get_instance().debug("---> GulpCollabBase self in __init__=%s" % self)
        super().__init__()

    @override
    def to_dict(
        self,
        nested: bool = False,
        hybrid_attributes: bool = False,
        exclude: List[str] | None = None,
        exclude_none: bool = False,
    ) -> dict:
        # same as super.to_dict() but with exclude_none parameter
        d = super().to_dict(nested, hybrid_attributes, exclude)
        if not exclude_none:
            return d

        return {k: v for k, v in d.items() if v is not None}

    @classmethod
    def _build_eager_loading_options(
        cls, recursive: bool = False, seen: set = None, path: str = ""
    ) -> list:
        """
        build query options for eager loading relationships.

        Args:
            recursive (bool): whether to load nested relationships recursively
            seen (set): set of classes already seen to prevent circular dependencies
            path (str): current relationship path for nested loading

        Returns:
            list: the list of loading options
        """
        # initialize seen set if not provided
        if seen is None:
            seen = set()

        # prevent circular dependencies
        if cls in seen:
            return []

        # add current class to seen set
        seen.add(cls)

        # get all direct relationships
        options = []

        for rel in inspect(cls).relationships:
            # add direct relationship
            rel_name = rel.key
            options.append(selectinload(getattr(cls, rel_name)))

            # handle recursive loading if requested
            if recursive:
                target_class = rel.mapper.class_

                # skip already seen classes to prevent circular dependencies
                if target_class in seen:
                    continue

                # get nested loading options with updated path
                nested_seen = seen.copy()
                nested_seen.add(target_class)

                # build path-based loader
                for nested_rel in inspect(target_class).relationships:
                    nested_rel_name = nested_rel.key
                    # create chain of relationship loading
                    options.append(
                        selectinload(getattr(cls, rel_name)).selectinload(
                            getattr(target_class, nested_rel_name)
                        )
                    )

                    # avoid going too deep with recursion
                    nested_target = nested_rel.mapper.class_
                    if nested_target not in nested_seen:
                        nested_seen.add(nested_target)

        return options

    @classmethod
    def build_base_object_dict(
        cls,
        object_data: dict,
        owner_id: str,
        obj_id: str = None,
        private: bool = True,
        **kwargs,
    ) -> dict:
        """
        build a dictionary to create a new base object

        Args:
            object_data (dict): The data to create the object with.
            owner_id (str): The ID of the user creating the object
            obj_id (str, optional): The ID of the object to create. Defaults to None (generate a unique ID).
            private (bool, optional): If True, the object is private (streamed only to ws_id websocket). Defaults to False.

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

        # remove None values
        obj: dict = {k: v for k, v in object_data.items() if v is not None}

        # add kwargs if any
        for k, v in kwargs.items():
            if v is not None:
                obj[k] = v

        obj["type"] = cls.__gulp_collab_type__
        obj["id"] = obj_id
        obj["time_created"] = time_created
        obj["time_updated"] = time_created
        obj["owner_user_id"] = owner_id

        # set user and group grants
        granted_user_ids = object_data.get("granted_user_ids", [])
        granted_user_group_ids = object_data.get("granted_user_group_ids", [])

        # determine final user grants
        if granted_user_ids:
            user_grants = granted_user_ids
        elif private:
            user_grants = [owner_id]  # private object, owner only
        else:
            user_grants = []  # public object

        # determine final group grants
        if granted_user_group_ids:
            group_grants = granted_user_group_ids
        else:
            group_grants = []

        obj["granted_user_ids"] = user_grants
        obj["granted_user_group_ids"] = group_grants

        if not obj.get("name", None):
            # set the name to the id if not provided
            obj["name"] = obj_id
        return obj

    @classmethod
    async def _create_internal(
        cls,
        sess: AsyncSession,
        object_data: dict,
        obj_id: str = None,
        ws_id: str = None,
        owner_id: str = None,
        ws_queue_datatype: str = None,
        ws_data: dict = None,
        req_id: str = None,
        private: bool = True,
        commit: bool = True,
        **kwargs,
    ) -> T:
        """
        Asynchronously creates and stores an instance of the class, also updating the websocket if required.

        NOTE: session is committed after the operation
        NOTE: the returned object is **eager loaded**

        Args:
            sess (AsyncSession): The database session to use.
            object_data (dict): The data to create the object with.
            obj_id (str, optional): The ID of the object to create. Defaults to None (generate a unique ID).
            operation_id (str, optional): The ID of the operation associated with the instance. Defaults to None.
            ws_id (str, optional): WebSocket ID associated with the instance. Defaults to None.
            owner_id (str, optional): The user to be set as the owner of the object. Defaults to None("admin" user will be set).
            ws_queue_datatype (str, optional): The type of the websocket queue data. Defaults to WSDATA_COLLAB_UPDATE.
            ws_data (dict, optional): data to send to the websocket. Defaults to the created object.
            req_id (str, optional): Request ID associated with the instance. Defaults to None.
            private (bool, optional): If True, the object is private (streamed only to ws_id websocket). Defaults to True.
            commit (bool): Whether to commit the session after creating the object. Defaults to True.
            **kwargs: Additional keyword arguments.
        Returns:
            T: The created instance of the class.
        Raises:
            Exception: If there is an error during the creation or storage process.
        """
        object_data = object_data or {}
        owner_id = owner_id or "admin"

        # build object dictionary with necessary attributes
        d = cls.build_base_object_dict(
            object_data, owner_id=owner_id, obj_id=obj_id, private=private, **kwargs
        )

        # create object with eager loading
        stmt = (
            select(cls)
            .options(*cls._build_eager_loading_options())
            .from_statement(insert(cls).values(**d).returning(cls))
        )
        # MutyLogger.get_instance().debug(f"creating instance of {cls.__name__}, base object dict: {d}, statement: {stmt}")

        try:
            res = await sess.execute(stmt)
            instance: GulpCollabBase = res.scalar_one()
        except Exception as ex:
            raise ex

        # MutyLogger.get_instance().debug(f"created instance: {instance.to_dict(nested=True, exclude_none=True)}")
        if commit:
            await sess.commit()
        if not ws_id:
            # no websocket, return the instance
            return instance

        from gulp.api.ws_api import (
            GulpCollabCreateUpdatePacket,
            GulpWsSharedQueue,
        )

        if not ws_queue_datatype:
            from gulp.api.ws_api import WSDATA_COLLAB_UPDATE

            ws_queue_datatype = WSDATA_COLLAB_UPDATE

        # use provided data or serialize the instance
        if ws_data:
            data = ws_data
        else:
            data = instance.to_dict(nested=True, exclude_none=True)

        p = GulpCollabCreateUpdatePacket(data=data, created=True)
        wsq = GulpWsSharedQueue.get_instance()
        await wsq.put(
            ws_queue_datatype,
            ws_id=ws_id,
            user_id=owner_id,
            operation_id=object_data.get("operation_id", None) if object_data else None,
            req_id=req_id,
            data=p.model_dump(exclude_none=True, exclude_defaults=True),
            private=private,
        )
        return instance

    async def add_default_grants(self, sess: AsyncSession):
        """
        shortcut to add default user and groups grants to the object

        NOTE: should be used only when resetting the database.

        Args:
            sess (AsyncSession): The session to use.
        """
        # add user grants, admin and guest are guaranteed to exist (cannot be deleted)
        await self.add_user_grant(sess, "admin", commit=False)
        await self.add_user_grant(sess, "guest", commit=False)

        try:
            await self.add_user_grant(sess, "ingest", commit=False)
        except ObjectNotFound:
            pass

        try:
            await self.add_user_grant(sess, "editor", commit=False)
        except ObjectNotFound:
            pass

        try:
            await self.add_user_grant(sess, "power", commit=False)
        except ObjectNotFound:
            pass

        # add group grants
        from gulp.api.collab.user_group import ADMINISTRATORS_GROUP_ID

        await self.add_group_grant(sess, ADMINISTRATORS_GROUP_ID, commit=False)
        await sess.commit()

    async def add_group_grant(
        self, sess: AsyncSession, group_id: str, commit: bool = True
    ) -> None:
        """
        grant a user group access to the object

        Args:
            sess (AsyncSession): The database session to use.
            group_id (str): The ID of the user group to add.
            commit (bool): Whether to commit the session after adding the group grant. Defaults to True.
                NOTE: if commit is False, the session must be committed elsewhere or the lock acquired by this function would remain held!
        Returns:
            None
        """
        # will except if the group do not exist!
        from gulp.api.collab.user_group import GulpUserGroup

        await GulpUserGroup.get_by_id(sess, group_id)

        if group_id not in self.granted_user_group_ids:
            MutyLogger.get_instance().debug(
                "adding granted user group %s to object %s" % (group_id, self.id)
            )

            try:
                await self.__class__.acquire_advisory_lock(sess, self.id)
                self.granted_user_group_ids.append(group_id)
                if commit:
                    await sess.commit()
            except Exception as e:
                await sess.rollback()
                raise e
        else:
            MutyLogger.get_instance().warning(
                "user group %s already granted on object %s" % (group_id, self.id)
            )

    async def remove_group_grant(
        self, sess: AsyncSession, group_id: str, commit: bool = True
    ) -> None:
        """
        remove a user group access to the object

        Args:
            sess (AsyncSession): The database session to use.
            group_id (str): The ID of the user group to remove.
            commit (bool): Whether to commit the session after removing the group grant. Defaults to True.
                NOTE: if commit is False, the session must be committed elsewhere or the lock acquired by this function would remain held!
        Returns:
            None
        """
        # will except if the group do not exist!
        from gulp.api.collab.user_group import GulpUserGroup

        await GulpUserGroup.get_by_id(sess, group_id)

        if group_id in self.granted_user_group_ids:
            MutyLogger.get_instance().info(
                "removing granted user group %s from object %s" % (group_id, self.id)
            )

            try:
                await self.__class__.acquire_advisory_lock(sess, self.id)
                self.granted_user_group_ids.remove(group_id)
                if commit:
                    await sess.commit()
            except Exception as e:
                await sess.rollback()
                raise e

        else:
            MutyLogger.get_instance().warning(
                "user group %s not in granted list on object %s" % (group_id, self.id)
            )

    async def add_user_grant(
        self, sess: AsyncSession, user_id: str, commit: bool = True
    ) -> None:
        """
        grant a user access to the object

        Args:
            sess (AsyncSession): The session to use for the query.
            user_id (str): The ID of the user to add.
            commit (bool): Whether to commit the session after adding the user grant. Defaults to True.
                NOTE: if commit is False, the session must be committed elsewhere or the lock acquired by this function would remain held!
        Returns:
            None
        """
        # will except if the user do not exist!
        from gulp.api.collab.user import GulpUser

        await GulpUser.get_by_id(sess, user_id)

        if user_id not in self.granted_user_ids:
            MutyLogger.get_instance().debug(
                "adding granted user %s to object %s" % (user_id, self.id)
            )

            try:
                await self.__class__.acquire_advisory_lock(sess, self.id)
                self.granted_user_ids.append(user_id)
                if commit:
                    await sess.commit()
            except Exception as e:
                await sess.rollback()
                raise e
        else:
            MutyLogger.get_instance().warning(
                "user %s already granted on object %s" % (user_id, self.id)
            )

    async def remove_user_grant(
        self, sess: AsyncSession, user_id: str, commit: bool = True
    ) -> None:
        """
        remove a user access to the object

        Args:
            sess (AsyncSession): The session to use for the query.
            user_id (str): The ID of the user to remove.
            commit (bool): Whether to commit the session after removing the user grant. Defaults to True.
                NOTE: if commit is False, the session must be committed elsewhere or the lock acquired by this function would remain held!

        Returns:
            None
        """

        # will except if the user do not exist!
        from gulp.api.collab.user import GulpUser

        await GulpUser.get_by_id(sess, user_id)

        if user_id in self.granted_user_ids:
            MutyLogger.get_instance().info(
                "removing granted user %s from object %s" % (user_id, self.id)
            )
            try:
                await self.__class__.acquire_advisory_lock(sess, self.id)
                self.granted_user_ids.remove(user_id)
                if commit:
                    await sess.commit()
            except Exception as e:
                await sess.rollback()
                raise e

            MutyLogger.get_instance().info(
                "removed granted user %s from object %s" % (user_id, self.id)
            )
        else:
            MutyLogger.get_instance().warning(
                "user %s not in granted list on object %s" % (user_id, self.id)
            )

    async def delete(
        self,
        sess: AsyncSession,
        ws_id: str = None,
        user_id: str = None,
        ws_queue_datatype: str = None,
        ws_data: dict = None,
        req_id: str = None,
        raise_on_error: bool = True,
    ) -> None:
        """
        deletes the object, also updating the websocket if required.

        Args:
            sess (AsyncSession): The database session to use.
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            user_id (str, optional): The ID of the user making the request. Defaults to None.
            ws_queue_datatype (str, optional): The type of the websocket queue data. Defaults to WSDATA_COLLAB_DELETE.
            ws_data (dict, optional): data to send to the websocket: if not set, a GulpDeleteCollabPacket with object id will be sent.
            req_id (str, optional): The ID of the request. Defaults to None.
            raise_on_error (bool): Whether to raise an exception on error. Defaults to True.
        Returns:
            None
        """
        operation_id: str = getattr(self, "operation_id", None)
        obj_id: str = self.id
        try:
            await self.__class__.acquire_advisory_lock(sess, obj_id)
            MutyLogger.get_instance().debug(
                "deleting object %s.%s, operation=%s"
                % (self.__class__, obj_id, operation_id)
            )
            await sess.delete(self)
            await sess.commit()
        except Exception as e:
            await sess.rollback()
            if raise_on_error:
                raise e

        if not ws_id:
            # done
            return

        # notify the websocket of the deletion
        from gulp.api.ws_api import (
            GulpCollabDeletePacket,
            GulpWsSharedQueue,
        )

        if not ws_queue_datatype:
            from gulp.api.ws_api import WSDATA_COLLAB_DELETE

            ws_queue_datatype = WSDATA_COLLAB_DELETE

        if ws_data:
            data = ws_data
        else:
            p: GulpCollabDeletePacket = GulpCollabDeletePacket(id=obj_id)
            data = p.model_dump()

        wsq = GulpWsSharedQueue.get_instance()
        await wsq.put(
            type=ws_queue_datatype,
            ws_id=ws_id,
            user_id=user_id,
            operation_id=operation_id,
            req_id=req_id,
            data=data,
        )

    def is_owner(self, user_id: str) -> bool:
        """
        check if the user is the owner of the object

        Args:
            user_id (str): The ID of the user to check.
        Returns:
            bool: True if the user is the owner, False otherwise.
        """
        return self.owner_user_id == user_id

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
            and self.granted_user_ids[0] == self.owner_user_id
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

        try:
            # private object = only owner or admin can see it
            await self.__class__.acquire_advisory_lock(sess, self.id)
            self.granted_user_ids = [self.owner_user_id]
            self.granted_user_group_ids = []
            await sess.commit()
            MutyLogger.get_instance().info(
                "object %s is now PRIVATE to user %s" % (self.id, self.owner_user_id)
            )
        except Exception as e:
            await sess.rollback()
            raise e

    async def make_public(self, sess: AsyncSession) -> None:
        """
        make the object public

        Args:
            sess (AsyncSession): The database session to use.
            user_id (str): The ID of the user making the request.
        Returns:
            None
        """
        try:
            await self.__class__.acquire_advisory_lock(sess, self.id)
            # clear all granted users and groups
            self.granted_user_group_ids = []
            self.granted_user_ids = []
            await sess.commit()
            MutyLogger.get_instance().info("object %s is now PUBLIC" % (self.id))
        except Exception as e:
            await sess.rollback()
            raise e

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
        subclasses.extend(GulpCollabObject.__subclasses__())
        for cls in subclasses:
            if cls.__gulp_collab_type__ == collab_type:
                return cls
        raise ValueError(f"no class found for collab type={collab_type}")

    async def update(
        self,
        sess: AsyncSession,
        d: dict,
        ws_id: str = None,
        user_id: str = None,
        ws_queue_datatype: str = None,
        ws_data: dict = None,
        req_id: str = None,
    ) -> dict:
        """
        updates the object, also updating the websocket if required.

        NOTE: session is committed after the operation

        Args:
            sess (AsyncSession): The database session to use: the session will be committed and refreshed after the update.
            d (dict): A dictionary containing the fields to update and their new values
            ws_id (str, optional): The ID of the websocket connection to send update to the websocket. Defaults to None (no update will be sent)
            user_id (str, optional): The ID of the user making the request. Defaults to None, ignored if ws_id is not provided.
            ws_queue_datatype (str, optional): The type of the websocket queue data, ignored if ws_id is not provided. Defaults to WSDATA_COLLAB_UPDATE.
            ws_data (dict, optional): data to send to the websocket. Defaults to the updated object, ignored if ws_id is not provided
            req_id (str, optional): The ID of the request, ignored if ws_id is not provided. Defaults to None.

        Returns:
            dict: The updated object as a dictionary.
        """
        try:
            await self.__class__.acquire_advisory_lock(sess, self.id)
            if d:
                # update instance from d, ensure d has no 'id' (the id cannot be updated)
                d.pop("id", None)
                for k, v in d.items():
                    # only update if the value is not None and different from the current value
                    if v is not None and getattr(self, k, None) != v:
                        # MutyLogger.get_instance().debug(f"setattr: {k}={v}")
                        setattr(self, k, v)

                # update time
                if not d.get("time_updated", None):
                    # set the time updated
                    d["time_updated"] = muty.time.now_msec()

            private = self.is_private()
            updated_dict = self.to_dict(nested=True, exclude_none=True)

            # commit
            await sess.commit()
            MutyLogger.get_instance().debug(
                "---> updated: %s"
                % (muty.string.make_shorter(str(updated_dict), max_len=260))
            )
        except Exception as e:
            await sess.rollback()
            raise e

        if not ws_id:
            # no websocket ID provided, return
            return updated_dict

        # send update to websocket
        from gulp.api.ws_api import (
            GulpCollabCreateUpdatePacket,
            GulpWsSharedQueue,
        )

        if not ws_queue_datatype:
            from gulp.api.ws_api import WSDATA_COLLAB_UPDATE

            ws_queue_datatype = WSDATA_COLLAB_UPDATE

        # notify the websocket of the collab object update
        if ws_data:
            data = ws_data
        else:
            data = updated_dict

        p = GulpCollabCreateUpdatePacket(data=data)
        wsq = GulpWsSharedQueue.get_instance()
        await wsq.put(
            type=ws_queue_datatype,
            ws_id=ws_id,
            user_id=user_id,
            operation_id=data.get("operation_id", None),
            req_id=req_id,
            data=p.model_dump(exclude_none=True, exclude_defaults=True),
            private=private,
        )
        return updated_dict

    @classmethod
    async def release_advisory_lock(cls, sess: AsyncSession, obj_id: str) -> None:
        """
        release an advisory lock

        Args:
            session (AsyncSession): The database session to use.
            obj_id (str): The ID of the object to unlock.
        """
        lock_id = muty.crypto.hash_xxh64_int("%s-%s" % (cls.__name__, obj_id))
        try:
            await sess.execute(
                text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": lock_id}
            )
            # MutyLogger.get_instance().debug(f"released advisory lock for {cls.__name__} {obj_id}: {lock_id}")
        except OperationalError as e:
            MutyLogger.get_instance().error(f"failed to release advisory lock: {e}")
            raise

    @classmethod
    async def acquire_advisory_lock(cls, sess: AsyncSession, obj_id: str) -> None:
        """
        Acquire an advisory lock

        Args:
            sess (AsyncSession): The database session to use.
            obj_id (str): The ID of the object to lock.
        """
        lock_id = muty.crypto.hash_xxh64_int("%s-%s" % (cls.__name__, obj_id))
        try:
            await sess.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"), {"lock_id": lock_id}
            )
            # MutyLogger.get_instance().debug(f"acquired advisory lock for {cls.__name__} {obj_id}: {lock_id}")
        except OperationalError as e:
            MutyLogger.get_instance().error(f"failed to acquire advisory lock: {e}")
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
    async def get_by_filter(
        cls,
        sess: AsyncSession,
        flt: GulpCollabFilter = None,
        throw_if_not_found: bool = True,
        user_id: str = None,
        recursive: bool = False,
    ) -> list[T]:
        """
        Asynchronously retrieves a list of objects based on the provided filter.
        Args:
            sess (AsyncSession): The database session to use.
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None (all objects).
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to True.
            user_id (str, optional): if set, only return objects that the user has access to.
            recursive (bool, optional): If True, loads nested relationships recursively. Defaults to False.
        Returns:
            list[T]: A list of objects that match the filter criteria.
        Raises:
            Exception: If there is an error during the query execution or result processing.
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
        else:
            # no user_id provided, assume admin
            is_admin = True

        # build and run query (ensure eager loading)
        if is_admin:
            # admin must see all
            flt.granted_user_ids = None
            flt.granted_user_group_ids = None
            flt.owner_user_ids = None
        else:
            # user can see only objects he has access to
            flt.granted_user_ids = [user_id]
            flt.granted_user_group_ids = group_ids

        # MutyLogger.get_instance().debug("pre to_select_query: user=%s, admin=%r, groups=%s, flt=%s" % (u.to_dict(), is_admin, group_ids, flt))
        q = flt.to_select_query(cls)
        q = q.options(*cls._build_eager_loading_options(recursive=recursive))
        # MutyLogger.get_instance().debug(
        #     "get_by_filter, user_id=%s, is_admin=%r, flt=%s, query:\n%s"
        #     % (user_id, is_admin, flt, q)
        # )
        res = await sess.execute(q)
        objects = res.scalars().all()
        if not objects:
            if throw_if_not_found:
                raise ObjectNotFound(f"No {cls.__name__} found with filter {str(flt)}")
            return []

        # MutyLogger.get_instance().debug("user_id=%s, POST-filtered objects: %s" % (user_id, objects))
        return objects

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
            user_id (str, optional): if set, only return objects that the user has access to.
        Returns:
            int: The number of objects deleted.
        Raises:
            Exception: If there is an error during the query execution or result processing.
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
            # MutyLogger.get_instance().debug("user=%s, admin=%r, groups=%s" % (u.to_dict(), is_admin, group_ids))

        # build and run query (ensure eager loading)
        if is_admin:
            # admin must see all
            flt.granted_user_ids = None
            flt.granted_user_group_ids = None
            flt.owner_user_ids = None
        else:
            # user can see only objects he has access to
            flt.granted_user_ids = [user_id] if user_id else None
            flt.granted_user_group_ids = group_ids

        q = flt.to_delete_query(cls)
        MutyLogger.get_instance().debug(
            "delete_by_filter, flt=%s, user_id=%s, query:\n%s" % (flt, user_id, q)
        )
        res = await sess.execute(q)
        if res is None or res.rowcount == 0:
            if throw_if_not_found:
                raise ObjectNotFound(
                    f"No {cls.__name__} found with filter {flt}", cls.__name__, str(flt)
                )
            return 0
        deleted_count = res.rowcount

        # commit the transaction
        await sess.commit()
        MutyLogger.get_instance().debug(
            "user_id=%s, deleted %d objects (optimized)" % (user_id, deleted_count)
        )

        return deleted_count

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
        Asynchronously retrieves the first object based on the provided filter.

        Args:
            sess (AsyncSession): The database session to use.
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None (all objects).
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to True.
            user_id (str, optional): if set, only return objects that the user has access to.
            recursive (bool, optional): If True, loads nested relationships recursively. Defaults to False.

        Returns:
            T: The first object that matches the filter criteria or None if not found.
        """
        obj = await cls.get_by_filter(
            sess,
            flt=flt,
            throw_if_not_found=throw_if_not_found,
            user_id=user_id,
            recursive=recursive,
        )

        if obj:
            return obj[0]
        return None

    @classmethod
    async def get_by_id_wrapper(
        cls,
        token: str,
        obj_id: str,
        permission: list[GulpUserPermission] = None,
        recursive: bool = False,
        enforce_owner: bool = False,
    ) -> dict:
        """
        helper to get an object by ID, handling session and ACL check

        Args:
            token (str): The user token.
            obj_id (str): The ID of the object to get.
            permission (list[GulpUserPermission], optional): The permission required to read the object. Defaults to GulpUserPermission.READ.
            recursive (bool, optional): If True, nested relationships will be loaded. Defaults to False.
            enforce_owner (bool, optional): If True, the user must be the owner of the object (or admin). Defaults to False.
        Returns:
            dict: The object as a dictionary

        Raises:
            MissingPermissionError: If the user does not have permission to read the object.
            ObjectNotFound: If the object is not found.
        """
        from gulp.api.collab.user_session import GulpUserSession
        from gulp.api.collab_api import GulpCollab

        if not permission:
            permission = [GulpUserPermission.READ]

        async with GulpCollab.get_instance().session() as sess:
            n: GulpCollabBase = await cls.get_by_id(sess, obj_id)

            # token needs at least read permission (or be the owner)
            await GulpUserSession.check_token(
                sess, token, permission=permission, obj=n, enforce_owner=enforce_owner
            )
            return n.to_dict(exclude_none=True, nested=recursive)

    @classmethod
    async def get_by_filter_wrapper(
        cls,
        token: str,
        flt: GulpCollabFilter,
        permission: list[GulpUserPermission] = None,
        throw_if_not_found: bool = False,
        recursive: bool = False,
    ) -> list[dict]:
        """
        helper to get objects by filter, handling session and ACL check for each returned object (based on token permission)

        Args:
            token (str): The user token.
            flt (GulpCollabFilter): The filter to apply to the query.
            permission (list[GulpUserPermission], optional): The permission required to read the object. Defaults to GulpUserPermission.READ.
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to False (return empty list).
            recursive (bool, optional): If True, nested relationships will be loaded. Defaults to False.
        Returns:
            list[dict]: The list of object dictionaries that match the filter criteria.
        """
        from gulp.api.collab.user_session import GulpUserSession
        from gulp.api.collab_api import GulpCollab

        if not permission:
            permission = [GulpUserPermission.READ]
        async with GulpCollab.get_instance().session() as sess:
            # token needs at least read permission
            s = await GulpUserSession.check_token(sess, token, permission=permission)
            user_id = s.user_id
            #MutyLogger.get_instance().debug("get_by_filter_wrapper, user_id=%s" % (user_id))

            objs = await cls.get_by_filter(
                sess,
                flt,
                throw_if_not_found=throw_if_not_found,
                user_id=user_id,
            )
            if not objs:
                return []

            data = []
            for o in objs:
                data.append(o.to_dict(exclude_none=True, nested=recursive))

            MutyLogger.get_instance().debug(
                "get_by_filter_wrapper, user_id: %s, result: %s"
                % (s.user.id, muty.string.make_shorter(str(data), max_len=260))
            )

            return data

    @classmethod
    async def delete_by_id(
        cls,
        token: str,
        obj_id: str,
        ws_id: str,
        req_id: str,
        permission: list[GulpUserPermission] = None,
    ) -> None:
        """
        helper to delete an object by ID, handling session and ACL check

        Args:
            token (str): The user token, pass None to skip token check.
            obj_id (str): The ID of the object to delete.
            ws_id (str): The websocket ID, ignored if token is None. May be None if no websocket broadcast is required.
            req_id (str): The request ID, may be None if no websocket broadcast is required.
            permission (list[GulpUserPermission], optional): The permission required to delete the object. Defaults to GulpUserPermission.DELETE.

        Raises:
            MissingPermissionError: If the user does not have permission to delete the object.
            ObjectNotFoundError: If the object is not found.
        """
        from gulp.api.collab.user_session import GulpUserSession
        from gulp.api.collab_api import GulpCollab

        if not permission:
            permission = [GulpUserPermission.DELETE]

        async with GulpCollab.get_instance().session() as sess:
            n: GulpCollabBase = await cls.get_by_id(sess, obj_id)
            if token:
                # token needs at least delete permission (or be the owner)
                s = await GulpUserSession.check_token(
                    sess, token, permission=permission, obj=n
                )
                user_id = s.user_id
            else:
                user_id = None
                ws_id = None

            # delete
            await n.delete(sess, ws_id=ws_id, user_id=user_id, req_id=req_id)

    @classmethod
    async def update_by_id(
        cls,
        token: str,
        obj_id: str,
        ws_id: str,
        req_id: str,
        d: dict = None,
        permission: list[GulpUserPermission] = None,
        **kwargs,
    ) -> dict:
        """
        helper to update an object by ID, handling session

        Args:
            token (str): The user token, pass None for internal calls skipping token check.
            obj_id (str): The ID of the object to update.
            ws_id (str): The websocket ID.
            req_id (str): The request ID.
            d (dict, optional): The data to update the object with. Defaults to None.
            permission (list[GulpUserPermission], optional): The permission required to update the object. Defaults to GulpUserPermission.EDIT.

        Returns:
            dict: The updated object as a dictionary.

        Raises:
            MissingPermissionError: If the user does not have permission to update the object.
        """
        from gulp.api.collab.user_session import GulpUserSession
        from gulp.api.collab_api import GulpCollab

        if not permission:
            permission = [GulpUserPermission.EDIT]
        async with GulpCollab.get_instance().session() as sess:

            n: GulpCollabBase = await cls.get_by_id(sess, obj_id)

            if token is not None:
                # token needs at least edit permission (or be the owner)
                s = await GulpUserSession.check_token(
                    sess, token, permission=permission, obj=n
                )
                user_id = s.user_id
            else:
                # internal call, no token check
                user_id = None
                ws_id = None

            await n.update(
                sess,
                d=d,
                ws_id=ws_id,
                user_id=user_id,
                req_id=req_id,
            )
            return n.to_dict(exclude_none=True)

    @classmethod
    async def create(
        cls,
        token: str,
        ws_id: str,
        req_id: str,
        object_data: dict,
        permission: list[GulpUserPermission] = None,
        obj_id: str = None,
        private: bool = True,
        operation_id: str = None,
        **kwargs,
    ) -> dict:
        """
        helper to create a new object, handling session

        Args:
            token (str): The user token.
            ws_id (str): The websocket ID: pass None to not notify the websocket.
            req_id (str): The request ID.
            object_data (dict): The data to create the object with.
            permission (list[GulpUserPermission], optional): The permission required to create the object. Defaults to GulpUserPermission.EDIT.
            obj_id (str, optional): The ID of the object to create. Defaults to None (generate a unique ID).
            private (bool, optional): If True, the object will be private. Defaults to False.
            operation_id (str, optional): The ID of the operation associated with the object to be created: if set, it will be checked for permission. Defaults to None.
            **kwargs: Any other additional keyword arguments to set as attributes on the instance, if any
        Returns:
            dict: The created object as a dictionary.

        Raises:
            MissingPermissionError: If the user does not have permission to create the object.
        """
        from gulp.api.collab.user_session import GulpUserSession
        from gulp.api.collab_api import GulpCollab

        if not permission:
            permission = [GulpUserPermission.EDIT]

        async with GulpCollab.get_instance().session() as sess:
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

            n: GulpCollabBase = await cls._create_internal(
                sess,
                object_data,
                obj_id=obj_id,
                owner_id=s.user_id,
                ws_id=ws_id,
                req_id=req_id,
                private=private,
                **kwargs,
            )
            return n.to_dict(exclude_none=True)


class GulpCollabObject(GulpCollabBase, type=COLLABTYPE_GENERIC, abstract=True):
    """
    base for all collaboration objects (notes, links, stories, highlights) related to an operation.

    those objects are meant to be shared among users.
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey(
            "operation.id",
            ondelete="CASCADE",
        ),
        doc="The id of the operation associated with the object.",
    )
    tags: Mapped[list[str]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="The tags associated with the object.",
    )
    color: Mapped[Optional[str]] = mapped_column(
        String, doc="The color associated with the object."
    )

    @override
    @classmethod
    def example(cls) -> dict:
        d = super().example()
        d.update(
            {
                "operation_id": "op1",
                "tags": ["tag1", "tag2"],
                "color": "#FF0000",
            }
        )
        return d

    @staticmethod
    def build_dict(
        operation_id: str,
        tags: list[str] = None,
        color: str = None,
        **kwargs,
    ) -> dict:
        """
        build a dictionary to create a new collaboration object

        Args:
            operation_id (str): The ID of the operation associated with the object.
            tags (list[str], optional): The tags associated with the object. Defaults to None.
            color (str, optional): The color associated with the object. Defaults to None.
            **kwargs: Any other additional keyword arguments to set as attributes on the instance, if any
        Returns:
            dict: The dictionary to create the object with.
        """
        d = {
            "operation_id": operation_id,
            "tags": tags,
            "color": color,
        }
        d.update(kwargs)
        return d

    @override
    def __init__(self, *args, **kwargs):
        if self.type == GulpCollabObject:
            raise NotImplementedError(
                "GulpCollabObject is an abstract class and cannot be instantiated directly."
            )
        super().__init__(*args, **kwargs)
        MutyLogger.get_instance().debug("---> GulpCollabObject: ", *args, kwargs)
