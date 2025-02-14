import json
import re
from enum import StrEnum
from typing import Any, List, Optional, TypeVar, override, TYPE_CHECKING

import muty.string
import muty.time
from muty.log import MutyLogger
from psycopg import OperationalError
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import (
    ARRAY,
    BIGINT,
    VARCHAR,
    Boolean,
    ColumnElement,
    ForeignKey,
    Select,
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
from sqlalchemy.types import Enum as SqlEnum
from sqlalchemy_mixins.serialize import SerializeMixin
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

if TYPE_CHECKING:
    from gulp.api.ws_api import GulpWsQueueDataType

from gulp.structs import GulpSortOrder, ObjectAlreadyExists, ObjectNotFound


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


class GulpCollabType(StrEnum):
    """
    defines the types in the collab database
    """

    GENERIC_OBJECT = "collab_obj"
    NOTE = "note"
    HIGHLIGHT = "highlight"
    STORY = "story"
    LINK = "link"
    STORED_QUERY = "stored_query"
    REQUEST_STATS = "request_stats"
    USER_DATA = "user_data"
    USER_SESSION = "user_session"
    CONTEXT = "context"
    USER = "user"
    GLYPH = "glyph"
    OPERATION = "operation"
    SOURCE = "source"
    USER_GROUP = "user_group"
    SOURCE_FIELDS = "source_fields"
    
    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"'{str(self)}'"

    def __json__(self) -> str:
        return str(self)


T = TypeVar("T", bound="GulpCollabBase")


class GulpCollabFilter(BaseModel):
    """
    defines filter to be applied to all objects in the collaboration system.

    - filtering by basic types in `GulpCollabBase` and `GulpCollabObject` (for collab objects) is always supported.
    - use % for wildcard instead of * (SQL LIKE operator).
    - custom fields are supported via `model_extra` as k: [v,v,v,...] pairs where v are strings to match against the column (case insensitive/OR match).
        i.e. `{"custom_field": ["val1", "val2"]}` will match all objects where `custom_field` is either "val1" or "val2".
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

    ids: Optional[list[str]] = Field(None, description="filter by the given id/s.")
    types: Optional[list[GulpCollabType]] = Field(
        None,
        description="filter by the given type/s.",
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

    def _case_insensitive_or_ilike(self, column, values: list) -> ColumnElement[bool]:
        """
        Create a case-insensitive OR query for the given column and values.

        Args:
            column: The column to apply the ilike condition.
            values: The list of values to match against the column.

        Returns:
            ColumnElement[bool]: The OR query.
        """
        # print("column=%s, values=%s" % (column, values))
        conditions = [column.ilike(value) for value in values]
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

    def to_select_query(self, type: T, with_for_update: bool = False) -> Select[Tuple]:
        """
        convert the filter to a select query

        Args:
            type (T): the type of the object (one derived from GulpCollabBase)

        Returns:
            Select[Tuple]: the select query
        """
        q: Select = select(type)
        if self.ids:
            q = q.filter(self._case_insensitive_or_ilike(type.id, self.ids))
        if self.types:
            # match if equal to any in the list
            q = q.filter(type.type.in_(self.types))
        if self.operation_ids and "operation_id" in type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(type.operation_id, self.operation_ids)
            )
        if self.context_ids and "context_id" in type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(type.context_id, self.context_ids)
            )
        if self.source_ids and "source_id" in type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(type.source_id, self.source_ids)
            )
        if self.owner_user_ids and "owner_user_id" in type.columns:
            q = q = q.filter(
                self._case_insensitive_or_ilike(type.owner_user_id, self.owner_user_ids)
            )

        if self.tags and "tags" in type.columns:
            lower_tags = [tag.lower() for tag in self.tags]
            if self.tags_and:
                # all tags must match (CONTAINS operator)
                q = q.filter(self._array_contains_all(type.tags, lower_tags))
            else:
                # at least one tag must match (OVERLAP operator)
                q = q.filter(self._array_contains_any(type.tags, lower_tags))

        if self.names and "name" in type.columns:
            q = q.filter(self._case_insensitive_or_ilike(type.name, self.names))
        if self.texts and "text" in type.columns:
            q = q.filter(self._case_insensitive_or_ilike(type.text, self.texts))

        if self.model_extra:
            # any extra fields to filter on (case-insensitive, OR, expects v to be an array of strings)
            for k, v in self.model_extra.items():
                if hasattr(type, k):
                    column = getattr(type, k)
                    # check if column type is ARRAY using SQLAlchemy's inspection
                    is_array = isinstance(getattr(column, 'type', None), ARRAY)                    
                    if is_array:
                        q = q.filter(self._array_contains_any(column, v))
                    else:
                        q = q.filter(self._case_insensitive_or_ilike(column, v))

        if self.doc_ids and "doc_ids" in type.columns:
            # return all collab objects that have at least one document with _id in doc_ids
            q = q.filter(type.doc_ids.op("&&")(self.doc_ids))

        if self.time_pin_range and "time_pin" in type.columns:
            # returns all collab objects that have time_pin in time_pin_range
            if self.time_pin_range[0]:
                q = q.where(type.time_pin >= self.time_pin_range[0])
            if self.time_pin_range[1]:
                q = q.where(type.time_pin <= self.time_pin_range[1])

        if self.doc_ids and "docs" in type.columns:
            # returns all collab objects that have at least one document with _id in doc_ids
            conditions = []
            for doc_id in self.doc_ids:
                # check if any document in the array has _id matching doc_id
                # using -> to navigate JSONB array and ->> to extract text
                conditions.append(
                    text(
                        """EXISTS (
                        SELECT 1 FROM unnest(docs) AS doc 
                        WHERE doc->>'_id'::text = :doc_id
                    )"""
                    ).bindparams(doc_id=doc_id.lower())
                )
            q = q.filter(or_(*conditions))
        if self.doc_time_range and "docs" in type.columns:
            # returns all collab objects that have at least one document with gulp.timestamp in doc_time_range
            conditions = []
            if self.doc_time_range[0]:
                conditions.append(
                    text(
                        """EXISTS (
                        SELECT 1 FROM unnest(docs) AS doc 
                        WHERE CAST(doc->>'gulp.timestamp' AS BIGINT) >= :start_time
                    )"""
                    ).bindparams(start_time=self.doc_time_range[0])
                )
            if self.doc_time_range[1]:
                conditions.append(
                    text(
                        """EXISTS (
                        SELECT 1 FROM unnest(docs) AS doc 
                        WHERE CAST(doc->>'gulp.timestamp' AS BIGINT) <= :end_time
                    )"""
                    ).bindparams(end_time=self.doc_time_range[1])
                )
            q = q.filter(and_(*conditions))

        # add sort
        if not self.sort:
            # default, time_created ASC, id ASC
            order_clauses = [
                type.time_created.asc(),
                type.id.asc(),
            ]
        else:
            order_clauses = []
            for field, direction in self.sort:
                if direction == GulpSortOrder.ASC:
                    order_clauses.append(getattr(type, field).asc())
                else:
                    order_clauses.append(getattr(type, field).desc())
        q = q.order_by(*order_clauses)

        if self.limit:
            q = q.limit(self.limit)
        if self.offset:
            q = q.offset(self.offset)

        if with_for_update:
            q = q.with_for_update()
        # MutyLogger.get_instance().debug(f"to_select_query: {q}")
        return q


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
    type: Mapped[GulpCollabType] = mapped_column(
        SqlEnum(GulpCollabType), doc="The type of the object."
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
            "id": "id1",
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

    def __init_subclass__(
        cls, type: GulpCollabType | str, abstract: bool = False, **kwargs
    ) -> None:
        """
        this is called automatically when a subclass is created, before __init__ on the instance is called

        Args:
            type (GulpCollabType|str): The type of the object.
            abstract (bool): If True, the class is abstract
            **kwargs: Additional keyword arguments.
        """
        # print(f"__init_subclass__: cls={cls}, type={type}, abstract={abstract}, kwargs={kwargs}")

        cls.__gulp_collab_type__ = type

        if abstract:
            # this is an abstract class
            cls.__abstract__ = True
        else:
            # set table name based on type
            cls.__tablename__ = str(type)

        cls.__mapper_args__ = {
            "polymorphic_identity": str(type),
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

    @staticmethod
    def _create_retry_decorator():
        # retry logic for database operations
        return retry(
            retry=retry_if_exception_type(OperationalError),
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=4, max=10),
            reraise=True,
        )

    @classmethod
    async def release_advisory_lock(cls, sess: AsyncSession, lock_id: int) -> None:
        """
        release an advisory lock

        Args:
            session (AsyncSession): The database session to use.
            lock_id (int): The lock ID to release.
        """
        await sess.execute(
            text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": lock_id}
        )

    @staticmethod
    @_create_retry_decorator()
    async def acquire_advisory_lock(sess: AsyncSession, lock_id: int) -> None:
        """
        Acquire an advisory lock, with retry logic.

        Args:
            sess (AsyncSession): The database session to use.
            lock_id (int): The lock ID to acquire.
        """
        try:
            await sess.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"), {"lock_id": lock_id}
            )
        except OperationalError as e:
            # Log the error
            MutyLogger.get_instance().error(f"Failed to acquire advisory lock: {e}")
            raise

    @staticmethod
    def _get_nested_relationships(model_class, seen=None):
        if seen is None:
            seen = set()

        # Prevent infinite recursion
        if model_class in seen:
            return []
        seen.add(model_class)

        load_options = []
        for rel in inspect(model_class).relationships:
            # Add loading option for this relationship
            load_opt = selectinload(getattr(model_class, rel.key))
            load_options.append(load_opt)

            # Recursively add nested relationships
            target_class = rel.mapper.class_
            nested_opts = GulpCollabBase._get_nested_relationships(target_class, seen)
            for nested_opt in nested_opts:
                load_options.append(load_opt.selectinload(nested_opt))

        return load_options

    @classmethod
    def _build_relationship_loading_options(
        cls, recursive: bool = False, seen: set = None
    ) -> list:
        """
        Build query options for eager loading relationships.

        Args:
            recursive (bool): Whether to load nested relationships recursively
            seen (set): Set of classes already seen to prevent circular dependencies
        Returns:
            list: The list of loading options
        """
        from sqlalchemy.orm import selectinload

        if seen is None:
            seen = set()

        if cls in seen:
            # prevent circular dependencies
            return []

        seen.add(cls)

        if recursive:
            options = []
            for rel in inspect(cls).relationships:
                # Add direct relationship
                load_opt = selectinload(getattr(cls, rel.key))
                options.append(load_opt)

                # Add nested relationships
                target_class = rel.mapper.class_
                nested_opts = cls._build_relationship_loading_options(
                    recursive=True, seen=seen.copy()
                )
                for nested_opt in nested_opts:
                    options.append(load_opt.selectinload(nested_opt))
            return options
        else:
            # Direct relationships only
            return [
                selectinload(getattr(cls, rel.key))
                for rel in inspect(cls).relationships
            ]

    @classmethod
    def build_base_object_dict(
        cls, object_data: dict, owner_id: str, id: str = None, private: bool = True
    ) -> dict:
        """
        build a dictionary to create a new base object

        Args:
            object_data (dict): The data to create the object with.
            owner_id (str): The ID of the user creating the object
            id (str, optional): The ID of the object to create. Defaults to None (generate a unique ID).
            private (bool, optional): If True, the object is private (streamed only to ws_id websocket). Defaults to False.

        Returns:
            dict: The dictionary to create the object with
        """
        if not id:
            # generate a unique ID if not provided or None
            id = muty.string.generate_unique()
        else:
            # check id is a valid string for a primary key (not having spaces, ...)
            if " " in id or not re.match(r"^[a-zA-Z0-9_\-@\.]+$", id):
                raise ValueError(f"invalid id: {id}")

        # set the time created
        time_created = muty.time.now_msec()

        # remove None values
        object_data = {k: v for k, v in object_data.items() if v is not None}

        object_data["type"] = cls.__gulp_collab_type__
        object_data["id"] = id
        object_data["time_created"] = time_created
        object_data["time_updated"] = time_created
        object_data["owner_user_id"] = owner_id
        object_data["granted_user_group_ids"] = []
        if private:
            object_data["granted_user_ids"] = [owner_id]
        else:
            object_data["granted_user_ids"] = []

        if not object_data.get("name", None):
            # set the name to the id if not provided
            object_data["name"] = id
        return object_data

    @classmethod
    async def _create(
        cls,
        sess: AsyncSession,
        object_data: dict,
        id: str = None,
        ws_id: str = None,
        owner_id: str = None,
        ws_queue_datatype: "GulpWsQueueDataType" = None,
        ws_data: dict = None,
        req_id: str = None,
        private: bool = True,
    ) -> T:
        """
        Asynchronously creates and stores an instance of the class, also updating the websocket if required.

        Args:
            sess (AsyncSession): The database session to use.
            object_data (dict): The data to create the object with.
            id (str, optional): The ID of the object to create. Defaults to None (generate a unique ID).
            operation_id (str, optional): The ID of the operation associated with the instance. Defaults to None.
            ws_id (str, optional): WebSocket ID associated with the instance. Defaults to None.
            owner_id (str, optional): The user to be set as the owner of the object. Defaults to None("admin" user will be set).
            ws_queue_datatype (GulpWsQueueDataType, optional): The type of the websocket queue data. Defaults to GulpWsQueueDataType.COLLAB_UPDATE.
            ws_data (dict, optional): data to send to the websocket. Defaults to the created object.
            req_id (str, optional): Request ID associated with the instance. Defaults to None.
            private (bool, optional): If True, the object is private (streamed only to ws_id websocket). Defaults to True.
        Returns:
            T: The created instance of the class.
        Raises:
            Exception: If there is an error during the creation or storage process.
        """
        if not object_data:
            object_data = {}

        owner_id = owner_id or "admin"
        d = cls.build_base_object_dict(
            object_data, owner_id=owner_id, id=id, private=private
        )
        # create select statement with eager loading
        stmt = (
            select(cls)
            .options(*cls._build_relationship_loading_options())
            .from_statement(insert(cls).values(**d).returning(cls))
        )

        result = await sess.execute(stmt)
        instance: GulpCollabBase = result.scalar_one()
        instance_dict = instance.to_dict(nested=True, exclude_none=True)
        await sess.commit()

        if ws_id:
            from gulp.api.ws_api import (
                GulpCollabCreateUpdatePacket,
                GulpSharedWsQueue,
                GulpWsQueueDataType,
            )

            if not ws_queue_datatype:
                ws_queue_datatype = GulpWsQueueDataType.COLLAB_UPDATE

            # notify the websocket of the collab object creation
            if ws_data:
                data = ws_data
            else:
                data = instance_dict

            # FIXME: data=p.model.dump() creates data.data, which is ugly!
            p = GulpCollabCreateUpdatePacket(data=data, created=True)
            GulpSharedWsQueue.get_instance().put(
                ws_queue_datatype,
                ws_id=ws_id,
                user_id=owner_id,
                operation_id=object_data.get("operation_id", None),
                req_id=req_id,
                data=p.model_dump(exclude_none=True, exclude_defaults=True),
                private=private,
            )
        MutyLogger.get_instance().debug("created instance: %s" % (instance_dict))
        return instance

    async def add_group_grant(self, sess: AsyncSession, group_id: str) -> None:
        """
        grant a user group access to the object

        Args:
            sess (AsyncSession): The database session to use.
            group_id (str): The ID of the user group to add.
        Returns:
            None
        """
        if group_id not in self.granted_user_group_ids:
            MutyLogger.get_instance().info(
                "Adding granted user group %s to object %s" % (group_id, self.id)
            )
            self.granted_user_group_ids.append(group_id)
            await sess.commit()
            await sess.refresh(self)
        else:
            MutyLogger.get_instance().warning(
                "User group %s already granted on object %s" % (group_id, self.id)
            )

    async def remove_group_grant(self, sess: AsyncSession, group_id: str) -> None:
        """
        remove a user group access to the object

        Args:
            sess (AsyncSession): The database session to use.
            group_id (str): The ID of the user group to remove.
        Returns:
            None
        """
        if group_id in self.granted_user_group_ids:
            self.granted_user_group_ids.remove(group_id)
            await sess.commit()
            await sess.refresh(self)
            MutyLogger.get_instance().info(
                "Removed granted user group %s from object %s" % (group_id, self.id)
            )
        else:
            MutyLogger.get_instance().warning(
                "User group %s not in granted list on object %s" % (group_id, self.id)
            )

    async def add_user_grant(self, sess: AsyncSession, user_id: str) -> None:
        """
        grant a user access to the object

        Args:
            sess (AsyncSession): The session to use for the query.
            user_id (str): The ID of the user to add.
        Returns:
            None
        """
        if user_id not in self.granted_user_ids:
            MutyLogger.get_instance().info(
                "Adding granted user %s to object %s" % (user_id, self.id)
            )
            self.granted_user_ids.append(user_id)
            await sess.commit()
            await sess.refresh(self)
        else:
            MutyLogger.get_instance().warning(
                "User %s already granted on object %s" % (user_id, self.id)
            )

    async def remove_user_grant(self, sess: AsyncSession, user_id: str) -> None:
        """
        remove a user access to the object

        Args:
            sess (AsyncSession): The session to use for the query.
            user_id (str): The ID of the user to remove.
        Returns:
            None
        """
        if user_id in self.granted_user_ids:
            self.granted_user_ids.remove(user_id)
            await sess.commit()
            await sess.refresh(self)
            MutyLogger.get_instance().info(
                "Removed granted user %s from object %s" % (user_id, self.id)
            )
        else:
            MutyLogger.get_instance().warning(
                "User %s not in granted list on object %s" % (user_id, self.id)
            )

    async def delete(
        self,
        sess: AsyncSession,
        ws_id: str = None,
        user_id: str = None,
        ws_queue_datatype: "GulpWsQueueDataType" = None,
        ws_data: dict = None,
        req_id: str = None,
    ) -> None:
        """
        deletes the object, also updating the websocket if required.

        Args:
            sess (AsyncSession): The database session to use.
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            user_id (str, optional): The ID of the user making the request. Defaults to None.
            ws_queue_datatype (GulpWsQueueDataType, optional): The type of the websocket queue data. Defaults to GulpWsQueueDataType.COLLAB_DELETE.
            ws_data (dict, optional): data to send to the websocket. Defaults to GulpDeleteCollabPacket.
            req_id (str, optional): The ID of the request. Defaults to None.
        Raises:
            ObjectNotFoundError: If throw_if_not_found is True and the object does not exist.
        Returns:
            None
        """
        # query to get the instance
        stmt = select(self.__class__).filter(self.__class__.id == self.id)
        result = await sess.execute(stmt)
        instance = result.scalar_one()
        await sess.delete(instance)
        await sess.commit()

        if ws_id:
            from gulp.api.ws_api import (
                GulpCollabDeletePacket,
                GulpSharedWsQueue,
                GulpWsQueueDataType,
            )

            if not ws_queue_datatype:
                ws_queue_datatype = GulpWsQueueDataType.COLLAB_DELETE

            # notify the websocket of the deletion
            if ws_data:
                data = ws_data
            else:
                p: GulpCollabDeletePacket = GulpCollabDeletePacket(id=self.id)
                data = p.model_dump()
            GulpSharedWsQueue.get_instance().put(
                type=ws_queue_datatype,
                ws_id=ws_id,
                user_id=user_id,
                operation_id=getattr(self, "operation_id", None),
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
        self.granted_user_ids = [self.owner_user_id]
        await sess.commit()
        await sess.refresh(self)
        MutyLogger.get_instance().info(
            "object %s is now PRIVATE to user %s" % (self.id, self.owner_user_id)
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
        # clear all granted users and groups
        self.granted_user_group_ids = []
        self.granted_user_ids = []
        await sess.commit()
        await sess.refresh(self)
        MutyLogger.get_instance().info("Object %s is now PUBLIC" % (self.id))

    @staticmethod
    def object_type_to_class(collab_type: GulpCollabType) -> T:
        """
        get the class of the given type

        Args:
            collab_type (GulpCollabType): The type of the object.
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
        raise ValueError(f"no class found for type {collab_type}")

    async def update(
        self,
        sess: AsyncSession,
        d: dict,
        ws_id: str = None,
        user_id: str = None,
        ws_queue_datatype: "GulpWsQueueDataType" = None,
        ws_data: dict = None,
        req_id: str = None,
        updated_instance: T = None,
    ) -> None:
        """
        updates the object, also updating the websocket if required.

        Args:
            sess (AsyncSession): The database session to use: the session will be committed and refreshed after the update.
            d (dict): A dictionary containing the fields to update and their new values, must be None and is ignored if updated_instance is provided.
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            user_id (str, optional): The ID of the user making the request. Defaults to None.
            ws_queue_datatype (GulpWsQueueDataType, optional): The type of the websocket queue data. Defaults to GulpWsQueueDataType.COLLAB_UPDATE.
            ws_data (dict, optional): data to send to the websocket. Defaults to the updated object.
            req_id (str, optional): The ID of the request. Defaults to None.
            updated_instance (T, optional): An already updated instance of the object, if set d is ignored. Defaults to None.
        """
        if updated_instance:
            # use updated_instance if provided
            instance = updated_instance
        else:
            # use dict, query our instance with lock
            stmt = (
                select(self.__class__)
                .filter(self.__class__.id == self.id)
                .options(selectinload("*"))
                .with_for_update()
            )
            result = await sess.execute(stmt)
            instance: GulpCollabBase = result.scalar_one()

            # update instance from d, ensure d has no 'id' (the id cannot be updated)
            d.pop("id", None)
            for k, v in d.items():
                # only update if the value is not None and different from the current value
                if v is not None and getattr(instance, k, None) != v:
                    # MutyLogger.get_instance().debug(f"setattr: {k}={v}")
                    setattr(instance, k, v)

        # update time
        instance.time_updated = muty.time.now_msec()
        updated_dict = instance.to_dict(nested=True, exclude_none=True)
        private = instance.is_private()

        # commit
        await sess.commit()

        MutyLogger.get_instance().debug("---> updated: %s" % (updated_dict))

        if ws_id:
            from gulp.api.ws_api import (
                GulpCollabCreateUpdatePacket,
                GulpSharedWsQueue,
                GulpWsQueueDataType,
            )

            if not ws_queue_datatype:
                ws_queue_datatype = GulpWsQueueDataType.COLLAB_UPDATE

            # notify the websocket of the collab object update
            if ws_data:
                data = ws_data
            else:
                data = updated_dict
                p = GulpCollabCreateUpdatePacket(data=data)
            GulpSharedWsQueue.get_instance().put(
                type=ws_queue_datatype,
                ws_id=ws_id,
                user_id=user_id,
                operation_id=data.get("operation_id", None),
                req_id=req_id,
                data=p.model_dump(exclude_none=True, exclude_defaults=True),
                private=private,
            )

    @classmethod
    async def get_by_id(
        cls,
        sess: AsyncSession,
        id: str,
        throw_if_not_found: bool = True,
        with_for_update: bool = False,
        recursive: bool = False,
    ) -> T:
        """
        Asynchronously retrieves an object of the class type with the specified ID.

        Args:
            sess (AsyncSession): The database session to use.
            id (str): The ID of the object to retrieve.
            throw_if_not_found (bool, optional): If True, raises an exception if the object is not found. Defaults to True.
            with_for_update (bool, optional): If True, the query will be executed with the FOR UPDATE clause (lock). Defaults to False.
            recursive (bool, optional): If True, loads nested relationships recursively. Defaults to False.
        Returns:
            T: The object with the specified ID or None if not found.
        Raises:
            ObjectNotFound: If the object with the specified ID is not found.
        """
        loading_options = cls._build_relationship_loading_options(recursive=recursive)

        stmt = select(cls).options(*loading_options).filter(cls.id == id)
        if with_for_update:
            stmt = stmt.with_for_update()
        res = await sess.execute(stmt)
        c = res.scalar_one_or_none()
        if not c and throw_if_not_found:
            raise ObjectNotFound(f'{cls.__name__} with id "{id}" not found')

        return c

    @classmethod
    async def get_by_filter(
        cls,
        sess: AsyncSession,
        flt: GulpCollabFilter = None,
        throw_if_not_found: bool = True,
        with_for_update: bool = False,
    ) -> list[T]:
        """
        Asynchronously retrieves a list of objects based on the provided filter.
        Args:
            sess (AsyncSession): The database session to use.
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None (all objects).
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to True.
            with_for_update (bool, optional): If True, the query will be executed with the FOR UPDATE clause (lock). Defaults to False.
        Returns:
            list[T]: A list of objects that match the filter criteria.
        Raises:
            Exception: If there is an error during the query execution or result processing.
        """

        # filter or empty filter
        flt = flt or GulpCollabFilter()

        # build and run query (ensure eager loading)
        q = flt.to_select_query(cls, with_for_update=with_for_update)
        q = q.options(*cls._build_relationship_loading_options())
        # MutyLogger.get_instance().debug(f"get_by_filter query:\n{q}")
        res = await sess.execute(q)
        objects = res.scalars().all()
        if not objects:
            if throw_if_not_found:
                raise ObjectNotFound(
                    f"No {cls.__name__} found with filter {flt}", cls.__name__, str(flt)
                )
            else:
                return []
        return objects

    @classmethod
    async def get_first_by_filter(
        cls,
        sess: AsyncSession,
        flt: GulpCollabFilter = None,
        throw_if_not_found: bool = True,
        with_for_update: bool = False,
    ) -> T:
        """
        Asynchronously retrieves the first object based on the provided filter.

        Args:
            sess (AsyncSession): The database session to use.
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None (all objects).
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to True.
            with_for_update (bool, optional): If True, the query will be executed

        Returns:
            T: The first object that matches the filter criteria or None if not found.
        """
        obj = await cls.get_by_filter(
            sess,
            flt=flt,
            throw_if_not_found=throw_if_not_found,
            with_for_update=with_for_update,
        )

        if obj:
            return obj[0]
        return None

    @classmethod
    async def get_by_id_wrapper(
        cls,
        token: str,
        id: str,
        with_for_update: bool = False,
        permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        nested: bool = False,
        enforce_owner: bool = False,
    ) -> dict:
        """
        helper to get an object by ID, handling session

        Args:
            token (str): The user token.
            id (str): The ID of the object to get.
            with_for_update (bool, optional): If True, the query will be executed with the FOR UPDATE clause (lock). Defaults to False.
            permission (list[GulpUserPermission], optional): The permission required to read the object. Defaults to GulpUserPermission.READ.
            nested (bool, optional): If True, nested relationships will be loaded. Defaults to False.
            enforce_owner (bool, optional): If True, the user must be the owner of the object (or admin). Defaults to False.

        Returns:
            dict: The object as a dictionary

        Raises:
            MissingPermissionError: If the user does not have permission to read the object.
            ObjectNotFound: If the object is not found.
        """
        from gulp.api.collab_api import GulpCollab
        from gulp.api.collab.user_session import GulpUserSession

        async with GulpCollab.get_instance().session() as sess:
            n: GulpCollabBase = await cls.get_by_id(
                sess, id, with_for_update=with_for_update
            )

            # token needs at least read permission (or be the owner)
            await GulpUserSession.check_token(
                sess, token, permission=permission, obj=n, enforce_owner=enforce_owner
            )
            return n.to_dict(exclude_none=True, nested=nested)

    @classmethod
    async def get_by_filter_wrapper(
        cls,
        token: str,
        flt: GulpCollabFilter,
        permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        throw_if_not_found: bool = False,
        nested: bool = False,
    ) -> list[dict]:
        """
        helper to get objects by filter, handling session

        Args:
            token (str): The user token.
            flt (GulpCollabFilter): The filter to apply to the query.
            permission (list[GulpUserPermission], optional): The permission required to read the object. Defaults to GulpUserPermission.READ.
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to False (return empty list).
            nested (bool, optional): If True, nested relationships will be loaded. Defaults to False.
        Returns:
            list[dict]: The list of object dictionaries that match the filter criteria.
        """
        from gulp.api.collab_api import GulpCollab
        from gulp.api.collab.user_session import GulpUserSession

        async with GulpCollab.get_instance().session() as sess:
            # token needs at least read permission
            s = await GulpUserSession.check_token(sess, token, permission=permission)
            objs = await cls.get_by_filter(
                sess, flt, throw_if_not_found=throw_if_not_found
            )
            if not objs:
                return []

            data = []
            for o in objs:
                o: GulpCollabBase
                # perform access checks on the object
                if s.user.check_object_access(o):
                    data.append(o.to_dict(exclude_none=True, nested=nested))
                else:
                    MutyLogger.get_instance().warning(
                        "User %s does not have permission to access object: %s"
                        % (
                            s.user.id,
                            json.dumps(
                                o.to_dict(exclude_none=True, nested=nested), indent=2
                            ),
                        )
                    )

            MutyLogger.get_instance().debug(
                "User %s get_by_filter_result: %s"
                % (
                    s.user.id,
                    json.dumps(data, indent=2),
                )
            )

            return data

    @classmethod
    async def delete_by_id(
        cls,
        token: str,
        id: str,
        ws_id: str,
        req_id: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.DELETE],
    ) -> None:
        """
        helper to delete an object by ID, handling session

        Args:
            token (str): The user token.
            id (str): The ID of the object to delete.
            ws_id (str): The websocket ID.
            req_id (str): The request ID.
            permission (list[GulpUserPermission], optional): The permission required to delete the object. Defaults to GulpUserPermission.DELETE.

        Raises:
            MissingPermissionError: If the user does not have permission to delete the object.
            ObjectNotFoundError: If the object is not found.
        """
        from gulp.api.collab_api import GulpCollab
        from gulp.api.collab.user_session import GulpUserSession

        async with GulpCollab.get_instance().session() as sess:
            n: GulpCollabBase = await cls.get_by_id(sess, id, with_for_update=True)

            # token needs at least delete permission (or be the owner)
            s = await GulpUserSession.check_token(
                sess, token, permission=permission, obj=n
            )

            # delete
            await n.delete(sess, ws_id=ws_id, user_id=s.user_id, req_id=req_id)

    @classmethod
    async def update_by_id(
        cls,
        token: str,
        id: str,
        ws_id: str,
        req_id: str,
        d: dict = None,
        updated_instance: T = None,
        permission: list[GulpUserPermission] = [GulpUserPermission.EDIT],
    ) -> dict:
        """
        helper to update an object by ID, handling session

        Args:
            token (str): The user token.
            id (str): The ID of the object to update.
            ws_id (str): The websocket ID.
            req_id (str): The request ID.
            d (dict, optional): The data to update the object with. Defaults to None.
            updated_instance (T, optional): An already updated instance of the object. Defaults to None.
            permission (list[GulpUserPermission], optional): The permission required to update the object. Defaults to GulpUserPermission.EDIT.

        Returns:
            dict: The updated object as a dictionary.

        Raises:
            ValueError: If both d and updated_instance are provided.
            MissingPermissionError: If the user does not have permission to update the object.
        """
        from gulp.api.collab_api import GulpCollab
        from gulp.api.collab.user_session import GulpUserSession

        async with GulpCollab.get_instance().session() as sess:
            if d and updated_instance:
                raise ValueError("only one of d or updated_instance should be provided")

            n: GulpCollabBase = await cls.get_by_id(sess, id, with_for_update=True)

            # token needs at least edit permission (or be the owner)
            s = await GulpUserSession.check_token(
                sess, token, permission=permission, obj=n
            )
            await n.update(
                sess,
                d=d,
                ws_id=ws_id,
                user_id=s.user_id,
                req_id=req_id,
                updated_instance=updated_instance,
            )
            return n.to_dict(exclude_none=True)

    @classmethod
    async def create(
        cls,
        token: str,
        ws_id: str,
        req_id: str,
        object_data: dict,
        permission: list[GulpUserPermission] = [GulpUserPermission.EDIT],
        id: str = None,
        private: bool = True,
    ) -> dict:
        """
        helper to create a new object, handling session

        Args:
            token (str): The user token.
            ws_id (str): The websocket ID: pass None to not notify the websocket.
            req_id (str): The request ID.
            object_data (dict): The data to create the object with.
            permission (list[GulpUserPermission], optional): The permission required to create the object. Defaults to GulpUserPermission.EDIT.
            id (str, optional): The ID of the object to create. Defaults to None (generate a unique ID).
            private (bool, optional): If True, the object will be private. Defaults to False.
        Returns:
            dict: The created object as a dictionary.

        Raises:
            MissingPermissionError: If the user does not have permission to create the object.
        """
        from gulp.api.collab_api import GulpCollab
        from gulp.api.collab.user_session import GulpUserSession

        async with GulpCollab.get_instance().session() as sess:

            # check permission for creation
            s = await GulpUserSession.check_token(sess, token, permission=permission)
            n: GulpCollabBase = await cls._create(
                sess,
                object_data,
                id=id,
                owner_id=s.user_id,
                ws_id=ws_id,
                req_id=req_id,
                private=private,
            )
            return n.to_dict(exclude_none=True)


class GulpCollabObject(
    GulpCollabBase, type=GulpCollabType.GENERIC_OBJECT, abstract=True
):
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
        MutyLogger.get_instance().debug("---> GulpCollabObject: " % (*args, kwargs))
