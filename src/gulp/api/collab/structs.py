import re
from enum import StrEnum
from typing import List, Optional, TypeVar, override

import muty.string
import muty.time
from muty.log import MutyLogger
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import (
    ARRAY,
    BIGINT,
    Boolean,
    ColumnElement,
    ForeignKey,
    Select,
    String,
    Tuple,
    func,
    insert,
    inspect,
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

from gulp.api.opensearch.structs import GulpBasicDocument
from gulp.api.ws_api import (
    GulpDeleteCollabPacket,
    GulpSharedWsQueue,
    GulpWsQueueDataType,
)
from gulp.structs import ObjectNotFound


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

    NOTE = "note"
    HIGHLIGHT = "highlight"
    STORY = "story"
    LINK = "link"
    STORED_QUERY = "stored_query"
    INGESTION_STATS = "ingestion_stats"
    USER_DATA = "user_data"
    USER_SESSION = "user_session"
    CONTEXT = "context"
    USER = "user"
    GLYPH = "glyph"
    OPERATION = "operation"
    SOURCE = "source"
    USER_GROUP = "user_group"


T = TypeVar("T", bound="GulpCollabBase")


class GulpCollabFilter(BaseModel):
    """
    defines filter to be applied to all objects in the collaboration system

    allow extra fields to be interpreted as additional filters on the object columns as simple key-value pairs
    """

    model_config = ConfigDict(extra="allow")

    id: Optional[list[str]] = Field(None, description="filter by the given id/s.")
    type: Optional[list[GulpCollabType]] = Field(
        None, description="filter by the given type/s."
    )
    operation_id: Optional[list[str]] = Field(
        None, description="filter by the given operation/s."
    )
    context_id: Optional[list[str]] = Field(
        None, description="filter by the given context/s."
    )
    source_id: Optional[list[str]] = Field(
        None, description="filter by the given source path/s or name/s."
    )
    owner_user_id: Optional[list[str]] = Field(
        None, description="filter by the given owner user id/s."
    )
    tags: Optional[list[str]] = Field(None, description="filter by the given tag/s.")
    name: Optional[list[str]] = Field(None, description="filter by the given name/s.")
    text: Optional[list[str]] = Field(
        None, description="filter by the given object text."
    )
    documents: Optional[list[GulpBasicDocument]] = Field(
        None,
        description="filter by the given event ID/s in a CollabObj.documents list of GulpBasicDocument.",
    )
    time_range: Optional[tuple[int, int]] = Field(
        None,
        description="if set, a `gulp.timestamp` range [start, end] relative to CollabObject.documents, inclusive, in nanoseconds from unix epoch.",
    )
    private: Optional[bool] = Field(
        False,
        description="if True, return only private objects. Default=False (return all).",
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

    @override
    def __str__(self) -> str:
        return self.model_dump_json(exclude_none=True)

    def _case_insensitive_or_ilike(self, column, values: list) -> ColumnElement[bool]:
        """
        Create a case-insensitive OR query with wildcards for the given column and values.

        Args:
            column: The column to apply the ilike condition.
            values: The list of values to match against the column.

        Returns:
            ColumnElement[bool]: The OR query.
        """
        # print("column=%s, values=%s" % (column, values))
        conditions = [column.ilike(f"%{value}%") for value in values]
        return or_(*conditions)

    def to_select_query(self, type: T, with_for_update: bool = False) -> Select[Tuple]:
        """
        convert the filter to a select query

        Args:
            type (T): the type of the object (one derived from GulpCollabBase)

        Returns:
            Select[Tuple]: the select query
        """
        q: Select = select(type)
        if self.id:
            q = q.filter(self._case_insensitive_or_ilike(type.id, self.id))
        if self.type:
            # match if equal to any in the list
            q = q.filter(type.type.in_(self.type))
        if self.operation_id and "operation_id" in type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(type.operation_id, self.operation_id)
            )
        if self.context_id and "context_id" in type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(type.context_id, self.context_id)
            )
        if self.source_id and "source_id" in type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(type.source_id, self.source_id)
            )
        if self.owner_user_id and "owner_user_id" in type.columns:
            q = q = q.filter(
                self._case_insensitive_or_ilike(type.owner_user_id, self.owner_user_id)
            )
        if self.tags and "tags" in type.columns:
            lower_tags = [tag.lower() for tag in self.tags]
            if self.tags_and:
                # all tags must match (CONTAINS operator)
                q = q.filter(func.lower(type.tags).op("@>")(lower_tags))
            else:
                # at least one tag must match (OVERLAP operator)
                q = q.filter(func.lower(type.tags).op("&&")(self.tags))
        if self.name and "name" in type.columns:
            q = q.filter(self._case_insensitive_or_ilike(type.name, self.name))
        if self.text and "text" in type.columns:
            q = q.filter(self._case_insensitive_or_ilike(type.text, self.text))

        if self.model_extra:
            # any extra k,v to filter on
            for k, v in self.model_extra.items():
                if k in type.columns:
                    q = q.filter(self._case_insensitive_or_ilike(getattr(type, k), v))

        if self.documents and "documents" in type.columns:
            if not self.time_range:
                # filter by collabobj.documents id
                lower_documents = [{"_id": doc_id.lower()} for doc_id in self.documents]
                conditions = [
                    func.lower(type.documents).op("@>")([{"_id": doc_id}])
                    for doc_id in lower_documents
                ]
                q = q.filter(or_(*conditions))
            else:
                # filter by time range on collabobj.documents["gulp.timestamp"]
                conditions = []
                if self.time_range[0]:
                    conditions.append(
                        f"(doc->>'gulp.timestamp')::bigint >= {self.time_range[0]}"
                    )
                if self.time_range[1]:
                    conditions.append(
                        f"(doc->>'gulp.timestamp')::bigint <= {self.time_range[1]}"
                    )

                # use a raw query to filter for the above conditions
                table_name = type.__tablename__
                condition_str = " AND ".join(conditions)
                raw_sql = f"""
                EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements({table_name}.documents) AS doc
                    WHERE {condition_str}
                )
                """
                q = q.filter(text(raw_sql))

        if self.limit:
            q = q.limit(self.limit)
        if self.offset:
            q = q.offset(self.offset)
        if self.private:
            q = q.where(GulpCollabObject.private is True)

        if with_for_update:
            q = q.with_for_update()
        # MutyLogger.get_instance().debug(f"to_select_query: {q}")
        return q


class GulpCollabBase(MappedAsDataclass, AsyncAttrs, DeclarativeBase, SerializeMixin):
    """
    base for everything on the collab database
    """

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
        doc="The ids of the users who have been granted access to the object.",
    )
    granted_user_group_ids: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="The ids of the user groups who have been granted access to the object.",
    )
    time_created: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        doc="The time the object was created, in milliseconds from unix epoch.",
    )
    time_updated: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        doc="The time the object was last updated, in milliseconds from unix epoch.",
    )

    __mapper_args__ = {
        "polymorphic_identity": "collab_base",
        "polymorphic_on": "type",
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

        # set attributes from keyword arguments
        if "id" not in kwargs or not kwargs["id"]:
            # generate a unique ID if not provided or None
            kwargs["id"] = muty.string.generate_unique()
        else:
            # check id is a valid string for a primary key (not having spaces, ...)
            k = kwargs["id"]
            if not k or " " in k or not re.match("^[a-zA-Z0-9_-]+$", k):
                raise ValueError(f"invalid id: {k}")

        for k, v in kwargs.items():
            setattr(self, k, v)

        # set the time_created and time_updated attributes
        self.time_created = muty.time.now_msec()
        self.time_updated = self.time_created
        self.granted_user_group_ids = []
        self.granted_user_ids = []

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
    def _build_relationship_loading_options(cls):
        """Helper method to build query options for eager loading relationships"""
        from sqlalchemy.orm import selectinload

        relationships = cls._get_nested_relationships(cls)
        return [selectinload(getattr(cls, rel_name)) for rel_name in relationships]

    @classmethod
    async def _create(
        cls,
        sess: AsyncSession,
        token: str = None,
        permission: list[GulpUserPermission] = None,
        id: str = None,
        ws_id: str = None,
        ws_queue_datatype: GulpWsQueueDataType = GulpWsQueueDataType.COLLAB_UPDATE,
        ws_data: dict = None,
        req_id: str = None,
        owner_user_id: str = None,
        **kwargs,
    ) -> T:
        """
        Asynchronously creates and stores an instance of the class.

        Args:
            sess (AsyncSession): The database session to use.
            token (str, optional): The token of the user making the request. Defaults to None (no check).
            permission (list[GulpUserPermission], optional): The permission required to create the object. Defaults to None.
            id (str, optional): The ID of the object to create. Defaults to None (generate a unique ID).
            ws_id (str, optional): WebSocket ID associated with the instance. Defaults to None.
            ws_queue_datatype (GulpWsQueueDataType, optional): The type of the websocket queue data. Defaults to GulpWsQueueDataType.COLLAB_UPDATE.
            ws_data (dict, optional): data to send to the websocket. Defaults to the created object.
            req_id (str, optional): Request ID associated with the instance. Defaults to None.
            owner_user_id (str, optional): The ID of the user who owns the instance. Defaults to None (get from token or "admin" if token is None).
            **kwargs: Additional keyword arguments to set as attributes on the instance.
        Returns:
            T: The created instance of the class.
        Raises:
            Exception: If there is an error during the creation or storage process.
        """
        if token:
            # check_token_permission here
            from gulp.api.collab.user_session import GulpUserSession

            user_session = await GulpUserSession.check_token(
                sess, token, permission=permission
            )
            owner = user_session.user_id
        else:
            # no token, use default owner "admin"
            owner = "admin"

        if owner_user_id:
            # force owner to id
            owner = owner_user_id

        # create select statement with eager loading
        stmt = (
            select(cls)
            .options(*cls._build_relationship_loading_options())
            .from_statement(
                insert(cls).values(id=id, user_id=owner, **kwargs).returning(cls)
            )
        )
        result = await sess.execute(stmt)
        instance: GulpCollabBase = result.scalar_one()
        await sess.commit()
        await sess.refresh(instance)

        if ws_id and ws_queue_datatype:
            # notify the websocket of the collab object creation
            data = instance.to_dict(exclude_none=True)
            data["created"] = True
            GulpSharedWsQueue.get_instance().put(
                ws_queue_datatype,
                ws_id=ws_id,
                user_id=owner,
                operation_id=data.get("operation_id", None),
                req_id=req_id,
                private=data.get("private", False),
                data=[data],
            )

        return instance

    async def grant_group(self, sess: AsyncSession, group_id: str) -> None:
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
        else:
            MutyLogger.get_instance().warning(
                "User group %s already granted on object %s" % (group_id, self.id)
            )

    async def ungrant_group(self, sess: AsyncSession, group_id: str) -> None:
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
            MutyLogger.get_instance().info(
                "Removed granted user group %s from object %s" % (group_id, self.id)
            )
        else:
            MutyLogger.get_instance().warning(
                "User group %s not in granted list on object %s" % (group_id, self.id)
            )

    async def grant_user(self, sess: AsyncSession, user_id: str) -> None:
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
        else:
            MutyLogger.get_instance().warning(
                "User %s already granted on object %s" % (user_id, self.id)
            )

    async def ungrant_user(self, sess: AsyncSession, user_id: str) -> None:
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
        token: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.DELETE],
        ws_id: str = None,
        ws_queue_datatype: GulpWsQueueDataType = GulpWsQueueDataType.COLLAB_DELETE,
        ws_data: dict = None,
        req_id: str = None,
    ) -> None:
        """
        deletes the object

        Args:
            sess (AsyncSession): The database session to use.
            token (str): The token of the user making the request.
            permission (list[GulpUserPermission], optional): The required permission to delete the object. Defaults to [GulpUserPermission.DELETE].
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            ws_queue_datatype (GulpWsQueueDataType, optional): The type of the websocket queue data. Defaults to GulpWsQueueDataType.COLLAB_DELETE.
            ws_data (dict, optional): data to send to the websocket. Defaults to the GulpDeleteCollabPacket.
            req_id (str, optional): The ID of the request. Defaults to None.
        Raises:
            ObjectNotFoundError: If throw_if_not_found is True and the object does not exist.
        Returns:
            None
        """
        from gulp.api.collab.user_session import GulpUserSession

        s = GulpUserSession.check_token(sess, token, permission=permission, obj=self)
        MutyLogger.get_instance().info(
            "Deleting object %s of type %s" % (self.id, self.__gulp_collab_type__)
        )
        user_id = s.user_id

        # query with lock
        stmt = (
            select(self.__class__)
            .filter(self.__class__.id == self.id)
            .with_for_update()
        )
        result = await sess.execute(stmt)
        instance = result.scalar_one()
        sess.delete(instance)
        await sess.commit()

        if ws_id:
            # notify the websocket of the deletion
            p: GulpDeleteCollabPacket = GulpDeleteCollabPacket(id=self.id)
            GulpSharedWsQueue.get_instance().put(
                type=ws_queue_datatype,
                ws_id=ws_id,
                user_id=user_id,
                operation_id=getattr(self, "operation_id", None),
                req_id=req_id,
                private=getattr(self, "private", False),
                data=ws_data or p.model_dump(),
            )

    async def update(
        self,
        sess: AsyncSession,
        token: str,
        d: dict,
        permission: list[GulpUserPermission] = None,
        ws_id: str = None,
        ws_queue_datatype: GulpWsQueueDataType = GulpWsQueueDataType.COLLAB_UPDATE,
        req_id: str = None,
        **kwargs,
    ) -> None:
        """
        Asynchronously updates the object with the specified fields and values.

        Args:
            sess (AsyncSession): The database session to use.
            token (str): The token of the user making the request, pass None to skip token check (internal only)
            d (dict): A dictionary containing the fields to update and their new values, ignored if updated_instance is provided in kwargs.
            permission (list[GulpUserPermission], optional): The required permission to update the object. Defaults to [GulpUserPermission.EDIT].
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            ws_queue_datatype (GulpWsQueueDataType, optional): The type of the websocket queue data. Defaults to GulpWsQueueDataType.COLLAB_UPDATE.
            req_id (str, optional): The ID of the request. Defaults to None.
            throw_if_not_found (bool, optional): If True, raises an exception if the object is not found. Defaults to True.
        """
        from gulp.api.collab.user_session import GulpUserSession

        if token:
            s = GulpUserSession.check_token(
                sess, token, permission=permission, obj=self
            )
            user_id = s.user_id
        else:
            # internal shortcut to update without checking permissions
            user_id = self.owner_user_id

        # query with lock
        stmt = (
            select(self.__class__)
            .filter(self.__class__.id == self.id)
            .with_for_update()
        )
        result = await sess.execute(stmt)
        instance: GulpCollabBase = result.scalar_one()

        # ensure d has no 'id'
        d.pop("id", None)
        for k, v in d.items():
            # MutyLogger.get_instance().debug(f"setattr: {k}={v}")
            setattr(self, k, v)

        # update time
        instance.time_updated = muty.time.now_msec()

        # commit
        await sess.commit()
        await sess.refresh(instance)

        MutyLogger.get_instance().debug("---> updated: %s" % (self))

        # notify the websocket of the collab object update
        if ws_id and ws_queue_datatype:
            data = self.to_dict(exclude_none=True, nested=True)
            GulpSharedWsQueue.get_instance().put(
                type=ws_queue_datatype,
                ws_id=ws_id,
                user_id=user_id,
                operation_id=data.get("operation_id", None),
                req_id=req_id,
                private=data.get("private", False),
                data=[data],
            )

    @classmethod
    async def get_by_id(
        cls,
        sess: AsyncSession,
        token: str,
        id: str,
        permission: list[GulpUserPermission] = None,
        throw_if_not_found: bool = True,
        with_for_update: bool = False,
    ) -> T:
        """
        Asynchronously retrieves an object of the class type with the specified ID.

        Args:
            sess (AsyncSession): The database session to use.
            token (str): The token of the user making the request, pass None to skip token check (internal only)
            id (str): The ID of the object to retrieve.
            permission (list[GulpUserPermission], optional): The required permission to retrieve the object. Defaults to None.
            throw_if_not_found (bool, optional): If True, raises an exception if the object is not found. Defaults to True.
            with_for_update (bool, optional): If True, the query will be executed with the FOR UPDATE clause (lock). Defaults to False.
        Returns:
            T: The object with the specified ID or None if not found.
        Raises:
            ObjectNotFound: If the object with the specified ID is not found.
        """
        from gulp.api.collab.user_session import GulpUserSession

        if token:
            # check_token_permission here
            GulpUserSession.check_token(sess, token, permission=permission)

        stmt = (
            select(cls)
            .options(*cls._build_relationship_loading_options())
            .filter(cls.id == id)
        )
        if with_for_update:
            stmt = stmt.with_for_update()
        res = await sess.execute(stmt)
        c = res.scalar_one_or_none()
        if not c and throw_if_not_found:
            raise ObjectNotFound(
                f"{cls.__name__} with id {id} not found", cls.__name__, id
            )
        return c

    @classmethod
    async def get_by_filter(
        cls,
        sess: AsyncSession,
        token: str,
        permission: list[GulpUserPermission] = None,
        flt: GulpCollabFilter = None,
        throw_if_not_found: bool = True,
        with_for_update: bool = False,
    ) -> list[T]:
        """
        Asynchronously retrieves a list of objects based on the provided filter.
        Args:
            sess (AsyncSession): The database session to use.
            token (str): The token of the user making the request, pass None to skip token check (internal only)
            permission (list[GulpUserPermission], optional): The required permission to retrieve the object/s. Defaults to None.
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None (all objects).
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to True.
            with_for_update (bool, optional): If True, the query will be executed with the FOR UPDATE clause (lock). Defaults to False.
        Returns:
            list[T]: A list of objects that match the filter criteria.
        Raises:
            Exception: If there is an error during the query execution or result processing.
        """
        if token:
            # check_token_permission here
            from gulp.api.collab.user_session import GulpUserSession

            GulpUserSession.check_token(sess, token, permission=permission)

        # filter or empty filter
        flt = flt or GulpCollabFilter()

        # build and run query (ensure eager loading)
        q = flt.to_select_query(cls, with_for_update=with_for_update)
        q = q.options(*cls._build_relationship_loading_options())
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


class GulpCollabConcreteBase(GulpCollabBase, type="collab_base"):
    """
    Concrete base class for GulpCollabBase to ensure a table is created.
    """

    pass


class GulpCollabObject(GulpCollabBase, type="collab_obj", abstract=True):
    """
    base for all collaboration objects (notes, links, stories, highlights) related to an operation
    """

    operation_id: Mapped[str] = mapped_column(
        ForeignKey(
            "operation.id",
            ondelete="CASCADE",
        ),
        doc="The id of the operation associated with the object.",
    )
    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"), doc="The glyph ID."
    )
    tags: Mapped[Optional[list[str]]] = mapped_column(
        MutableList.as_mutable(ARRAY(String)),
        doc="The tags associated with the object.",
    )
    color: Mapped[Optional[str]] = mapped_column(
        String, doc="The color associated with the object."
    )
    name: Mapped[Optional[str]] = mapped_column(
        String, doc="The display name of the object."
    )
    description: Mapped[Optional[str]] = mapped_column(
        String, doc="The description of the object."
    )
    private: Mapped[Optional[bool]] = mapped_column(
        Boolean,
        doc="If True, the object is private (only the owner can see it).",
    )

    @override
    def __init__(self, *args, **kwargs):
        if self.type == GulpCollabObject:
            raise NotImplementedError(
                "GulpCollabObject is an abstract class and cannot be instantiated directly."
            )
        super().__init__(*args, **kwargs)
        MutyLogger.get_instance().debug("---> GulpCollabObject: " % (*args, kwargs))
