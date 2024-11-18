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
    Result,
    Select,
    String,
    Tuple,
    func,
    inspect,
    or_,
    select,
    text,
)
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    joinedload,
    mapped_column,
    selectinload,
)
from sqlalchemy.types import Enum as SqlEnum
from sqlalchemy_mixins.serialize import SerializeMixin

from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.structs import GulpBasicDocument
from gulp.api.ws_api import GulpSharedWsQueue, WsQueueDataType
from gulp.structs import ObjectNotFound


class SessionExpired(Exception):
    """if the user session has expired"""


class WrongUsernameOrPassword(Exception):
    """if the user provides wrong username or password"""


class MissingPermission(Exception):
    """if the user does not have the required permission"""


COLLAB_MAX_NAME_LENGTH = 128


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
    owner_id: Optional[list[str]] = Field(
        None, description="filter by the given owner user id/s."
    )
    tags: Optional[list[str]] = Field(None, description="filter by the given tag/s.")
    title: Optional[list[str]] = Field(None, description="filter by the given title/s.")
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

    def to_select_query(self, type: T) -> Select[Tuple]:
        """
        convert the filter to a select query

        Args:
            type (T): the type of the object (one derived from GulpCollabBase)

        Returns:
            Select[Tuple]: the select query
        """
        q = select(type)
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
        if self.owner_id and "owner_id" in type.columns:
            q = q = q.filter(
                self._case_insensitive_or_ilike(type.owner_id, self.owner_id)
            )
        if self.tags and "tags" in type.columns:
            lower_tags = [tag.lower() for tag in self.tags]
            if self.tags_and:
                # all tags must match (CONTAINS operator)
                q = q.filter(func.lower(type.tags).op("@>")(lower_tags))
            else:
                # at least one tag must match (OVERLAP operator)
                q = q.filter(func.lower(type.tags).op("&&")(self.tags))
        if self.title and "title" in type.columns:
            q = q.filter(self._case_insensitive_or_ilike(type.title, self.title))
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

        MutyLogger.get_instance().debug(f"to_select_query: {q}")
        return q


class GulpCollabBase(MappedAsDataclass, AsyncAttrs, DeclarativeBase, SerializeMixin):
    """
    base for everything on the collab database
    """

    id: Mapped[str] = mapped_column(
        String(COLLAB_MAX_NAME_LENGTH),
        primary_key=True,
        unique=True,
        doc="The unque id/name of the object.",
    )
    type: Mapped[GulpCollabType] = mapped_column(
        SqlEnum(GulpCollabType), doc="The type of the object."
    )
    owner_id: Mapped[str] = mapped_column(
        ForeignKey("user.id", ondelete="CASCADE"),
        doc="The id of the user who created(=owns) the object.",
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
        d = super().to_dict(nested, hybrid_attributes, exclude)
        if not exclude_none:
            return d

        return {k: v for k, v in d.items() if v is not None}

    @classmethod
    async def _create(
        cls,
        token: str = None,
        id: str = None,
        required_permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        ensure_eager_load: bool = False,
        **kwargs,
    ) -> T:
        """
        Asynchronously creates and stores an instance of the class.

        Args:
            token (str, optional): The token of the user making the request. Defaults to None (no check).
            id (str, optional): The ID of the object to create. Defaults to None (generate a unique ID).
            required_permission (list[GulpUserPermission], optional): The required permission to create the object. Defaults to [GulpUserPermission.READ].
            ws_id (str, optional): WebSocket ID associated with the instance. Defaults to None.
            req_id (str, optional): Request ID associated with the instance. Defaults to None.
            sess (AsyncSession, optional): The database session to use.<br>
                If None, a new session is created and committed in a transaction.<br>
                either the caller must handle the transaction and commit itself. Defaults to None (create and commit).
            ensure_eager_load (bool, optional): If True, eagerly load the instance with all related attributes. Defaults to False.
            **kwargs: Additional keyword arguments to set as attributes on the instance.
                - "owner_id" is a special keyword argument that can be used to set the owner of the instance to the specified ID.
        Returns:
            T: The created instance of the class.
        Raises:
            Exception: If there is an error during the creation or storage process.
        """

        async def _create_internal(
            token: str,
            id: str,
            required_permission: list[GulpUserPermission],
            ws_id: str,
            req_id: str,
            sess: AsyncSession,
            ensure_eager_load: bool,
            **kwargs,
        ) -> T:
            if token:
                # check_token_permission here
                from gulp.api.collab.user_session import GulpUserSession

                user_session = await GulpUserSession.check_token_permission(
                    token, required_permission, sess=sess
                )
                owner = user_session.user_id
            else:
                # no token, use default owner
                owner = "admin"

            if "owner_id" in kwargs:
                # force owner to id
                owner = kwargs.pop("owner_id")

            # create instance initializing the base class object (time created, time updated will be set by __init__)
            instance = cls(id=id, owner_id=owner, **kwargs)

            # and put on db
            sess.add(instance)
            await sess.commit()

            MutyLogger.get_instance().debug(
                f"---> _create_internal: object created: {instance.id}, type={cls.__gulp_collab_type__}, owner_id={owner}"
            )
            if ensure_eager_load:
                # eagerly load the instance with all related attributes
                instance = await instance.eager_load(sess)

            if ws_id and isinstance(instance, GulpCollabObject):
                # notify the websocket of the collab object creation
                data = instance.to_dict(exclude_none=True)
                data["created"] = True
                await GulpSharedWsQueue.get_instance().put(
                    WsQueueDataType.COLLAB_UPDATE,
                    ws_id=ws_id,
                    user_id=owner,
                    operation_id=data.get("operation", None),
                    req_id=req_id,
                    private=data.get("private", False),
                    data=[data],
                )

            return instance

        MutyLogger.get_instance().debug(
            "---> _create: id=%s, type=%s, token=%s, required_permission=%s, ws_id=%s, req_id=%s, sess=%s, ensure_eager_load=%s, kwargs=%s"
            % (
                id,
                cls.__gulp_collab_type__,
                token,
                required_permission,
                ws_id,
                req_id,
                sess,
                ensure_eager_load,
                muty.string.make_shorter(kwargs, 100),
            )
        )
        if not sess:
            sess = GulpCollab.get_instance().session()
            async with sess:
                return await _create_internal(
                    token=token,
                    id=id,
                    required_permission=required_permission,
                    ws_id=ws_id,
                    req_id=req_id,
                    sess=sess,
                    ensure_eager_load=ensure_eager_load,
                    **kwargs,
                )
        return await _create_internal(
            token=token,
            id=id,
            required_permission=required_permission,
            ws_id=ws_id,
            req_id=req_id,
            sess=sess,
            ensure_eager_load=ensure_eager_load,
            **kwargs,
        )

    async def eager_load(self, sess: AsyncSession = None) -> T:
        """
        Asynchronously retrieves the current object with all related attributes eagerly loaded.

        Args:
            sess (AsyncSession, optional): The session to use for the query. Defaults to None.
        Returns:
            T: The current object with all related attributes eagerly loaded.
        """

        async def _load_with_relationships(sess: AsyncSession):
            # recursively build loading options for all relationships
            def _get_load_options(cls, depth=2):
                if depth == 0:
                    return []

                load_options = []
                for rel in inspect(cls).relationships:
                    attr = getattr(cls, rel.key)
                    if rel.uselist:
                        loader = selectinload(attr)
                    else:
                        loader = joinedload(attr)

                    # recursively load nested relationships
                    nested_options = _get_load_options(rel.mapper.class_, depth - 1)
                    if nested_options:
                        loader = loader.options(*nested_options)
                    load_options.append(loader)
                return load_options

            # get all load options starting from the current class
            load_options = _get_load_options(self.__class__)

            # build and execute the query with all load options
            stmt = select(self.__class__).options(*load_options).filter_by(id=self.id)
            result = await sess.execute(stmt)
            instance = result.scalar_one()

            # access all column attributes to ensure they're loaded
            for attr in instance.__mapper__.column_attrs:
                getattr(instance, attr.key)

            # detach the instance after everything is loaded
            sess.expunge(instance)
            return instance

        MutyLogger.get_instance().debug(
            "---> eager_load: %s, sess=%s" % (self.id, sess)
        )
        if not sess:
            sess = GulpCollab.get_instance().session()
            async with sess:
                return await _load_with_relationships(sess)
        return await _load_with_relationships(sess)

    @classmethod
    async def eager_load_by_id(cls, id: str, sess: AsyncSession = None) -> T:
        """
        Asynchronously retrieves an object by its ID with all related attributes eagerly loaded.

        Args:
            id (str): The ID of the object to retrieve.
            sess (AsyncSession, optional): The session to use for the query. Defaults to None.

        Returns:
            T: The object with the specified ID, eagerly loaded with all related attributes.
        """

        async def _eager_load_by_id_internal(sess: AsyncSession):
            # retrieve the instance by ID
            q = select(cls).filter_by(id=id)
            res = await sess.execute(q)
            instance = res.scalar_one()

            return await instance.eager_load(sess)

        MutyLogger.get_instance().debug(
            "---> get: eager_load_by_id: %s, sess=%s" % (id, sess)
        )
        if not sess:
            sess = GulpCollab.get_instance().session()
            async with sess:
                return await _eager_load_by_id_internal(sess)

        return await _eager_load_by_id_internal(sess)

    async def delete(
        self,
        token: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.DELETE],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
    ) -> None:
        """
        Asynchronously deletes an object from the database.
        Args:
            token (str): The token of the user making the request.
            permission (list[GulpUserPermission], optional): The required permission to delete the object. Defaults to [GulpUserPermission.DELETE].
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            sess (AsyncSession, optional): The database session to use.<br>
                If None, a new session is created and committed in a transaction.<br>
                either the caller must handle the transaction and commit itself. Defaults to None (create and commit).
            throw_if_not_found (bool, optional): If True, raises an exception if the object does not exist. Defaults to True.
        Raises:
            ObjectNotFoundError: If throw_if_not_found is True and the object does not exist.
        Returns:
            None
        """

        async def _delete_internal(
            token: str, permission: list[GulpUserPermission], sess: AsyncSession
        ) -> None:
            if token:
                user_id = await self.check_token_against_object(
                    token, permission, sess=sess
                )
            sess.delete(self)
            await sess.commit()

            if ws_id and isinstance(self, GulpCollabObject):
                # notify the websocket of the collab object creation
                data = self.to_dict(exclude_none=True)
                await GulpSharedWsQueue.get_instance().put(
                    WsQueueDataType.COLLAB_DELETE,
                    ws_id=ws_id,
                    # user_id is always set unless debug options like debug_allow_any_token_as_admin is set
                    user_id=user_id or self.owner_id,
                    operation_id=data.get("operation", None),
                    req_id=req_id,
                    private=data.get("private", False),
                    data=[data],
                )

        MutyLogger.get_instance().debug(
            "---> delete: obj_id=%s, type=%s, sess=%s" % (self.id, self.type, sess)
        )
        if not sess:
            sess = GulpCollab.get_instance().session()
            async with sess:
                await _delete_internal(token, permission, sess)
        else:
            await _delete_internal(token, permission, sess)

    @classmethod
    async def delete_by_id(
        cls,
        token: str,
        id: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.DELETE],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
    ) -> None:
        """
        Asynchronously deletes an object of the specified type by its ID.
        Args:
            token (str): The token of the user making the request
            id (str): The ID of the object to be deleted.
            permission (list[GulpUserPermission], optional): The required permission to delete the object. Defaults to [GulpUserPermission.DELETE].
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            sess (AsyncSession, optional): The database session to use.<br>
                If None, a new session is created and committed in a transaction.<br>
                either the caller must handle the transaction and commit itself. Defaults to None (create and commit).
            throw_if_not_found (bool, optional): If True, raises an exception if the object does not exist. Defaults to True.
        Raises:
            ObjectNotFoundError: If throw_if_not_found is True and the object does not exist.
        Returns:
            None
        """
        MutyLogger.get_instance().debug(
            "---> delete_by_id: obj_id=%s, type=%s, sess=%s"
            % (id, cls.__gulp_collab_type__, sess)
        )
        obj: GulpCollabBase = await cls.get_one_by_id(
            id=id,
            ws_id=ws_id,
            req_id=req_id,
            sess=sess,
            throw_if_not_found=throw_if_not_found,
        )
        if obj:
            await obj.delete(
                token=token,
                permission=permission,
                ws_id=ws_id,
                req_id=req_id,
                sess=sess,
                throw_if_not_found=throw_if_not_found,
            )

    @classmethod
    async def update_by_id(
        cls,
        token: str,
        id: str,
        d: dict,
        permission: list[GulpUserPermission] = [GulpUserPermission.EDIT],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
        **kwargs,
    ) -> T:
        """
        Asynchronously updates an object of the specified type with the given data.
        Args:
            token (str): The token of the user making the request.
            id (str): The ID of the object to update.
            d (dict): A dictionary containing the fields to update and their new values.
            permission (list[GulpUserPermission], optional): The required permission to update the object. Defaults to [GulpUserPermission.EDIT].
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            sess (AsyncSession, optional): The database session to use.<br>
                If None, a new session is created and committed in a transaction.<br>
                either the caller must handle the transaction and commit itself. Defaults to None (create and commit).
            throw_if_not_found (bool, optional): If True, raises an exception if the object is not found. Defaults to True.
            **kwargs (dict, optional): Additional keyword arguments, these will be sent over the websocket only. Defaults to None.
        Returns:
            T: The updated object.
        Raises:
            Exception: If the object with the specified ID is not found.
        """
        MutyLogger.get_instance().debug(
            f"---> update_by_id: obj_id={id}, type={cls.__gulp_collab_type__}, d={d}"
        )
        obj: GulpCollabBase = await cls.get_one_by_id(
            id=id,
            ws_id=ws_id,
            req_id=req_id,
            sess=sess,
            throw_if_not_found=throw_if_not_found,
        )
        if obj:
            return await obj.update(
                token=token,
                d=d,
                permission=permission,
                ws_id=ws_id,
                req_id=req_id,
                sess=sess,
                throw_if_not_found=throw_if_not_found,
                **kwargs,
            )

    async def update(
        self,
        token: str,
        d: dict,
        permission: list[GulpUserPermission] = None,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
        **kwargs,
    ) -> T:
        """
        Asynchronously updates the object with the specified fields and values.
        Args:
            token (str): The token of the user making the request.
            d (dict): A dictionary containing the fields to update and their new values.
            permission (list[GulpUserPermission], optional): The required permission to update the object. Defaults to [GulpUserPermission.EDIT].
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            sess (AsyncSession, optional): The database session to use.<br>
                If None, a new session is created and committed in a transaction.<br>
                either the caller must handle the transaction and commit itself. Defaults to None (create and commit).
            throw_if_not_found (bool, optional): If True, raises an exception if the object is not found. Defaults to True.
            **kwargs (dict, optional): Additional keyword arguments, these will be sent over the websocket only. Defaults to None.
        Returns:
            T: The updated object.
        Raises:
            Exception: If the object with the specified ID is not found.
        """

        async def _update_internal(
            token: str,
            id: str,
            d: dict,
            permission: list[GulpUserPermission],
            throw_if_not_found: bool,
        ) -> T:
            if not permission:
                # default
                permission = [GulpUserPermission.EDIT]

            if token:
                # chcek token permission here
                user_id = await self.check_token_against_object(
                    token, permission, sess=sess
                )

            # ensure d has no 'id' (cannot be updated)
            d.pop("id", None)

            # load the instance from the session
            self_in_session = await sess.get(self.__class__, self.id)
            if not self_in_session:
                raise ObjectNotFound(
                    f"{self.__class__.__name__} with id={self.id} not found"
                )

            # update from d
            for k, v in d.items():
                # MutyLogger.get_instance().debug(f"setattr: {k}={v}")
                setattr(self_in_session, k, v)

            # sess update time
            self_in_session.time_updated = muty.time.now_msec()
            await sess.flush()
            await sess.commit()

            # ensure the object is eager loaded before returning
            obj = await self_in_session.eager_load(sess)
            MutyLogger.get_instance().debug("---> updated: %s" % (obj))

            if ws_id and isinstance(obj, GulpCollabObject):
                # notify the websocket of the collab object update
                data = obj.to_dict(exclude_none=True)
                await GulpSharedWsQueue.get_instance().put(
                    WsQueueDataType.COLLAB_UPDATE,
                    ws_id=ws_id,
                    # user_id is always set unless debug options like debug_allow_any_token_as_admin is set
                    user_id=user_id or self_in_session.owner_id,
                    operation_id=data.get("operation", None),
                    req_id=req_id,
                    private=data.get("private", False),
                    data=[data],
                )

            return obj

        MutyLogger.get_instance().debug(
            f"---> update: obj_id={self.id}, type={self.__class__}, d={d}"
        )
        if not sess:
            sess = GulpCollab.get_instance().session()
        async with sess:
            return await _update_internal(
                token, self.id, d, permission, throw_if_not_found
            )

    @classmethod
    async def get_one_by_id(
        cls,
        id: str,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
        ensure_eager_load: bool = True,
    ) -> T:
        """
        Asynchronously retrieves an object of the specified type by its ID.
        Args:
            id (str): The ID of the object to retrieve.
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            sess (AsyncSession, optional): The database session to use. If None, a new session is created. Defaults to None.
            throw_if_not_found (bool, optional): If True, raises an exception if the object is not found. Defaults to True.
            ensure_eager_load (bool, optional): If True, eagerly loads all related attributes. Defaults to True.
        Returns:
            T: The object with the specified ID or None if not found.
        Raises:
            ObjectNotFound: If the object with the specified ID is not found.
        """
        # MutyLogger.get_instance().debug(f"---> get_one_by_id: obj_id={id}, type={cls.__gulp_collab_type__}, sess={sess}")
        o = await cls.get_one(
            GulpCollabFilter(id=[id], type=[cls.__gulp_collab_type__]),
            ws_id,
            req_id,
            sess,
            throw_if_not_found=throw_if_not_found,
            ensure_eager_load=ensure_eager_load,
        )
        return o

    @classmethod
    async def get_one(
        cls,
        flt: GulpCollabFilter = None,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
        ensure_eager_load: bool = True,
    ) -> T:
        """
        shortcut to get one (the first found) object using get()
        Args:
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None.
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            sess (AsyncSession, optional): The database session to use. If None, a new session is created. Defaults to None.
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to True.
            ensure_eager_load (bool, optional): If True, eagerly loads all related attributes. Defaults to True.
        Returns:
            T: The object that matches the filter criteria or None if not found.
        Raises:
            Exception: If there is an error during the query execution or result processing.
        """

        # MutyLogger.get_instance().debug("---> get_one: type=%s, filter=%s, sess=%s" % (cls.__name__, flt, sess))
        c = await cls.get(
            flt, ws_id, req_id, sess, throw_if_not_found, ensure_eager_load
        )
        if c:
            return c[0]
        return None

    @classmethod
    async def get(
        cls,
        flt: GulpCollabFilter = None,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
        ensure_eager_load: bool = True,
    ) -> list[T]:
        """
        Asynchronously retrieves a list of objects based on the provided filter.
        Args:
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None.
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            sess(AsyncSession, optional): The database session to use. If None, a new session is created and used as a transaction. Defaults to None.
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to True.
            ensure_eager_load (bool, optional): If True, eagerly loads all related attributes. Defaults to True.
        Returns:
            list[T]: A list of objects that match the filter criteria.
        Raises:
            Exception: If there is an error during the query execution or result processing.
        """

        async def _get_internal(
            flt: GulpCollabFilter,
            sess: AsyncSession,
            throw_if_not_found: bool,
            ensure_eager_load: bool,
        ):
            flt = flt or GulpCollabFilter()
            q = flt.to_select_query(cls)
            MutyLogger.get_instance().debug("---> get: query=\n%s\n" % (q))
            res = await sess.execute(q)
            c = cls.get_all_results_or_throw(
                res, throw_if_not_found=throw_if_not_found, detail=flt
            )
            if not c:
                return []

            if ensure_eager_load:
                # eagerly load all related attributes
                for i, cc in enumerate(c):
                    ccb: GulpCollabBase = cc
                    c[i] = await ccb.eager_load(sess=sess)

            MutyLogger.get_instance().debug("---> get: found %d objects" % (len(c)))

            # TODO: handle websocket ?
            return c

        # MutyLogger.get_instance().debug("---> get: type=%s, filter=%s, sess=%s, ensure_eager_load=%r"% (cls.__name__, flt, sess, ensure_eager_load))
        if not sess:
            sess = GulpCollab.get_instance().session()
            async with sess:
                return await _get_internal(
                    flt, sess, throw_if_not_found, ensure_eager_load
                )

        return await _get_internal(flt, sess, throw_if_not_found, ensure_eager_load)

    @classmethod
    def get_all_results_or_throw(
        cls, res: Result, throw_if_not_found: bool = True, detail: any = None
    ) -> list[T]:
        """
        gets all results or throws an exception

        Args:
            res (Result): The result.
            throw_if_not_found (bool, optional): If True, throws an exception if the result is empty. Defaults to True.
            detail (any, optional): Additional detail to include in the exception message. Defaults to None.
        Returns:
            list[T]: The list of objects or None if not found
        """
        c = res.scalars().all()
        if len(c) == 0:
            msg = "no %s found!\ndetail:\n%s" % (cls.__name__, detail)
            if throw_if_not_found:
                raise ObjectNotFound(msg)
            else:
                MutyLogger.get_instance().warning(msg)
                return None

        return c

    @classmethod
    def get_one_result_or_throw(
        cls,
        res: Result,
        obj_id: str = None,
        throw_if_not_found: bool = True,
    ) -> T:
        """
        gets one result or throws an exception

        Args:
            res (Result): The result.
            obj_id (str, optional): The id of the object, just for the debug print. Defaults to None.
            throw_if_not_found (bool, optional): If True, throws an exception if the result is empty. Defaults to True.

        Returns:
            T: The object or None if not found.
        """
        c = res.scalar_one_or_none()
        if c is None:
            msg = "%s, id=%s not found!" % (cls.__name__, obj_id)
            if throw_if_not_found:
                raise ObjectNotFound(msg)
            else:
                MutyLogger.get_instance().warning(msg)
                return None
        return c

    async def check_token_against_object(
        self,
        token: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        allow_owner: bool = True,
        sess: AsyncSession = None,
        throw_on_no_permission: bool = True,
    ) -> bool:
        """
        Check if the current object is owned by the user represented by token and the user has the required permissions.

        Args:
            token (str): The token representing the user's session.
            allow_owner (bool, optional): If True, always allows the owner of the object to perform the operation. Defaults to True.
            permission (list[GulpUserPermission], optional): A list of required permissions to operate on NON-OWNED objects. Defaults to [GulpUserPermission.READ].
            sess (AsyncSession, optional): The database session to use. Defaults to None.
            throw_on_no_permission (bool, optional): If True, raises an exception if the user does not have the required permissions. Defaults to True.

        Raises:
            MissingPermission: If the user does not have the required permissions and throw_on_no_permission is True.
        Returns:
            user_id (str): The user ID of the user that has the required permissions, or None if the user does not have the required permissions.
        """

        # get the user session from the token
        from gulp.api.collab.user_session import GulpUserSession

        user_session: GulpUserSession = await GulpUserSession.get_by_token(
            token, sess=sess
        )
        if user_session.user.is_admin():
            # admin is always granted
            return user_session.user_id

        # check if the user is the owner of the object
        if self.owner_id == user_session.user_id and allow_owner:
            return user_session.user_id

        # user do not own the object, check permissions
        if user_session.user.has_permission(permission):
            return user_session.user_id

        if throw_on_no_permission:
            raise MissingPermission(
                f"User {user_session.user_id} does not have the required permissions {permission} to perform this operation."
            )
        return None

    @staticmethod
    async def check_token_against_object_by_id(
        id: str,
        token: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        allow_owner: bool = True,
        sess: AsyncSession = None,
        throw_on_no_permission: bool = True,
    ) -> str:
        """
        check if the object identified by "id" is owned by the user represented by "token" and the user has the required permissions.

        Args:
            id (str): The identifier of the object to check ownership for.
            token (str): The token representing the user's session.
            allow_owner (bool, optional): If True, always allows the owner of the object to perform the operation. Defaults to True.
            permission (list[GulpUserPermission], optional): A list of required permissions to operate on NON-OWNED objects. Defaults to [GulpUserPermission.READ].
            sess (AsyncSession, optional): The database session to use. Defaults to None.
            throw_on_no_permission (bool, optional): If True, raises an exception if the user does not have the required permissions. Defaults to True.

        Returns:
            bool: the user_id of the user that has the required permissions, or None if the user does not have the required permissions.
        """
        obj: GulpCollabBase = await GulpCollabBase.get_one_by_id(id, sess=sess)
        return await obj.check_token_against_object(
            token, permission, allow_owner, sess, throw_on_no_permission
        )


class GulpCollabConcreteBase(GulpCollabBase, type="collab_base"):
    """
    Concrete base class for GulpCollabBase to ensure a table is created.
    """

    pass


class GulpCollabObject(GulpCollabBase, type="collab_obj", abstract=True):
    """
    base for all collaboration objects (notes, links, stories, highlights) related to an operation
    """

    operation_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey(
            "operation.id",
            ondelete="CASCADE",
        ),
        doc="The id of the operation associated with the object.",
    )
    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        doc="The glyph ID.",
    )
    tags: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String),
        doc="The tags associated with the object.",
    )
    color: Mapped[Optional[str]] = mapped_column(
        String, doc="The color associated with the object."
    )
    title: Mapped[Optional[str]] = mapped_column(
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
    def __init__(
        self,
        id: str,
        type: GulpCollabType,
        owner_id: str,
        operation_id: str = None,
        glyph_id: str = None,
        color: str = None,
        tags: list[str] = None,
        title: str = None,
        description: str = None,
        private: bool = False,
        **kwargs,
    ) -> None:
        """
        Initialize a GulpCollabObject.
        Args:
            id (str): The unique identifier for the collaboration object.
            type (GulpCollabType): The type of the collaboration object.
            owner_id (str): The user ID of the owner of the collaboration object.
            operation_id (str, optional): The operation performed on the collaboration object. Defaults to None.
            glyph_id (str, optional): The glyph associated with the collaboration object. Defaults to None (uses default).
            color (str, optional): The color associated with the collaboration object. Defaults to None (uses default).
            tags (list[str], optional): A list of tags associated with the collaboration object. Defaults to None.
            title (str, optional): The title of the collaboration object. Defaults to None.
            description (str, optional): The description of the collaboration object. Defaults to None.
            private (bool, optional): Indicates if the collaboration object is private. Defaults to False.
            **kwargs: Additional keyword arguments passed to the GulpCollabBase initializer.
        """
        if self.type == GulpCollabObject:
            raise NotImplementedError(
                "GulpCollabObject is an abstract class and cannot be instantiated directly."
            )
        super().__init__(id=id, type=type, owner_id=owner_id, **kwargs)
        self.operation_id = operation_id
        self.glyph_id = glyph_id
        self.tags = tags
        self.title = title
        self.description = description
        self.color = color
        self.private = private
        MutyLogger.get_instance().debug(
            "---> GulpCollabObject: id=%s, type=%s, user_id=%s, operation_id=%s, glyph=%s, color=%s, tags=%s, title=%s, description=%s, private=%s"
            % (
                id,
                type,
                owner_id,
                operation_id,
                glyph_id,
                color,
                tags,
                title,
                description,
                private,
            )
        )
