from enum import Flag, StrEnum, auto
import json
from typing import Optional, TypeVar, override
import muty.time
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.types import Enum as SqlEnum
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
    or_,
    select,
    text,
)
from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    MappedAsDataclass,
    DeclarativeBase,
)
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession
from sqlalchemy_mixins import SerializeMixin
from gulp.api.collab_api import session
from gulp.api.elastic.structs import GulpAssociatedDocument
from gulp.defs import ObjectNotFound
from gulp.utils import logger


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


class GulpUserPermission(Flag):
    """represent the permission of a user in the Gulp platform."""

    # can read only
    READ = auto()
    # can edit own highlights, notes, stories, links + ingest data
    EDIT = auto()
    # can delete others highlights, notes, stories, links + ingest data
    DELETE = auto()
    # can do anything, including creating new users and change permissions
    ADMIN = auto()


PERMISSION_MASK_EDIT = GulpUserPermission.READ | GulpUserPermission.EDIT
PERMISSION_MASK_DELETE = (
    GulpUserPermission.READ | GulpUserPermission.EDIT | GulpUserPermission.DELETE
)


class GulpCollabType(StrEnum):
    """
    defines the type of collaboration object
    """

    NOTE = "note"
    HIGHLIGHT = "highlight"
    STORY = "story"
    LINK = "link"
    STORED_QUERY = "stored_query"
    STORED_SIGMA = "stored_sigma"
    STATS_INGESTION = "stats_ingestion"
    USER_DATA = "user_data"
    CONTEXT = "context"
    USER = "user"
    GLYPH = "glyph"
    OPERATION = "operation"
    SESSION = "session"


T = TypeVar("T", bound="GulpCollabBase")


class GulpCollabFilter(BaseModel):
    """
    defines filter to be applied to all objects in the collaboration system
    """

    id: Optional[list[str]] = Field(None, description="filter by the given id/s.")
    type: Optional[list[GulpCollabType]] = Field(
        None, description="filter by the given type/s."
    )
    operation: Optional[list[str]] = Field(
        None, description="filter by the given operation/s."
    )
    context: Optional[list[str]] = Field(
        None, description="filter by the given context/s."
    )
    log_file_path: Optional[list[str]] = Field(
        None, description="filter by the given source path/s or name/s."
    )
    owner: Optional[list[str]] = Field(
        None, description="filter by the given owner id/s."
    )
    tags: Optional[list[str]] = Field(None, description="filter by the given tag/s.")
    title: Optional[list[str]] = Field(None, description="filter by the given title/s.")
    text: Optional[list[str]] = Field(
        None, description="filter by the given object text."
    )
    documents: Optional[list[GulpAssociatedDocument]] = Field(
        None,
        description="filter by the given event ID/s in a CollabObj.documents list of GulpAssociatedDocument.",
    )
    opt_time_range: Optional[tuple[int, int]] = Field(
        None,
        description="if set, a `@timestamp` range [start, end] relative to CollabObject.documents, inclusive, in nanoseconds from unix epoch.",
    )
    opt_private: Optional[bool] = Field(
        False,
        description="if True, return only private objects. Default=False (return all).",
    )
    opt_limit: Optional[int] = Field(
        None,
        description='to be used together with "offset", maximum number of results to return. default=return all.',
    )
    opt_offset: Optional[int] = Field(
        None,
        description='to be used together with "limit", number of results to skip from the beginning. default=0 (from start).',
    )
    opt_tags_and: Optional[bool] = Field(
        False,
        description="if True, all tags must match. Default=False (at least one tag must match).",
    )

    @model_validator(mode="before")
    @classmethod
    def validate(cls, data: str | dict = None):
        if not data:
            return {}

        if isinstance(data, dict):
            return data
        return json.loads(data)

    def _case_insensitive_or_ilike(self, column, values: list) -> ColumnElement[bool]:
        """
        Create a case-insensitive OR query with wildcards for the given column and values.

        Args:
            column: The column to apply the ilike condition.
            values: The list of values to match against the column.

        Returns:
            ColumnElement[bool]: The OR query.
        """
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
            q = q.filter(self._case_insensitive_or_ilike(type.type, self.type))
        if self.operation and "operation" in type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(type.operation, self.operation)
            )
        if self.context and "context" in type.columns:
            q = q.filter(self._case_insensitive_or_ilike(type.context, self.context))
        if self.log_file_path and "log_file_path" in type.columns:
            q = q.filter(
                self._case_insensitive_or_ilike(type.log_file_path, self.log_file_path)
            )
        if self.owner and "owner" in type.columns:
            q = q = q.filter(self._case_insensitive_or_ilike(type.owner, self.owner))
        if self.tags and "tags" in type.columns:
            lower_tags = [tag.lower() for tag in self.tags]
            if self.opt_tags_and:
                # all tags must match (CONTAINS operator)
                q = q.filter(func.lower(type.tags).op("@>")(lower_tags))
            else:
                # at least one tag must match (OVERLAP operator)
                q = q.filter(func.lower(type.tags).op("&&")(self.tags))
        if self.title and "title" in type.columns:
            q = q.filter(self._case_insensitive_or_ilike(type.title, self.title))
        if self.text and "text" in type.columns:
            q = q.filter(self._case_insensitive_or_ilike(type.text, self.text))

        if self.documents and "documents" in type.columns:
            if not self.opt_time_range:
                # filter by collabobj.documents id
                lower_documents = [{"_id": doc_id.lower()} for doc_id in self.documents]
                conditions = [
                    func.lower(type.documents).op("@>")([{"_id": doc_id}])
                    for doc_id in lower_documents
                ]
                q = q.filter(or_(*conditions))
            else:
                # filter by time range on collabobj.documents["@timestamp"]
                conditions = []
                if self.opt_time_range[0]:
                    conditions.append(
                        f"(doc->>'@timestamp')::bigint >= {self.opt_time_range[0]}"
                    )
                if self.opt_time_range[1]:
                    conditions.append(
                        f"(doc->>'@timestamp')::bigint <= {self.opt_time_range[1]}"
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

        if self.opt_limit:
            q = q.limit(self.opt_limit)
        if self.opt_offset:
            q = q.offset(self.opt_offset)
        if self.opt_private:
            q = q.where(GulpCollabObject.private is True)

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
    owner: Mapped[str] = mapped_column(
        ForeignKey("user.id", ondelete="SET NULL"),
        doc="The user who 'owns' the object. If None, this is a root object which cannot be deleted.",
        nullable=True,
    )
    time_created: Mapped[int] = mapped_column(
        BIGINT,
        doc="The time the object was created, in milliseconds from unix epoch.",
    )
    time_updated: Mapped[int] = mapped_column(
        BIGINT,
        doc="The time the object was last updated, in milliseconds from unix epoch.",
    )

    __mapper_args__ = {
        "polymorphic_identity": "collab_base",
        "polymorphic_on": "type",
    }

    @override
    def __init__(self, id: str, type: GulpCollabType, owner: str, **kwargs) -> None:
        """
        Initialize a GulpCollabBase instance.
        Args:
            id (str): The identifier for the object.
            type (GulpCollabType): The type of the object.
            owner (str): The user ID of the owner of the object.
            **kwargs: Additional keyword arguments.
        """
        if self.__class__ is GulpCollabBase:
            raise NotImplementedError(
                "GulpCollabBase is an abstract class and cannot be instantiated directly."
            )
        super().__init__()
        self.id = id
        self.type = type
        self.owner = owner
        for k, v in kwargs.items():
            setattr(self, k, v)

        self.time_created = muty.time.now_msec()
        self.time_updated = self.time_created

        logger().debug(
            "---> GulpCollabBase: id=%s, type=%s, owner=%s" % (id, type, owner)
        )

    @classmethod
    async def _create(
        cls,
        id: str,
        type: GulpCollabType,
        owner: str,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        **kwargs,
    ) -> T:
        """
        Asynchronously creates and stores an instance of the class.
        Args:
            id (str): The unique identifier for the instance.
            type (GulpCollabType): The type of the instance.
            owner (str): The ID of the user associated with the instance.
            ws_id (str, optional): WebSocket ID associated with the instance. Defaults to None.
            req_id (str, optional): Request ID associated with the instance. Defaults to None.
            sess (AsyncSession, optional): The database session to use.<br>
                If None, a new session is created and committed in a transaction.<br>
                either the caller must handle the transaction and commit itself. Defaults to None (create and commit).
            **kwargs: Additional keyword arguments to set as attributes on the instance.
        Returns:
            T: The created instance of the class.
        Raises:
            Exception: If there is an error during the creation or storage process.
        """

        # create instance
        instance = cls(id, type, owner, 0, 0, **kwargs)
        committed = False
        if sess is not None:
            # just add
            await sess.add(instance)
        else:
            sess = await session()
            committed = True
            async with sess:
                sess.add(instance)
                await sess.commit()

        # TODO: notify websocket
        logger().info("---> create: %s, committed=%r" % (instance, committed))
        return instance

    async def delete(
        self,
        ws_id: str = None,
        req_id: str = None,
        throw_if_not_found: bool = True,
    ) -> None:
        """
        Asynchronously deletes an object from the database.
        Args:
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            throw_if_not_found (bool, optional): If True, raises an exception if the object does not exist. Defaults to True.
        Raises:
            ObjectNotFoundError: If throw_if_not_found is True and the object does not exist.
        Returns:
            None
        """
        logger().debug("---> delete: obj_id=%s, type=%s" % (id, self.__class__))
        sess = await session()

        async with sess:
            q = select(self.__class__).where(self.__class__.id == id).with_for_update()
            res = await sess.execute(q)
            obj = self.__class__.get_one_result_or_throw(
                res, obj_id=id, throw_if_not_found=throw_if_not_found
            )
            if obj is not None:
                sess.delete(obj)
                await sess.commit()
                logger().info("---> deleted: %s" % (obj))

                # TODO: notify websocket

    @classmethod
    async def delete_by_id(
        cls,
        id: str,
        ws_id: str = None,
        req_id: str = None,
        throw_if_not_found: bool = True,
    ) -> None:
        """
        Asynchronously deletes an object of the specified type by its ID.
        Args:
            id (str): The ID of the object to be deleted.
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            throw_if_not_found (bool, optional): If True, raises an exception if the object does not exist. Defaults to True.
        Raises:
            ObjectNotFoundError: If throw_if_not_found is True and the object does not exist.
        Returns:
            None
        """
        obj = await cls.get_one_by_id(id, ws_id, req_id, throw_if_not_found)
        if obj:
            return await obj.delete(ws_id, req_id, throw_if_not_found)
        return None

    @classmethod
    async def update_by_id(
        cls,
        id: str,
        d: dict,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
        **kwargs,
    ) -> T:
        """
        Asynchronously updates an object of the specified type with the given data.
        Args:
            id (str): The ID of the object to update.
            d (dict): A dictionary containing the fields to update and their new values.
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
        obj = await cls.get_one_by_id(id, ws_id, req_id, sess, throw_if_not_found)
        if obj:
            return await obj.update(
                d, ws_id, req_id, sess, throw_if_not_found, **kwargs
            )
        return None

    async def update(
        self,
        d: dict,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
        **kwargs,
    ) -> T:
        """
        Asynchronously updates the object with the specified fields and values.
        Args:
            d (dict): A dictionary containing the fields to update and their new values.
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
            id, d, throw_if_not_found, commit: bool = False
        ) -> T:
            q = select(self.__class__).where(self.__class__.id == id).with_for_update()
            res = await sess.execute(q)
            obj = await self.__class__.get_one_result_or_throw(
                res, obj_id=id, throw_if_not_found=throw_if_not_found
            )
            if obj:
                # ensure d has no 'id' (cannot be updated)
                d.pop("id", None)

                # update from dict
                for k, v in d.items():
                    setattr(obj, k, v)

                # update time
                obj.time_updated = muty.time.now_msec()

                sess.add(obj)
                if commit:
                    await sess.commit()
                logger().debug("---> updated: %s, committed=%r" % (obj, commit))

            return obj

        logger().debug(f"---> update: obj_id={id}, type={self.__class__}, d={d}")
        if sess is None:
            # create a new session
            sess = await session()
            async with sess:
                obj = await _update_internal(
                    self.id, d, throw_if_not_found, commit=True
                )
        else:
            # use existing session
            obj = await _update_internal(self.id, d, throw_if_not_found, commit=True)

        if obj is not None:
            # TODO: handle websocket, add each **kwargs too

            pass

        return obj

    @classmethod
    async def get_one_by_id(
        cls,
        id: str,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
    ) -> T:
        """
        Asynchronously retrieves an object of the specified type by its ID.
        Args:
            id (str): The ID of the object to retrieve.
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            sess (AsyncSession, optional): The database session to use. If None, a new session is created. Defaults to None.
            throw_if_not_found (bool, optional): If True, raises an exception if the object is not found. Defaults to True.
        Returns:
            T: The object with the specified ID or None if not found.
        Raises:
            ObjectNotFound: If the object with the specified ID is not found.
        """
        logger().debug(f"---> get_one_by_id: obj_id={id}, type={cls.type}")
        o = cls.get_one(
            GulpCollabFilter(id=[id], type=[cls.type]),
            ws_id,
            req_id,
            sess,
            throw_if_not_found,
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
    ) -> T:
        """
        shortcut to get one (the first found) object using get()
        Args:
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None.
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            sess (AsyncSession, optional): The database session to use. If None, a new session is created. Defaults to None.
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to True.
        Returns:
            T: The object that matches the filter criteria or None if not found.
        Raises:
            Exception: If there is an error during the query execution or result processing.
        """

        logger().debug("---> get_one: type=%s, filter=%s" % (cls.__name__, flt))
        c = await cls.get(flt, ws_id, req_id, sess, throw_if_not_found)
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
    ) -> list[T]:
        """
        Asynchronously retrieves a list of objects based on the provided filter.
        Args:
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None.
            sess (AsyncSession, optional): The database session to use. If None, a new session is created. Defaults to None.
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            sess(AsyncSession, optional): The database session to use. If None, a new session is created and used as a transaction. Defaults to None.
            throw_if_not_found (bool, optional): If True, raises an exception if no objects are found. Defaults to True.
        Returns:
            list[T]: A list of objects that match the filter criteria.
        Raises:
            Exception: If there is an error during the query execution or result processing.
        """

        # TODO: handle websocket
        logger().debug("---> get: type=%s, filter=%s" % (cls.__name__, flt))
        flt = flt or GulpCollabFilter()
        created: bool = False
        if sess is None:
            sess = await session()
            created = True
        try:
            # build query
            q = flt.to_select_query(cls)
            res = await sess.execute(q)
            c = cls.get_all_results_or_throw(res, throw_if_not_found=throw_if_not_found)
            if c is not None:
                logger().debug("---> get: found %d objects" % (len(c)))
                return c

            return []
        finally:
            if created:
                await sess.close()

    @classmethod
    async def get_all_results_or_throw(
        cls, res: Result, throw_if_not_found: bool = True
    ) -> list[T]:
        """
        gets all results or throws an exception

        Args:
            res (Result): The result.
            throw_if_not_found (bool, optional): If True, throws an exception if the result is empty. Defaults to True.

        Returns:
            list[T]: The list of objects or None if not found
        """
        c = res.scalars().all()
        if len(c) == 0:
            msg = "no %s found!" % (cls.__name__)
            if throw_if_not_found:
                raise ObjectNotFound(msg)
            else:
                logger().warning(msg)
                return None

        return c

    @classmethod
    async def get_one_result_or_throw(
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
                logger().warning(msg)
                return None
        return c

    @staticmethod
    async def check_permission(
        id: str,
        token: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        sess: AsyncSession = None,
    ) -> None:
        """
        Check if the user associated with the provided token has the required permission to access the object with the given id.
        Args:
            id (str): The identifier of the object to check permission for.
            token (str): The token representing the user's session.
            permission (list[GulpUserPermission], optional): A list of required permissions. Defaults to [GulpUserPermission.READ].
            sess (AsyncSession, optional): The database session to use. Defaults to None.
        Raises:
            MissingPermission: If the user does not have the required permission to access the object.
        """

        # get the user session from the token
        from gulp.api.collab.user_session import GulpUserSession

        # get session and object
        user_session: GulpUserSession = await GulpUserSession.get_by_token(
            token, sess=sess
        )
        obj = await GulpCollabBase.get_one(GulpCollabFilter(id=[id]), sess=sess)
        if obj.user != user_session.user_id:
            # check if the user is an admin or permission is enough
            if not user_session.user.has_permission(permission):
                raise MissingPermission(
                    f"token {token} (user={user_session.user_id}) does not have permission to access object {id}"
                )


class GulpCollabConcreteBase(GulpCollabBase):
    """
    Concrete base class for GulpCollabBase to ensure a table is created.
    """

    __tablename__ = "collab_base"

    __mapper_args__ = {
        "polymorphic_identity": "collab_base",
    }


class GulpCollabObject(GulpCollabBase):
    """
    base for all collaboration objects (notes, links, stories, highlights)
    """

    # the following are present in all collab objects regardless of type
    operation: Mapped[str] = mapped_column(
        ForeignKey(
            "operation.id",
            ondelete="CASCADE",
        ),
        doc="The operation associated with the object.",
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"), doc="The glyph ID."
    )
    tags: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String), doc="The tags associated with the object."
    )
    title: Mapped[Optional[str]] = mapped_column(String, doc="The title of the object.")
    private: Mapped[Optional[bool]] = mapped_column(
        Boolean,
        doc="If True, the object is private (only the owner can see it).",
    )

    __mapper_args__ = {
        "polymorphic_identity": "collab_obj",
    }
    __abstract__ = True

    @override
    def __init__(
        self,
        id: str,
        type: GulpCollabType,
        owner: str,
        operation: str,
        glyph: str = None,
        tags: list[str] = None,
        title: str = None,
        private: bool = False,
        **kwargs,
    ) -> None:
        """
        Initialize a GulpCollabObject.
        Args:
            id (str): The unique identifier for the collaboration object.
            type (GulpCollabType): The type of the collaboration object.
            owner (str): The user ID of the owner of the collaboration object.
            operation (str): The operation performed on the collaboration object.
            glyph (str, optional): The glyph associated with the collaboration object. Defaults to None.
            tags (list[str], optional): A list of tags associated with the collaboration object. Defaults to None.
            title (str, optional): The title of the collaboration object. Defaults to None.
            private (bool, optional): Indicates if the collaboration object is private. Defaults to False.
            **kwargs: Additional keyword arguments passed to the GulpCollabBase initializer.
        """
        if type(self) is GulpCollabObject:
            raise NotImplementedError(
                "GulpCollabObject is an abstract class and cannot be instantiated directly."
            )

        super().__init__(id, type, owner, **kwargs)
        self.operation = operation
        self.glyph = glyph
        self.tags = tags
        self.title = title
        self.private = private
        logger().debug(
            "---> GulpCollabObject: id=%s, type=%s, user=%s, operation=%s, glyph=%s, tags=%s, title=%s, private=%s"
            % (id, type, owner, operation, glyph, tags, title, private)
        )

    def set_private(self, private: bool) -> None:
        """
        Set the private flag for the object.
        Args:
            private (bool): The value to set the private flag to.
        """
        self.private = private
        logger().debug(f"---> set_private: id={self.id}, private={private}")
