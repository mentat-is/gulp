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
    joinedload,
    MappedAsDataclass,
    DeclarativeBase,
)
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession
from sqlalchemy_mixins.serialize import SerializeMixin
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
    """Gulp request status codes."""

    ONGOING = "ongoing"
    DONE = "done"
    FAILED = "failed"
    CANCELED = "canceled"


class GulpUserPermission(StrEnum):
    """represent the permission of a user in the Gulp platform."""

    # can read only
    READ = "read"
    # can edit own highlights, notes, stories, links + ingest data
    EDIT = "edit"
    # can delete others highlights, notes, stories, links + ingest data
    DELETE = "delete"
    # can do anything, including creating new users and change permissions
    ADMIN = "admin"


PERMISSION_MASK_EDIT = [GulpUserPermission.READ, GulpUserPermission.EDIT]
PERMISSION_MASK_DELETE = [
    GulpUserPermission.READ, GulpUserPermission.EDIT, GulpUserPermission.DELETE]

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
            # match if equal to any in the list
            q = q.filter(type.type.in_(self.type))
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

    def __init_subclass__(cls, type: GulpCollabType|str, abstract: bool=False, **kwargs) -> None:
        """
        this is called automatically when a subclass is created

        Args:
            type (GulpCollabType|str): The type of the object.
            abstract (bool): If True, the class is abstract
            **kwargs: Additional keyword arguments.
        """
        cls.__gulp_collab_type__=type

        if abstract:
            cls.__abstract__ = True
        else:
            cls.__tablename__ = str(type)
        cls.__mapper_args__ = {
            "polymorphic_identity": str(type),
        }

        #print("type=%s, cls.__name__=%s, abstract=%r, cls.__abstract__=%r, cls.__mapper_args__=%s" % (cls.__gulp_collab_type__, cls.__name__, abstract, cls.__abstract__, cls.__mapper_args__))
        super().__init_subclass__(**kwargs)
    
    def __init__(self, *args, **kwargs):
        """
        prevents instantiation of this class
        """
        if self.__class__ == GulpCollabBase:
            raise Exception("GulpCollabBase is an abstract class and cannot be instantiated directly.")
        
    @classmethod
    async def eager_load_by_id(cls, id: str, sess: AsyncSession=None) -> T:
        """
        Asynchronously retrieves an object by its ID with all related attributes eagerly loaded.

        Args:
            id (str): The ID of the object to retrieve.
            sess (AsyncSession, optional): The database session to use. If None, a new session is created. Defaults to None.
        
        Returns:
            T: The object with the specified ID.
        """
        created=False
        if not sess:
            sess = await session()
            created=True

        try:
            instance = await sess.execute(
                select(cls).options(joinedload('*')).filter_by(id=id)
            )
            instance = instance.scalar_one()
            return instance
        finally:
            if created:
                await sess.close()

    @classmethod
    async def _create(
        cls,
        id: str,
        owner: str,
        token: str = None,
        required_permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        ensure_eager_load: bool=True,
        **kwargs,
    ) -> T:
        """
        Asynchronously creates and stores an instance of the class.
        Args:
            id (str): The unique identifier for the instance.
            owner (str): The ID of the user associated with the instance.
            token (str, optional): The token of the user making the request. Defaults to None (no check).
            required_permission (list[GulpUserPermission], optional): The required permission to create the object. Defaults to [GulpUserPermission.READ].
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
        # print(f"****** GulpCollabBase _create: id={id}, type={cls.__gulp_collab_type__}, owner={owner}")
        instance = cls(id, cls.__gulp_collab_type__, owner, 0, 0, **kwargs)
        instance.id = id        
        instance.owner = owner
        for k, v in kwargs.items():
            setattr(instance, k, v)

        instance.time_created = muty.time.now_msec()
        instance.time_updated = instance.time_created

        logger().debug(
            "---> GulpCollabBase: id=%s, type=%s, owner=%s, sess=%s" % (id, cls.__gulp_collab_type__, owner, sess)
        )

        if token:
            # check required creation permission
            GulpCollabBase.check_token_permission(token, permission=required_permission, sess=sess)

        created=False
        try:
            if sess:
                # just add
                sess.add(instance)            
            else:
                sess = await session()
                created=True
                sess.add(instance)                
                await sess.commit()

            if ensure_eager_load:
                # eagerly load the instance with all related attributes
                instance = await cls.eager_load_by_id(instance.id, sess=sess)
            
            # TODO: notify websocket
            return instance
        finally:
            if created:
                await sess.close()

    async def delete(
        self,
        token: str = None,
        permission: list[GulpUserPermission] = [GulpUserPermission.DELETE],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
    ) -> None:
        """
        Asynchronously deletes an object from the database.
        Args:
            token (str, optional): The token of the user making the request. Defaults to None.
            permission (list[GulpUserPermission], optional): The required permission to delete the object. Defaults to [GulpUserPermission.DELETE].
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            sess (AsyncSession, optional): The database session to use. If None, a new session is created. Defaults to None.
            throw_if_not_found (bool, optional): If True, raises an exception if the object does not exist. Defaults to True.
        Raises:
            ObjectNotFoundError: If throw_if_not_found is True and the object does not exist.
        Returns:
            None
        """
        logger().debug("---> delete: obj_id=%s, type=%s, sess=%s" % (self.id, self.type, sess))
        created = False
        if not sess:
            created=True
            sess = await session()
        try:
            if token:
                # check_token_permission here
                await GulpCollabBase.check_token_permission(token, permission, sess=sess)
            
            await sess.delete(self)
            if created:
                await sess.commit()
            
            # TODO: notify websocket

        finally:
            if created:
                await sess.close()


    @classmethod
    async def delete_by_id(
        cls,
        id: str,
        token: str = None,
        permission: list[GulpUserPermission] = [GulpUserPermission.DELETE],
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        throw_if_not_found: bool = True,
    ) -> None:
        """
        Asynchronously deletes an object of the specified type by its ID.
        Args:
            id (str): The ID of the object to be deleted.
            token (str, optional): The token of the user making the request. Defaults to None.
            permission (list[GulpUserPermission], optional): The required permission to delete the object. Defaults to [GulpUserPermission.DELETE].
            ws_id (str, optional): The ID of the websocket connection. Defaults to None.
            req_id (str, optional): The ID of the request. Defaults to None.
            sess (AsyncSession, optional): The database session to use. If None, a new session is created. Defaults to None.
            throw_if_not_found (bool, optional): If True, raises an exception if the object does not exist. Defaults to True.
        Raises:
            ObjectNotFoundError: If throw_if_not_found is True and the object does not exist.
        Returns:
            None
        """
        created=False
        if not sess:
            created = True
            sess = await session()

        try:
            obj:GulpCollabBase = await cls.get_one_by_id(id, ws_id, req_id, sess=sess, throw_if_not_found=throw_if_not_found)
            if obj:
                #logger().debug("---> delete_by_id: obj=%s" % (await obj))
                await obj.delete(token=token, permission=permission, ws_id=ws_id, req_id=req_id, sess=sess, throw_if_not_found=throw_if_not_found)
        finally:
            if created:
                await sess.close()

    @classmethod
    async def update_by_id(
        cls,
        id: str,
        d: dict,
        token: str=None,
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
            id (str): The ID of the object to update.
            d (dict): A dictionary containing the fields to update and their new values.
            token (str, optional): The token of the user making the request.
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
        obj: GulpCollabBase = await cls.get_one_by_id(id, ws_id, req_id, sess, throw_if_not_found)
        if obj:
            # check permission on the object
            return await obj.update(
                d, token, permission, ws_id, req_id, sess, throw_if_not_found, **kwargs
            )
        return None

    async def update(
        self,
        d: dict,
        token: str=None,
        permission: list[GulpUserPermission] = [GulpUserPermission.EDIT],
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
            token (str, optional): The token of the user making the request.
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
            id, d, token, permission, throw_if_not_found, commit: bool = False
        ) -> T:
            if token:
                await self.check_object_permission(token, permission, sess=sess)
            q = select(self.__class__).where(self.__class__.id == id).with_for_update()
            res = await sess.execute(q)
            obj = self.__class__.get_one_result_or_throw(
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
                    self.id, d, token, permission, throw_if_not_found, commit=True
                )
        else:
            # use existing session
            obj = await _update_internal(self.id, d, token, permission, throw_if_not_found, commit=True)

        if obj and ws_id:
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
        logger().debug(f"---> get_one_by_id: obj_id={id}, type={cls.__gulp_collab_type__}")
        o = await cls.get_one(
            GulpCollabFilter(id=[id], type=[cls.__gulp_collab_type__]),
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
        ensure_eager_load: bool=True
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
            ensure_eager_load (bool, optional): If True, eagerly loads all related attributes. Defaults to True.
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
            logger().debug("---> get: created=%r, query=\n%s\n" % (created, q))
            res = await sess.execute(q)
            c = cls.get_all_results_or_throw(res, throw_if_not_found=throw_if_not_found, detail=q)
            if c:
                if ensure_eager_load:
                    # eagerly load all related attributes                    
                    for cc in c:
                        await cls.eager_load_by_id(cc.id, sess=sess)

                logger().debug("---> get: found %d objects" % (len(c)))
                return c

            return []
        finally:
            if created:
                await sess.close()

    @classmethod
    def get_all_results_or_throw(
        cls, res: Result, throw_if_not_found: bool = True,
        detail: any = None
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
                logger().warning(msg)
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
                logger().warning(msg)
                return None
        return c

    @staticmethod
    async def check_token_permission(
        token: str,
        permission: list[GulpUserPermission] = [GulpUserPermission.READ],
        sess: AsyncSession = None,
        throw_on_no_permission: bool = True) -> bool:
        """
        Check if the user represented by token has the required permissions.

        Args:
            token (str): The token representing the user's session.
            permission (list[GulpUserPermission], optional): A list of required permissions. Defaults to [GulpUserPermission.READ].
            sess (AsyncSession, optional): The database session to use. Defaults to None.
            throw_on_no_permission (bool, optional): If True, raises an exception if the user does not have the required permissions. Defaults to True.
        
        Returns:
            bool: True if the user has the required permissions (or is admin).
        """
        from gulp.api.collab.user_session import GulpUserSession
        # get session
        user_session: GulpUserSession = await GulpUserSession.get_by_token(
            token, sess=sess
        )
        if user_session.user.has_permission(permission):
            return True
        if throw_on_no_permission:
            raise MissingPermission(
                f"User {user_session.user_id} does not have the required permissions {permission} to perform this operation."
            )
        return False
            
    async def check_object_permission(self,
                          token: str,
                          permission: list[GulpUserPermission] = [GulpUserPermission.READ],
                          allow_owner: bool=True,
                          sess: AsyncSession=None,
                          throw_on_no_permission: bool=True) -> bool:
        """
        Check if the current object is owned by the user represented by token and the user has the required permissions.

        Args:
            token (str): The token representing the user's session.
            allow_owner (bool, optional): If True, always allows the owner of the object to perform the operation. Defaults to True.
            permission (list[GulpUserPermission], optional): A list of required permissions to operate on NON-OWNED objects. Defaults to [GulpUserPermission.READ].
            sess (AsyncSession, optional): The database session to use. Defaults to None.
            throw_on_no_permission (bool, optional): If True, raises an exception if the user does not have the required permissions. Defaults to True.
        
        Returns:
            bool: True if the user is the owner of the object, or an administrator, AND have the required permissions.
        """

        # get the user session from the token
        from gulp.api.collab.user_session import GulpUserSession
        
        # get session and object
        user_session: GulpUserSession = await GulpUserSession.get_by_token(
            token, sess=sess
        )
        if user_session.user.is_admin():
            # admin is always granted
            return True

        # check if the user is the owner of the object        
        if self.owner == user_session.user_id and allow_owner:
            return True

        # user do not own the object, check permissions 
        if user_session.user.has_permission(permission):
            return True
        
        if throw_on_no_permission:
            raise MissingPermission(
                f"User {user_session.user_id} does not have the required permissions {permission} to perform this operation."
            )
        return False

    @staticmethod
    async def check_object_permission_by_id(id: str,
                          token: str,
                          permission: list[GulpUserPermission] = [GulpUserPermission.READ],
                          allow_owner: bool=True,
                          sess: AsyncSession=None,
                          throw_on_no_permission: bool=True) -> bool:
        """
        Check if the object is owned by the user and the user has the required permissions.
        Args:
            id (str): The identifier of the object to check ownership for.
            token (str): The token representing the user's session.
            allow_owner (bool, optional): If True, always allows the owner of the object to perform the operation. Defaults to True.
            permission (list[GulpUserPermission], optional): A list of required permissions to operate on NON-OWNED objects. Defaults to [GulpUserPermission.READ].
            sess (AsyncSession, optional): The database session to use. Defaults to None.
            throw_on_no_permission (bool, optional): If True, raises an exception if the user does not have the required permissions. Defaults to True.
        
        Returns:
            bool: True if the user is the owner of the object, or an administrator, AND have the required permissions.
        """
        obj: GulpCollabBase = await GulpCollabBase.get_one_by_id(id, sess=sess)
        return await obj.check_object_permission(token, permission, allow_owner, sess, throw_on_no_permission)
    

class GulpCollabConcreteBase(GulpCollabBase, type="collab_base"):
    """
    Concrete base class for GulpCollabBase to ensure a table is created.
    """
    pass

class GulpCollabObject(GulpCollabBase, type="collab_obj", abstract=True):
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
