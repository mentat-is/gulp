from enum import StrEnum
import json
from typing import Optional, TypeVar
import muty.time
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import (
    ARRAY,
    BIGINT,
    Boolean,
    ColumnElement,
    ForeignKey,
    Index,
    Result,
    Select,
    String,
    Tuple,
    func,
    or_,
    select,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, MappedAsDataclass, DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession
from sqlalchemy_mixins import SerializeMixin
from gulp.api.collab_api import session
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

    T = TypeVar("T", bound="GulpCollabBase")

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
    documents: list[dict] = Field(
        None,
        description="filter by the given event ID/s in a CollabObj.documents list of GulpDocument.",
    )
    opt_time_range: tuple[int, int] = Field(
        None,
        description="if set, a `@timestamp` range [start, end] relative to CollabObject.documents, inclusive, in nanoseconds from unix epoch.",
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
        if self.user and "user" in type.columns:
            q = q = q.filter(self._case_insensitive_or_ilike(type.user, self.user))
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
                    FROM jsonb_array_elements({table_name}.documents) AS evt
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

    T = TypeVar("T", bound="GulpCollabBase")

    id: Mapped[str] = mapped_column(
        String(COLLAB_MAX_NAME_LENGTH),
        primary_key=True,
        unique=True,
        doc="The id of the object.",
    )
    type: Mapped[GulpCollabType] = mapped_column(String, doc="The type of the object.")
    time_created: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The time the object was created, in milliseconds from unix epoch.",
    )
    time_updated: Mapped[Optional[int]] = mapped_column(
        BIGINT,
        default=0,
        doc="The time the object was last updated, in milliseconds from unix epoch.",
    )

    __mapper_args__ = {
        "polymorphic_identity": "collab_base",
        "polymorphic_on": "type",
    }

    async def store(self, sess: AsyncSession = None) -> None:
        """
        Asynchronously stores the current instance in the database session.
        If no session is provided, a new session is created. The instance is added
        to the session and committed to the database.

        Args:
            sess (AsyncSession, optional): The database session to use. If None, a new session is created.
        Returns:
            None
        """

        if sess is None:
            sess = await session()
        async with sess:
            sess.add(self)
            await sess.commit()
            logger().info("---> store: stored %s" % (self))

            # TODO: notify websocket

    @staticmethod
    async def delete(obj_id: str, type: T, throw_if_not_found: bool = True) -> None:
        """
        Asynchronously deletes an object from the database.
        Args:
            obj_id (str): The ID of the object to be deleted.
            type (T): The type of the object to be deleted.
            throw_if_not_found (bool, optional): If True, raises an exception if the object does not exist. Defaults to True.
        Raises:
            ObjectNotFoundError: If throw_if_not_found is True and the object does not exist.
        Returns:
            None
        """

        logger().debug("---> delete: obj_id=%s, type=%s" % (obj_id, type))
        async with await session() as sess:
            q = select(type).where(type.id == obj_id).with_for_update()
            res = await sess.execute(q)
            c = GulpCollabBase.get_one_result_or_throw(
                res, obj_id=obj_id, type=type, throw_if_not_found=throw_if_not_found
            )
            if c is not None:
                sess.delete(c)
                await sess.commit()
                logger().info("---> deleted: %s" % (c))

                # TODO: notify websocket

    @staticmethod
    async def update(obj_id: str, type: T, d: dict) -> T:
        """
        Asynchronously updates an object of the specified type with the given data.
        Args:
            obj_id (str): The ID of the object to update.
            type (T): The type of the object to update.
            d (dict): A dictionary containing the fields to update and their new values.
        Returns:
            T: The updated object.
        Raises:
            Exception: If the object with the specified ID is not found.
        """

        logger().debug("---> update: obj_id=%s, type=%s, d=%s" % (obj_id, type, d))
        async with await session() as sess:
            q = select(type).where(type.id == obj_id).with_for_update()
            res = await sess.execute(q)
            c = GulpCollabBase.get_one_result_or_throw(res, obj_id=obj_id, type=type)
            if c is not None:
                # update
                for k, v in d.items():
                    setattr(c, k, v)
                c.time_updated = muty.time.now_msec()

                await sess.commit()
                logger().debug("---> updated: %s" % (c))

                # TODO: notify websocket

            return c

    @staticmethod
    async def get(type: T, flt: GulpCollabFilter = None) -> list[T]:
        """
        Asynchronously retrieves a list of objects of the specified type based on the provided filter.
        Args:
            type (T): The type of objects to retrieve.
            flt (GulpCollabFilter, optional): The filter to apply to the query. Defaults to None.
        Returns:
            list[T]: A list of objects of the specified type that match the filter criteria.
        Raises:
            Exception: If there is an error during the query execution or result processing.
        """

        logger().debug("---> get: type=%s, filter=%s" % (type, flt))
        flt = flt or GulpCollabFilter()

        async with await session() as sess:
            # build query
            q = flt.to_select_query(type)
            res = await sess.execute(q)
            c = GulpCollabBase.get_all_results_or_throw(res, type)
            if c is not None:
                logger().debug("---> get: found %d objects" % (len(c)))
                return c
        return []

    @staticmethod
    async def get_all_results_or_throw(
        res: Result, type: T, throw_if_not_found: bool = True
    ) -> list[T]:
        """
        gets all results or throws an exception

        Args:
            res (Result): The result.
            type (GulpCollabType): The type of the object.
            throw_if_not_found (bool, optional): If True, throws an exception if the result is empty. Defaults to True.

        Returns:
            list[T]: The list of objects or None if not found
        """
        c = res.scalars().all()
        if len(c) == 0:
            msg = "no %s found!" % (type)
            if throw_if_not_found:
                raise ObjectNotFound(msg)
            else:
                logger().warning(msg)
                return None

        return c

    @staticmethod
    async def get_one_result_or_throw(
        res: Result,
        obj_id: str = None,
        type: T = None,
        throw_if_not_found: bool = True,
    ) -> T:
        """
        gets one result or throws an exception

        Args:
            res (Result): The result.
            obj_id (str, optional): The id of the object, just for the debug print. Defaults to None.
            type (GulpCollabType, optional): The type of the object, just for the debug print. Defaults to None.
            throw_if_not_found (bool, optional): If True, throws an exception if the result is empty. Defaults to True.

        Returns:
            T: The object or None if not found.
        """
        c = res.scalar_one_or_none()
        if c is None:
            msg = "%s, id=%s not found!" % (type, obj_id)
            if throw_if_not_found:
                raise ObjectNotFound(msg)
            else:
                logger().warning(msg)
                return None
        return c


class GulpCollabObject(GulpCollabBase):
    """
    base for all collaboration objects (notes, links, stories, highlights)
    """

    __tablename__ = "collab_obj"

    # index for operation
    __table_args__ = (Index("idx_collab_obj_operation", "operation"),)

    # the following are present in all collab objects regardless of type
    id: Mapped[int] = mapped_column(ForeignKey("collab_base.id"), primary_key=True)
    user: Mapped[Optional[str]] = mapped_column(
        ForeignKey("user.id", ondelete="CASCADE")
    )
    operation: Mapped[Optional[str]] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE")
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"), default=None
    )
    tags: Mapped[Optional[list[str]]] = mapped_column(ARRAY(String), default=None)
    title: Mapped[Optional[str]] = mapped_column(String, default=None)
    private: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    data: Mapped[Optional[dict]] = mapped_column(JSONB, default=None)

    __mapper_args__ = {
        "polymorphic_identity": "collab_obj",
    }
