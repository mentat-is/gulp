from enum import StrEnum
import json
from typing import Optional, TypeVar
import muty.time
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import (
    BIGINT,
    ColumnElement,
    ForeignKey,
    Result,
    Select,
    String,
    Tuple,
    func,
    or_,
    select,
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
    events: list[dict] = Field(
        None,
        description="filter by the given event ID/s in CollabObj.events list of GulpDocument.",
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

        if self.events and "events" in type.columns:
            if self.opt_private:
                q = q.where(GulpCollabObject.private is True)
            if flt.name is not None:
                q = q.where(GulpCollabObject.name.in_(flt.name))
            if flt.operation_id is not None:
                q = q.where(GulpCollabObject.operation_id.in_(flt.operation_id))
            if flt.context:
                qq = [GulpCollabObject.context.ilike(x) for x in flt.context]
                q = q.filter(or_(*qq))
            if flt.src_file:
                qq = [GulpCollabObject.source.ilike(x) for x in flt.src_file]
                q = q.filter(or_(*qq))

            if flt.time_created_start is not None:
                q = q.where(
                    GulpCollabObject.time_created is not None
                    and GulpCollabObject.time_created >= flt.time_created_start
                )
            if flt.time_created_end is not None:
                q = q.where(
                    GulpCollabObject.time_created is not None
                    and GulpCollabObject.time_created <= flt.time_created_end
                )
            if flt.opt_time_start_end_events:
                # filter by collabobj.events["@timestamp"] time range
                conditions = []
                if flt.time_start is not None:
                    conditions.append(
                        f"(evt->>'@timestamp')::bigint >= {flt.time_start}"
                    )
                if flt.time_end is not None:
                    conditions.append(f"(evt->>'@timestamp')::bigint <= {flt.time_end}")

                # use a raw query to filter for the above condition
                condition_str = " AND ".join(conditions)
                raw_sql = f"""
                EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(collab_obj.events) AS evt
                    WHERE {condition_str}
                )
                """
                q = q.filter(text(raw_sql))
            else:
                if flt.time_start is not None:
                    # filter by collabobj time_start (pin)
                    q = q.where(
                        GulpCollabObject.time_start is not None
                        and GulpCollabObject.time_start >= flt.time_start
                    )
                if flt.time_end is not None:
                    # filter by collabobj time_end (pin)
                    q = q.where(
                        GulpCollabObject.time_end is not None
                        and GulpCollabObject.time_end <= flt.time_end
                    )

            if flt.text is not None:
                qq = [GulpCollabObject.text.ilike(x) for x in flt.text]
                q = q.filter(or_(*qq))

            if flt.events is not None:
                event_conditions = [
                    GulpCollabObject.events.op("@>")([{"id": event_id}])
                    for event_id in flt.events
                ]
                q = q.filter(or_(*event_conditions))

            if flt.tags is not None:
                if flt.opt_tags_and:
                    # all tags must match (CONTAINS operator)
                    q = q.filter(GulpCollabObject.tags.op("@>")(flt.tags))
                else:
                    # at least one tag must match (OVERLAP operator)
                    q = q.filter(GulpCollabObject.tags.op("&&")(flt.tags))

            if flt.data is not None:
                # filter is a key-value dict
                flt_data_jsonb = func.jsonb_build_object(
                    *itertools.chain(*flt.data.items())
                )
                q = q.filter(GulpCollabObject.data.op("@>")(flt_data_jsonb))

            if flt.limit is not None:
                q = q.limit(flt.limit)
            if flt.offset is not None:
                q = q.offset(flt.offset)


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
    async def delete(obj_id: str, type: T, throw_if_not_exists: bool = True) -> None:
        """
        Asynchronously deletes an object from the database.
        Args:
            obj_id (str): The ID of the object to be deleted.
            type (T): The type of the object to be deleted.
            throw_if_not_exists (bool, optional): If True, raises an exception if the object does not exist. Defaults to True.
        Raises:
            ObjectNotFoundError: If throw_if_not_exists is True and the object does not exist.
        Returns:
            None
        """

        logger().debug("---> delete: obj_id=%s, type=%s" % (obj_id, type))
        async with await session() as sess:
            q = select(type).where(type.id == obj_id).with_for_update()
            res = await sess.execute(q)
            c = GulpCollabObject.get_one_result_or_throw(
                res, obj_id=obj_id, type=type, throw_if_not_exists=throw_if_not_exists
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
            c = GulpCollabObject.get_one_result_or_throw(res, obj_id=obj_id, type=type)

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
        logger().debug("---> get: type=%s, filter=%s" % (type, flt))
        if flt is None:
            flt = GulpCollabFilter()

        async with await session() as sess:
            q = select(type)
            # TODO: make q

            res = await sess.execute(q)
            if flt is not None and flt.opt_basic_fields_only:
                # just the selected columns
                objs = res.fetchall()
            else:
                # full objects
                objs = res.scalars().all()
            if len(objs) == 0:
                logger().warning("no CollabObj found (flt=%s)" % (flt))
                return []
                # raise ObjectNotFound("no objects found (flt=%s)" % (flt))

            if flt is not None and flt.opt_basic_fields_only:
                # we will return an array of dict instead of full ORM objects
                oo = []
                for o in objs:
                    oo.append(
                        {
                            "id": o[0],
                            "name": o[1],
                            "type": o[2],
                            "owner_user_id": o[3],
                            "operation_id": o[4],
                            "time_created": o[5],
                            "time_updated": o[6],
                            "edits": o[7],
                        }
                    )
                objs = oo
            logger().debug("---> get: found %d objects" % (len(objs)))
            return objs

    @staticmethod
    async def get_one_result_or_throw(
        res: Result,
        obj_id: str = None,
        type: T = None,
        throw_if_not_exists: bool = True,
    ) -> T:
        """
        gets one result or throws an exception

        Args:
            res (Result): The result.
            obj_id (str, optional): The id of the object, just for the debug print. Defaults to None.
            type (GulpCollabType, optional): The type of the object, just for the debug print. Defaults to None.
            throw_if_not_exists (bool, optional): If True, throws an exception if the object does not exist. Defaults to True.
        """
        c = res.scalar_one_or_none()
        if c is None:
            msg = "%s, id=%s not found!" % (type, obj_id)
            if throw_if_not_exists:
                raise ObjectNotFound(msg)
            else:
                logger().warning(msg)
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
