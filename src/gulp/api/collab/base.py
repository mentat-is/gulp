import itertools
from typing import Optional, TypeVar, Union

import muty.crypto
import muty.string
import muty.time
from sqlalchemy import (
    BIGINT,
    Boolean,
    ForeignKey,
    Index,
    Integer,
    Result,
    String,
    func,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm import Mapped, mapped_column, MappedAsDataclass, DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy_mixins import SerializeMixin
from gulp.api.collab import user_session
from gulp.api import collab_api
from gulp.api.collab_api import GulpCollabLevel, session
from gulp.api.collab.structs import COLLAB_MAX_NAME_LENGTH, GulpAssociatedEvent, GulpCollabFilter, GulpCollabType, GulpUserPermission
from gulp.defs import InvalidArgument, ObjectAlreadyExists, ObjectNotFound
from gulp.utils import logger
from gulp.api.collab.structs import GulpCollabFilter

class GulpCollabBase(MappedAsDataclass, AsyncAttrs, DeclarativeBase, SerializeMixin):
    """
    base for everything on the collab database
    """
    T = TypeVar("T", bound="GulpCollabBase")
    
    __tablename__ = "collab_base"

    id: Mapped[str] = mapped_column(String(COLLAB_MAX_NAME_LENGTH), primary_key=True, unique=True, doc="The id of the object.")
    type: Mapped[GulpCollabType] = mapped_column(String, doc="The type of the object.")
    time_created: Mapped[Optional[int]] = mapped_column(BIGINT, default=0, doc="The time the object was created, in milliseconds from unix epoch.")
    time_updated: Mapped[Optional[int]] = mapped_column(BIGINT, default=0, doc="The time the object was last updated, in milliseconds from unix epoch.")
                                           
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
    async def get(type: T, flt: GulpCollabFilter = None
    ) -> list[T]:
        logger().debug("---> get: type=%s, filter=%s" % (type, flt))
        if flt is None:
            flt = GulpCollabFilter()

        async with await session() as sess:
            q = select(type)
            if flt is not None:
                if flt.id is not None:
                    q = q.where(type.id.in_(flt.id))
                if flt.type is not None:
                    q = q.where(type.type.in_(flt.type))
                if flt.operation is not None and 'operation' in type.__annotations__:
                    q = q.where(type.operation.in_(flt.operation))
                if flt.context is not None and hasattr(type, "context"):
                    q = q.where(type.context.in_(flt.context))
                if flt.log_file_path is not None and hasattr(type, "log_file_path"):
                    q = q.where(type.log_file_path.in_(flt.log_file_path))
                if flt.user is not None and hasattr(type, "user"):
                    q = q.where(type.user.in_(flt.user))
                if flt.tags is not None and hasattr(type, "tags"):
                    if flt.opt_tags_and:
                        # all tags must match (CONTAINS operator)
                        q = q.filter(type.tags.op("@>")(flt.tags))
                    else:
                        # at least one tag must match (OVERLAP operator)
                        q = q.filter(type.tags.op("&&")(flt.tags))
                if flt.title is not None and hasattr(type, "title"):
                    q = q.where(type.title.in_(flt.title))
                if flt.text is not None and hasattr(type, "text"):
                    qq = [type.text.ilike(x) for x in flt.text]
                    q = q.filter(or_(*qq))
                if flt.events is not None and hasattr(type, "events"):

















                if flt.private_only:
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
                        conditions.append(
                            f"(evt->>'@timestamp')::bigint <= {flt.time_end}"
                        )

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
        type: GulpCollabType = None,
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
            msg = "collab type=%s, id=%s not found!" % (type, obj_id)
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
    

