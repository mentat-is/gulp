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
from gulp.api.collab_api import GulpCollabLevel
from gulp.api.collab.structs import COLLAB_MAX_NAME_LENGTH, GulpAssociatedEvent, GulpCollabFilter, GulpCollabType, GulpUserPermission
from gulp.defs import InvalidArgument, ObjectAlreadyExists, ObjectNotFound
from gulp.utils import logger

T = TypeVar("T", bound="BaseObj")
class Base(MappedAsDataclass, AsyncAttrs, DeclarativeBase, SerializeMixin):
    __tablename__ = "collab_base"
    __table_args__ = (Index("idx_collab_obj_operation", "operation"),)

    id: Mapped[str] = mapped_column(String(COLLAB_MAX_NAME_LENGTH), primary_key=True, unique=True)
    type: Mapped[GulpCollabType] = mapped_column(String())
    time_created: Mapped[Optional[int]] = mapped_column(BIGINT, default=0)
    time_updated: Mapped[Optional[int]] = mapped_column(BIGINT, default=0)
                                                 

T = TypeVar("T", bound="BaseObj")
class BaseObj(MappedAsDataclass, AsyncAttrs, DeclarativeBase, SerializeMixin):
    """
    base for all collaboration objects (notes, links, stories, highlights)
    """

    __tablename__ = "collab_obj"
    __table_args__ = (Index("idx_collab_obj_operation", "operation"),)

    # the following are present in all collab objects regardless of type
    id: Mapped[str] = mapped_column(String(COLLAB_MAX_NAME_LENGTH), primary_key=True, unique=True)
    type: Mapped[GulpCollabType] = mapped_column(String())
    owner: Mapped[Optional[str]] = mapped_column(
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
    time_created: Mapped[Optional[int]] = mapped_column(BIGINT, default=0)
    time_updated: Mapped[Optional[int]] = mapped_column(BIGINT, default=0)
    private: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    data: Mapped[Optional[dict]] = mapped_column(JSONB, default=None)

    # the following must be refactored for specific tables
    context: Mapped[Optional[str]] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"), default=None # for notes, highlight
    )
    source: Mapped[Optional[str]] = mapped_column(String, default=None) # for notes, highlight
    time_start: Mapped[Optional[int]] = mapped_column(BIGINT, default=0) # for higlight 
    time_end: Mapped[Optional[int]] = mapped_column(BIGINT, default=0) # for highlight
    events: Mapped[Optional[list[dict]]] = mapped_column(JSONB, default=None) # for story, notes, link
    text: Mapped[Optional[str]] = mapped_column(String, default=None) # for notes, story
    
    __mapper_args__ = {
        "polymorphic_identity": "collab_obj",
        "polymorphic_on": "type",
    }    
    
    async def store(self, sess: AsyncSession = None) -> None:
        """
        stores the collaboration object in the database

        Args:
            sess (AsyncSession, optional): The session to use. Defaults to None (creates a new session).
        """
        if sess is None:
            sess = await session()
        async with sess:
            sess.add(self)
            await sess.commit()
            logger().info("---> store: stored %s" % (self))

    @staticmethod
    async def delete(obj_id: str, t: T, throw_if_not_exists: bool = True) -> None:
        """
        deletes a collaboration object

        Args:
            obj_id (str): The id of the object.
            t (T): The class of the object, derived from CollabBase.
            throw_if_not_exists (bool, optional): If True, throws an exception if the object does not exist. Defaults to True.
        """
        logger().debug("---> delete: obj_id=%s, type=%s" % (obj_id, t))
        async with await session() as sess:
            q = select(T).where(T.id == obj_id).with_for_update()
            res = await sess.execute(q)
            c = BaseObj.get_one_result_or_throw(
                res, obj_id=obj_id, t=t, throw_if_not_exists=throw_if_not_exists
            )
            if c is not None:
                sess.delete(c)
                await sess.commit()
                logger().info("---> deleted: %s" % (c))

    @staticmethod
    async def update(obj_id: str, t: T, d: dict) -> T:
        """
        updates a collaboration object

        Args:
            obj_id (str): The id of the object.
            t (T): The type of the object.
            d (dict): The data to update.
            done (bool, optional): If True, sets the object as done. Defaults to False.

        Returns:
            T: The updated object.
        """
        logger().debug("---> update: obj_id=%s, type=%s, d=%s" % (obj_id, t, d))
        async with await session() as sess:
            q = select(T).where(T.name == obj_id).with_for_update()
            res = await sess.execute(q)
            c = BaseObj.get_one_result_or_throw(res, obj_id=obj_id, t=t)

            # update
            for k, v in d.items():
                setattr(c, k, v)
            c.time_updated = muty.time.now_msec()

            await sess.commit()
            logger().debug("---> updated: %s" % (c))
            return c

    @staticmethod
    async def get_one_result_or_throw(
        res: Result,
        obj_id: str = None,
        t: GulpCollabType = None,
        throw_if_not_exists: bool = True,
    ) -> T:
        """
        gets one result or throws an exception

        Args:
            res (Result): The result.
            obj_id (str, optional): The id of the object, just for the debug print. Defaults to None.
            t (GulpCollabType, optional): The type of the object, just for the debug print. Defaults to None.
            throw_if_not_exists (bool, optional): If True, throws an exception if the object does not exist. Defaults to True.
        """
        c = res.scalar_one_or_none()
        if c is None:
            msg = "collabobject type=%s, name=%s not found!" % (t, obj_id)
            if throw_if_not_exists:
                raise ObjectNotFound(msg)
            else:
                logger().warning(msg)
        return c

    
    
    @staticmethod
    async def create(
        engine: AsyncEngine,
        token: str,
        req_id: str,
        t: GulpCollabType,
        ws_id: str,
        operation_id: int = None,
        context: str = None,
        src_file: str = None,
        name: str = None,
        description: str = None,
        txt: str = None,
        time_start: int = None,
        time_end: int = None,
        events: list[GulpAssociatedEvent] = None,
        data: dict = None,
        tags: list[str] = None,
        glyph_id: int = None,
        internal_user=None,
        internal_user_id: int = None,
        skip_ws: bool = False,
        private: bool = False,
        level: GulpCollabLevel = GulpCollabLevel.DEFAULT,
    ) -> "BaseObj":
        """
        Creates a new collaboration object.

        Args:
            engine (AsyncEngine): The database engine.
            token (str): The user token.
            req_id (str): The request ID.
            t (GulpCollabType): The type of the collaboration object.
            ws_id (str): The websocket ID
            operation_id (int, optional): The operation ID. Defaults to None.
            context (str, optional): The context of the collaboration object. Defaults to None.
            src_file (str, optional): The source file of the collaboration object. Defaults to None.
            name (str, optional): The name of the collaboration object. Defaults to None.
            description (str, optional): The description of the collaboration object. Defaults to None.
            txt (str, optional): The text of the collaboration object. Defaults to None.
            time_start (int, optional): The start time of the collaboration object. Defaults to None.
            time_end (int, optional): The end time of the collaboration object. Defaults to None.
            events (list[CollabEvent], optional): The events related to the collaboration object. Defaults to None.
            data (dict, optional): Additional data for the collaboration object. Defaults to None.
            tags (list[str], optional): The tags of the collaboration object. Defaults to None.
            glyph_id (int, optional): The glyph ID. Defaults to None.
            internal_user (User, optional): internal usage only, an User object to skip token check
            internal_user_id (int, optional): internal usage only, to skip token check
            skip_ws (bool, optional): Whether to skip notifying the websocket. Defaults to False.
            private (bool, optional): Whether the collaboration object is private(=not published on the websocket). Defaults to False.
            level (GulpCollabLevel, optional): The level of the collaboration object. Defaults to GulpCollabLevel.DEFAULT.
        Returns:
            CollabObj: The created collaboration object.

        Raises:
            ObjectAlreadyExists: If a collaboration object with the same hash already exists.
            MissingPermission: If the user does not have permission to create the collaboration object.
        """

        import gulp.api.rest.ws as ws_api
        from gulp.api.collab.session import GulpUserSession
        from gulp.api.rest.ws import WsQueueDataType

        internal_username = None
        """
        logger().debug(
            "---> create: t=%s, operation_id=%s, context=%s, src_file=%s, name=%s, time_start=%s, time_end=%s, events=%s, text=%s, description=%s, data=%s, tags=%s, level=%s"
            % (
                t,
                operation_id,
                context,
                src_file,
                name,
                time_start,
                time_end,
                events,
                muty.string.make_shorter(txt),
                muty.string.make_shorter(description),
                muty.string.make_shorter(str(data)),
                tags,
                level,
            )
        )
        """
        if time_start is not None and time_end is not None:
            if time_start > 0 and time_end < time_start:
                raise InvalidArgument("time_end must be greater than time_start")

        # to create a collab obj we need at least edit permission
        if internal_user_id is not None:
            # using the internal user id (skipping token check)
            user = None
            internal_username = "admin"
        else:
            if internal_user is None:
                user, _ = await GulpUserSession.check_token(
                    engine, token, GulpUserPermission.EDIT
                )
            else:
                user = internal_user
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            # check if it already exists
            h = muty.crypto.hash_blake2b(
                str(t)
                + str(operation_id)
                + str(context)
                + str(src_file)
                + str(name)
                + str(time_start)
                + str(time_end)
                + str(events)
                + str(txt)
                + str(description)
            )
            q = select(BaseObj).where(BaseObj.hash == h)
            res = await sess.execute(q)
            obj = res.scalar_one_or_none()
            if obj is not None:
                logger().warning("collab object with hash=%s already exists !" % (h))
                raise ObjectAlreadyExists(
                    "collab object with hash=%s already exists" % (h)
                )

            # create new collaboration object
            now = muty.time.now_msec()
            obj = BaseObj(
                owner_user_id=user.id if user is not None else internal_user_id,
                type=t,
                hash=h,
                operation_id=operation_id,
                time_created=now,
                context=context,
                source=src_file,
                name=name,
                description=description,
                text=txt,
                time_start=time_start,
                time_end=time_end,
                events=[x.to_dict() for x in events] if events is not None else [],
                glyph_id=glyph_id,
                tags=tags,
                data=data,
                private=private,
                level=level,
            )

            # record edit(creation) in the edits table
            obj.edits = []
            obj.time_updated = now
            sess.add(obj)
            await sess.flush()
            await sess.commit()
            # ollab_api.logger().info("---> create: %s" % (obj))

            if not skip_ws:
                # push to websocket queue
                username = user.name if user is not None else "admin"
                ws_api.shared_queue_add_data(
                    WsQueueDataType.COLLAB_CREATE,
                    req_id,
                    {"collabs": [obj.to_dict()]},
                    username=username,
                    ws_id=ws_id,
                )
            return obj

    @staticmethod
    async def update(
        engine: AsyncEngine,
        token: str,
        req_id: str,
        object_id: int,
        ws_id: str,
        operation_id: int = None,
        name: str = None,
        description: str = None,
        txt: str = None,
        time_start: int = None,
        time_end: int = None,
        events: list[GulpAssociatedEvent] = None,
        data: dict = None,
        tags: list[str] = None,
        glyph_id: int = None,
        t: GulpCollabType = None,
        private: bool = None,
    ) -> "BaseObj":
        """
        Update a collab object with the specified parameters.

        Parameters:
        - engine (AsyncEngine): The database engine.
        - token (str): The user token.
        - req_id (str): The request ID.
        - object_id (int): The ID of the collab object to update.
        - ws_id (str): The websocket ID.
        - operation_id (int, optional): The new operation ID for the collab object.
        - name (str, optional): The new name for the collab object.
        - description (str, optional): The new description for the collab object.
        - txt (str, optional): The new text for the collab object.
        - time_start (int, optional): The new start time for the collab object.
        - time_end (int, optional): The new end time for the collab object.
        - events (list[CollabEvent], optional): The new events for the collab object.
        - data (dict, optional): The new data for the collab object.
        - tags (list[str], optional): The new tags for the collab object.
        - glyph_id (int, optional): The new glyph ID for the collab object.
        - t (GulpCollabType): The type of the collab object (if None, no object type check is performed).
        - private (bool, optional): Whether the collaboration object is private(=not published on the websocket). Defaults to False.

        Returns:
        - CollabObj: The updated collab object.

        Raises:
        - ObjectNotFound: If the collab object with the specified ID is not found.
        - MissingPermission: If the user does not have permission to update the collab object.
        """

        import gulp.api.rest.ws as ws_api
        from gulp.api.collab.session import GulpUserSession
        from gulp.api.rest.ws import WsQueueDataType

        # to update a collab obj we need at least edit permission
        user, _ = await GulpUserSession.check_token(
            engine, token, GulpUserPermission.EDIT, object_id
        )

        async with AsyncSession(engine, expire_on_commit=False) as sess:
            if t is not None:
                q = (
                    select(BaseObj)
                    .where(BaseObj.id == object_id, BaseObj.type == t)
                    .with_for_update()
                )
            else:
                q = select(BaseObj).where(BaseObj.id == object_id).with_for_update()
            res = await sess.execute(q)
            obj = res.scalar_one_or_none()
            if obj is None:
                raise ObjectNotFound(
                    "collab object id=%d, type=%d not found ! " % (object_id, t)
                )
            logger().debug(
                "---> found object: %s, type=%d"
                % (obj, t if t is not None else obj.type)
            )
            if name is not None:
                obj.name = name
            if operation_id is not None:
                obj.operation_id = operation_id
            if tags is not None:
                obj.tags = tags
            if description is not None:
                obj.description = description
            if txt is not None:
                obj.text = txt
            if data is not None:
                obj.data = data
            if time_start is not None:
                obj.time_start = time_start
            if time_end is not None:
                obj.time_end = time_end
            if events is not None:
                obj.events = [x.to_dict() for x in events]
            if glyph_id is not None:
                obj.glyph_id = glyph_id
            if private is not None:
                obj.private = private
            if obj.events is not None and obj.time_start is not None:
                raise InvalidArgument(
                    "CollabObj %s cannot have both events and time_start set!" % (obj)
                )

            # set update time and recalculate hash
            obj.time_updated = muty.time.now_msec()
            h = muty.crypto.hash_blake2b(
                str(obj.type)
                + str(obj.operation_id)
                + str(obj.context)
                + str(obj.source)
                + str(obj.name)
                + str(obj.text)
                + str(time_start)
                + str(time_end)
                + str(events)
                + str(obj.description)
            )
            obj.hash = h

            # record edit in the edits table
            sess.add(obj)

            #  and finally commit
            await sess.commit()
            logger().info("---> update: updated collab object: %s" % (obj))

            # push to websocket queue
            ws_api.shared_queue_add_data(
                WsQueueDataType.COLLAB_UPDATE,
                req_id,
                obj.to_dict(),
                user.name,
                ws_id=ws_id,
            )
            return obj

    @staticmethod
    async def delete(
        engine: AsyncEngine,
        token: str,
        req_id: str,
        object_id: int,
        t: GulpCollabType = None,
        ws_id: str = None,
    ) -> None:
        """
        Deletes a collaboration object.

        Args:
            engine (AsyncEngine): The database engine.
            token (str): The user token.
            req_id (str): The request ID.
            object_id (int): The object ID.
            t (GulpCollabType, optional): The type of the collaboration object. Defaults to None.
            ws_id (str, optional): The websocket ID.

        Raises:
            ObjectNotFound: If the object is not found.
            MissingPermission: If the user does not have permission to delete the object.
        """
        import gulp.api.rest.ws as ws_api
        from gulp.api.collab.session import GulpUserSession
        from gulp.api.rest.ws import WsQueueDataType

        logger().debug("---> delete: id=%d" % (object_id))

        user, _ = await GulpUserSession.check_token(
            engine, token, GulpUserPermission.DELETE, object_id
        )
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            if t is not None:
                q = select(BaseObj).where(
                    BaseObj.id == object_id, BaseObj.type == t
                )
            else:
                q = select(BaseObj).where(BaseObj.id == object_id)
            res = await sess.execute(q)
            obj = res.scalar_one_or_none()
            if obj is None:
                raise ObjectNotFound(
                    "collab object id=%d (type=%d) not found" % (object_id, t)
                )
            await sess.delete(obj)
            await sess.commit()
            logger().debug(
                "---> delete: deleted object id=%d, type=%d"
                % (object_id, t if t is not None else obj.type)
            )

            # push to websocket queue
            ws_api.shared_queue_add_data(
                WsQueueDataType.COLLAB_DELETE,
                req_id,
                obj.to_dict(),
                user.name,
                ws_id=ws_id,
            )

    @staticmethod
    async def get_edits_by_object(
        engine: AsyncEngine, object_id: int
    ) -> list[CollabEdits]:
        """
        Gets the list of editors for a collaboration object.

        Args:
            engine (AsyncEngine): The database engine.
            object_id (int): The object ID.

        Returns:
            list[CollabEdit]: the list of edits
        Raises:
            ObjectNotFound: If no editors are found for the collaboration object.
        """
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(CollabEdits).where(CollabEdits.collab_obj_id == object_id)
            res = await sess.execute(q)
            edits = res.scalars().all()
            if len(edits) == 0:
                raise ObjectNotFound(
                    "no editors found for collab object id=%d" % (object_id)
                )
            return edits

    @staticmethod
    async def get_edits(
        engine: AsyncEngine, flt: GulpCollabFilter = None
    ) -> list[CollabEdits]:
        """
        Gets edits by filter

        Args:
            engine (AsyncEngine): The database engine.
            flt (GulpCollabFilter, optional): The filter (available filters: owner_id=user_id, time_created_start, time_created_end). Defaults to None.

        Returns:
            list[CollabEdit]: the list of edits (resolved).
        Raises:
            ObjectNotFound: If no edits are found
        """
        logger().debug("---> get: filter=%s" % (flt))
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(CollabEdits)
            if flt is not None:
                if flt.owner_id is not None:
                    q = q.where(CollabEdits.user_id.in_(flt.owner_id))
                if flt.time_start is not None:
                    q = q.where(CollabEdits.time_edit >= flt.time_created_start)
                if flt.time_end is not None:
                    q = q.where(CollabEdits.time_edit <= flt.time_created_end)

            res = await sess.execute(q)
            if flt is not None and flt.opt_basic_fields_only:
                # just the selected columns
                objs = res.fetchall()
            else:
                # full objects
                objs = res.scalars().all()
            if len(objs) == 0:
                raise ObjectNotFound("no objects found (flt=%s)" % (flt))

            logger().debug("---> get: found %d edits" % (len(objs)))
            return objs

    @staticmethod
    async def get(
        engine: AsyncEngine, flt: GulpCollabFilter = None
    ) -> Union[list["BaseObj"], list[dict]]:
        """
        Gets collaboration objects.

        Args:
            engine (AsyncEngine): The database engine.
            flt (GulpCollabFilter, optional): The filter (available filters: id, level, private, owner_id, name, type, operation_id, context, src_file, text, time_start, time_end, events, opt_basic_fields_only, limit, offset, tags, data). Defaults to None.

        Returns:
            Union[list[CollabObj], list[dict]]: The list of collaboration objects (list of dict if opt_basic_fields_only is specified in the filter).
        Raises:
            ObjectNotFound: If no objects are found.

        """
        logger().debug("---> get: filter=%s" % (flt))
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(BaseObj)
            if flt is not None:
                if flt.opt_basic_fields_only:
                    q = select(
                        BaseObj.id,
                        BaseObj.name,
                        BaseObj.type,
                        BaseObj.owner_user_id,
                        BaseObj.operation_id,
                        BaseObj.time_created,
                        BaseObj.time_updated,
                        BaseObj.edits,
                        BaseObj.level,
                        BaseObj.private,
                    )

                if flt.id is not None:
                    q = q.where(BaseObj.id.in_(flt.id))
                if flt.level is not None:
                    q = q.where(BaseObj.level.in_(flt.level))
                if flt.private_only:
                    q = q.where(BaseObj.private is True)
                if flt.owner_id is not None:
                    q = q.where(BaseObj.owner_user_id.in_(flt.owner_id))
                if flt.name is not None:
                    q = q.where(BaseObj.name.in_(flt.name))
                if flt.type is not None:
                    q = q.where(BaseObj.type.in_(flt.type))
                if flt.operation_id is not None:
                    q = q.where(BaseObj.operation_id.in_(flt.operation_id))
                if flt.context:
                    qq = [BaseObj.context.ilike(x) for x in flt.context]
                    q = q.filter(or_(*qq))
                if flt.src_file:
                    qq = [BaseObj.source.ilike(x) for x in flt.src_file]
                    q = q.filter(or_(*qq))

                if flt.time_created_start is not None:
                    q = q.where(
                        BaseObj.time_created is not None
                        and BaseObj.time_created >= flt.time_created_start
                    )
                if flt.time_created_end is not None:
                    q = q.where(
                        BaseObj.time_created is not None
                        and BaseObj.time_created <= flt.time_created_end
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
                            BaseObj.time_start is not None
                            and BaseObj.time_start >= flt.time_start
                        )
                    if flt.time_end is not None:
                        # filter by collabobj time_end (pin)
                        q = q.where(
                            BaseObj.time_end is not None
                            and BaseObj.time_end <= flt.time_end
                        )

                if flt.text is not None:
                    qq = [BaseObj.text.ilike(x) for x in flt.text]
                    q = q.filter(or_(*qq))

                if flt.events is not None:
                    event_conditions = [
                        BaseObj.events.op("@>")([{"id": event_id}])
                        for event_id in flt.events
                    ]
                    q = q.filter(or_(*event_conditions))

                if flt.tags is not None:
                    if flt.opt_tags_and:
                        # all tags must match (CONTAINS operator)
                        q = q.filter(BaseObj.tags.op("@>")(flt.tags))
                    else:
                        # at least one tag must match (OVERLAP operator)
                        q = q.filter(BaseObj.tags.op("&&")(flt.tags))

                if flt.data is not None:
                    # filter is a key-value dict
                    flt_data_jsonb = func.jsonb_build_object(
                        *itertools.chain(*flt.data.items())
                    )
                    q = q.filter(BaseObj.data.op("@>")(flt_data_jsonb))

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
