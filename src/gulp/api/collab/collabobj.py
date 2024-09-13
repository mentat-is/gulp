import itertools
from typing import Optional, Union

import muty.crypto
import muty.string
import muty.time
from sqlalchemy import (
    BIGINT,
    Boolean,
    ForeignKey,
    Index,
    Integer,
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
from sqlalchemy.orm import Mapped, mapped_column

import gulp.api.collab_api as collab_api
from gulp.api.collab.base import (
    CollabBase,
    GulpAssociatedEvent,
    GulpCollabFilter,
    GulpCollabLevel,
    GulpCollabType,
    GulpUserPermission,
)
from gulp.defs import InvalidArgument, ObjectAlreadyExists, ObjectNotFound


class CollabEdits(CollabBase):
    """
    Represents an edit on a collaboration object.
    """

    __tablename__ = "edits"
    __table_args__ = (
        Index('idx_edits_collab_obj_id', 'collab_obj_id'),
    )    
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    username: Mapped[str] = mapped_column(String(32))
    collab_obj_id: Mapped[int] = mapped_column(
        ForeignKey("collab_obj.id", ondelete="CASCADE")
    )
    time_edit: Mapped[int] = mapped_column(BIGINT, default=0)

    def to_dict(self):
        d = {
            "id": self.id,
            "user_id": self.user_id,
            "username": self.username,
            "collab_obj_id": self.collab_obj_id,
            "time_edit": self.time_edit,
        }
        return d


class CollabObj(CollabBase):
    """
    represents a collaboration object (i.e. note, highlight, story, link, etc.)
    """

    __tablename__ = "collab_obj"
    __table_args__ = (
        Index('idx_collab_obj_operation_id', 'operation_id'),
    )    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    owner_user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE")
    )
    type: Mapped[GulpCollabType] = mapped_column(Integer)
    hash: Mapped[str] = mapped_column(String(128), unique=True)
    operation_id: Mapped[int] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"), default=None, nullable=True
    )
    time_created: Mapped[int] = mapped_column(BIGINT, default=None, nullable=True)
    time_updated: Mapped[int] = mapped_column(BIGINT, default=None, nullable=True)
    time_start: Mapped[int] = mapped_column(BIGINT, default=None, nullable=True)
    time_end: Mapped[int] = mapped_column(BIGINT, default=None, nullable=True)
    events: Mapped[Optional[list[dict]]] = mapped_column(
        JSONB, default=None, nullable=True
    )
    context: Mapped[Optional[str]] = mapped_column(String, default=None, nullable=True)
    src_file: Mapped[Optional[str]] = mapped_column(String, default=None, nullable=True)
    name: Mapped[str] = mapped_column(String(128), default=None, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(
        String, default=None, nullable=True
    )
    text: Mapped[str] = mapped_column(String, default=None, nullable=True)
    tags: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String), default=None, nullable=True
    )
    glyph_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"), default=None, nullable=True
    )
    data: Mapped[Optional[dict]] = mapped_column(JSONB, default=None, nullable=True)
    edits: Mapped[Optional[list[int]]] = mapped_column(ARRAY(Integer), default=None)
    level: Mapped[GulpCollabLevel] = mapped_column(
        Integer, default=GulpCollabLevel.DEFAULT
    )
    private: Mapped[bool] = mapped_column(Boolean, default=False)

    def __repr__(self):
        return (
            f"<CollabObj id={self.id}, owner_user_id={self.owner_user_id}, type={self.type}, level={self.level}"
            f"time_created={self.time_created}, time_updated={self.time_updated}, "
            f"time_start={self.time_start}, time_end={self.time_end}, operation_id={self.operation_id}, "
            f"context={self.context}, src_file={self.src_file}, name={self.name}, "
            f"description={muty.string.make_shorter(self.description)}, "
            f"text={muty.string.make_shorter(self.text)}, "
            f"glyph_id={self.glyph_id}, "
            f"tags={self.tags}, events={self.events}, data={self.data}, edits={self.edits}, private={self.private}>"
        )

    def to_dict(self, extras: dict = None):
        d = {
            "id": self.id,
            "level": self.level,
            "owner_user_id": self.owner_user_id,
            "type": self.type,
            "time_created": self.time_created,
            "time_updated": self.time_updated,
            "time_start": self.time_start,
            "time_end": self.time_end,
            "operation_id": self.operation_id,
            "context": self.context,
            "src_file": self.src_file,
            "name": self.name,
            "description": self.description,
            "text": self.text,
            "glyph_id": self.glyph_id,
            "tags": self.tags,
            "events": self.events,
            "data": self.data,
            "edits": self.edits,
            "private": self.private,
        }
        if extras is not None:
            # add extras
            d.update(extras)

        return d

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
    ) -> "CollabObj":
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
        from gulp.api.collab.session import UserSession
        from gulp.api.rest.ws import WsQueueDataType

        internal_username = None
        """
        collab_api.logger().debug(
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
                user, _ = await UserSession.check_token(
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
            q = select(CollabObj).where(CollabObj.hash == h)
            res = await sess.execute(q)
            obj = res.scalar_one_or_none()
            if obj is not None:
                collab_api.logger().warning(
                    "collab object with hash=%s already exists !" % (h)
                )
                raise ObjectAlreadyExists(
                    "collab object with hash=%s already exists" % (h)
                )

            # create new collaboration object
            now = muty.time.now_msec()
            obj = CollabObj(
                owner_user_id=user.id if user is not None else internal_user_id,
                type=t,
                hash=h,
                operation_id=operation_id,
                time_created=now,
                context=context,
                src_file=src_file,
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
            await CollabObj._store_edit(
                sess,
                obj,
                user.id if user is not None else internal_user_id,
                user.name if user is not None else internal_username,
                obj.time_updated,
            )
            await sess.commit()
            #ollab_api.logger().info("---> create: %s" % (obj))

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
    async def _store_edit(
        sess: AsyncSession,
        obj: "CollabObj",
        user_id: int,
        username: str,
        time_edit: int,
    ) -> None:
        # record edit in the edits table
        e = CollabEdits(
            user_id=user_id,
            username=username,
            collab_obj_id=obj.id,
            time_edit=time_edit,
        )
        sess.add(e)
        await sess.flush()

        # add edit id to the collab obj
        obj.edits.append(e.id)
        q = (
            update(CollabObj)
            .where(CollabObj.id == obj.id)
            .values(edits=obj.edits, data=obj.data)
        )
        await sess.execute(q)

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
    ) -> "CollabObj":
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
        from gulp.api.collab.session import UserSession
        from gulp.api.rest.ws import WsQueueDataType

        # to update a collab obj we need at least edit permission
        user, _ = await UserSession.check_token(
            engine, token, GulpUserPermission.EDIT, object_id
        )

        async with AsyncSession(engine, expire_on_commit=False) as sess:
            if t is not None:
                q = (
                    select(CollabObj)
                    .where(CollabObj.id == object_id, CollabObj.type == t)
                    .with_for_update()
                )
            else:
                q = select(CollabObj).where(CollabObj.id == object_id).with_for_update()
            res = await sess.execute(q)
            obj = res.scalar_one_or_none()
            if obj is None:
                raise ObjectNotFound(
                    "collab object id=%d, type=%d not found ! " % (object_id, t)
                )
            collab_api.logger().debug(
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
                + str(obj.src_file)
                + str(obj.name)
                + str(obj.text)
                + str(time_start)
                + str(time_end)
                + str(events)
                + str(obj.description)
            )
            obj.hash = h

            # record edit in the edits table
            await CollabObj._store_edit(sess, obj, user.id, user.name, obj.time_updated)
            sess.add(obj)

            #  and finally commit
            await sess.commit()
            collab_api.logger().info("---> update: updated collab object: %s" % (obj))

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
        from gulp.api.collab.session import UserSession
        from gulp.api.rest.ws import WsQueueDataType

        collab_api.logger().debug("---> delete: id=%d" % (object_id))

        user, _ = await UserSession.check_token(
            engine, token, GulpUserPermission.DELETE, object_id
        )
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            if t is not None:
                q = select(CollabObj).where(
                    CollabObj.id == object_id, CollabObj.type == t
                )
            else:
                q = select(CollabObj).where(CollabObj.id == object_id)
            res = await sess.execute(q)
            obj = res.scalar_one_or_none()
            if obj is None:
                raise ObjectNotFound(
                    "collab object id=%d (type=%d) not found" % (object_id, t)
                )
            await sess.delete(obj)
            await sess.commit()
            collab_api.logger().debug(
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
        collab_api.logger().debug("---> get: filter=%s" % (flt))
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

            collab_api.logger().debug("---> get: found %d edits" % (len(objs)))
            return objs

    @staticmethod
    async def get(
        engine: AsyncEngine, flt: GulpCollabFilter = None
    ) -> Union[list["CollabObj"], list[dict]]:
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
        collab_api.logger().debug("---> get: filter=%s" % (flt))
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(CollabObj)
            if flt is not None:
                if flt.opt_basic_fields_only:
                    q = select(
                        CollabObj.id,
                        CollabObj.name,
                        CollabObj.type,
                        CollabObj.owner_user_id,
                        CollabObj.operation_id,
                        CollabObj.time_created,
                        CollabObj.time_updated,
                        CollabObj.edits,
                        CollabObj.level,
                        CollabObj.private,
                    )

                if flt.id is not None:
                    q = q.where(CollabObj.id.in_(flt.id))
                if flt.level is not None:
                    q = q.where(CollabObj.level.in_(flt.level))
                if flt.private_only:
                    q = q.where(CollabObj.private is True)
                if flt.owner_id is not None:
                    q = q.where(CollabObj.owner_user_id.in_(flt.owner_id))
                if flt.name is not None:
                    q = q.where(CollabObj.name.in_(flt.name))
                if flt.type is not None:
                    q = q.where(CollabObj.type.in_(flt.type))
                if flt.operation_id is not None:
                    q = q.where(CollabObj.operation_id.in_(flt.operation_id))
                if flt.context:
                    qq = [CollabObj.context.ilike(x) for x in flt.context]
                    q = q.filter(or_(*qq))
                if flt.src_file:
                    qq = [CollabObj.src_file.ilike(x) for x in flt.src_file]
                    q = q.filter(or_(*qq))

                if flt.time_created_start is not None:
                    q = q.where(
                        CollabObj.time_created is not None
                        and CollabObj.time_created >= flt.time_created_start
                    )
                if flt.time_created_end is not None:
                    q = q.where(
                        CollabObj.time_created is not None
                        and CollabObj.time_created <= flt.time_created_end
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
                            CollabObj.time_start is not None
                            and CollabObj.time_start >= flt.time_start
                        )
                    if flt.time_end is not None:
                        # filter by collabobj time_end (pin)
                        q = q.where(
                            CollabObj.time_end is not None
                            and CollabObj.time_end <= flt.time_end
                        )

                if flt.text is not None:
                    qq = [CollabObj.text.ilike(x) for x in flt.text]
                    q = q.filter(or_(*qq))

                if flt.events is not None:
                    event_conditions = [
                        CollabObj.events.op("@>")([{"id": event_id}])
                        for event_id in flt.events
                    ]
                    q = q.filter(or_(*event_conditions))

                if flt.tags is not None:
                    if flt.opt_tags_and:
                        # all tags must match (CONTAINS operator)
                        q = q.filter(CollabObj.tags.op("@>")(flt.tags))
                    else:
                        # at least one tag must match (OVERLAP operator)
                        q = q.filter(CollabObj.tags.op("&&")(flt.tags))

                if flt.data is not None:
                    # filter is a key-value dict
                    flt_data_jsonb = func.jsonb_build_object(
                        *itertools.chain(*flt.data.items())
                    )
                    q = q.filter(CollabObj.data.op("@>")(flt_data_jsonb))

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
                collab_api.logger().warning("no CollabObj found (flt=%s)" % (flt))
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
            collab_api.logger().debug("---> get: found %d objects" % (len(objs)))
            return objs
