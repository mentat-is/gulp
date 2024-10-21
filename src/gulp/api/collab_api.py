import json
import os
from enum import StrEnum
from importlib import resources as impresources
from typing import Optional, TypeVar

import muty.file
import muty.time
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import Result, select, text
from sqlalchemy.ext.asyncio import (
    AsyncAttrs,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, MappedAsDataclass
from sqlalchemy_mixins.serialize import SerializeMixin

from gulp import config
from gulp.api.collab import db
from gulp.defs import ObjectAlreadyExists, ObjectNotFound
from gulp.utils import logger

COLLAB_MAX_NAME_LENGTH = 128

class SessionExpired(Exception):
    """if the user session has expired"""


class WrongUsernameOrPassword(Exception):
    """if the user provides wrong username or password"""


class MissingPermission(Exception):
    """if the user does not have the required permission"""

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

class GulpCollabLevel(StrEnum):
    """Defines the level of collaboration object, mainly used for notes."""

    DEFAULT = "default"
    WARNING = "warning"
    ERROR = "error"


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


T = TypeVar("T", bound="CollabBase")


class CollabBase(MappedAsDataclass, AsyncAttrs, DeclarativeBase, SerializeMixin):
    """
    base class for all objects stored in the collaboration database
    """

    def __init__(self, name: str, t: GulpCollabType, *args, **kwargs):
        self.name = name
        self.type = t
        self.time_created = muty.time.now_msec()
        self.time_updated = self.time_created
        self.time_finished = None
        for k, v in kwargs.items():
            setattr(self, k, v)

        super().__init__(*args, **kwargs)

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
    async def delete(name: str, t: T, throw_if_not_exists: bool = True) -> None:
        """
        deletes a collaboration object

        Args:
            name (str): The name of the object.
            t (T): The class of the object, derived from CollabBase.
            throw_if_not_exists (bool, optional): If True, throws an exception if the object does not exist. Defaults to True.
        """
        logger().debug("---> delete: name=%s, type=%s" % (name, t))
        async with await session() as sess:
            q = select(T).where(T.name == name).with_for_update()
            res = await sess.execute(q)
            c = CollabBase.get_one_result_or_throw(
                res, name=name, t=t, throw_if_not_exists=throw_if_not_exists
            )
            if c is not None:
                sess.delete(c)
                await sess.commit()
                logger().info("---> deleted: %s" % (c))

    @staticmethod
    async def update(name: str, t: T, d: dict, done: bool=False) -> T:
        """
        updates a collaboration object

        Args:
            name (str): The name of the object.
            t (T): The type of the object.
            d (dict): The data to update.
            done (bool, optional): If True, sets the object as done. Defaults to False.

        Returns:
            T: The updated object.
        """
        logger().debug("---> update: name=%s, type=%s, d=%s" % (name, t, d))
        async with await session() as sess:
            q = select(T).where(T.name == name).with_for_update()
            res = await sess.execute(q)
            c = CollabBase.get_one_result_or_throw(res, name=name, t=t)

            # update
            for k, v in d.items():
                setattr(c, k, v)
            if done:
                c.time_finished = muty.time.now_msec()
                c.time_updated = c.time_finished

            else:
                c.time_updated = muty.time.now_msec()

            await sess.commit()
            logger().debug("---> updated: %s" % (c))
            return c

    @staticmethod
    async def get_one_result_or_throw(
        res: Result,
        name: str = None,
        t: GulpCollabType = None,
        throw_if_not_exists: bool = True,
    ) -> T:
        """
        gets one result or throws an exception

        Args:
            res (Result): The result.
            name (str, optional): The name of the object, just for the debug print. Defaults to None.
            t (GulpCollabType, optional): The type of the object, just for the debug print. Defaults to None.
            throw_if_not_exists (bool, optional): If True, throws an exception if the object does not exist. Defaults to True.
        """
        c = res.scalar_one_or_none()
        if c is None:
            msg = "collabobject type=%s, name=%s not found!" % (t, name)
            if throw_if_not_exists:
                raise ObjectNotFound(msg)
            else:
                logger().warning(msg)
        return c


class GulpCollabFilter(BaseModel):
    """
    defines filter to be applied to all collaboration objects

    each filter specified as list, i.e. *operation_id*, matches as OR (one or more may match).<br>
    combining multiple filters (i.e. id + operation + ...) matches as AND (all must match).<br><br>

    for string fields, where not specified otherwise, the match is case-insensitive and supports SQL-like wildcards (% and _)<br>

    an empty filter returns all objects.<br>

    **NOTE**: not all fields are applicable to all types.
    """

    name: list[str] = Field(None, description="filter by the given name/s.")
    level: list[GulpCollabLevel] = Field(
        None, description="filter by the given level/s."
    )
    client: list[str] = Field(None, description="filter by the given client/s.")
    context: list[str] = Field(None, description="filter by the given context/s.")
    operation: list[str] = Field(None, description="filter by the given operation/s.")

    text: list[str] = Field(
        None, unique=True, description="filter by the given object text."
    )
    operation_id: list[int] = Field(
        None, unique=True, description="filter by the given operation/s."
    )
    context: list[str] = Field(
        None, unique=True, description="filter by the given context/s."
    )
    src_file: list[str] = Field(
        None, unique=True, description="filter by the given source file/s."
    )
    type: list[GulpCollabType] = Field(
        None, unique=True, description="filter by the given type/s."
    )
    status: list[GulpRequestStatus] = Field(
        None, unique=True, description="filter by the given status/es."
    )
    time_created_start: int = Field(
        None,
        description="creation timestamp range start (matches only AFTER a certain timestamp, inclusive, milliseconds from unix epoch).",
    )
    time_created_end: int = Field(
        None,
        description="creation timestamp range end  (matches only BEFORE a certain timestamp, inclusive, milliseconds from unix epoch).",
    )
    time_start: int = Field(
        None,
        description="timestamp range start (matches only AFTER a certain timestamp, inclusive, milliseconds from unix epoch).",
    )
    time_end: int = Field(
        None,
        description="timestamp range end  (matches only BEFORE a certain timestamp, inclusive, milliseconds from unix epoch).",
    )
    events: list[str] = Field(
        None,
        unique=True,
        description="filter by the given event ID/s in CollabObj.events list of CollabEvents.",
    )
    data: dict = Field(
        None,
        unique=True,
        description='filter by the given { "key": "value" } in data (exact match).',
    )
    tags: list[str] = Field(None, unique=True, description="filter by the given tag/s.")
    limit: int = Field(
        None,
        description='to be used together with "offset", maximum number of results to return. default=return all.',
    )
    offset: int = Field(
        None,
        description='to be used together with "limit", number of results to skip from the beginning. default=0 (from start).',
    )
    private_only: bool = Field(
        False,
        description="if True, return only private objects. Default=False (return all).",
    )
    opt_time_start_end_events: bool = Field(
        False,
        description="if True, time_start and time_end are relative to the CollabObj.events list of CollabEvents. Either, they refer to CollabObj.time_start and CollabObj.time_end as usual.",
    )
    opt_basic_fields_only: bool = Field(
        False,
        description="return only the basic fields (id, name, ...) for the collaboration object. Default=False (return all).",
    )
    opt_tags_and: bool = Field(
        False,
        description="if True, all tags must match. Default=False (at least one tag must match).",
    )

    @model_validator(mode="before")
    @classmethod
    def to_py_dict(cls, data: str | dict):
        if data is None or len(data) == 0:
            return {}

        if isinstance(data, dict):
            return data
        return json.loads(data)


class GulpAssociatedEvent(BaseModel):
    """
    Represents an event in the collab API needing an "events" field.

    when used for filtering with a GulpCollabFilter, only "id" and "@timestamp" (if GulpCollabFilter.opt_time_start_end_events) are taken into account.
    """

    id: str = Field(None, description="the event ID")
    timestamp: int = Field(None, description="the event timestamp")
    operation_id: Optional[int] = Field(
        None, description="operation ID the event is associated with."
    )
    context: Optional[str] = Field(
        None, description="context the event is associated with."
    )
    src_file: Optional[str] = Field(
        None, description="source file the event is associated with."
    )

    def to_dict(self) -> dict:
        return {
            "_id": self.id,
            "@timestamp": self.timestamp,
            "gulp.operation.id": self.operation_id,
            "gulp.context": self.context,
            "gulp.source.file": self.src_file,
        }

    @staticmethod
    def from_dict(d: dict) -> "GulpAssociatedEvent":
        return GulpAssociatedEvent(
            id=d["id"],
            timestamp=d["@timestamp"],
            operation_id=d.get("gulp.operation.id"),
            context=d.get("gulp.context"),
            src_file=d.get("gulp.source.file"),
        )

    @model_validator(mode="before")
    @classmethod
    def to_py_dict(cls, data: str | dict):
        # print('*********** events: %s, %s' % (data, type(data)))
        if data is None or len(data) == 0:
            return {}

        if isinstance(data, dict):
            # print('****** events (after): %s, %s' % (data, type(data)))
            return data
        return json.loads(data)


_engine: AsyncEngine = None
_collab_sessionmaker = None

async def _setup_stats_expiration() -> None:
    logger().debug("setting up stats expiration with pg_cron ...")
    async with await session() as sess:
        # create pg_cron extension
        await sess.execute(text("CREATE EXTENSION IF NOT EXISTS pg_cron;"))

        # create function to delete expired rows (runs every 5 minutes)
        await sess.execute(
            text(
                """
            CREATE OR REPLACE FUNCTION delete_expired_rows() RETURNS void AS $$
            BEGIN
                DELETE FROM stats WHERE time_expire < (EXTRACT(EPOCH FROM NOW()) * 1000);
            END;
            $$ LANGUAGE plpgsql;
        """
            )
        )

        await sess.execute(
            text(
                """
            SELECT cron.schedule('*/5 * * * *', 'SELECT delete_expired_rows();');
        """
            )
        )
        await sess.commit()


async def _create_default_data() -> None:
    # context
    from gulp.api.collab.client import Client
    from gulp.api.collab.context import GulpContext
    from gulp.api.collab.glyph import Glyph
    from gulp.api.collab.operation import Operation
    from gulp.api.collab.user import User

    assets_path = impresources.files("gulp.api.collab.assets")

    # create default context
    context = await GulpContext.create("default", "#6fcee4")

    # glyphs
    user_b = await muty.file.read_file_async(
        muty.file.safe_path_join(assets_path, "user.png")
    )
    client_b = await muty.file.read_file_async(
        muty.file.safe_path_join(assets_path, "client.png")
    )
    operation_b = await muty.file.read_file_async(
        muty.file.safe_path_join(assets_path, "operation.png")
    )
    user_glyph = await Glyph.create(user_b, "user")

    d = os.path.dirname(__file__)
    with open(
        muty.file.safe_path_join(d, "assets/user.png", allow_relative=True), "rb"
    ) as f:
        img = f.read()
        try:
            the_user = await Glyph.create(engine, img, "user")
        except ObjectAlreadyExists:
            # already exists
            the_user = await Glyph.get(engine, GulpCollabFilter(name=["user"]))
            the_user = the_user[0]
    with open(
        muty.file.safe_path_join(d, "assets/client.png", allow_relative=True), "rb"
    ) as f:
        img = f.read()
        try:
            the_client = await Glyph.create(engine, img, "client")
        except ObjectAlreadyExists:
            the_client = await Glyph.get(engine, GulpCollabFilter(name=["client"]))
            the_client = the_client[0]
    with open(
        muty.file.safe_path_join(d, "assets/operation.png", allow_relative=True), "rb"
    ) as f:
        img = f.read()
        try:
            the_op = await Glyph.create(engine, img, "operation")
        except ObjectAlreadyExists:
            the_op = await Glyph.get(engine, GulpCollabFilter(name=["operation"]))
            the_op = the_op[0]
    # users
    try:
        admin_user = await User.create(
            engine,
            "admin",
            "admin",
            permission=GulpUserPermission.ADMIN,
            glyph_id=the_user.id,
        )
    except ObjectAlreadyExists:
        admin_user = await User.get(engine, GulpCollabFilter(name=["admin"]))
        admin_user = admin_user[0]

    try:
        await User.create(
            engine,
            "guest",
            "guest",
            permission=GulpUserPermission.READ,
            glyph_id=the_user.id,
        )
    except ObjectAlreadyExists:
        pass

    if __debug__:
        # also create these additional data on debug builds
        try:
            await User.create(
                engine,
                "ingest",
                "ingest",
                permission=GulpUserPermission.INGEST,
                glyph_id=the_user.id,
            )
        except ObjectAlreadyExists:
            pass

        try:
            await User.create(
                engine,
                "test1",
                "test",
                permission=PERMISSION_EDIT,
                glyph_id=the_user.id,
            )
        except ObjectAlreadyExists:
            pass

        try:
            await User.create(
                engine,
                "test2",
                "test",
                permission=PERMISSION_DELETE,
                glyph_id=the_user.id,
            )
        except ObjectAlreadyExists:
            pass

        # operation
        try:
            testop = await Operation.create(
                engine, "testoperation", "testidx", "testdesc", the_op.id
            )
        except ObjectAlreadyExists:
            testop = await Operation.get(
                engine, GulpCollabFilter(name=["testoperation"])
            )
            testop = testop[0]

        # client
        try:
            testclient = await Client.create(
                engine,
                "testclient",
                GulpClientType.SLURP,
                version="1.0.0",
                glyph_id=the_client.id,
            )
        except ObjectAlreadyExists:
            testclient = await Client.get(engine, GulpCollabFilter(name=["testclient"]))
            testclient = testclient[0]

        # stored query
        try:
            sigma_matchcontext = "dGl0bGU6IFRlc3RTaWdtYQppZDogMmRjY2E3YjQtNGIzYS00ZGI2LTkzNjQtYTAxOWQ1NDkwNGJmCnN0YXR1czogdGVzdApkZXNjcmlwdGlvbjogVGhpcyBpcyBhIHRlc3QKcmVmZXJlbmNlczoKICAtIHJlZjEKICAtIHJlZjIKdGFnczoKICAtIGF0dGFjay5leGVjdXRpb24KICAtIGF0dGFjay50MTA1OQphdXRob3I6IEd1bHAgVGVhbQpkYXRlOiAyMDIwLTA3LTEyCmxvZ3NvdXJjZToKICBjYXRlZ29yeTogcHJvY2Vzc19jcmVhdGlvbgogIHByb2R1Y3Q6IHdpbmRvd3MKZGV0ZWN0aW9uOgogIHNlbGVjdGlvbjoKICAgIEV2ZW50SUQ6IDQ3MzIKICAgIFNvdXJjZUhvc3RuYW1lfGVuZHN3aXRoOiBjb250ZXh0CiAgY29uZGl0aW9uOiBzZWxlY3Rpb24KZmllbGRzOgogIC0gRXZlbnRJZAogIC0gU291cmNlSG9zdG5hbWUKZmFsc2Vwb3NpdGl2ZXM6CiAgLSBFdmVyeXRoaW5nCmxldmVsOiBtZWRpdW0KCg=="
            gqp = GulpQueryParameter(
                type=GulpQueryType.SIGMA_YAML,
                rule=base64.b64decode(sigma_matchcontext).decode(),
                pysigma_plugin="windows",
            )
            r = await query_utils.gulpqueryparam_to_gulpquery(engine, gqp)
            await CollabObj.create(
                engine,
                None,
                None,
                GulpCollabType.STORED_QUERY,
                None,
                name="test_stored_query",
                data=r.to_dict(),
                internal_user_id=admin_user.id,
                skip_ws=True,
            )
        except ObjectAlreadyExists:
            pass

        # shared data (sigma group filter)
        try:
            expr = "((66d31e5f-52d6-40a4-9615-002d3789a119 AND 0d7a9363-af70-4e7b-a3b7-1a176b7fbe84) AND (0d7a9363-af70-4e7b-a3b7-1a176b7fbe84 AFTER 66d31e5f-52d6-40a4-9615-002d3789a119)) OR 0f06a3a5-6a09-413f-8743-e6cf35561297"
            await CollabObj.create(
                engine,
                None,
                None,
                GulpCollabType.SHARED_DATA_GENERIC,
                None,
                name="test APT",
                data={"type": "sigma_group_filter", "name": "test APT", "expr": expr},
                internal_user_id=admin_user.id,
                skip_ws=True,
            )
        except ObjectAlreadyExists:
            pass


async def engine() -> AsyncEngine:
    """
    Returns the per-process collab database engine.

    Returns:
        AsyncEngine: The collab database engine.
    """
    global _engine
    if _engine is not None:
        # just return the existing engine
        return _engine

    url = config.postgres_url()

    # check for ssl connection preferences
    # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    certs_dir = config.certs_directory()
    postgres_ssl = config.postgres_ssl()
    verify_certs = config.postgres_verify_certs()
    if verify_certs:
        sslmode = "verify-full"
    else:
        sslmode = "prefer"
    logger().debug(
        "---> collab: creating AsyncEngine connection, sslmode=%s..." % (sslmode)
    )

    if certs_dir is not None and postgres_ssl:
        # https and certs_dir is set
        ca: str = muty.file.abspath(
            muty.file.safe_path_join(certs_dir, "postgres-ca.pem")
        )

        # check if client certificate exists. if so, it will be used
        client_cert = muty.file.safe_path_join(certs_dir, "postgres.pem")
        client_key = muty.file.safe_path_join(certs_dir, "postgres.key")
        client_key_password = config.postgres_client_cert_password()
        if os.path.exists(client_cert) and os.path.exists(client_key):
            logger().debug(
                "using client certificate: %s, key=%s, ca=%s"
                % (client_cert, client_key, ca)
            )
            connect_args = {
                "sslrootcert": ca,
                "sslcert": client_cert,
                "sslkey": client_key,
                "sslmode": sslmode,
                "sslpassword": client_key_password,
            }
        else:
            # no client certificate
            logger().debug("using server CA certificate only: %s" % (ca))
            connect_args = {"sslrootcert": ca, "sslmode": sslmode}
    else:
        # no SSL
        connect_args = {}

    # create engine
    e = create_async_engine(
        url, echo=config.debug_collab(), pool_timeout=30, connect_args=connect_args
    )
    logger().info("engine %s created/initialized, url=%s ..." % (e, url))
    return e


async def session() -> AsyncSession:
    """
    Returns a session (preconfigured with expire_on_commit=False) to the collab database, per-process engine is created if needed.

    WARNING: to call this, an event loop must be running.

    Returns:
        AsyncSession: The session to the collab database
    """

    global _collab_sessionmaker, _engine
    if _collab_sessionmaker is not None and _engine is not None:
        # just create a new session
        return _collab_sessionmaker()

    _engine = await engine()
    _collab_sessionmaker = async_sessionmaker(bind=_engine, expire_on_commit=False)

    # return session
    return _collab_sessionmaker()


async def shutdown() -> None:
    """
    Shuts down the per-process collab database engine.

    Returns:
        None
    """
    global _engine, _collab_sessionmaker
    if _engine is not None:
        logger().warning(
            "shutting down collab database engine and invalidate existing connections ..."
        )
        await _engine.dispose()
        _engine = None
        _collab_sessionmaker = None


async def setup(force_recreate: bool = False) -> None:
    """
    Sets up the collab database, creating tables and default data if needed.

    Args:
        force_recreate (bool, optional): Whether to drop and recreate the database. Defaults to False.
    Returns:
        None
    Raises:
        Exception: If an error occurs while setting up the database.
    """
    try:
        url = config.postgres_url()
        if not await db.exists(url) or force_recreate:
            logger().warning("creating collab database ...")
            # shutdown existing connections and recreate the database
            await shutdown()
            await db.drop(url)
            await db.create(url)

            # recreate tables and default data
            await session()
            async with _engine.begin() as conn:
                from gulp.api.collab.context import GulpContext

                await conn.run_sync(CollabBase.metadata.create_all)
            await shutdown()
            await _setup_stats_expiration()
            await _create_default_data()
    except Exception as ex:
        logger().error("error setting up collab database: %s" % (ex))
        raise ex
