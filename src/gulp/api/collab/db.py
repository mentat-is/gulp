import asyncio
import base64
import os

import muty.file
import psycopg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy_utils import create_database, database_exists, drop_database

import gulp.config as config
from gulp.api.collab.base import (
    PERMISSION_DELETE,
    PERMISSION_EDIT,
    CollabBase,
    GulpClientType,
    GulpCollabFilter,
    GulpCollabType,
    GulpUserPermission,
)
from gulp.api.collab.client import Client
from gulp.api.collab.collabobj import CollabObj
from gulp.api.collab.context import Context
from gulp.api.collab.glyph import Glyph
from gulp.api.collab.operation import Operation
from gulp.api.collab.user import User
from gulp.api.elastic import query_utils
from gulp.api.elastic.structs import GulpQueryParameter, GulpQueryType
from gulp.defs import ObjectAlreadyExists, ObjectNotFound
from gulp.utils import logger


async def _setup_stats_expiration(conn: psycopg.AsyncConnection) -> None:
    # create pg_cron extension
    await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_cron;"))

    # create function to delete expired rows (runs every 5 minutes)
    await conn.execute(
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

    await conn.execute(
        text(
            """
        SELECT cron.schedule('*/5 * * * *', 'SELECT delete_expired_rows();');
    """
        )
    )


async def _engine_get_internal(
    url: str, sql_alchemy_debug: bool = False, create_tables: bool = False
) -> AsyncEngine:

    # check for ssl connection preferences
    # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    certs_dir = config.certs_directory()
    postgres_ssl = config.postgres_ssl()
    verify_certs = config.postgres_verify_certs()
    if verify_certs:
        sslmode = "verify-full"
    else:
        sslmode = "prefer"

    logger().debug("sslmode=%s" % (sslmode))
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
    engine = create_async_engine(
        url, echo=sql_alchemy_debug, pool_timeout=30, connect_args=connect_args
    )

    if create_tables:
        # create database tables
        async with engine.begin() as conn:
            try:
                await conn.run_sync(CollabBase.metadata.create_all)
            except psycopg.ProgrammingError as ex:
                if not isinstance(ex, psycopg.errors.DuplicateTable):
                    logger().exception(ex)
                    raise ex

        async with engine.begin() as conn:
            # setup stats expiration
            await _setup_stats_expiration(conn)
    logger().info("engine %s created/initialized, url=%s ..." % (engine, url))
    return engine


async def _create_default_data(engine: AsyncEngine) -> None:
    # context
    try:
        the_context = await Context.create(engine, "default", "#6fcee4")
    except ObjectAlreadyExists:
        the_context = await Context.get(engine, GulpCollabFilter(name=["default"]))
        the_context = the_context[0]

    # glyphs
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


async def engine_get(
    url: str, sql_alchemy_debug: bool = False, create_tables: bool = False
) -> AsyncEngine:
    """
    Retrieves or creates a database engine for the given URL, and ensures that the tables and default data is created.

    Args:
        url (str): The URL of the database.
        sql_alchemy_debug (bool, optional): Whether to enable SQL Alchemy debug logging. Defaults to False.
        create_tables (bool, optional): Whether to create the tables and default data. Defaults to False.
    Returns:
        AsyncEngine: The database engine.

    Raises:
        Exception: If an error occurs while retrieving or creating the engine.
    """
    logger().debug("---> engine_get: url=%s" % (url))
    engine = await _engine_get_internal(
        url, sql_alchemy_debug=sql_alchemy_debug, create_tables=create_tables
    )
    return engine


async def exists(url: str) -> bool:
    """
    Check if a database exists at the given URL.

    Args:
        url (str): The URL of the database.

    Returns:
        bool: True if the database exists, False otherwise.
    """
    logger().debug("---> exists: url=%s" % (url))
    return await asyncio.to_thread(database_exists, url=url)


async def drop(
    url: str, raise_if_not_exists: bool = False, recreate: bool = True
) -> None:
    """
    Drops a database specified by the given URL.

    Args:
        url (str): The URL of the database to drop.
        raise_if_not_exists (bool, optional): Whether to raise an exception if the database does not exist. Defaults to False.
        recreate (bool, optional): Whether to recreate the database after dropping it. Defaults to True.
    Note:
        if recreate is specified, only the database is created. to create tables and the default data, use engine_get then.
    """

    def _drop_internal(url: str, raise_if_not_exists: bool, recreate: bool):
        """
        internal function to run async
        """
        if database_exists(url):
            drop_database(url)
            logger().info("database %s dropped ..." % (url))
        else:
            if raise_if_not_exists:
                raise ObjectNotFound("database %s does not exist!" % (url))
        if recreate:
            # recreate database with default data
            create_database(url)

    logger().debug(
        "---> drop: url=%s, raise_if_not_exists=%s, recreate=%s"
        % (url, raise_if_not_exists, recreate)
    )
    await asyncio.to_thread(_drop_internal, url, raise_if_not_exists, recreate)

    if recreate:
        # recreate database
        engine = await _engine_get_internal(url, create_tables=True)
        await _create_default_data(engine)
        await engine_close(engine)


async def engine_close(engine: AsyncEngine) -> None:
    """
    Closes the connection with the given engine.

    Args:
        engine (AsyncEngine): The engine to close.

    Returns:
        None
    """
    logger().debug("---> engine_close: engine=%s" % (engine))
    if engine is not None:
        logger().info("closing connection with %s ..." % (engine))
        await engine.dispose()
