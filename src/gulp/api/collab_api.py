import os

import muty.file
import psycopg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.ext.asyncio.session import AsyncSession

import gulp.config as config
from gulp.api.collab import db
from gulp.api.collab.base import CollabBase
from gulp.utils import logger

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
    from gulp.api.collab.context import GulpContext

    the_context = await GulpContext.create("default", "#6fcee4")

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
