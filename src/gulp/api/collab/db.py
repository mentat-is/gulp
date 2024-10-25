import asyncio
from importlib import resources as impresources

from sqlalchemy import text
from sqlalchemy.sql import text
from sqlalchemy_utils import create_database, database_exists, drop_database
import muty.file
from gulp import config
from gulp.api.collab.structs import (
    PERMISSION_DELETE,
    PERMISSION_EDIT,
    GulpCollabBase,
    GulpCollabFilter,
    GulpCollabType,
    GulpUserPermission,
)
from gulp.api.collab_api import (
    engine,
    session,
    shutdown,
)
from gulp.defs import ObjectAlreadyExists, ObjectNotFound
from gulp.utils import logger


async def exists(url: str) -> bool:
    """
    Check if a database exists at the given URL.

    Args:
        url (str): The URL of the database.

    Returns:
        bool: True if the database exists, False otherwise.
    """
    b = await asyncio.to_thread(database_exists, url=url)
    logger().debug("---> exists: url=%s, result=%r" % (url, b))
    return b


async def drop(url: str, raise_if_not_exists: bool = False) -> None:
    """
    Drops a database specified by the given URL.

    Args:
        url (str): The URL of the database to drop.
        raise_if_not_exists (bool, optional): Whether to raise an exception if the database does not exist. Defaults to False.
        recreate (bool, optional): Whether to recreate the database (including the default data) after dropping it. Defaults to True.
    Note:
        if recreate is specified, only the database is created. to create tables and the default data, use engine_get then.
    """

    def _blocking_drop(url: str, raise_if_not_exists: bool = False):
        """
        internal function to drop, and possibly recreate, the database: this is blocking, so this is wrapped in a thread.
        """
        if database_exists(url):
            drop_database(url)
            logger().info("--> drop: database %s dropped ..." % (url))
        else:
            logger().warning("--> drop: database %s does not exist!" % (url))
            if raise_if_not_exists:
                raise ObjectNotFound("database %s does not exist!" % (url))

    logger().debug(
        "---> drop: url=%s, raise_if_not_exists=%r" % (url, raise_if_not_exists)
    )
    await asyncio.to_thread(_blocking_drop, url, raise_if_not_exists)


async def create(url: str) -> None:
    """
    Create a database at the given URL.

    Args:
        url (str): The URL of the database to create.
    """
    logger().debug("---> create: url=%s" % (url))
    await asyncio.to_thread(create_database, url=url)


async def _create_default_data() -> None:
    # context
    from gulp.api.collab.context import GulpContext
    from gulp.api.collab.glyph import Glyph
    from gulp.api.collab.operation import Operation
    from gulp.api.collab.user import User

    assets_path = impresources.files("gulp.api.collab.assets")

    # create default context
    context = GulpContext("default", "#6fcee4")
    await context.store()
    return

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


async def _setup_collab_expirations() -> None:
    logger().debug("setting up stats and tokens expiration with pg_cron ...")

    async with await session() as sess:
        # create pg_cron extension
        await sess.execute(text("CREATE EXTENSION IF NOT EXISTS pg_cron;"))

        # create function to delete expired rows (runs every 5 minutes)
        await sess.execute(
            text(
                """
            CREATE OR REPLACE FUNCTION delete_expired_stats_rows() RETURNS void AS $$
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
            CREATE OR REPLACE FUNCTION delete_expired_tokens_rows() RETURNS void AS $$
            BEGIN
                DELETE FROM session WHERE time_expire < (EXTRACT(EPOCH FROM NOW()) * 1000);
            END;
            $$ LANGUAGE plpgsql;
        """
            )
        )

        await sess.execute(
            text(
                """
                SELECT cron.schedule('*/5 * * * *', 'SELECT delete_expired_stats_rows();');
                """
            )
        )

        await sess.execute(
            text(
                """
                SELECT cron.schedule('* * * * *', 'SELECT delete_expired_tokens_rows();');
                """
            )
        )
        await sess.commit()


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

    async def _recreate_internal(url: str):
        await shutdown()
        await drop(url)
        await create(url)

        # recreate tables and default data
        e = await engine()
        async with e.begin() as conn:
            from gulp.api.collab.context import GulpContext

            await conn.run_sync(GulpCollabBase.metadata.create_all)
        await shutdown()
        await _setup_collab_expirations()
        await _create_default_data()

    url = config.postgres_url()
    if force_recreate:
        logger().warning(
            "force_recreate=True, dropping and recreating collab database ..."
        )
        await _recreate_internal(url)
    else:
        if await exists(config.postgres_url()):
            # check if tables exist
            async with await session() as sess:
                res = await sess.execute(
                    text("SELECT to_regclass('public.context') AS exists")
                )
                if res.scalar_one_or_none() is not None:
                    # tables ok
                    logger().info("collab database exists and tables are ok.")
                    return

            # recreate tables
            logger().warning("collab database exists but tables are missing.")
            await _recreate_internal(url)
