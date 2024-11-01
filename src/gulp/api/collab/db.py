import asyncio
import importlib
import pkgutil
from importlib import resources as impresources
from sqlalchemy import text
from sqlalchemy.sql import text
from sqlalchemy_utils import create_database, database_exists, drop_database
import muty.file
from gulp import config
from gulp.api.collab.structs import (
    PERMISSION_MASK_DELETE,
    PERMISSION_MASK_EDIT,
    GulpCollabBase,
    GulpUserPermission,
)
from gulp.api.collab_api import (
    engine,
    session,
    shutdown,
)
from gulp.defs import ObjectNotFound
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
            logger().info("--> drop: dropping database %s ..." % (url))
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
                DELETE FROM stats WHERE time_expire < (EXTRACT(EPOCH FROM NOW()) * 1000) AND time_expire > 0;
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
                DELETE FROM session WHERE time_expire < (EXTRACT(EPOCH FROM NOW()) * 1000) AND time_expire > 0;
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


async def _create_default_data() -> None:
    """
    Initializes the default data for the application.
    This function performs the following tasks:
    1. Dynamically imports all modules under the `gulp.api.collab` package.
    2. Imports necessary classes from the `gulp.api.collab` package.
    3. Creates database tables and functions.
    4. Reads glyph assets from the specified path.
    5. Creates an admin user with administrative permissions.
    6. Creates glyphs for user and operation.
    7. Updates the admin user with the created user glyph.
    8. Creates a default context.
    9. Creates a default operation.
    10. Creates additional users with varying permissions: guest(read), editor(edit), and power(delete).
    Raises:
        Any exceptions that occur during the execution of the function.
    """

    # import everything under gulp.api.collab
    package_name = "gulp.api.collab"
    package = importlib.import_module(package_name)
    for _, module_name, _ in pkgutil.iter_modules(package.__path__):
        importlib.import_module(f"{package_name}.{module_name}")

    from gulp.api.collab.user import GulpUser
    from gulp.api.collab.glyph import GulpGlyph
    from gulp.api.collab.operation import GulpOperation
    from gulp.api.collab.context import GulpContext

    # create database tables and functions
    e = await engine()
    async with e.begin() as conn:
        from gulp.api.collab.context import GulpContext

        await conn.run_sync(GulpCollabBase.metadata.create_all)
    await shutdown()
    await _setup_collab_expirations()

    # read glyphs
    assets_path = impresources.files("gulp.api.collab.assets")
    user_b = await muty.file.read_file_async(
        muty.file.safe_path_join(assets_path, "user.png")
    )
    operation_b = await muty.file.read_file_async(
        muty.file.safe_path_join(assets_path, "operation.png")
    )

    # create admin user, which is the root of everything else
    admin_user: GulpUser = await GulpUser.create(
        "admin",
        "admin",
        permission=[GulpUserPermission.ADMIN],
    )

    # create glyphs
    user_glyph = await GulpGlyph.create("user", admin_user.id, user_b)
    operation_glyph = await GulpGlyph.create("operation", admin_user.id, operation_b)
    await admin_user.update({"glyph": user_glyph.id})

    # create default context
    context = await GulpContext.create("test_context", admin_user.id)

    # create default operation
    operation = await GulpOperation.create(
        "test_operation", admin_user.id, index="testidx", glyph=operation_glyph.id
    )

    # create other users
    guest_user = await GulpUser.create(
        "guest",
        "guest",
        glyph=user_glyph.id,
    )
    editor_user = await GulpUser.create(
        "editor",
        "editor",
        permission=PERMISSION_MASK_EDIT,
        glyph=user_glyph.id,
    )
    power_user = await GulpUser.create(
        "power", "power", permission=PERMISSION_MASK_DELETE, glyph=user_glyph.id
    )


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
        # drop and recreate database
        await shutdown()
        await drop(url)
        await create(url)

        # recreate tables and default data
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
