import os
import pkgutil

import muty.file
import muty.time
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy_utils import create_database, database_exists, drop_database
import asyncio
from importlib import resources, import_module

from gulp import config
from gulp.defs import ObjectNotFound
from gulp.utils import GulpLogger

class GulpCollab:
    """
    singleton class, represents the collab database connection.

    init() must be called first to initialize the connection.

    for ssl connection, it will use the certificates in the config.path_certs() directory if they exist.

    they should be named "postgres-ca.pem", "postgres.pem", "postgres.key" for the CA, client certificate, and client key respectively.

    """
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        raise RuntimeError("call get_instance() instead")

    @classmethod
    def get_instance(cls) -> "GulpCollab":
        """
        returns the singleton instance of the collab database connection.
        """
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
            
        return cls._instance

    def _initialize(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._setup_done = False
            self._engine: AsyncEngine = None
            self._collab_sessionmaker = None

    async def init(self, force_recreate: bool=False, expire_on_commit: bool = False) -> None:
        """
        initializes the collab database connection (create the engine and configure it) in the singleton instance.

        if called on an already initialized instance, the existing engine is disposed and a new one is created.

        Args:
            expire_on_commit (bool, optional): whether to expire sessions returned by session() on commit. Defaults to False.
        """
        if self._engine is not None:
            await self._engine.dispose()

        self._engine = await self._create_engine()
        self._collab_sessionmaker = async_sessionmaker(bind=self._engine, expire_on_commit=expire_on_commit)

        if force_recreate:
            await self._ensure_setup(force_recreate=True, expire_on_commit=expire_on_commit)
        else:
            await self._ensure_setup(expire_on_commit=expire_on_commit)

    async def _create_engine(self) -> AsyncEngine:
        """
        creates the collab database engine

        Returns:
            AsyncEngine: The collab database engine.
        """
        url = config.postgres_url()

        # check for ssl connection preferences
        # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
        certs_dir = config.path_certs()
        postgres_ssl = config.postgres_ssl()
        verify_certs = config.postgres_verify_certs()
        if verify_certs:
            sslmode = "verify-full"
        else:
            sslmode = "prefer"
        GulpLogger.get_instance().debug(
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
                GulpLogger.get_instance().debug(
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
                GulpLogger.get_instance().debug("using server CA certificate only: %s" % (ca))
                connect_args = {"sslrootcert": ca, "sslmode": sslmode}
        else:
            # no SSL
            connect_args = {}

        # create engine
        _engine = create_async_engine(
            url, echo=config.debug_collab(), pool_timeout=30, connect_args=connect_args
        )

        GulpLogger.get_instance().info("engine %s created/initialized, url=%s ..." % (_engine, url))
        return _engine

    def session(self) -> AsyncSession:
        """
        Returns a session (preconfigured with expire_on_commit=False) to the collab database, per-process engine is created if needed.

        WARNING: to call this, an event loop must be running.

        Returns:
            AsyncSession: The session to the collab database
        """
        if not self._setup_done:
            raise Exception("collab not initialized, call GulpCollab().init() first!")

        return self._collab_sessionmaker()


    async def shutdown(self) -> None:
        """
        Shuts down the per-process collab database engine.

        after calling this, the engine is invalidated and all existing connections are disposed and GulpCollab().init() must be called again to reinitialize the engine in the same process.
        
        Returns:
            None
        """
        GulpLogger.get_instance().warning(
            "shutting down collab database engine and invalidate existing connections ..."
        )
        await self._engine.dispose()
        self._setup_done = False
        self._engine = None
        self._collab_sessionmaker = None


    @staticmethod
    async def exists(url: str) -> bool:
        """
        Check if a database exists at the given URL.

        Args:
            url (str): The URL of the database.

        Returns:
            bool: True if the database exists, False otherwise.
        """
        b = await asyncio.to_thread(database_exists, url=url)
        GulpLogger.get_instance().debug("---> exists: url=%s, result=%r" % (url, b))
        return b

    @staticmethod
    async def drop_db(url: str, raise_if_not_exists: bool = False) -> None:
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
                GulpLogger.get_instance().info("--> drop: dropping database %s ..." % (url))
                drop_database(url)
                GulpLogger.get_instance().info("--> drop: database %s dropped ..." % (url))
            else:
                GulpLogger.get_instance().warning("--> drop: database %s does not exist!" % (url))
                if raise_if_not_exists:
                    raise ObjectNotFound("database %s does not exist!" % (url))

        GulpLogger.get_instance().debug(
            "---> drop: url=%s, raise_if_not_exists=%r" % (url, raise_if_not_exists)
        )
        await asyncio.to_thread(_blocking_drop, url, raise_if_not_exists)


    @staticmethod
    async def create_db(url: str) -> None:
        """
        Create a database at the given URL.

        Args:
            url (str): The URL of the database to create.
        """
        GulpLogger.get_instance().debug("---> create: url=%s" % (url))
        await asyncio.to_thread(create_database, url=url)


    async def _setup_collab_expirations(self) -> None:
        # TODO: check issues with pg-cron process dying
        GulpLogger.get_instance().debug("setting up stats and tokens expiration with pg_cron ...")

        async with self.session() as sess:
            # create pg_cron extension
            await sess.execute(text("CREATE EXTENSION IF NOT EXISTS pg_cron;"))

            await sess.execute(
                text(
                    """
                CREATE OR REPLACE FUNCTION delete_expired_stats_rows() RETURNS void AS $$
                BEGIN
                    DELETE FROM stats_ingestion WHERE (EXTRACT(EPOCH FROM NOW()) * 1000) > time_expire AND time_expire > 0;
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
                    DELETE FROM session WHERE (EXTRACT(EPOCH FROM NOW()) * 1000) > time_expire AND time_expire > 0;
                END;
                $$ LANGUAGE plpgsql;
            """
                )
            )

            # purge stats and tokens every 1 minutes
            await sess.execute(
                text(
                    """
                    SELECT cron.schedule('* * * * *', 'SELECT delete_expired_stats_rows();');
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


    async def _create_default_data(self) -> None:
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
        from gulp.api.collab.structs import PERMISSION_MASK_DELETE, PERMISSION_MASK_EDIT, GulpCollabBase, GulpUserPermission

        from gulp.api.collab.user import GulpUser
        from gulp.api.collab.glyph import GulpGlyph
        from gulp.api.collab.operation import GulpOperation
        from gulp.api.collab.context import GulpContext

        # create database tables and functions
        async with self._engine.begin() as conn:
            from gulp.api.collab.context import GulpContext

            await conn.run_sync(GulpCollabBase.metadata.create_all)
        await self._setup_collab_expirations()

        # read glyphs
        assets_path = resources.files("gulp.api.collab.assets")
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

        # login admin user        
        admin_user, admin_session = await GulpUser.login("admin", "admin")
        
        # create glyphs
        user_glyph = await GulpGlyph.create("user", user_b, token=admin_session.id)
        
        operation_glyph = await GulpGlyph.create("operation", operation_b, token=admin_session.id)

        await admin_user.update({"glyph": user_glyph.id}, token=admin_session.id)

        # create default context
        context = await GulpContext.create("test_context", token=admin_session.id)

        # create default operation
        operation = await GulpOperation.create(
            "test_operation", index="testidx", glyph=operation_glyph.id, token=admin_session.id
        )

        # create other users
        guest_user = await GulpUser.create(
            "guest",
            "guest",
            glyph=user_glyph.id, token=admin_session.id
        )
        editor_user = await GulpUser.create(
            "editor",
            "editor",
            permission=PERMISSION_MASK_EDIT,
            glyph=user_glyph.id,
            token=admin_session.id
        )
        power_user = await GulpUser.create(
            "power", "power", permission=PERMISSION_MASK_DELETE, glyph=user_glyph.id, token=admin_session.id
        )


    async def _ensure_setup(self, force_recreate: bool = False, expire_on_commit: bool=False) -> None:
        """
        ensure the collab database is set up and ready to use.

        Args:
            force_recreate (bool, optional): Whether to drop and recreate the database. Defaults to False.
            expire_on_commit (bool, optional): Whether to expire sessions returned by session() on commit. Defaults to False, ignored if force_recreate is not set
                and the database already exist
        Returns:
            None
        Raises:
            Exception: If an error occurs while setting up the database.
        """

        async def _recreate_internal(url: str, expire_on_commit: bool = False) -> None:
            # drop and recreate database
            await self.shutdown()
            await GulpCollab.drop_db(url)
            await GulpCollab.create_db(url)

            # recreate tables and default data
            self._engine = await self._create_engine()
            self._collab_sessionmaker = async_sessionmaker(bind=self._engine, expire_on_commit=expire_on_commit)
            self._setup_done = True
            await self._create_default_data()

        # import everything under gulp.api.collab
        # NOTE: i am not quite sure why this is needed, seems like sqlalchemy needs all the classes to be loaded before accessing the tables.
        package_name = "gulp.api.collab"
        package = import_module(package_name)
        for _, module_name, _ in pkgutil.iter_modules(package.__path__):
            import_module(f"{package_name}.{module_name}")

        url = config.postgres_url()
        if force_recreate:
            GulpLogger.get_instance().warning(
                "force_recreate=True, dropping and recreating collab database ..."
            )
            await _recreate_internal(url, expire_on_commit=expire_on_commit)
        else:
            if await GulpCollab.exists(config.postgres_url()):
                # check if tables exist
                async with self._collab_sessionmaker() as sess:
                    res = await sess.execute(
                        text("SELECT to_regclass('public.context') AS exists")
                    )
                    if res.scalar_one_or_none():
                        # tables ok
                        GulpLogger.get_instance().info("collab database exists and tables are ok.")
                        self._setup_done = True
                        return

                # recreate tables
                GulpLogger.get_instance().warning("collab database exists but tables are missing.")
                await _recreate_internal(url, expire_on_commit=expire_on_commit)
