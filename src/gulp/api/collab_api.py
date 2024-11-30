import asyncio
import json
import os
import pkgutil
from importlib import import_module, resources

import muty.file
import muty.time
from muty.log import MutyLogger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy_utils import create_database, database_exists, drop_database

from gulp.api.collab.structs import PERMISSION_MASK_INGEST, GulpCollabBase
from gulp.config import GulpConfig
from gulp.structs import ObjectNotFound


class GulpCollab:
    """
    singleton class, represents the collab database connection.

    init() must be called first to initialize the connection.

    for ssl connection, it will use the certificates in the GulpConfig.get_instance().path_certs() directory if they exist.

    they should be named "postgres-ca.pem", "postgres.pem", "postgres.key" for the CA, client certificate, and client key respectively.

    """

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

    async def init(
        self, force_recreate: bool = False, expire_on_commit: bool = False
    ) -> None:
        """
        initializes the collab database connection (create the engine and configure it) in the singleton instance.

        if called on an already initialized instance, the existing engine is disposed and a new one is created.

        Args:
            expire_on_commit (bool, optional): whether to expire sessions returned by session() on commit. Defaults to False.
        """
        if self._engine is not None:
            await self._engine.dispose()

        self._engine = await self._create_engine()
        self._collab_sessionmaker = async_sessionmaker(
            bind=self._engine, expire_on_commit=expire_on_commit
        )

        if force_recreate:
            await self._ensure_setup(
                force_recreate=True, expire_on_commit=expire_on_commit
            )
        else:
            await self._ensure_setup(expire_on_commit=expire_on_commit)

    async def _create_engine(self) -> AsyncEngine:
        """
        creates the collab database engine

        Returns:
            AsyncEngine: The collab database engine.
        """
        url = GulpConfig.get_instance().postgres_url()

        # check for ssl connection preferences
        # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS
        certs_dir = GulpConfig.get_instance().path_certs()
        postgres_ssl = GulpConfig.get_instance().postgres_ssl()
        verify_certs = GulpConfig.get_instance().postgres_verify_certs()
        if verify_certs:
            sslmode = "verify-full"
        else:
            sslmode = "prefer"
        # MutyLogger.get_instance().debug("---> collab: creating AsyncEngine connection, sslmode=%s..." % (sslmode))

        if certs_dir is not None and postgres_ssl:
            # https and certs_dir is set
            ca: str = muty.file.abspath(
                muty.file.safe_path_join(certs_dir, "postgres-ca.pem")
            )

            # check if client certificate exists. if so, it will be used
            client_cert = muty.file.safe_path_join(certs_dir, "postgres.pem")
            client_key = muty.file.safe_path_join(certs_dir, "postgres.key")
            client_key_password = (
                GulpConfig.get_instance().postgres_client_cert_password()
            )
            if os.path.exists(client_cert) and os.path.exists(client_key):
                MutyLogger.get_instance().debug(
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
                MutyLogger.get_instance().debug(
                    "using server CA certificate only: %s" % (ca)
                )
                connect_args = {"sslrootcert": ca, "sslmode": sslmode}
        else:
            # no SSL
            connect_args = {}

        # create engine
        _engine = create_async_engine(
            url,
            echo=GulpConfig.get_instance().debug_collab(),
            pool_timeout=30,
            connect_args=connect_args,
        )

        MutyLogger.get_instance().info(
            "engine %s created/initialized, url=%s ..." % (_engine, url)
        )
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
        MutyLogger.get_instance().warning(
            "shutting down collab database engine and invalidate existing connections ..."
        )
        await self._engine.dispose()
        self._setup_done = False
        self._engine = None
        self._collab_sessionmaker = None

    @staticmethod
    async def db_exists(url: str) -> bool:
        """
        Check if a database exists at the given URL.

        Args:
            url (str): The URL of the database.

        Returns:
            bool: True if the database exists, False otherwise.
        """
        b = await asyncio.to_thread(database_exists, url=url)
        MutyLogger.get_instance().debug("---> exists: url=%s, result=%r" % (url, b))
        return b

    @staticmethod
    async def db_drop(url: str, raise_if_not_exists: bool = False) -> None:
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
                MutyLogger.get_instance().info(
                    "--> drop: dropping database %s ..." % (url)
                )
                drop_database(url)
                MutyLogger.get_instance().info(
                    "--> drop: database %s dropped ..." % (url)
                )
            else:
                MutyLogger.get_instance().warning(
                    "--> drop: database %s does not exist!" % (url)
                )
                if raise_if_not_exists:
                    raise ObjectNotFound("database %s does not exist!" % (url))

        MutyLogger.get_instance().debug(
            "---> drop: url=%s, raise_if_not_exists=%r" % (url, raise_if_not_exists)
        )
        await asyncio.to_thread(_blocking_drop, url, raise_if_not_exists)

    @staticmethod
    async def db_create(url: str) -> None:
        """
        Create a database at the given URL.

        Args:
            url (str): The URL of the database to create.
        """
        MutyLogger.get_instance().debug("---> create: url=%s" % (url))
        await asyncio.to_thread(create_database, url=url)

    async def _setup_collab_expirations(self) -> None:
        # TODO: check issues with pg-cron process dying
        MutyLogger.get_instance().debug(
            "setting up stats and tokens expiration with pg_cron ..."
        )

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
        from gulp.api.collab.context import GulpContext
        from gulp.api.collab.glyph import GulpGlyph
        from gulp.api.collab.operation import GulpOperation
        from gulp.api.collab.source import GulpSource
        from gulp.api.collab.structs import (
            PERMISSION_MASK_DELETE,
            PERMISSION_MASK_EDIT,
            GulpCollabBase,
            GulpUserPermission,
        )
        from gulp.api.collab.user import GulpUser
        from gulp.api.collab.user_group import GulpUserGroup

        # create database tables and functions
        async with self._engine.begin() as conn:
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

        async with self._collab_sessionmaker() as sess:
            # create admin user, which is the root of everything else
            admin_user: GulpUser = await GulpUser.create(
                sess,
                "admin",
                "admin",
                permission=[GulpUserPermission.ADMIN],
            )

            # login admin user
            admin_session = await GulpUser.login(sess, "admin", "admin", None, None)

            # create glyphs
            user_glyph = await GulpGlyph.create(
                sess,
                user_id=admin_user.id,
                img=user_b,
                name="user",
            )

            operation_glyph = await GulpGlyph.create(
                sess,
                user_id=admin_user.id,
                img=operation_b,
                name="operation",
            )

            await admin_user.update(
                sess,
                d={"glyph_id": user_glyph.id},
                user_session=admin_session,
            )

            # create user groups
            group: GulpUserGroup = await GulpUserGroup.create(
                sess,
                owner_id=admin_user.id,
                name="test_group",
                permission=[GulpUserPermission.ADMIN],
            )
            # add admin to group
            await group.add_user(sess, admin_user.id)

            # create default operation
            operation: GulpOperation = await GulpOperation.create(
                sess,
                user_id=admin_user.id,
                name="test operation",
                index="test_idx",
                glyph_id=operation_glyph.id,
            )

            # add sources to context and context to operation
            ctx = await operation.add_context(
                sess, user_id=admin_user.id, context_id="test_context"
            )
            await ctx.add_source(sess, admin_user.id, "test source 1")
            await ctx.add_source(sess, admin_user.id, "test source 2")

            operations: list[GulpOperation] = await GulpOperation.get_by_filter(sess)
            for op in operations:
                MutyLogger.get_instance().debug(
                    json.dumps(op.to_dict(nested=True), indent=4)
                )

            groups: list[GulpUserGroup] = await GulpUserGroup.get_by_filter(sess)
            for group in groups:
                MutyLogger.get_instance().debug(
                    json.dumps(group.to_dict(nested=True), indent=4)
                )

            MutyLogger.get_instance().debug(
                json.dumps(admin_user.to_dict(nested=True), indent=4)
            )

            # create other users
            guest_user = await GulpUser.create(
                sess,
                user_id="guest",
                password="guest",
                glyph_id=user_glyph.id,
            )
            editor_user = await GulpUser.create(
                sess,
                user_id="editor",
                password="editor",
                permission=PERMISSION_MASK_EDIT,
                glyph_id=user_glyph.id,
            )
            ingest_user = await GulpUser.create(
                sess,
                user_id="ingest",
                password="ingest",
                permission=PERMISSION_MASK_INGEST,
                glyph_id=user_glyph.id,
            )
            power_user = await GulpUser.create(
                sess,
                user_id="power",
                password="power",
                permission=PERMISSION_MASK_DELETE,
                glyph_id=user_glyph.id,
            )

    async def _check_all_tables_exist(self, sess: AsyncSession) -> bool:
        """
        check if all tables exist in the database.

        Args:
            sess (AsyncSession): The database session to use.
        Returns:
            bool: True if all tables exist, False otherwise.
        """

        # get all table names from metadata
        table_names = GulpCollabBase.metadata.tables.keys()

        # build query to check all tables
        tables_check = []
        for table in table_names:
            tables_check.append(f"to_regclass('public.{table}') AS {table}")

        query = text(f"SELECT {', '.join(tables_check)}")

        # execute query
        result = await sess.execute(query)
        row = result.one()

        # check if any table is missing
        return all(row)

    async def _ensure_setup(
        self, force_recreate: bool = False, expire_on_commit: bool = False
    ) -> None:
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
            await GulpCollab.db_drop(url)
            await GulpCollab.db_create(url)

            # recreate tables and default data
            self._engine = await self._create_engine()
            self._collab_sessionmaker = async_sessionmaker(
                bind=self._engine, expire_on_commit=expire_on_commit
            )
            self._setup_done = True
            await self._create_default_data()

        # import everything under gulp.api.collab
        # NOTE: i am not quite sure why this is needed, seems like sqlalchemy needs all the classes to be loaded before accessing the tables.
        package_name = "gulp.api.collab"
        package = import_module(package_name)
        for _, module_name, _ in pkgutil.iter_modules(package.__path__):
            import_module(f"{package_name}.{module_name}")

        url = GulpConfig.get_instance().postgres_url()
        if force_recreate:
            MutyLogger.get_instance().warning(
                "force_recreate=True, dropping and recreating collab database ..."
            )
            await _recreate_internal(url, expire_on_commit=expire_on_commit)
        else:
            if await GulpCollab.db_exists(GulpConfig.get_instance().postgres_url()):
                async with self._collab_sessionmaker() as sess:
                    if await self._check_all_tables_exist(sess):
                        # tables ok
                        MutyLogger.get_instance().info(
                            "collab database exists and tables are ok."
                        )
                        self._setup_done = True
                        return

                # recreate tables
                MutyLogger.get_instance().warning(
                    "collab database exists but tables are missing."
                )
                await _recreate_internal(url, expire_on_commit=expire_on_commit)
