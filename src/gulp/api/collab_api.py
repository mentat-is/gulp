"""
collab_api module for managing PostgreSQL database connections and operations.

this module provides the GulpCollab class, a singleton that handles database connections,
table creation, and default data initialization for the gulp collaboration database.
it supports ssl connections, connection pooling, and session management.

the module handles:
- database connection creation and management
- table creation and initialization
- default data creation (users, operations, contexts, glyphs)
- session management for database operations
"""

import asyncio
import orjson
import os
import pkgutil
import re
from importlib import import_module, resources
from sqlalchemy.ext.asyncio import AsyncSession

import muty.file
from muty.log import MutyLogger
import muty.string
from sqlalchemy import Table, insert, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy_utils import create_database, database_exists, drop_database

from gulp.api.collab.structs import GulpCollabBase
from gulp.config import GulpConfig
from gulp.structs import ObjectNotFound


# raised when the database schema does not match the expected schema
class SchemaMismatch(Exception):
    pass


class GulpCollab:
    """
    singleton class, represents the collab database connection.

    init() must be called first to initialize the connection.

    for ssl connection, it will use the certificates in the GulpConfig.get_instance().path_certs() directory if they exist.

    they should be named "postgres-ca.pem", "postgres.pem", "postgres.key" for the CA, client certificate, and client key respectively.

    """

    _instance: "GulpCollab" = None

    def __init__(self):
        self._initialized: bool = False
        self._engine: AsyncEngine = None
        self._collab_sessionmaker: async_sessionmaker = None

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpCollab":
        """
        returns the singleton instance of the collab database connection.

        Returns:
            GulpCollab: The singleton instance of the collab database connection
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    @staticmethod
    def init_mappers() -> None:
        """
        initializes all the collab mappers by importing all the modules in the gulp.api.collab package.

        NOTE: this fixes sqlalchemy errors i.e. when constructing ORM objects outside of gulp (i.e. in tests)
        """
        package_name = "gulp.api.collab"
        package = import_module(package_name)
        for _, module_name, _ in pkgutil.iter_modules(package.__path__):
            import_module(f"{package_name}.{module_name}")

    async def init(
        self,
        force_recreate: bool = False,
        main_process: bool = False,
    ) -> None:
        """
        initializes the collab database connection (create the engine and configure it) in the singleton instance.

        Args:
            force_recreate (bool, optional): whether to drop and recreate the database tables. Defaults to False (ignore if not main_process).
            main_process (bool, optional): whether this is the main process. Defaults to False.
        """
        if self._initialized and not force_recreate:
            # already initialized
            return

        url = GulpConfig.get_instance().postgres_url()

        # NOTE: i am not quite sure why this is needed, seems like sqlalchemy needs all the classes to be loaded before accessing the tables.
        GulpCollab.init_mappers()

        # ensure no engine is already running, either shutdown it before reinit
        if main_process:
            MutyLogger.get_instance().debug("init in MAIN process ...")
            if force_recreate:
                # drop and recreate the database
                await GulpCollab.db_drop(url)
                await GulpCollab.db_create(url)

            self._engine = await self._create_engine()
            self._collab_sessionmaker = async_sessionmaker(
                bind=self._engine, expire_on_commit=False
            )
            if force_recreate:
                await self.create_tables()
                await self.create_default_users()
                await self.create_default_glyphs()

            # check tables exists
            async with self._collab_sessionmaker() as sess:
                if not await self._check_all_tables_exist(sess):
                    self._initialized = True  # we're initialized anyway
                    raise SchemaMismatch(
                        "collab database exists but (some) tables are missing."
                    )
        else:
            MutyLogger.get_instance().debug("init in worker process ...")
            self._engine = await self._create_engine()
            self._collab_sessionmaker = async_sessionmaker(
                bind=self._engine, expire_on_commit=False
            )

        # init done
        self._initialized = True

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

        # determine pool size for adaptive configuration, if set. either, use default for pool_size and max_overflow
        pool_size: int = None
        max_overflow: int = 10
        if GulpConfig.get_instance().postgres_adaptive_pool_size():
            # pool sizing should be based on per-process concurrency (child concurrency),
            # not multiplied by number of worker processes. previously this used
            # total_concurrency = num_tasks_per_worker * num_workers which caused
            # each worker to create a large pool and ultimately exhaust PG max connections.
            num_tasks_per_worker: int = GulpConfig.get_instance().concurrency_num_tasks()
            # derive a conservative pool size from per-worker concurrency
            pool_size = min(200, max(10, max(1, num_tasks_per_worker) // 4))
            max_overflow = min(100, max(10, max(1, num_tasks_per_worker) // 3))
            MutyLogger.get_instance().debug(
                "using postgres adaptive pool size, calculated pool_size=%d, max_overflow=%d (num_tasks_per_worker=%d) ..."
                % (pool_size, max_overflow, num_tasks_per_worker)
            )

        # create engine
        kw = dict(
            echo=GulpConfig.get_instance().debug_collab(),
            connect_args=connect_args,
            pool_pre_ping=True,  # Enables connection health checks
            pool_recycle=3600,  # Recycle connections after 1 hour
            max_overflow=max_overflow,
            pool_timeout=30,  # Wait up to 30 seconds for available connection
        )
        if pool_size:
            kw["pool_size"] = pool_size
        _engine = create_async_engine(
            url,
            **kw
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
        if not self._initialized:
            raise Exception("collab not initialized, call GulpCollab().init() first!")

        return self._collab_sessionmaker()

    async def shutdown(self) -> None:
        """
        Shuts down the per-process collab database engine.

        after calling this, the engine is invalidated and all existing connections are disposed and GulpCollab().init() must be called again to reinitialize the engine in the same process.

        Returns:
            None
        """
        if self._engine:
            MutyLogger.get_instance().debug(
                "shutting down collab database engine and invalidate existing connections ..."
            )
            await self._engine.dispose()
        self._initialized = False

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
        async with self._collab_sessionmaker() as sess:
            # create pg_cron extension
            await sess.execute(text("CREATE EXTENSION IF NOT EXISTS pg_cron;"))

            await sess.execute(
                text(
                    """
                    CREATE OR REPLACE FUNCTION delete_expired_stats_rows() RETURNS void AS $$
                    BEGIN
                        WITH deleted AS (
                            DELETE FROM request_stats
                            WHERE (EXTRACT(EPOCH FROM NOW()) * 1000) > time_expire AND time_expire > 0
                            RETURNING id
                        )
                        DELETE FROM task WHERE req_id IN (SELECT id FROM deleted);
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
                    DELETE FROM user_session WHERE (EXTRACT(EPOCH FROM NOW()) * 1000) > time_expire AND time_expire > 0;
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

    async def create_table(self, t: Table) -> None:
        """
        creates a table in the database.

        Args:
            t (Table): The table to create.
        """
        async with self._engine.begin() as conn:
            await conn.run_sync(t.create, checkfirst=True)

    async def drop_table(self, t: Table) -> None:
        """
        drops a table in the database.

        Args:
            t (Table): The table to drop.
        """
        async with self._engine.begin() as conn:
            await conn.run_sync(t.drop, checkfirst=True)

    async def create_tables(self) -> None:
        """
        creates the database tables and functions.
        """
        # create database tables and functions
        async with self._engine.begin() as conn:
            await conn.run_sync(GulpCollabBase.metadata.create_all)
        await self._setup_collab_expirations()

    async def get_table_names(self, sess: AsyncEngine) -> list[str]:
        """
        retrieves all table names from the database (public schema) using raw sql query.

        Args:
            sess (AsyncEngine): The database session to use.

        Returns:
            list[str]: list of table names in the public schema.
        """
        # query to get all table names from public schema
        result = await sess.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"
            )
        )
        # extract table names from result
        tables = [row[0] for row in result.fetchall()]
        return tables

    async def clear_tables(
        self, sess: AsyncSession, tables: list[str] = None, exclude: list[str] = None
    ) -> None:
        """
        clears (delete data without dropping the table) the database tables

        Args:
            sess (AsyncSession): The database session to use.
            tables (list[str], optional): The list of tables to clear. Defaults to None (meaning all tables will be cleared).
            exclude (list[str], optional): The list of tables to exclude from clearing. Defaults to None.
        """
        if not tables:
            # clear all tables
            tables = await self.get_table_names()

        for t in tables:
            if exclude and t in exclude:
                MutyLogger.get_instance().debug(
                    "---> skipping clearing table: %s (excluded)" % (t)
                )
                continue

            MutyLogger.get_instance().debug("clearing table: %s ..." % (t))
            await sess.execute(
                text('TRUNCATE TABLE "%s" RESTART IDENTITY CASCADE;' % (t))
            )

    async def create_default_users(self) -> None:
        """
        create default users and user groups

        Args:
            user_id (str): The (admin) user ID to use (will be set as the owner of the created objects).
        """
        from gulp.api.collab.structs import (
            PERMISSION_MASK_DELETE,
            PERMISSION_MASK_EDIT,
            PERMISSION_MASK_INGEST,
            GulpUserPermission,
        )
        from gulp.api.collab.user import GulpUser
        from gulp.api.collab.user_group import GulpUserGroup

        async with self._collab_sessionmaker() as sess:
            sess: AsyncSession

            # create user groups
            from gulp.api.collab.user_group import ADMINISTRATORS_GROUP_ID

            # create admin user, which is the root of everything else
            admin_user: GulpUser = await GulpUser.create_user(
                sess,
                "admin",
                "admin",
                permission=[GulpUserPermission.ADMIN],
            )

            # login admin user
            # admin_session = await GulpUser.login(sess, "admin", "admin", None, None)

            # create other users
            _ = await GulpUser.create_user(
                sess,
                user_id="guest",
                password="guest",
            )
            group: GulpUserGroup = await GulpUserGroup.create_internal(
                sess,
                admin_user.id,
                name=ADMINISTRATORS_GROUP_ID,
                obj_id=ADMINISTRATORS_GROUP_ID,
                private=False,
                permission=[GulpUserPermission.ADMIN],
            )

            # add admin to administrators group
            await group.add_user(sess, admin_user.id)
            await sess.refresh(admin_user)

            # dump groups
            MutyLogger.get_instance().debug("---> groups:")
            groups: list[GulpUserGroup] = await GulpUserGroup.get_by_filter(
                sess, user_id="admin"
            )
            for group in groups:
                MutyLogger.get_instance().debug(
                    orjson.dumps(
                        group.to_dict(nested=True), option=orjson.OPT_INDENT_2
                    ).decode()
                )

            # dump admin user
            MutyLogger.get_instance().debug("---> admin user:")
            MutyLogger.get_instance().debug(
                orjson.dumps(
                    admin_user.to_dict(nested=True), option=orjson.OPT_INDENT_2
                ).decode()
            )

    async def _load_icons(self, sess: AsyncSession, user_id: str) -> None:
        """
        load icons from the included zip file

        Args:
            sess (AsyncSession): The database session to use.
            user_id (str): The user ID to use (will be set as the owner of the created objects).

        """
        from gulp.api.collab.glyph import GulpGlyph

        assets_path = resources.files("gulp.api.collab.assets")
        zip_path = muty.file.safe_path_join(assets_path, "icons.zip")

        # unzip to temp dir
        unzipped_dir: str = None
        try:
            unzipped_dir = await muty.file.unzip(zip_path, None)

            # load each icon
            files = await muty.file.list_directory_async(
                unzipped_dir, "*.svg", case_sensitive=False
            )
            MutyLogger.get_instance().debug(
                "found %d files in %s" % (len(files), unzipped_dir)
            )
            glyphs: list[dict] = []
            l: int = len(files)
            chunk_size = 256 if l > 256 else l

            for f in files:
                # read file, get bare filename without extension
                icon_b = await muty.file.read_file_async(f)
                bare_filename: str = os.path.basename(f)
                bare_filename = os.path.splitext(bare_filename)[0].replace(" ", "_")
                # print("** processing icon: %s (%s)" % (bare_filename, f))
                # glyphs are public
                d = GulpGlyph.build_object_dict(
                    user_id,
                    name=bare_filename,
                    obj_id=bare_filename,
                    private=False,
                    img=icon_b,
                )

                glyphs.append(d)
                if len(glyphs) == chunk_size:
                    # insert bulk
                    await sess.execute(insert(GulpGlyph).values(glyphs))
                    MutyLogger.get_instance().debug(
                        "inserted bulk of %d glyphs ..." % (len(glyphs))
                    )
                    glyphs = []

            if glyphs:
                # insert remaining
                MutyLogger.get_instance().debug(
                    "last chunk, inserting bulk of %d glyphs ..." % (len(glyphs))
                )
                await sess.execute(insert(GulpGlyph).values(glyphs))

            # load done
            await sess.commit()
        except Exception as e:
            MutyLogger.get_instance().error(
                "error loading icons: %s" % (str(e)), exc_info=True
            )
            raise e
        finally:
            if unzipped_dir:
                # remove temp dir
                await muty.file.delete_file_or_dir_async(unzipped_dir)

    async def create_default_glyphs(self) -> None:
        """
        create default glyphs and assign them to users.
        """

        from gulp.api.collab.glyph import GulpGlyph
        from gulp.api.collab.user import GulpUser

        async with self._collab_sessionmaker() as sess:
            sess: AsyncSession

            # get users
            admin_user: GulpUser = await GulpUser.get_by_id(sess, "admin")
            guest_user: GulpUser = await GulpUser.get_by_id(sess, "guest")

            # create glyphs from files
            await self._load_icons(sess, admin_user.id)

            # get user glyph
            user_glyph: GulpGlyph = await GulpGlyph.get_by_id(sess, "user-round")

            # pylint: disable=protected-access

            # assign glyphs
            admin_user.glyph_id = user_glyph.id
            if guest_user:
                guest_user.glyph_id = user_glyph.id
            await sess.commit()

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
