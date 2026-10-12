"""
collab_api module for managing PostgreSQL database connections and operations.

this module provides the GulpCollab class, a singleton that handles database connections,
table creation, and default data initialization for the gulp collaboration database.
it supports connection pooling and session management.

the module handles:
- database connection creation and management
- table creation and initialization
- default data creation (users, operations, contexts, glyphs)
- session management for database operations
"""

import asyncio
import pkgutil
import re
from importlib import import_module, resources

import aiofiles
import muty.string
from sqlalchemy import Table, create_engine, insert, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy_utils import create_database, database_exists, drop_database

from gulp.api.collab.structs import GulpCollabBase
from gulp.config import GulpConfig
from gulp.glug import Glug
from gulp.structs import ObjectNotFound


# raised when the database schema does not match the expected schema
class SchemaMismatch(Exception):
    pass


class GulpCollab:
    """
    singleton class, represents the collab database connection.

    init() must be called first to initialize the connection.

    """

    _instance: "GulpCollab" = None
    _POSTGRES_CONNECTION_WARNING_THRESHOLD = 80

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

        # ensure no previous engine/sessionmaker survives a re-init path
        if self._engine or self._collab_sessionmaker:
            await self.shutdown()

        if main_process:
            Glug.get_instance().debug("init in MAIN process ...")
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

            # check tables exists
            try:
                async with self._collab_sessionmaker() as sess:
                    if not await self._check_all_tables_exist(sess):
                        self._initialized = True  # we're initialized anyway
                        raise SchemaMismatch(
                            "collab database exists but (some) tables are missing."
                        )
            except OperationalError:
                # hide_password=True: postgres_url embeds credentials in plaintext,
                # never log it as-is
                Glug.get_instance().exception(
                    "failed to connect to the collab database at "
                    f"{make_url(url).render_as_string(hide_password=True)}",
                    hint="verify PostgreSQL is running and reachable at the "
                    "configured postgres_url",
                )
                raise
        else:
            Glug.get_instance().debug("init in worker process ...")
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
        config = GulpConfig.get_instance()
        url = config.postgres_url()

        postgres_options = [
            "-c lock_timeout=%d" % config.postgres_lock_timeout_ms(),
            "-c statement_timeout=%d" % config.postgres_statement_timeout_ms(),
            "-c idle_in_transaction_session_timeout=%d"
            % config.postgres_idle_in_transaction_session_timeout_ms(),
        ]
        connect_args = {"options": " ".join(postgres_options)}

        # The task scheduler now adapts dispatch under PostgreSQL pressure, so
        # the SQLAlchemy pool should be the healthy ceiling, not a second throttle.
        pool_size: int = config.postgres_pool_size()
        max_overflow: int = config.postgres_max_overflow()

        pool_timeout: int = config.postgres_pool_timeout_sec()
        pool_recycle: int = config.postgres_pool_recycle_sec()
        max_connections_per_process = pool_size + max_overflow
        try:
            processes_per_instance = 1 + max(0, config.workers_max())
        except Exception:
            processes_per_instance = 1
        max_connections_per_instance = (
            processes_per_instance * max_connections_per_process
        )

        Glug.get_instance().info(
            f"postgres pool budget: max_connections_per_process={max_connections_per_process}, "
            f"processes_per_instance={processes_per_instance}, "
            f"max_connections_per_instance={max_connections_per_instance}, "
            f"pool_timeout_sec={pool_timeout}, pool_recycle_sec={pool_recycle}"
        )
        if (
            max_connections_per_instance
            >= self._POSTGRES_CONNECTION_WARNING_THRESHOLD
        ):
            Glug.get_instance().warning(
                "postgres connection budget is high because "
                f"max_connections_per_instance={max_connections_per_instance} "
                f"meets the warning threshold={self._POSTGRES_CONNECTION_WARNING_THRESHOLD} "
                f"(processes_per_instance={processes_per_instance}, pool_size={pool_size}, "
                f"max_overflow={max_overflow}). Estimated cluster total is "
                "gulp_instances * max_connections_per_instance. Reduce "
                "postgres_pool_size, postgres_max_overflow, "
                "workers_max, or instance count before raising "
                "caps.",
                hint="lower postgres_pool_size/postgres_max_overflow/workers_max, "
                "or reduce the number of gulp instances, to stay under PostgreSQL's "
                "max_connections",
            )

        # create engine
        kw = dict(
            echo=config.debug_collab(),
            connect_args=connect_args,
            pool_pre_ping=True,  # Enables connection health checks
            pool_recycle=pool_recycle,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
        )
        _engine = create_async_engine(url, **kw)

        # hide_password=True: url embeds credentials in plaintext, never log it as-is
        Glug.get_instance().info(
            f"engine {_engine} created/initialized, "
            f"url={make_url(url).render_as_string(hide_password=True)}"
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
        engine = self._engine
        self._engine = None
        self._collab_sessionmaker = None
        if engine:
            Glug.get_instance().debug(
                "shutting down collab database engine and invalidate existing connections ..."
            )
            await engine.dispose()
            Glug.get_instance().debug("collab database engine shutdown complete")
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
        Glug.get_instance().debug(
            f"db_exists: url={make_url(url).render_as_string(hide_password=True)}, "
            f"result={b!r}"
        )
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

        def _terminate_postgres_backends(url: str) -> None:
            parsed_url = make_url(url)
            if parsed_url.get_backend_name() != "postgresql" or not parsed_url.database:
                return

            maintenance_db = "postgres"
            if parsed_url.database == maintenance_db:
                maintenance_db = "template1"

            drivername = parsed_url.drivername
            if drivername.endswith("+asyncpg"):
                drivername = "postgresql+psycopg"

            maintenance_url = parsed_url.set(
                drivername=drivername,
                database=maintenance_db,
            )
            engine = create_engine(maintenance_url, isolation_level="AUTOCOMMIT")
            try:
                with engine.connect() as conn:
                    conn.execute(
                        text("""
                            SELECT pg_terminate_backend(pid)
                            FROM pg_stat_activity
                            WHERE datname = :database_name
                            AND pid <> pg_backend_pid()
                            """),
                        {"database_name": parsed_url.database},
                    )
            finally:
                engine.dispose()

        def _blocking_drop(url: str, raise_if_not_exists: bool = False):
            """
            internal function to drop, and possibly recreate, the database: this is blocking, so this is wrapped in a thread.
            """
            # hide_password=True everywhere below: url embeds credentials in plaintext,
            # never log or raise it as-is (an ObjectNotFound message can reach an API
            # response body, not just a log line)
            masked_url = make_url(url).render_as_string(hide_password=True)
            if database_exists(url):
                Glug.get_instance().info(f"db_drop: dropping database {masked_url}")
                _terminate_postgres_backends(url)
                drop_database(url)
                Glug.get_instance().info(f"db_drop: database {masked_url} dropped")
            else:
                Glug.get_instance().warning(f"db_drop: database {masked_url} does not exist")
                if raise_if_not_exists:
                    raise ObjectNotFound(f"database {masked_url} does not exist!")

        Glug.get_instance().debug(
            f"db_drop: url={make_url(url).render_as_string(hide_password=True)}, "
            f"raise_if_not_exists={raise_if_not_exists!r}"
        )
        await asyncio.to_thread(_blocking_drop, url, raise_if_not_exists)

    @staticmethod
    async def db_create(url: str) -> None:
        """
        Create a database at the given URL.

        Args:
            url (str): The URL of the database to create.
        """
        Glug.get_instance().debug(
            f"db_create: url={make_url(url).render_as_string(hide_password=True)}"
        )
        await asyncio.to_thread(create_database, url=url)

    async def _setup_collab_expirations(self) -> None:
        # TODO: check issues with pg-cron process dying
        Glug.get_instance().debug(
            "setting up stats and tokens expiration with pg_cron ..."
        )
        async with self._collab_sessionmaker() as sess:
            # create pg_cron extension
            await sess.execute(text("CREATE EXTENSION IF NOT EXISTS pg_cron;"))

            await sess.execute(text("""
                    CREATE OR REPLACE FUNCTION delete_expired_stats_rows() RETURNS void AS $$
                    BEGIN
                            DELETE FROM request_stats
                            WHERE (EXTRACT(EPOCH FROM NOW()) * 1000) > time_expire AND time_expire > 0
                            AND status != 'ongoing';
                    END;
                    $$ LANGUAGE plpgsql;
            """))

            await sess.execute(text("""
                CREATE OR REPLACE FUNCTION delete_expired_tokens_rows() RETURNS void AS $$
                BEGIN
                    DELETE FROM user_session WHERE (EXTRACT(EPOCH FROM NOW()) * 1000) > time_expire AND time_expire > 0;
                END;
                $$ LANGUAGE plpgsql;
            """))

            # purge stats and tokens every 1 minute. Named pg_cron jobs are
            # updated in place on repeated startup instead of duplicated.
            await sess.execute(text("""
                    SELECT cron.schedule('gulp-delete-expired-stats', '* * * * *', 'SELECT delete_expired_stats_rows();');
                    """))
            await sess.execute(text("""
                    SELECT cron.schedule('gulp-delete-expired-tokens', '* * * * *', 'SELECT delete_expired_tokens_rows();');
                    """))
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
                Glug.get_instance().debug(f"skipping clearing table: {t} (excluded)")
                continue

            Glug.get_instance().debug(f"clearing table: {t} ...")
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
                glyph_id="UserCross",
            )

            # login admin user
            # admin_session = await GulpUser.login(sess, "admin", "admin", None, None)

            # create other users
            _ = await GulpUser.create_user(
                sess,
                user_id="guest",
                password="guest",
                glyph_id="UserCross",
            )
            _ = await GulpUser.create_user(
                sess,
                user_id="ingest",
                password="ingest",
                permission=[GulpUserPermission.INGEST],
                glyph_id="UserCross",
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

            groups: list[GulpUserGroup] = await GulpUserGroup.get_by_filter(
                sess, user_id="admin"
            )
            # pwd_hash popped out: even hashed, no reason to put it in a log record
            admin_user_detail = admin_user.to_dict(nested=True)
            admin_user_detail.pop("pwd_hash", None)
            Glug.get_instance().debug(
                f"created default groups ({[g.id for g in groups]}) and admin user",
                detail={
                    "groups": [g.to_dict(nested=True) for g in groups],
                    "admin_user": admin_user_detail,
                },
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
