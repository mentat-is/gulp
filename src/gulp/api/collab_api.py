import os

import muty.file
import muty.time
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from gulp import config
from gulp.utils import GulpLogger
class GulpCollab:
    """
    singleton class, represents the collab database connection.

    for ssl connection, it will use the certificates in the config.path_certs() directory if they exist.
    
    they should be named "postgres-ca.pem", "postgres.pem", "postgres.key" for the CA, client certificate, and client key respectively.

    """
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, "_initialized"):
            self._initialized = True
            self._init_called = False
            self._engine: AsyncEngine = None
            self._collab_sessionmaker = None

    async def init(self, expire_on_commit: bool = False) -> None:
        """
        initializes the collab database connection (create the engine and configure it) in the singleton instance.

        if called on an already initialized instance, the existing engine is disposed and a new one is created.

        Args:
            expire_on_commit (bool, optional): whether to expire sessions returned by session() on commit. Defaults to False.
        """
        if self._engine is not None:
            self._engine.dispose()

        self._engine: AsyncEngine = await self._create_engine()
        self._collab_sessionmaker = async_sessionmaker(bind=self._engine, expire_on_commit=expire_on_commit)
        self._init_called = True

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
        GulpLogger().debug(
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
                GulpLogger().debug(
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
                GulpLogger().debug("using server CA certificate only: %s" % (ca))
                connect_args = {"sslrootcert": ca, "sslmode": sslmode}
        else:
            # no SSL
            connect_args = {}

        # create engine
        _engine = create_async_engine(
            url, echo=config.debug_collab(), pool_timeout=30, connect_args=connect_args
        )

        GulpLogger().info("engine %s created/initialized, url=%s ..." % (_engine, url))
        return _engine

    async def session(self) -> AsyncSession:
        """
        Returns a session (preconfigured with expire_on_commit=False) to the collab database, per-process engine is created if needed.

        WARNING: to call this, an event loop must be running.

        Returns:
            AsyncSession: The session to the collab database
        """
        if not self._init_called:
            raise Exception("collab not initialized, call GulpCollab().init() first!")

        return self._collab_sessionmaker()


    async def shutdown(self) -> None:
        """
        Shuts down the per-process collab database engine.

        after calling this, the engine is invalidated and all existing connections are disposed and GulpCollab().init() must be called again to reinitialize the engine in the same process.
        
        Returns:
            None
        """
        if not self._init_called:
            raise Exception("collab not initialized, call GulpCollab().init() first!")

        GulpLogger().warning(
            "shutting down collab database engine and invalidate existing connections ..."
        )
        await self._engine.dispose()
        self._init_called = False
        self._engine = None
        self._collab_sessionmaker = None
