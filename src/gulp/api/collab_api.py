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
from gulp.utils import logger

_engine: AsyncEngine = None
_collab_sessionmaker = None


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
    certs_dir = config.path_certs()
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
    _engine = create_async_engine(
        url, echo=config.debug_collab(), pool_timeout=30, connect_args=connect_args
    )

    logger().info("engine %s created/initialized, url=%s ..." % (_engine, url))
    return _engine


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
