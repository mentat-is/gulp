import logging

from sqlalchemy.ext.asyncio import AsyncEngine

import gulp.api.collab.db as collab_db
import gulp.config as config

_logger: logging.Logger = None

_collab: AsyncEngine = None

def logger() -> logging.Logger:
    """
    Returns the logger instance used for logging in the collab_api module.

    Returns:
        logging.Logger: The logger instance.
    """
    return _logger


def init(l: logging.Logger) -> None:
    """
    Initializes the collab API **in the current process**.

    Args:
        l (logging.Logger): The logger object to be used for logging.

    Returns:
        None
    """
    global _logger
    _logger = l

async def collab(invalidate: bool = False) -> AsyncEngine:
    """
    Retrieves the collab object.

    If the collab object is not already created, it will be created using the
    PostgreSQL URL specified in the configuration.

    Args:
        invalidate (bool, optional): Whether to invalidate the current collab object. Defaults to False.
    Returns:
        The collab object.
    """
    global _collab
    if invalidate:
        if _collab is not None:
            await _collab.dispose()
            _collab = None

    if _collab is None:
        # create
        _collab = await collab_db.engine_get(
            config.postgres_url(), sql_alchemy_debug=config.debug_collab()
        )
    return _collab
