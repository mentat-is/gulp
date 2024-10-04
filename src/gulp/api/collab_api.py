
from sqlalchemy.ext.asyncio import AsyncEngine

import gulp.api.collab.db as collab_db
import gulp.config as config
from gulp.utils import logger
from sqlalchemy.sql import text

_collab: AsyncEngine = None

async def check_alive(engine: AsyncEngine) -> None:
    """
    Checks if the collab database is alive.

    Raises:
        Exception: If the collab database is not reachable.
    """
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))

    logger().info("Collab database is reachable!")
    
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
