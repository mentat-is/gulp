import asyncio

from sqlalchemy import text
from sqlalchemy.sql import text
from sqlalchemy_utils import create_database, database_exists, drop_database

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
