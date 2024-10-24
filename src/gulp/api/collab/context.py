from typing import Optional, Union

from sqlalchemy import Result, String, select
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api import collab_api
from gulp.api.collab.structs import COLLAB_MAX_NAME_LENGTH, GulpCollabFilter
from gulp.api.collab.base import GulpCollabBase
from gulp.defs import ObjectNotFound
from gulp.utils import logger


class GulpContext(GulpCollabBase):
    """
    Represents a context object: in gulp terms, a context is used to group a set of data coming from the same host.

    Attributes:
        id (int): The unique identifier of the context.
        name (str): The name of the context.
        color (str): A color hex string (0xffffff, #ffffff)
    """

    __tablename__ = "context"
    id: Mapped[str] = mapped_column(
        String(COLLAB_MAX_NAME_LENGTH),
        unique=True,
        primary_key=True,
    )
    color: Mapped[Optional[str]] = mapped_column(String(32), default="#ffffff")

    def __init__(self, name: str, color: str) -> None:
        self.id = name
        self.color = color
        logger().debug("---> GulpContext: name=%s, color=%s" % (name, color))

    async def store(self, sess: AsyncSession = None) -> None:
        """
        Asynchronously stores the current context in the database.
        If no session is provided, a new session is created using `collab_api.session()`.
        Args:
            sess (AsyncSession, optional): The database session to use. If None, a new session is created.
        Returns:
            None
        """

        if sess is None:
            sess = await collab_api.session()
        async with sess:
            sess.add(self)
            await sess.commit()
            logger().info("---> store: stored context=%s" % (self.id))

    @staticmethod
    async def update(name: str, d: dict) -> "GulpContext":

        logger().debug("---> update: name=%s, d=%s" % (name, color))
        async with await collab_api.session() as sess:
            q = select(GulpContext).where(GulpContext.id == name).with_for_update()
            res = await sess.execute(q)
            c = GulpContext.get_result_or_throw(res, name)

            # update
            c.color = color
            await sess.commit()
            logger().info("---> update: updated context name=%d" % (name))
            return c

    @staticmethod
    async def delete(name: str) -> None:
        """
        Deletes a context

        Args:
            engine (AsyncEngine): The database engine.
            id (int): The id of the context.

        Returns:
            None
        """

        logger().debug("---> delete: name=%s" % (name))
        async with await collab_api.session() as sess:
            q = select(GulpContext).where(GulpContext.id == name)
            res = await sess.execute(q)
            c = GulpContext.get_result_or_throw(res, name)

            # delete
            await sess.delete(c)
            await sess.commit()
            logger().info("---> delete: deleted context name=%d" % (name))

    @staticmethod
    async def get(
        flt: GulpCollabFilter = None,
    ) -> Union[list["GulpContext"]]:
        """
        Get contexts.

        Args:
            engine (AsyncEngine): The database engine.
            flt (GulpCollabFilter, optional): The filter (name, opt_basic_fields_only, limit, offset). Defaults to None (get all).

        Returns:
            Union[list['GulpContext']: The list of Context objects.

        Raises:
            ObjectNotFound: If no contexts are found.
        """
        logger().debug("---> get: flt=%s" % (flt))

        # check each part of flt and build the query
        q = select(GulpContext)
        if flt is not None:
            if flt.opt_basic_fields_only:
                q = select(GulpContext.id, GulpContext.id)
            if flt.name is not None:
                q = q.where(GulpContext.id.in_(flt.context))
            if flt.limit is not None:
                q = q.limit(flt.limit)
            if flt.offset is not None:
                q = q.offset(flt.offset)

        async with await collab_api.session() as sess:
            res = await sess.execute(q)
            contexts = res.scalars().all()
            if not contexts:
                raise ObjectNotFound("no contexts found")
            logger().info("---> get: found %d contexts" % (len(contexts)))
            return contexts
