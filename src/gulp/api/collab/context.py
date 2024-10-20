from typing import Optional, Union

from sqlalchemy import Result, String, select
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api import collab_api
from gulp.api.collab.base import CollabBase, GulpCollabFilter
from gulp.defs import ObjectNotFound
from gulp.utils import logger


class GulpContext(CollabBase):
    """
    Represents a context object: in gulp terms, a context is used to group a set of data coming from the same host.

    Attributes:
        id (int): The unique identifier of the context.
        name (str): The name of the context.
        color (str): A color hex string (0xffffff, #ffffff)
    """

    __tablename__ = "context"
    name: Mapped[str] = mapped_column(String(128), unique=True, primary_key=True)
    color: Mapped[Optional[str]] = mapped_column(String(32), default="#ffffff")
    # TODO: also add created_on, updated_on, created_by, updated_by ?

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "color": self.color,
        }

    @staticmethod
    async def get_result_or_throw(
        res: Result["GulpContext"], name: str
    ) -> "GulpContext":
        """
        Throws an ObjectNotFound exception if the context is not found, or returns the context.

        Args:
            res (Result[Context]): The result of the query.
            name (str): The name of the context.

        Returns:
            Context: The context object.
        """
        c = res.scalar_one_or_none()
        if c is None:
            raise ObjectNotFound('context "%s" not found' % (name))
        return c

    @staticmethod
    async def create(name: str, color: str) -> "GulpContext":
        """
        Creates a new context (admin only)

        Args:
            name (str): The name of the context.
            color (str): The color of the context.

        Returns:
            Context: The created Context object.
        """
        # only admin can create contexts
        logger().debug("---> create: name=%s, color=%s" % (name, color))
        async with await collab_api.session() as sess:
            c = GulpContext(name=name, color=color)
            sess.add(c)
            await sess.commit()
            logger().info("---> create: created context id=%s" % (c.name))
            return c

    @staticmethod
    async def update(name: str, color: str) -> "GulpContext":
        """
        Updates a context

        Args:
            engine (AsyncEngine): The database engine.
            context_id (int): The id of the context.
            name (str, optional): The name of the context.
            color (str, optional): The color of the context.

        Returns:
            Context: The updated Context object.
        """
        logger().debug("---> update: name=%s, color=%s" % (name, color))
        async with await collab_api.session() as sess:
            q = select(GulpContext).where(GulpContext.name == name).with_for_update()
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
            q = select(GulpContext).where(GulpContext.name == name)
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
                q = select(GulpContext.id, GulpContext.name)
            if flt.name is not None:
                q = q.where(GulpContext.name.in_(flt.context))
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
