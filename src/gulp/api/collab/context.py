import base64
import logging
from typing import Union

from sqlalchemy import LargeBinary, String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm import Mapped, mapped_column

import gulp.api.collab_api as collab_api
from gulp.api.collab.base import CollabBase, GulpCollabFilter
from gulp.defs import ObjectAlreadyExists, ObjectNotFound


class Context(CollabBase):
    """
    Represents a context object.

    Attributes:
        id (int): The unique identifier of the context.
        name (str): The name of the context.
        color (str): A color hex string (0xffffff, #ffffff)
    """

    __tablename__ = "context"
    id: Mapped[int] = mapped_column(
        primary_key=True, autoincrement=True, init=False)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    color: Mapped[str] = mapped_column(String(32))
    # TODO: also add created_on, updated_on, created_by, updated_by ?

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "color": self.color,
        }

    @staticmethod
    def _validate_format(color: str):
        """
        Checks if color has a valid format

        Args:
            color (str): value to verify.

        Returns:
            color: if color is valid, else exception is raised
        """
        # TODO: implement validation on format

        return color

    @staticmethod
    async def create(engine: AsyncEngine, name: str, color: str) -> "Context":
        """
        Creates a new context (admin only)

        Args:
            engine (AsyncEngine): The database engine.
            name (str): The name of the context.
            color (str): The color of the context.

        Returns:
            Context: The created Context object.
        """
        color = Context._validate_format(color)

        # only admin can create contexts
        collab_api.logger().debug("---> create: name=%s..., color=%s" %
                                  (name, color))

        async with AsyncSession(engine, expire_on_commit=False) as sess:
            # check if it already exists
            q = select(Context).where(Context.name == name)
            res = await sess.execute(q)
            c = res.scalar_one_or_none()
            if c is not None:
                raise ObjectAlreadyExists("context %s already exists" % (name))

            c = Context(name=name, color=color)
            sess.add(c)
            await sess.commit()
            collab_api.logger().info("create: created context id=%d" % (c.id))
            return c

    @staticmethod
    async def update(engine: AsyncEngine, context_id: int, name: str, color: str) -> "Context":
        """
        Updates a context (admin only)

        Args:
            engine (AsyncEngine): The database engine.
            context_id (int): The id of the context.
            name (str, optional): The name of the context.
            color (str, optional): The color of the context.

        Returns:
            Context: The updated Context object.
        """
        color = Context._validate_format(color)

        collab_api.logger().debug(
            "---> update: id=%s, name=%s, color=%s..." % (
                context_id, name, color)
        )

        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(Context).where(
                Context.id == context_id).with_for_update()
            res = await sess.execute(q)
            c = res.scalar_one_or_none()
            if c is None:
                raise ObjectNotFound("context %d not found" % context_id)

            c.name = name
            c.color = color
            await sess.commit()
            collab_api.logger().info("update: updated context id=%d" % (context_id))
            return c

    @staticmethod
    async def delete(engine: AsyncEngine, context_id: int) -> None:
        """
        Deletes a context (admin only)

        Args:
            engine (AsyncEngine): The database engine.
            id (int): The id of the context.

        Returns:
            None
        """

        collab_api.logger().debug("---> delete: id=%s" % (context_id))
        async with AsyncSession(engine) as sess:
            q = select(Context).where(Context.id == context_id)
            res = await sess.execute(q)
            c = res.scalar_one_or_none()
            if c is None:
                raise ObjectNotFound("context %d not found" % context_id)

            await sess.delete(c)
            await sess.commit()
            collab_api.logger().info("delete: deleted context id=%d" % (context_id))

    @staticmethod
    async def get(
        engine: AsyncEngine, flt: GulpCollabFilter = None
    ) -> Union[list["Context"], list[dict]]:
        """
        Get contexts.

        Args:
            engine (AsyncEngine): The database engine.
            flt (GulpCollabFilter, optional): The filter (name, id, opt_basic_fields_only, limit, offset). Defaults to None (get all).

        Returns:
            Union[list['Context'], list[dict]]: The list of Context objects or the list of dictionaries with basic fields.

        Raises:
            ObjectNotFound: If no contexts are found.
        """
        collab_api.logger().debug("---> get: flt=%s" % (flt))

        # check each part of flt and build the query
        q = select(Context)
        if flt is not None:
            if flt.opt_basic_fields_only:
                q = select(Context.id, Context.name)

            if flt.id is not None:
                q = q.where(Context.id.in_(flt.id))
            if flt.name is not None:
                q = q.where(Context.name.in_(flt.name))
            if flt.limit is not None:
                q = q.limit(flt.limit)
            if flt.offset is not None:
                q = q.offset(flt.offset)

        async with AsyncSession(engine) as sess:
            res = await sess.execute(q)
            if flt is not None and flt.opt_basic_fields_only:
                # just the selected columns
                contexts = res.fetchall()
            else:
                # full objects
                contexts = res.scalars().all()
            if len(contexts) == 0:
                raise ObjectNotFound("no contexts found")

            if flt is not None and flt.opt_basic_fields_only:
                # we will return an array of dict instead of full ORM objects
                cc = []
                for c in contexts:
                    cc.append({"id": c[0], "name": c[1], "color": c[2]})
                contexts = cc

            collab_api.logger().info("contexts retrieved: %d ..." % (len(contexts)))
            return contexts
