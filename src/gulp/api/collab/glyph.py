import base64
from typing import Union, override

from sqlalchemy import LargeBinary, String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api import collab_api
from gulp.api.collab.structs import COLLAB_MAX_NAME_LENGTH, GulpCollabFilter
from gulp.api.collab_api import CollabBase
from gulp.defs import ObjectAlreadyExists, ObjectNotFound
from gulp.utils import logger


class Glyph(CollabBase):
    """
    Represents a glyph object.

    Attributes:
        id (int): The unique identifier of the glyph.
        img (bytes): The image data of the glyph as binary blob.
        name (str): The name of the glyph.
    """

    __tablename__ = "glyph"
    id: Mapped[str] = mapped_column(String(COLLAB_MAX_NAME_LENGTH), primary_key=True, unique=True)
    img: Mapped[bytes] = mapped_column(LargeBinary)

    @override
    def to_dict(self, *args, **kwargs) -> dict:
        d = super().to_dict(*args, **kwargs)
        d["img"] = base64.b64encode(self.img).decode()
        return d

    @staticmethod
    async def create(img: bytes, name: str) -> "Glyph":
        """
        Creates a new glyph (admin only)

        Args:
            img (bytes): The image data.
            name (str, optional): The name of the glyph

        Returns:
            Glyph: The created Glyph object.
        """
        # only admin can create glyph
        logger().debug("---> create: img=%s..., name=%s" % (img[0:4], name))
        async with await collab_api.session() as sess:
            # check if it already exists
            q = select(Glyph).where(Glyph.id == name)
            res = await sess.execute(q)
            g = res.scalar_one_or_none()
            if g is not None:
                raise ObjectAlreadyExists("glyph %s already exists" % (name))

            g = Glyph(img=img, id=name)
            sess.add(g)
            await sess.commit()
            logger().info("create: created glyph id=%d" % (g.id))
            return g

    @staticmethod
    async def update(engine: AsyncEngine, glyph_id: int, img: bytes) -> "Glyph":
        """
        Updates a glyph (admin only)

        Args:
            engine (AsyncEngine): The database engine.
            glyph_id (int): The id of the glyph.
            img (bytes): The image data

        Returns:
            Glyph: The updated Glyph object.
        """

        logger().debug("---> update: id=%s, img=%s..." % (glyph_id, img[0:4]))

        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(Glyph).where(Glyph.id == glyph_id).with_for_update()
            res = await sess.execute(q)
            g = res.scalar_one_or_none()
            if g is None:
                raise ObjectNotFound("glyph %d not found" % glyph_id)

            g.img = img
            await sess.commit()
            logger().info("update: updated glyph id=%d" % (glyph_id))
            return g

    @staticmethod
    async def delete(engine: AsyncEngine, glyph_id: int) -> None:
        """
        Deletes a glyph (admin only)

        Args:
            engine (AsyncEngine): The database engine.
            id (int): The id of the glyph.

        Returns:
            None
        """

        logger().debug("---> delete: id=%s" % (glyph_id))
        async with AsyncSession(engine) as sess:
            q = select(Glyph).where(Glyph.id == glyph_id)
            res = await sess.execute(q)
            g = res.scalar_one_or_none()
            if g is None:
                raise ObjectNotFound("glyph %d not found" % glyph_id)

            await sess.delete(g)
            await sess.commit()
            logger().info("delete: deleted glyph id=%d" % (glyph_id))

    @staticmethod
    async def get(
        engine: AsyncEngine, flt: GulpCollabFilter = None
    ) -> Union[list["Glyph"], list[dict]]:
        """
        Get glyphs.

        Args:
            engine (AsyncEngine): The database engine.
            flt (GulpCollabFilter, optional): The filter (name, id, opt_basic_fields_only, limit, offset). Defaults to None (get all).

        Returns:
            Union[list['Glyph'], list[dict]]: The list of Glyph objects or the list of dictionaries with basic fields.

        Raises:
            ObjectNotFound: If no glyphs are found.
        """
        logger().debug("---> get: flt=%s" % (flt))

        # check each part of flt and build the query
        q = select(Glyph)
        if flt is not None:
            if flt.opt_basic_fields_only:
                q = select(Glyph.id, Glyph.id)

            if flt.id is not None:
                q = q.where(Glyph.id.in_(flt.id))
            if flt.name is not None:
                q = q.where(Glyph.id.in_(flt.name))
            if flt.limit is not None:
                q = q.limit(flt.limit)
            if flt.offset is not None:
                q = q.offset(flt.offset)

        async with AsyncSession(engine) as sess:
            res = await sess.execute(q)
            if flt is not None and flt.opt_basic_fields_only:
                # just the selected columns
                glyphs = res.fetchall()
            else:
                # full objects
                glyphs = res.scalars().all()
            if len(glyphs) == 0:
                raise ObjectNotFound("no glyphs found")

            if flt is not None and flt.opt_basic_fields_only:
                # we will return an array of dict instead of full ORM objects
                gg = []
                for g in glyphs:
                    gg.append({"id": g[0], "name": g[1]})
                glyphs = gg

            logger().info("glyphs retrieved: %d ..." % (len(glyphs)))
            return glyphs
