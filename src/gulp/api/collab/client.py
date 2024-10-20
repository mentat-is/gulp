from typing import Optional

from sqlalchemy import ForeignKey, Integer, String, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm import Mapped, mapped_column

from gulp.api.collab.base import CollabBase, GulpClientType, GulpCollabFilter
from gulp.defs import ObjectAlreadyExists, ObjectNotFound
from gulp.utils import logger


class Client(CollabBase):
    """
    Represents a client to ingest events."""

    __tablename__ = "client"
    name: Mapped[str] = mapped_column(String(128), primary_key=True, unique=True)
    type: Mapped[GulpClientType] = mapped_column(Integer, default=GulpClientType.SLURP)
    version: Mapped[Optional[str]] = mapped_column(
        String(32), default=None
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.name", ondelete="SET NULL"), default=None
    )

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "type": self.type,
            "version": self.version,
            "glyph": self.glyph_id,
        }

    @staticmethod
    async def update(
        engine: AsyncEngine,
        client_id: int,
        operation_id: int = None,
        version: str = None,
        glyph_id: int = None,
    ) -> "Client":
        """
        Updates a client (admin only)

        Args:
            engine (AsyncEngine): The database engine.
            client_id (int): The id of the client.
            operation_id (int, optional): The id of the operation. Defaults to None.
            version (str, optional): The version of the client. Defaults to None.
            glyph_id (int, optional): The id of the glyph. Defaults to None.

        Returns:
            Client: The updated Client object.

        Raises:
            ObjectNotFound: If the client is not found.
        """
        logger().debug(
            "---> update: client_id=%s, operation_id=%s, version=%s, glyph_id=%s"
            % (client_id, operation_id, version, glyph_id)
        )
        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(Client).where(Client.id == client_id).with_for_update()
            res = await sess.execute(q)
            c = res.scalar_one_or_none()
            if c is None:
                raise ObjectNotFound("client %d not found" % client_id)

            if operation_id:
                c.operation_id = operation_id
            if version:
                c.version = version
            if glyph_id:
                c.glyph_id = glyph_id

            sess.add(c)
            await sess.commit()
            logger().info("update: updated client %s" % (c))
            return c

    @staticmethod
    async def create(
        engine: AsyncEngine,
        name: str,
        t: GulpClientType,
        operation_id: int = None,
        version: str = None,
        glyph_id: int = None,
    ) -> "Client":
        """
        Creates a new client (admin only)

        Args:
            engine (AsyncEngine): The database engine.
            name (str): The name of the client.
            t (GulpClientType): The type of the client.
            operation_id (int, optional): The id of the operation. Defaults to None.
            version (str, optional): The version of the client. Defaults to None.
            glyph_id (int, optional): The id of the glyph. Defaults to None.

        Returns:
            Client: The created Client object.
        """
        logger().debug(
            "---> create: name=%s, t=%s, version=%s, glyph_id=%s"
            % (name, t, version, glyph_id)
        )

        async with AsyncSession(engine, expire_on_commit=False) as sess:
            # check if it already exists
            q = select(Client).where(Client.name == name)
            res = await sess.execute(q)
            c = res.scalar_one_or_none()
            if c is not None:
                raise ObjectAlreadyExists("client %s already exists" % (name))

            c = Client(
                name=name,
                type=t,
                operation_id=operation_id,
                version=version,
                glyph_id=glyph_id,
            )
            sess.add(c)
            await sess.commit()
            logger().info("create: created client %s" % (c))
            return c

    @staticmethod
    async def delete(engine: AsyncEngine, client_id: int) -> None:
        """
        Deletes a client from the system (admin only)

        Args:
            engine (AsyncEngine): The database engine.
            client_id (int): The id of the client to be deleted.

        Raises:
            ObjectNotFound: If the client is not found.
        """

        logger().debug("---> delete: client_id=%s" % (client_id))

        async with AsyncSession(engine) as sess:
            q = select(Client).where(Client.id == client_id)
            res = await sess.execute(q)
            c = res.scalar_one_or_none()
            if c is None:
                raise ObjectNotFound("client %d not found" % client_id)

            await sess.delete(c)
            await sess.commit()
            logger().info("client deleted: %s ..." % (c))

    @staticmethod
    async def get(engine: AsyncEngine, flt: GulpCollabFilter = None) -> list["Client"]:
        """
        Get clients from the system.

        Args:
            engine (AsyncEngine): The database engine.
            flt (GulpCollabFilter): The filter (id, name, client_type, client_version, operation_id, limit, offset).

        Returns:
            list[Client]: The list of clients.

        Raises:
            ObjectNotFound: If no clients are found.
        """

        logger().debug("---> get: flt=%s" % (flt))
        async with AsyncSession(engine) as sess:
            # build query based on filter
            query = select(Client)
            if flt is not None:
                if flt.id:
                    query = query.where(Client.id.in_(flt.id))
                if flt.name:
                    query = query.where(Client.name.in_(flt.name))
                if flt.client_type:
                    query = query.where(Client.type.in_(flt.client_type))
                if flt.client_version:
                    qq = [Client.version.ilike(x) for x in flt.client_version]
                    query = query.filter(or_(*qq))
                if flt.operation_id:
                    query = query.where(Client.operation_id.in_(flt.operation_id))
                if flt.limit:
                    query = query.limit(flt.limit)
                if flt.offset:
                    query = query.offset(flt.offset)

            res = await sess.execute(query)
            clients = res.scalars().all()
            if len(clients) == 0:
                raise ObjectNotFound("no clients found")

            logger().info("get: clients=%s" % (clients))
            return clients
