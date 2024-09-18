from typing import Optional

from sqlalchemy import ForeignKey, Integer, String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio.engine import AsyncEngine
from sqlalchemy.orm import Mapped, mapped_column

from gulp.utils import logger
from gulp.api.collab.base import CollabBase, GulpCollabFilter, GulpUserPermission
from gulp.defs import ObjectAlreadyExists, ObjectNotFound


class Operation(CollabBase):
    """
    Represents an operation in the gulp system."""

    __tablename__ = "operation"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    name: Mapped[str] = mapped_column(String(128), unique=True)
    index: Mapped[Optional[str]] = mapped_column(
        String(128), default=None)
    description: Mapped[Optional[str]] = mapped_column(
        String, default=None, nullable=True
    )
    glyph_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("glyph.id", ondelete="SET NULL"),
        default=None,
        nullable=True,
    )
    workflow_id: Mapped[Optional[int]] = mapped_column(
        Integer, default=None, nullable=True
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "glyph_id": self.glyph_id,
            "workflow_id": self.workflow_id,
            "index": self.index,
        }

    @staticmethod
    async def create(
        engine: AsyncEngine,
        name: str,
        index: str,
        description: str = None,
        glyph_id: int = None,
        workflow_id: int = None,
    ) -> "Operation":
        """
        Creates a new operation (admin only).

        Args:
            engine (AsyncEngine): The database engine.
            name (str): The name of the operation.
            index (str): The elasticsearch index to associate the operation with.
            description (str, optional): The description of the operation. Defaults to None.
            glyph_id (int, optional): The glyph ID. Defaults to None.
            workflow_id (int, optional): The workflow ID. Defaults to None.
            check_token (bool, optional): Whether to check the token. Defaults to True.

        Returns:
            Operation: The created operation.

        Raises:
            ObjectAlreadyExists: If the operation already exists.
        """

        logger().debug(
            "---> create: name=%s, description=%s" % (name, description)
        )

        async with AsyncSession(engine, expire_on_commit=False) as sess:
            q = select(Operation).where(Operation.name == name and Operation.index == index)
            res = await sess.execute(q)
            op = res.scalar_one_or_none()
            if op is not None:
                raise ObjectAlreadyExists("operation %s already exists" % (name))

            # create new operation
            op = Operation(
                name=name,
                index=index,
                description=description,
                glyph_id=glyph_id,
                workflow_id=workflow_id,
            )
            sess.add(op)
            await sess.commit()
            logger().info("---> create: created operation %s" % (op))
            return op

    @staticmethod
    async def delete(engine: AsyncEngine, operation_id: int) -> None:
        """
        Deletes an operation (admin only).

        Args:
            engine (AsyncEngine): The database engine.
            operation_id (int): The operation ID.

        Raises:
            ObjectNotFound: If the operation is not found.
        """
        logger().debug("---> delete: operation_id=%s" % (operation_id))

        async with AsyncSession(engine) as sess:
            # check if operation exists
            q = select(Operation).where(Operation.id == operation_id)
            res = await sess.execute(q)
            op = res.scalar_one_or_none()
            if op is None:
                raise ObjectNotFound("operation %s not found" % (operation_id))

            # delete the operation
            logger().debug("---> delete: operation found: %s" % (op))
            try:
                await sess.delete(op)
                await sess.commit()
            except Exception as e:
                logger().exception(
                    "delete: failed to delete operation %d: %s" % (operation_id, e)
                )
                await sess.rollback()
                raise
            logger().info("delete: deleted operation %d" % (operation_id))

    @staticmethod
    async def update(
        engine: AsyncEngine,
        operation_id: int,
        description: str = None,
        glyph_id: int = None,
        workflow_id: int = None,
    ) -> "Operation":
        """
        Updates an operation (admin only).

        Args:
            engine (AsyncEngine): The database engine.
            operation_id (int): The operation ID.
            description (str, optional): The description of the operation. Defaults to None.
            glyph_id (int, optional): The glyph ID. Defaults to None.
            workflow_id (int, optional): The workflow ID. Defaults to None.

        Returns:
            Operation: The updated operation.

        Raises:
            MissingPermission: If the token does not have permission to update an operation.
            ObjectNotFound: If the operation is not found.
        """
        logger().debug(
            "---> update: operation_id=%s, description=%s, glyph=%s, workflow_id=%s"
            % (operation_id, description, glyph_id, workflow_id)
        )

        async with AsyncSession(engine, expire_on_commit=False) as sess:
            # check if operation exists
            q = select(Operation).where(Operation.id == operation_id).with_for_update()
            res = await sess.execute(q)
            op = res.scalar_one_or_none()
            if op is None:
                raise ObjectNotFound("operation %s not found" % (operation_id))

            # update the operation
            if description is not None:
                op.description = description
            if glyph_id is not None:
                op.glyph_id = glyph_id
            if workflow_id is not None:
                op.workflow_id = workflow_id

            await sess.commit()
            logger().info("update: updated operation %s" % (op))
            return op

    @staticmethod
    async def get(
        engine: AsyncEngine,
        flt: GulpCollabFilter = None
    ) -> list["Operation"]:
        """
        Get a list of operations

        Args:
            engine (AsyncEngine): The database engine.
            flt (GulpCollabFilter, optional): The filter: name, id, index, limit, offset. Defaults to None.

        Returns:
            list[Operation]: The list of operations.

        Raises:
            ObjectNotFound: If no operations are found.
        """
        logger().debug("---> get: filter=%s" % (flt))

        async with AsyncSession(engine) as sess:
            q = select(Operation)
            if flt is not None:
                if flt.id is not None:
                    q = q.where(Operation.id.in_(flt.id))
                if flt.name is not None:
                    q = q.where(Operation.name.in_(flt.name))
                if flt.index is not None:
                    q = q.where(Operation.index.in_(flt.index))
                if flt.limit is not None:
                    q = q.limit(flt.limit)
                if flt.offset is not None:
                    q = q.offset(flt.offset)
            res = await sess.execute(q)
            ops = res.scalars().all()
            if len(ops) == 0:
                raise ObjectNotFound("no operations found (flt=%s)" % (flt))
            logger().info("get: retrieved operations %s" % (ops))
            return ops
