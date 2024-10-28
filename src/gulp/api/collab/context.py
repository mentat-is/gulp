from typing import Optional, Union, override

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T
from gulp.utils import logger


class GulpContext(GulpCollabBase):
    """
    Represents a context object: in gulp terms, a context is used to group a set of data coming from the same host.

    Attributes:
        id (int): The unique identifier of the context.
        name (str): The name of the context.
        color (str): A color hex string (0xffffff, #ffffff)
    """

    __tablename__ = GulpCollabType.CONTEXT.value
    color: Mapped[Optional[str]] = mapped_column(
        String, default="#ffffff", doc="The color of the context."
    )

    __mapper_args__ = {
        "polymorphic_identity": GulpCollabType.CONTEXT.value,
    }

    @classmethod
    async def create(
        cls,
        id: str,
        owner: str,
        color: str = None,
        ws_id: str = None,
        req_id: str = None,
        sess: AsyncSession = None,
        commit: bool = True,
        **kwargs,
    ) -> T:
        args = {"color": color}
        return await super()._create(
            id,
            owner,
            ws_id,
            req_id,
            sess,
            commit,
            **args,
        )
