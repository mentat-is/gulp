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
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        default=None,
        doc="The glyph associated with the context.",
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
        glyph: str = None,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> T:
        args = {"color": color, "glyph": glyph}
        return await super()._create(
            id,
            GulpCollabType.CONTEXT,
            owner,
            ws_id,
            req_id,
            **args,
        )
