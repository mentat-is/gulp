from typing import Optional, Union, override

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T
from gulp.utils import logger


class GulpOperation(GulpCollabBase):
    """
    Represents an operation in the gulp system.
    """

    __tablename__ = GulpCollabType.OPERATION.value
    index: Mapped[Optional[str]] = mapped_column(
        String(),
        default=None,
        doc="The opensearch index to associate the operation with.",
    )
    description: Mapped[Optional[str]] = mapped_column(
        String(), default=None, doc="The description of the operation."
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        default=None,
        doc="The glyph associated with the operation.",
    )

    __mapper_args__ = {
        "polymorphic_identity": GulpCollabType.OPERATION.value,
    }

    @override
    @classmethod
    async def create(
        cls,
        id: str,
        owner: str,
        index: str = None,
        glyph: str = None,
        description: str = None,
        ws_id: str = None,
        req_id: str = None,
        **kwargs,
    ) -> T:
        args = {
            "index": index,
            "description": description,
            "glyph": glyph,
        }
        return await super()._create(
            id,
            GulpCollabType.OPERATION,
            owner,
            ws_id,
            req_id,
            **args,
        )
