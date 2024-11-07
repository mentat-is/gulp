from typing import Optional, Union, override

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T, GulpUserPermission
from gulp.utils import GulpLogger


class GulpContext(GulpCollabBase, type=GulpCollabType.CONTEXT):
    """
    Represents a context object
    
    in gulp terms, a context is used to group a set of data coming from the same host.
    """

    color: Mapped[Optional[str]] = mapped_column(
        String, default="#ffffff", doc="The color of the context."
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        default=None,
        doc="The glyph associated with the context.",
    )

    @classmethod
    async def create(
        cls,
        id: str,
        color: str = None,
        glyph: str = None,
        token: str = None,
        **kwargs,
    ) -> T:
        """
        Create a new context object.

        Args:
            id (str): The unique identifier of the context.
            color (str, optional): The color of the context. Defaults to None.
            glyph (str, optional): The glyph associated with the context. Defaults to None.
            token (str, optional): The authentication token. Defaults to None
            **kwargs: Arbitrary keyword arguments.  
        Returns:
            T: The created context object
        """
        args = {"color": color, "glyph": glyph, **kwargs}
        return await super()._create(
            id,
            token=token,
            required_permission=[GulpUserPermission.ADMIN],
            **args,
        )
