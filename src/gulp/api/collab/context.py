from typing import Optional, Union, override

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T, GulpUserPermission
from gulp.utils import GulpLogger


class GulpContext(GulpCollabBase, type=GulpCollabType.CONTEXT):
    """
    Represents a context object
    
    in gulp terms, a context is used to group a set of data coming from the same host.
    """
    title: Mapped[Optional[str]] = mapped_column(
        String, doc="Display name for the context."
    )
    operation: Mapped["GulpOperation"] = relationship(
        "GulpOperation",
        back_populates="context",
        doc="The operation associated with the context.",
    )
    operation_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        doc="The ID of the operation associated with the context."
    )
    color: Mapped[Optional[str]] = mapped_column(
        String, default="#ffffff", doc="The color of the context."
    )
    glyph: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        default=None,
        doc="The glyph associated with the context.",
    )

    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.CONTEXT, **kwargs)

    @classmethod
    async def create(
        cls,
        token: str,
        id: str,
        title: str = None,
        color: str = None,
        glyph: str = None,
        **kwargs,
    ) -> T:
        """
        Create a new context object on the collab database.

        Args:
            token (str): The authentication token (must have ADMIN permission).
            id (str): The name of the context (must be unique and not containing spaces)
            title (str, optional): The display name for the context. Defaults to id.
            color (str, optional): The color of the context. Defaults to white.
            glyph (str, optional): The id of the glyph associated with the context. Defaults to None.
            **kwargs: Arbitrary keyword arguments.  
        Returns:
            T: The created context object
        """
        args = {"color": color or 'white',
                "glyph": glyph, 
                "title": title or id, 
                **kwargs}
        return await super()._create(
            id=id,
            token=token,
            required_permission=[GulpUserPermission.ADMIN],
            **args,
        )
