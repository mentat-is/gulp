from typing import Optional, Union, override

from sqlalchemy import ForeignKey, PrimaryKeyConstraint, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.structs import GulpCollabBase, GulpCollabType, T, GulpUserPermission
from gulp.utils import GulpLogger


class GulpSource(GulpCollabBase, type=GulpCollabType.SOURCE):
    """
    Represents a source of data being processed by the gulp system.
    
    it has always associated a context and an operation, and the tuple composed by the three is unique.
    """
    operation: Mapped["GulpOperation"] = relationship(
        "GulpOperation",
        back_populates="source",
        doc="The operation associated with the source.",
    )
    operation_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("operation.id", ondelete="CASCADE"),
        doc="The ID of the operation associated with the context.", primary_key=True
    )
    context: Mapped["GulpContext"] = relationship(
        "GulpContext",
        back_populates="source",
        doc="The context associated with the source.",
    )
    context_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("context.id", ondelete="CASCADE"),
        doc="The ID of the context associated with this source.", primary_key=True
    )
    title: Mapped[Optional[str]] = mapped_column(
        String, doc="The title of the source (i.e. log file name/path)."
    )
    color: Mapped[Optional[str]] = mapped_column(
        String, default="purple", doc="The color of the context."
    )
    glyph_id: Mapped[Optional[str]] = mapped_column(
        ForeignKey("glyph.id", ondelete="SET NULL"),
        default=None,
        doc="The glyph associated with the context.",
    )

    # composite primary key
    __table_args__ = (PrimaryKeyConstraint("operation_id", "context_id", "id"),)

    @override
    def __init__(self, *args, **kwargs):
        # initializes the base class
        super().__init__(*args, type=GulpCollabType.SOURCE, **kwargs)

    @classmethod
    async def create(
        cls,
        token: str,
        id: str,
        operation_id: str,
        context_id: str,
        title: str,
        color: str = None,
        glyph_id: str = None,
        **kwargs,
    ) -> T:
        """
        Create a new source object on the collab database.

        Args:
            token (str): The authentication token (must have INGEST permission).
            id (str): The name of the source
            operation_id (str): The id of the operation associated with the context.
            context_id (str): The id of the context associated with the source.
            title (str, optional): The display name of the source (i.e. log file name/path). defaults to id.
            color (str, optional): The color of the context. Defaults to purple.
            glyph (str, optional): The id of the glyph associated with the context. Defaults to None.
            **kwargs: Arbitrary keyword arguments.  
        Returns:
            T: The created context object
        """
        args = {"color": color or 'purple',
                "glyph_id": glyph_id, 
                "operation_id": operation_id,
                "context_id": context_id,
                "title": title or id
                **kwargs}
        return await super()._create(
            id=id,
            token=token,
            required_permission=[GulpUserPermission.INGEST],
            **args,
        )
